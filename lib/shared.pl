# SHARED CODE, robots + ttclient

# create a continuum from a list of items; make $vnodes virtual nodes per item
sub hashring {
  my ($vnodes, $list) = @_;
  my @ring = ();
  for my $item (@$list){
    push @ring, [ md5_hex("$item:$_"), $item ] for (1..$vnodes);
  }
  @ring = sort { $a->[0] cmp $b->[0] } @ring;
  return \@ring;
}

# return the preferred order of items in a consistent-hashing ring according to a key
sub preflist {
  my ($key, $ring) = @_;
  my $keyhash = ($key =~ /^[a-f\d]{32}$/i) ? $key : md5_hex($key);
  my @ring = @$ring;

  my $keyindex = vnode_index($key, $ring);

  # tick tock
  my %seen = ();
  my @pref =  map { $_->[1] } grep { ! $seen{$_->[1]}++ } @ring[$keyindex..$#ring];
  push @pref, map { $_->[1] } grep { ! $seen{$_->[1]}++ } @ring[0..$keyindex-1]
    if($keyindex > 0);

  return \@pref;
}

# smarter search, via brad via rj. This screams for a radix tree.
sub vnode_index {
  my ($key, $ring) = @_;
  my $keyhash = ($key =~ /^[a-f\d]{32}$/i) ? $key : md5_hex($key);

  my $zeroval = '0' x 32; # "null md5_hex"
  my ($lo, $hi) = (0, scalar(@$ring)-1);

  while (1) {
    my $mid           = int(($lo + $hi) / 2);
    my $val_at_mid    = $ring->[$mid]->[0];
    my $val_one_below = $mid ? $ring->[$mid-1]->[0] : $zeroval;

    # match
    return $mid if
      $keyhash le $val_at_mid && $keyhash gt $val_one_below;

    # wrap-around match
    return $mid if $lo == $hi;

    # too low, go up.
    if ($val_at_mid lt $keyhash) {
      $lo = $mid + 1;
      $lo = $hi if $lo > $hi;
    }
    # too high
    else {
      $hi = $mid - 1;
      $hi = $lo if $hi < $lo;
    }
  }
}

# STATUS SERVICE COMMUNICATIONS
#
{
  my ($ua, $url);
  sub setsvc {
    ( $ua, $url ) = @_;
  }

  sub object_count {
    my ( $type, $state ) = @_;
    my $uri = "$url/$type";
    $uri .= "/$state" if defined $state;
    my $log = logger("object count on $type");
    my $request = HTTP::Request->new( GET => $uri );
    my $ref;
    my $i = 0;
    while (1) {
      my $response = $ua->request($request);
      if ( $response->is_success ) {
        $ref = eval $response->content;
        last;
      }
      elsif( $response->code == 404) {
        $log->( 0, "status svc has no $type objex. Sleeping". 2^$i ."seconds\n");
        sleep 2**$i++;
      } else {
        $log->( 0, "no status svc, abandoning job, exiting (". $response->code .")\n"); 
        die; 
      }
    }
    return @$ref;
  }

  sub statshift {
    my ( $action, $type, $id ) = @_;
    my $request = HTTP::Request->new( PUT => "$url/$action/$type/$id" );
    return $ua->request($request);
  }

  # register a type-labeled list of records, possibly retiring a source item atomically
  sub register {
    my ( $resulttype, $records, $origtype, $origid ) = @_;
    $records = join( "\n", @$records );
    my $uri = "$url/$resulttype";
    $uri .= "/$origtype/$origid"
      if defined $origtype
      and defined $origid;
    my $request = 
      HTTP::Request->new( POST => $uri, 
                          ['Content-Length' => length($records)], 
                          $records);
    return $ua->request($request);
  }
} # STATUS SVC


# LOGGER SYSTEM
{
  my $fd;
  my $level = 0;

  sub setlog {
    my ($filename, $lvl) = @_;
    $level = $lvl;
    die $! unless $fd = FileHandle->new($filename);
  }

  sub logger {
    my ($id) = @_;
    return sub {
      my($msglevel, $message) = @_;
      if($msglevel >= $level){
        print $fd localtime() . " [$id] " . $message;
      }
    };
  }
}


# CONFIG

# Make an anonymous variable reference.
# This is necessary to abide by the Getopt::Long way.
sub ar {
  my $var = shift;
  return \$var;
}

# Get all configuration info, with precedence given to cmdline
sub cmdlineconf {
  my %opts = @_;  

  GetOptions( %opts );
  
  my %cmdline = 
    map { (split '=', $_)[0] => expand_number(${$opts{$_}}) } 
    grep { ${$opts{$_}} } 
    keys %opts;
  
  return %cmdline;
}  
 


sub readconf {
  my $fname = shift;
  return () unless -e $fname; 
  my @c     = ();
  open F, "<$fname";
  while (<F>) {
    chomp;
    s/\r//;
    next unless $_;
    next if /^#/;
    push @c, [ split /\t+/ ];
  }
  my %conf = ();
  for my $list (@c) {
    if ( @$list > 2 ) {    # this is a vector
      push @{ $conf{ shift @$list } }, [@$list];
    }
    else {                 # this is a single key/value
      push @{ $conf{ $list->[0] } }, expand_number($list->[1]);
    }
  }
  
  $conf{$_} = $conf{$_}->[0] for grep { @{$conf{$_}} == 1 } keys %conf;
  
  return %conf;
}


sub readlist {
  my $fname = shift;
  return [] unless -e $fname; 
  my @list     = ();
  open F, "<$fname";
  while (<F>) {
    chomp;
    s/\r//;
    next unless $_;
    next if /^#/;
    push @list, $_;
  }
  return \@list;
}


sub writeconf {
  my %conf = %{ shift @_ };
  my $dest = shift;
  open CONF, ">$dest" or die $!;
  for my $key (keys %conf){
    if(ref $conf{$key}){
      for my $val ( @{ $conf{$key} } ){
        print CONF $key . "\t" . abbrev_number($val) . "\n";
      }
    } else {
      print CONF $key . "\t" . abbrev_number($conf{$key}) . "\n";
    }
  }
  close CONF;
}


sub writelist {
  my ($file, $list) = @_;
  open LIST, ">$file" or die $!;
  print LIST join("\n", @$list);
  close LIST;
}


sub abbrev_number {
  my $n = shift;
  return $n unless ($n =~ m|^\d+$|);
  $n =~ s/0{9}$/G/;
  $n =~ s/0{6}$/M/;
  $n =~ s/0{3}$/K/;
  return $n;
}

sub expand_number {
  my $n = shift;
  return $n unless $n =~ /^\d+[KMG]$/i;
  $n =~ s/G$/000000000/i;
  $n =~ s/M$/000000/i;
  $n =~ s/K$/000/i;
  return $n;
}


# Corpus handle guesser


# construct a corpus type
sub corpus {
  my $corpus = shift;
  my ($cluster, $name) = cluster_and_corpus($corpus);
  my %conf;
  if( -e "clusters/$cluster/$name" ){
    %conf = readconf("clusters/$cluster/$name");
  } else {
	%conf = readconf('conf/ct.conf');
  }
  $conf{hosts} = readlist("clusters/$cluster/.hosts");
  $conf{files} = readlist("clusters/$cluster/$name.files");
  $conf{name}    = $name;
  $conf{cluster} = $cluster;
  return \%conf;
}


# nail down which cluster and corpus name
sub cluster_and_corpus {
  my $corp = shift;
  die "No corpus specified" unless $corp;
  my @spec = split /\//, $corp;
  die "cluster/corpus is malspecified: $corp" if @spec > 2;
  if(@spec == 2){
    die "No such cluster $spec[0]" unless -d "clusters/$spec[0]";
    return @spec;
  }
  my %conf = readconf('conf/ct.conf'); 
  return ($conf{cluster}, $corp) if -e "clusters/$conf{cluster}/$corp";
  my @candidates = glob("clusters/*/" . quotemeta $corp);
  return ($conf{cluster}, $corp) if @candidates == 0; # non-existent
  die "$corp exists in multiple non-default clusters" if @candidates > 1;
  $candidates[0] =~ s|^clusters/||;
  return (split m|/|, $candidates[0]);
}


1;

