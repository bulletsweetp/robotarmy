# SHARED CODE, robots+ttclient


# return the preferred order of a consistent-hashing ring according to a key
sub consistenthashring {
  my ($key, $list) = @_;
  my %seen;
  $seen{$_}++ for @$list;
  my $keyhash = sha1_hex($key);
  my @hashes;
  for my $item (keys %seen){
    push @hashes, [ sha1_hex("$item:$_"), $item ] for (1..$seen{$item});
  }
  @hashes = sort @hashes;
  my $keyindex = 0;
  for (0..$#hashes){
    next if $hashes[$_]->[0] lt $keyhash;
    $keyindex = $_;
    last;
  }
  if($keyindex != 0 and $keyindex != $#hashes){
    @hashes = (@hashes[$keyindex .. $#hashes],
               @hashes[0         .. $keyindex-1]);
  }
  %seen = ();
  my @ring = map { $_->[1] } grep { ! $seen{$_->[1]}++ } @hashes;
  return \@ring;
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


1;

