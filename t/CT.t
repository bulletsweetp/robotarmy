
use Test::More tests => 12;

my @corps = qw{ odyssey iliad };

for my $corp (@corps){
  system "bin/robots init $corp -rs='\"</DOC>\\n\"' -chunksize=1k";
  system "gunzip -c t/texts/$corp.txt.gz | bin/robots add $corp";
  my ($records) = `gunzip -c t/texts/$corp.txt.gz | grep -c '<DOC>'`;
  my ($bytes)   = `gunzip -c t/texts/$corp.txt.gz | wc -c`;
  my(undef,$crecords,$cbytes) = chunkmeta( %{corpus($corp)} );
  ok($crecords == $records);
  ok($cbytes   == $bytes);
}


system "bin/robots init worksbyhomer -rs='\"</DOC>\\n\"' -chunksize=1K";
system "gunzip -c t/texts/\*.txt.gz | bin/robots add worksbyhomer";
my ($records) = `gunzip -c t/texts/\*.txt.gz | grep -c '<DOC>'`;
my ($bytes)   = `gunzip -c t/texts/\*.txt.gz | wc -c`;
my(undef,$crecords,$cbytes) = chunkmeta( %{corpus('worksbyhomer')} );
ok($crecords == $records, "streamed has $crecords records, orig has $records records");
ok($cbytes   == $bytes,   "streamed has $cbytes bytes, orig has $bytes bytes");


system "bin/robots copy odyssey worksbyhomerAPPEND";
system "bin/robots append iliad worksbyhomerAPPEND";
(undef, $crecords, $cbytes) = chunkmeta( %{corpus("worksbyhomerAPPEND")} );
ok($crecords == $records, "appended has $crecords records, orig has $records records");
ok($cbytes   == $bytes,   "appended has $cbytes bytes, orig has $bytes bytes");




# delete test corpora
push @corps, qw{ worksbyhomer worksbyhomerAPPEND };
system "bin/robots delete " . join(' ', @corps);
for my $corp (@corps){
  ok(!-e "corpora/$corp");
}


exit;









# Utility pasted from bin/robots


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


sub chunkmeta {
  my %conf = @_;
  return (0,0,0) unless corpusexists(\%conf);
  my ($chunks, $records, $bytes) = (scalar @{$conf{files}},0,0);
  for ( @{$conf{files}} ){
    if(m|^[\da-f]{40}\.(\d+)\.(\d+)$|){
      $records += $1;
      $bytes   += $2;
    }
  }
  return ($chunks, $records, $bytes);
}



sub corpusexists {
  my %conf = %{ $_[0] };
  return 1 if -e "clusters/$conf{cluster}/$conf{name}";
  return 0;
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
  my @candidates = glob("clusters/*/$corp");
  return ($conf{cluster}, $corp) if @candidates == 0; # non-existent
  die "$corp exists in multiple non-default clusters" if @candidates > 1;
  $candidates[0] =~ s|^clusters/||;
  return (split m|/|, $candidates[0]);
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
      push @{ $conf{ $list->[0] } }, $list->[1];
    }
  }
  
  $conf{$_} = $conf{$_}->[0] for grep { @{$conf{$_}} == 1 } keys %conf;
  
  return %conf;
}

