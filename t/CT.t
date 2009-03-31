
use Test::More tests => 12;
require "lib/shared.pl";

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






# Utilities pasted from bin/robots

sub chunkmeta {
  my %conf = @_;
  return (0,0,0) unless corpusexists(\%conf);
  my ($chunks, $records, $bytes) = (scalar @{$conf{files}},0,0);
  for ( @{$conf{files}} ){
    if(m|^[\da-f]{32}\.(\d+)\.(\d+)$|){
      $records += $1;
      $bytes   += $2;
    }
  }
  return ($chunks, $records, $bytes);
}



sub corpusexists {
  my %conf = %{ $_[0] };
  return 1 if -e "clusters/$conf{ring}/$conf{name}";
  return 0;
}

