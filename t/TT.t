use strict;
use Test::More tests => 8;
my $stem = 'ROBOTSTESTING-2062696e2f726f626f74730a';
my $errflag = 0;


# delete any previous test corpora
for my $corp ( qw{ odyssey odyssey_wc_kv-kv odyssey_wc_txt-kv odyssey_wc_kv-txt odyssey_wc_txt-txt odyssey_uc odyssey_lines_kv odyssey_lines_txt } ){
  system "bin/robots delete $stem-$corp";
}


# create odyssey corpus on default cluster
my $start = time;
system "bin/robots init $stem-odyssey -rs='\"</DOC>\\n\"' -chunksize=4K";
system "gunzip -c t/texts/odyssey.txt.gz | bin/robots add $stem-odyssey";

system "gunzip -c t/texts/odyssey.txt.gz > od-orig.txt";
system "bin/robots cat $stem-odyssey > od-test.txt";
my ($origcksum) = split /\s+/, `cksum od-orig.txt`;
my ($cksum)     = split /\s+/, `cksum od-test.txt`;
ok( $origcksum == $cksum, "orig text cksum ($origcksum) matches distributed corpus cksum ($cksum) (".(time-$start)." secs)");
unlink "od-orig.txt", "od-test.txt" if ($origcksum == $cksum);



# perform a simple mapping to uppercase and compare with preprocessed result
$start = time;
print STDERR "\n\nCAPITALIZE (map)\n";
system "cd programs; ../bin/robots map $stem-odyssey ./capitalize.m $stem-odyssey_uc";

my ($wc_orig) = `gunzip -c t/texts/odyssey.txt.gz | wc`; chomp $wc_orig;
my ($wc_uc)   = `bin/robots cat $stem-odyssey_uc | wc`;  chomp $wc_uc;
ok( $wc_orig eq $wc_uc, "orig corpus wc ($wc_orig) matches capitalized corpus wc ($wc_uc) (".(time-$start)." secs)");




# Perform two mapreduce operations to distribute 
# The Odyssey randomly line-by-line across all hosts; 
# Create one kv and one txt corpus.
system "gunzip -c t/texts/odyssey.txt.gz | programs/wc.m | sort -k1,1 | programs/wc.r | sort -nrk2,2 -o od-wc-orig.txt";
my ($origwcsum) = split /\s+/, `cksum od-wc-orig.txt`;

my @types = qw{ txt kv };
for my $type (@types){
  $start = time;
  print STDERR "\n\nPARTITION ($type)\n";
  system "cd programs; ../bin/robots mapreduce $stem-odyssey ./partition.m ./remove_keys.r $stem-odyssey_lines_$type -format=$type";
  my ($wc_lines)   = `bin/robots cat $stem-odyssey_lines_$type | wc`; chomp $wc_lines;
  ok( $wc_orig eq $wc_lines, "orig corpus wc ($wc_orig) matches line-partitioned $type corpus wc ($wc_lines) (".(time-$start)." secs)");
  unless($wc_lines eq $wc_orig){
    $errflag = 1;
    print STDERR "Check $stem-odyssey $stem-odyssey_lines_$type for errors\n";
  }
}


my @maps = (
  [ txt => 'txt' ],
  [ txt => 'kv'  ],
  [ kv  => 'kv'  ],
  [ kv  => 'txt' ],
);
for my $test (@maps){
  $start = time;
  my ($src, $tgt) = @$test;
  my $name = "$src-$tgt";
  print STDERR "\n\nWORDCOUNT ($name)\n";
  system "cd programs; ../bin/robots mapreduce $stem-odyssey_lines_$src ./wc.m ./wc.r $stem-odyssey_wc_$name -format=$tgt";
  system "bin/robots cat $stem-odyssey_wc_$name | sort -nrk2,2 -o od-wc-$name-test.txt";
  ($cksum) = split /\s+/, `cksum od-wc-$name-test.txt`;
  ok( $cksum == $origwcsum, "odyssey $name word count file cksum $cksum == orig cksum $origwcsum (".(time-$start)." secs)");
  unless($cksum == $origwcsum){
    $errflag = 1;
    print STDERR "Check $stem-odyssey_lines_$src, $stem-odyssey_wc_$name, od-wc-$name-test.txt for errors\n";
  } else {
    system "bin/robots delete $stem-odyssey_wc_$name";
    unlink "od-wc-$name-test.txt";
    system "rm -rf /tmp/robotarmy-$stem-odyssey_wc_$name-*";
  }
}


# delete test corpora only if everything passed
unless($errflag){
  print STDERR "Cleaning up test corpora...";
  for my $corp ( qw{ odyssey odyssey_uc odyssey_lines_txt odyssey_lines_kv } ){
    system "bin/robots delete $stem-$corp";
  }
  unlink "od-wc-orig.txt";
  system "rm -rf /tmp/robotarmy-$stem-*";
  print STDERR " done\n";
}

exit;









