
use Test::More tests => 7;


# create odyssey corpus on default cluster
my $start = time;
system "bin/robots init odyssey -rs='\"</DOC>\\n\"' -chunksize=4K";
system "gunzip -c t/texts/odyssey.txt.gz | bin/robots add odyssey";

system "gunzip -c t/texts/odyssey.txt.gz > od-orig.txt";
system "bin/robots cat odyssey > od-test.txt";
@difflines = `diff od-orig.txt od-test.txt`;
ok( @difflines == 0, "orig text matches distributed corpus text (".(time-$start)." secs)");
unlink "od-orig.txt", "od-test.txt" unless @difflines;



# perform a simple mapping to uppercase and compare with preprocessed result
$start = time;
print STDERR "\n\nSIMPLEMAP ==================================>>>\n";
system "cd programs; ../bin/robots map odyssey ./capitalize.m odyssey_uc";

($wc_orig) = `gunzip -c t/texts/odyssey.txt.gz | wc`;
($wc_uc)   = `bin/robots cat odyssey_uc | wc`;
ok( $wc_orig eq $wc_uc, "orig corpus wc matches capitalized corpus wc (".(time-$start)." secs)");




# Perform two mapreduce operations: one to distribute 
# The Odyssey randomly line-by-line across all hosts; 
# one to do a unique word count on those lines.
# Then compare the results to the known correct ones.

$start = time;
print STDERR "\n\nPARTITION ==================================>>>\n";
system "cd programs; ../bin/robots mapreduce odyssey ./partition.m ./remove_keys.r odyssey_lines";

print STDERR "\n\nWORDCOUNT ===============================================>>>\n";
system "cd programs; ../bin/robots mapreduce odyssey_lines ./wc.m ./wc.r odyssey_wc";

# check the results against a local run
print STDERR "\n\nCHECKING....\n";
system "gunzip -c t/texts/odyssey.txt.gz | programs/wc.m | sort -k1,1 | programs/wc.r | sort -nrk2,2 -o od-wc-orig.txt";
system "bin/robots cat odyssey_wc | sort -nrk2,2 -o od-wc-test.txt";
@difflines = `diff od-wc-test.txt od-wc-orig.txt`;
ok( @difflines == 0, "odyssey word count differs by " . scalar @difflines . " (".(time-$start)." secs)");
unlink "od-wc-test.txt", "od-wc-orig.txt" unless @difflines;


# delete test corpora
for $corp ( qw{ odyssey odyssey_wc odyssey_uc odyssey_lines } ){
  $start = time;
  system "bin/robots delete $corp";
  ok(!-e "corpora/$corp", "test corpus $corp deleted (".(time-$start)." secs)");
}

system "rm -rf /tmp/work-odyssey*";

exit;









