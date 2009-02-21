#!/usr/bin/perl -w
use strict;


my $line = <>;
chomp $line;
my ($word, $docid, $rec) = split /\t/, $line;
my $tf = $rec =~ /,/g;
my $cf = $tf;
my $df = 1;
$rec = "$docid:$tf($rec)";

while(<>){
  chomp;
  my($newword, $docid, $newrec) = split /\t/;
  if($word eq $newword){
    $tf = $newrec =~ /,/g;
	$cf += $tf;
	$df++;
    $rec .= ",$docid:$tf($newrec)";
    next;
  }

  print "$word\t$df\t$cf\t$rec\n";
  $word = $newword;
  $tf = $rec =~ /(,)/g;
  $cf = $tf;
  $df = 1;
  $rec = "$docid:$tf($newrec)";

}

print "$word\t$df\t$cf\t$rec\n" if $word;


