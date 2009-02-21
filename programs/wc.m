#!/usr/bin/perl -w
use strict;

$/ = $ENV{TTRS} if exists $ENV{TTRS};
$/ = eval $/ if ($/ =~ m|^['"]|);

while(<>){
  my %words;
  $words{$_}++ for split /\W+/;
  delete $words{''} if exists $words{''};
  print "$_\t$words{$_}\n" for keys %words;
  #print STDERR "Got ", scalar keys %words, " unique words in file.\n";
}



