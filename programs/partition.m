#!/usr/bin/perl
use strict;

# set the input record separator
$/ = $ENV{TTRS} if exists $ENV{TTRS};
$/ = eval $/ if $/ =~ /^['"]/;

# Returns a random number as key, entire record as val
while(<>){
  my ( $key ) = 1 + int rand 999999;
  print $key, "\t", $_;
}


