#!/usr/bin/perl -w
use strict;

# set the input record separator
$/ = $ENV{TTRS} if exists $ENV{TTRS};
$/ = eval $/ if $/ =~ /^['"]/;

# remove the key
while(<>){
  s/^[^\t]*\t//;
  print;
}


