#!/usr/bin/perl -w
use strict;

$/ = '</page>';
my %words;
while(<>){
  $words{$_}++ for split /\W+/;
  if(5000000 <= (scalar keys %words)){
    delete $words{''} if exists $words{''};
    print "$_\t$words{$_}\n" for keys %words;
    %words = ();
  }
}

delete $words{''} if exists $words{''};
print "$_\t$words{$_}\n" for keys %words;
 

