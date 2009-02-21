#!/usr/bin/perl -w
use strict;

my $line = <>;
chomp $line;
my ($key, $sum) = split /\t/, $line;

while(<>){
  chomp;
  my($newkey, $val) = split /\t/;
  if($key eq $newkey){
    $sum += $val;
    next;
  }

  print "$key\t$sum\n";
  $key = $newkey;
  $sum = $val;
}

print "$key\t$sum\n" if $key;

