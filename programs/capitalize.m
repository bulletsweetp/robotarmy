#!/usr/bin/perl -w
use strict;

while(<>){
  print uc $_;
  print STDERR "Yup, capitalized ", length $_, " letters.\n";
}



