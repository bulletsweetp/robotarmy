#!/usr/bin/perl -w
use strict;


$/ = '</DOC>';
while(<>){
  my ($docid) = (m|<DOCNO>(.+?)</DOCNO>|ms);
  my ($text)  = (m|<TEXT>(.+?)</TEXT>|ms);

  next unless $text and $docid;

  my %words;
  my $i = 0;
  for my $word (split /[^\w\_]+/, $text){
    $words{$word} .= $i++ . ',';
  }

  while(my($word, $list) = each %words){
    print "$word\t$docid\t$list\n";
  }
}



