#!/usr/bin/perl -w
use strict;

# Robot Army utility to scan for all worker identities
# Specify a cluster name or no cluster to designate all machines in all clusters
# USAGE: scan.pl {cluster}


my $cluster = shift;
die "wrong no. of args" if @ARGV;

my @hfiles;
if($cluster){
  @hfiles = ("clusters/$cluster/.hosts");
} else {
  @hfiles = glob "clusters/*/.hosts";
}

my %hosts;
for my $hfile (@hfiles){
  open HFILE, "<$hfile";
  while(<HFILE>){
    next if /^#/; 
    chomp; 
    next unless $_; 
    s/^\w+\@//;
    $hosts{$_} = 1;
  }
  close HFILE;
}

print STDERR "Gathering keys for ", scalar keys %hosts, " hosts...";

open KEYSCAN, "| ssh-keyscan -t rsa -f - 2>/dev/null";
print KEYSCAN join("\n", keys %hosts), "\n";
close KEYSCAN;

print STDERR "done.\n";
