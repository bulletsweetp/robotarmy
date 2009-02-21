#!/usr/bin/perl -w
use strict;

# Robot Army utility to push files/dirs to worker machines
# Specify a cluster name or no cluster to designate all machines in all clusters
# USAGE: push.pl cluster source {target}


my ($cluster, $source, $target);
if(@ARGV==3){
  ($cluster, $source, $target) = @ARGV;
} elsif (@ARGV==2){
  ($cluster, $source) = @ARGV;
  $target = $source;
} else {
  die "Wrong no. of args";
}
@ARGV = ();

die "No such file or dir '$source'" unless -e $source;

my @hfiles = ("clusters/$cluster/.hosts");

my %hosts;
for my $hfile (@hfiles){
  open HFILE, "<$hfile";
  while(<HFILE>){
    next if /^#/; 
    chomp; 
    next unless $_; 
    $hosts{$_} = 1;
  }
  close HFILE;
}

for (keys %hosts){
  print "$_:\n";
  my $retval = system "scp -o 'ConnectTimeout 1' -r $source $_:$target";
  print "scp return value ", ($retval>>8), ".\n" if $retval;
}



