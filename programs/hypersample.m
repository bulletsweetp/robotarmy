#!/usr/bin/perl -w
use strict;

srand 987656789;
my @nums = _sample(100,1000);

$/ = '</DOC>';
while(<>){
  last unless @nums;
  next unless $. >= $nums[0];
  shift @nums;
  print;
}


# creates a list of sorted integers in the range of N
sub sample {
    my ($r, $N) = @_;
    return [0..$N-1] if $r > $N;
    $r *= $N if $r<1; # ratio?

    my $pop = $N;
    my @ids = ();
    for my $samp (reverse 1..$r){
        my $cumprob = 1.0;
        my $x = rand;
        while($x < $cumprob){
            $cumprob -= $cumprob * $samp / $pop;
            $pop--;
        }
        push @ids, $N-$pop-1;
    }
    return \@ids;
}

