use 5.006;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
  NAME              => 'RobotArmy',
  VERSION_FROM      => 'bin/robots', # finds $VERSION
  PREREQ_PM         => { Getopt::Long => 0.00,
                         Digest::SHA1 => 0.00,
                         LWP => 0.00 , 
                         FCGI => 0.67,
                         JSON => 1.14, },
  ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
    (
     AUTHOR         => 'ira at sweetpota dot to') : ()),
);
