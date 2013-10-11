use 5.10.1;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Thrift::PreForkingServer',
    VERSION_FROM      => 'lib/Thrift/PreForkingServer.pm', # finds $VERSION
    PREREQ_PM         => {
				'Thrift::Server'	=> 0,
				'POSIX'			=> 0,
				'IO::Socket'		=> 0,
				'Symbol'		=> 0,
				'Time::HiRes'		=> 0,	
			 },
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Thrift/PreForkingServer.pm', # retrieve abstract from module
       AUTHOR         => 'Greg Hellings <greg@thesub.net>') : ()),
);
