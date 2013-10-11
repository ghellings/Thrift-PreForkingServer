use 5.10.1;
use strict;
use warnings;
our $VERSION = '0.01';

package Thrift::PreForkingServer;
use Thrift::Server;
use POSIX qw{:sys_wait_h :signal_h sigprocmask};
use IO::Socket;
use Symbol;
use Time::HiRes qw{ usleep };
use base qw{ Thrift::ForkingServer };

=head1 Methods

=cut

=head2 new

Create Thrift::MyPreForking object

=cut

sub new {
    my $classname = shift;
    my $self = {
      	processor              => shift,
        serverTransport        => shift,
        inputProtocolFactory   => Thrift::BinaryProtocolFactory->new(),
        outputProtocolFactory  => Thrift::BinaryProtocolFactory->new(),
	PREFORK                => shift || 6,        			# number of children to maintain
	MAX_CLIENTS_PER_CHILD  => shift || 1024,       			# number of clients each child should process
	children               => { },       				# keys are current child process IDs
	children_num           => 0,        				# current number of children
	MAX_THREADS_PER_CHILD  => shift || 0,				# number of threads to launch per child, if 0 threading is disabled
	transport	       => shift || 'framed',			# Type of transport to use
	
    };
    if ( lc($self->{'transport'}) eq 'framed') {
	use Thrift::MyFramedTransportFactory;
    	$self->{'inputTransportFactory'}  = Thrift::FramedTransportFactory->new();
        $self->{'outputTransportFactory'} = Thrift::FramedTransportFactory->new();
    }
    else {
    	$self->{'inputTransportFactory'}  = Thrift::BufferedTransportFactory->new();
        $self->{'outputTransportFactory'} = Thrift::BufferedTransportFactory->new();
    }
    if ( $self->{'MAX_THREADS_PER_CHILD'} > 0 ) {	
    	use threads;
    }
    return bless($self,$classname);
}

=head2 make_new_child

Fork a proc to handle socket connections in a threaded manner 

=cut

sub make_new_child {
	my $self = shift;
	my $pid;
	my $sigset;

	# block signal for fork
	$sigset = POSIX::SigSet->new(POSIX::SIGINT);
	POSIX::sigprocmask(POSIX::SIG_BLOCK, $sigset)
	  or die "Can't block SIGINT for fork: $!\n";

	die "fork: $!" unless defined ($pid = fork);

	if ($pid) {
	  # Parent records the child's birth and returns.
	  POSIX::sigprocmask(POSIX::SIG_UNBLOCK, $sigset)
	      or die "Can't unblock SIGINT for fork: $!\n";
	  $self->{'children'}->{$pid} = 1;
	  $self->{'children_num'}++;
	  return;
	}
	else {
	  # Child can *not* return from this subroutine.
	  $SIG{CHLD} = 'DEFAULT';      
	  $SIG{INT} = 'DEFAULT';      # make SIGINT kill us as it did before

	  # unblock signals
	  POSIX::sigprocmask(POSIX::SIG_UNBLOCK, $sigset)
	      or die "Can't unblock SIGINT for fork: $!\n";
	  # handle connections until we've reached $MAX_CLIENTS_PER_CHILD
	  for (my $i=0; $i < $self->{'MAX_CLIENTS_PER_CHILD'}; $i++) {
		# Create thread for each socket connection
		if ($self->{'MAX_THREADS_PER_CHILD'} > 0 ) {
			my $thr = threads::create( { 'stack_size' => 10485760, 'exit' => 'threads_only' }, sub {
				my $client = $self->{serverTransport}->accept() or last;
				eval {
					$self->_child($client);
				}; if ($@) {
					$self->_handleException($@);
				}
			});
			# Join threads after reaching MAX_THREADS_PER_CHILD
			if ( threads->list > $self->{'MAX_THREADS_PER_CHILD'} ) {
				map $_->join, threads->list;
			}
		}
		else {
			my $client = $self->{serverTransport}->accept() or last;
			eval {
				$self->_child($client);
			}; if ($@) {
				$self->_handleException($@);
			}
		}
	  }
	  # Join any remaining threads
	  map { $_->join; } threads->list if $self->{'MAX_THREADS_PER_CHILD'};


	  # this exit is VERY important, otherwise the child will become
	  # a producer of more and more children, forking yourself into
	  # process death.
	  exit;
	}
}

=head2 serve

Daemon to handle cycling and maintaining number of forked procs

=cut

sub serve {
	my $self = shift;
	# Open socket and listen for connections
	$self->{serverTransport}->listen();
	# Fork off our children.
	for (1 .. $self->{'PREFORK'}) {
		$self->make_new_child( );
	}
	# Loop to maintain number of forked procs
	while ( 1 ) {
		# Install signal handlers.
		$SIG{CHLD} = sub { $self->REAPER };
		$SIG{INT}  = sub { $self->HUNTSMAN };	
		Time::HiRes::usleep(1);                          # sleep for a microsecond
		# maintain the population
		for (my $i = $self->{'children_num'}; $i < $self->{'PREFORK'}; $i++) {
			$self->make_new_child( );           # top up the child pool
			# Catch zombies we might have missed SIG_CHLD for 
			my $kid;
			do { 
				$kid = waitpid(-1, POSIX::WNOHANG);
				if ($kid) {
					$self->{'children_num'}--;
					delete $self->{'children'}->{$kid};
				}
			} while $kid > 0;
		}
	}
}

=head2 _child

Handle socket connections for thrift

=cut

sub _child {
    my $self   = shift;
    my $client = shift;
    my $itrans = $self->{inputTransportFactory}->getTransport($client);
    my $otrans = $self->{outputTransportFactory}->getTransport($client);
    my $iprot  = $self->{inputProtocolFactory}->getProtocol($itrans);
    my $oprot  = $self->{outputProtocolFactory}->getProtocol($otrans);
    $self->_clientBegin($iprot, $oprot);

    my $ecode = 0;
    eval {
        while (1) {
            $self->{processor}->process($iprot, $oprot);
        }
    }; if($@) {
        $ecode = 1;
        $self->_handleException($@);
    }

    $self->tryClose($itrans);
    $self->tryClose($otrans);

    return ($ecode);
}


=head2 REAPER

Reap child procs

=cut

sub REAPER {                        # takes care of dead children
	my $self = shift;
	# $SIG{CHLD} = \&REAPER;
	my $pid = wait;
	$self->{'children_num'} --;
	delete $self->{'children'}->{$pid};
}

=head2 HUNTSMAN

Catch SIG_INT and kill child procs

=cut

sub HUNTSMAN {                      # signal handler for SIGINT
	my $self = shift;
	local($SIG{CHLD}) = 'IGNORE';   # we're going to kill our children
	kill 'INT' => keys %{$self->{'children'}};
	exit;                           # clean up with dignity
}


package Thrift::FramedTransportFactory;
use Thrift::FramedTransport;

sub new {
    my $classname = shift;
    my $self      = {};

    return bless($self,$classname);
}



sub getTransport {
    my $self  = shift;
    my $trans = shift;

    my $framed = Thrift::FramedTransport->new($trans);
    return $framed;
}


1;

__END__

=head1 NAME

Thrift::PreForkingServer 

=head1 SYNOPSIS


	use Thrift::Server;
	use Thrift::Socket;
	use Thrift::PreForkingServer;
	use myservice::MyService;
	use MyServiceHandler;

	my $prefork = 15;
	my $max_clients_per_child = 50;
	my $max_threads_per_child = 20; # if defined as 0, threading is disabled
	my $transport_type = 'framed';

	eval {
		my $handler       = new MyServiceHandler;
		my $processor     = new myservice::MyServiceProcessor($handler);
		my $serversocket  = new Thrift::ServerSocket(9098);

		my $forkingserver = new Thrift::PreForkingServer(
			$processor, 
			$serversocket, 
			$prefork, 
			$max_clients_per_child, 
			$max_threads_per_child,
			$transport_type,
		);
		$forkingserver->serve();
	}; if ($@) {

		if ($@ =~ m/TException/ and exists $@->{message}) {
			my $message = $@->{message};
			my $code    = $@->{code};
			my $out     = $code . ':' . $message;
			die $out;
		} 
		
		else {
			die $@;
		}

	}




=head1 DESCRIPTION

The module is a PreForking Threaded replacement for Thrift::ForkingServer

=head2 EXPORT

None by default.

=head1 SEE ALSO

http://thrift.apache.org/


=cut




=head1 AUTHOR

ghellings@spokeo.com, E<lt>Greg Hellings<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by ghellings@spokeo.com

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
