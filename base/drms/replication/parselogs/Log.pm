#####################################################
# 2004/10/14 : igor     : Original version
# 2004/10/20 : igor     : Added process id
# 2005/02/16 : oneiros  : 1.0.0 : Moved namespace for CPAN
# 2005/04/28 : igor     : Added fdebug 
# 2005/05/05 : oneiros  : 1.0.1 : fixed bareword warning
#####################################################
package Log;

our $VERSION = 1.00_01;

use strict;
#use Date::Manip;
use Data::Dumper;
use vars qw (@ISA @EXPORT $VERSION);
require Exporter;

@ISA = qw (Exporter);
@EXPORT = qw(logMessage logTarget logThreshold logDepth fdebug debug info notice warning err error crit critical alert emerg emergency); 

$VERSION = 1.00_00;
our @MONTH = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my $__threshold=0;
my $__depth=0;
my $__lineNo=1;

my $__sep=":-:";

my %__priority = (  debug    => 0,
										info     => 1,
										notice	 => 2,
										warning  => 3,
										err      => 4,
										error    => 4,
										crit     => 5,
										critical => 5,
										alert    => 6,
										emerg    => 7,
										emergency		 => 7 );

my %__fullName = (	err		=> "error",
										crit	=> "critical",
										emerg	=> "emergency" );

my %__fDescCache = ();

BEGIN
{
	no strict 'refs';
	foreach my $logMethod ( qw( debug info notice warning err error crit critical alert emerg emergency ) )
		{
			*{$logMethod} = sub { my $logDesc = join ("",@_);
											return if ($__threshold > $__priority{$logMethod});
  										my @caller0=caller(0 + $__depth);
											my @caller1=caller(1 + $__depth);
                      $__depth=0;                        ## reset depth
											my $lMethod = (exists $__fullName{$logMethod})? $__fullName{$logMethod} : $logMethod;
											logMessage ($lMethod,$logDesc,$caller0[0],$caller0[1],$caller1[3],$caller0[2]);
											};
    }

}

sub logTarget
  {
		my ($target, $fileName) = @_;

    unless ( $__fDescCache{$target}->{fd} ) {
      my $fh=undef;
      $fileName = "/tmp/$target.log" unless ( $fileName);
      open $fh, ">>$fileName";
      $__fDescCache{$target}->{fd} = $fh;
      $__fDescCache{$target}->{fileName} = $fileName;
    }
  }

sub logThreshold 
	{
		my $lThreshold = shift;
		$__threshold = (defined $lThreshold)? (defined $__priority{$lThreshold})?  $__priority{$lThreshold}: 0 :0;
	}

sub logDepth
	{
		$__depth= shift;
	}


sub logMessage
{
  my ($priority, @rest) = @_;

  my $fd = $__fDescCache{"--all"}->{fd}
           ? $__fDescCache{"--all"}->{fd}
           : $__fDescCache{$priority}->{fd}
             ? $__fDescCache{$priority}->{fd}
             :\*STDERR;
  _logMessage ($fd, @_);
}

sub _logMessage
{
  my ($fileDesc, $priority, $logDesc, $package, $filename, $subroutine, $line ) = @_;

  $priority = (defined $priority)?$priority:"debug";

  if (!defined($package))
    {
      my @caller0=caller(0);
      my @caller1=caller(1);
      $package=$caller0[0];
      $filename = $caller0[1];
      $line     = $caller0[2];
      $subroutine = $caller1[3];
    }

  my $buffer;
  ($buffer,$filename) = ($filename =~ /(.*\/)?(.*$)/);
  my $origin = (defined $subroutine)?"($filename: $subroutine:$line)":"($filename: $line)";
  
  $buffer  = datePrefix();
  $buffer .= $__sep;
  $buffer .= $priority;
  $buffer .= $__sep;
  $buffer .= "[".$__lineNo++."]";
  $buffer .= $__sep;
  $buffer .= $logDesc;
  $buffer .= $__sep;
  $buffer .= $origin;
  $buffer .= "\n";

  print $fileDesc $buffer;

}

sub fdebug {
#return;
  my (%args) = @_;
  my $dumper = ($args{'-dump'})?$args{'-dump'}:undef;
  my $text   = ($args{'-text'})?$args{'-text'}:"";
  my $target = ($args{'-target'})?$args{'-target'}:undef;
 
  my $fileDesc = undef;
  if ($target) {
    unless ( $__fDescCache{$target}->{fd} ) {
      my $fh=undef;
      open $fh, ">/tmp/$target.log";
      $fileDesc = $fh;
      $__fDescCache{$target}->{fd} = $fh
    } else {
     $fileDesc =  $__fDescCache{$target}->{fd}; }
  } else { $fileDesc = \*STDERR; }

  my @caller0=caller(0 + $__depth +1 );
	my @caller1=caller(1 + $__depth +1);
  $__depth=0;                        ## reset depth
  if ($dumper) {
    _logMessage($fileDesc, 0, Dumper($dumper), $caller0[0],$caller0[1],$caller1[3],$caller0[2]);
  } else {
    _logMessage($fileDesc, 0, $text, $caller0[0],$caller0[1],$caller1[3],$caller0[2]);
  }
}


sub datePrefix 
{
    my ($sec,$min,$hour,$mday,$mon,$year,@rest)=  localtime(time);
    ## I rather set the month to string so there isn't any kind of
    ## uncertainty between month and day
    my $monthStr = $MONTH[$mon];
    ##2010 Apr 05 00:00:00
    my $date = sprintf "%.4d %s %.2d %.2d:%.2d:%.2d", ($year+1900, $monthStr, $mday, $hour, $min, $sec);
    return $date;
}

#sub datePrefix
#{
#   return UnixDate("today", '%Y %b %d %H:%M:%S');
#}

sub DESTROY {
  for my $key (keys %__fDescCache) {
    close $__fDescCache{$key};
  }
}
1;
__END__
=head1 NAME
                                                                                            
Log - Provides logging functionality
 
=head1 SYNOPSIS
 
  use Log;

=head1 DESCRIPTION

Log formats the log message in an standard fashion.

=head2 Methods

=over 4

=item logMessage()

  handles soap Data request to the File Data Server
  Arguments:
    priority    - defines the priority for the message
                  Namely:
                      debug
                      info
                      notice
                      warning
                      err
                      error
                      crit
                      critical
                      emerg
                      emergency
    logDesc    - log message
    package    - Perl package name
    filename   - Perl file name
    subroutine - Perl Subroutine
    line       - line No in file

=item debug()

=item info()

=item notice()

=item warning()

=item error()

=item err()

=item crit()

=item critical()

=item emerg()

=item emergency()

  Arguments: (for all the above methods)
    A log message string

=item logTarget()

  Sets the output target for the log. If no target is specified
  it defaults to STDERR

      logTarget("info","/var/logs/VSO/cart_info.logs);

  A target of "--all" will summon all "priorities" into the same log file
  If no fileName is specified it will default to /tmp/target.log

  Arguments:
    target, fileName 

=item logThreshold()

  The priority level allows to set a threshold so that log
  calls below that threshold don't get executed.

      logThreshold("info");

  The priority will stay set till another call to logThreshold occurs

  Arguments:
    priority

=item logDepth()

  Call this method before the log call, passing a number greater than cero.

      logDepth($n)

  The log routine will go back 'n' frames before the current one
  The Depth level will be reset to cero after the following call
  to a log method.
  Arguments:
    integer 

=cut
