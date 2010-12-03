#!/home/jsoc/bin/linux_x86_64/perl -w
package Logmontools;

use strict;
require Exporter;

# This causes package Logmontools to inherit from the Exporter package. The only 
# function that needs to be inherited is 'import'. When the user of Logmontools
# uses this package with the 'use' directive, this will cause the symbols in 
# @EXPORT_OK to be exported into the user of Logmontools. If the following 
# statement is omitted, then the symbols in @EXPORT_OK will never get exported
# to the user's namespace. Because of the following statement, the user can 
# call loggerMsgPrt directly, without having to prepend 'Logmontools::'.
our @ISA = qw(Exporter);

# Do not use @EXPORT - this is an unconditional export of all items in the array.
# Also, there is no need to use %EXPORT_TAGS - this is used for grouping symbols
# to be exported en masse.
# The items in @EXPORT_OK will be exported only if the user asks for them specfically.
our @EXPORT_OK = qw(loggerLog loggerMsgPrt);

# This function always opens and closes the log file. We probably want to change this
# behavior to keep the fp open during the entire life of the script.
sub loggerLog($$$@)
{
  # get arguments passed
  my($logfile,$status,$message, @logger_init)= @_;

  # local variables
  my $fptr;
  my $logger_string;
  my $new_date;
  my $ret_print=0;
  my($username,$parentprocessid,$processid,$machinename);
  my($second, $minute, $hour, $dayOfMonth, $monthOffset, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings);
  my ($year,$month);

  # open logfile passed in as argument
  open($fptr,">>$logfile") || die  "ERROR in logger.pl:loggerLog()(0):Can't Open log file <$logfile>: $!\n; exit;";


  # get date in this format 2010.09.13_19:11:17 - > utc 
  ($second, $minute, $hour, $dayOfMonth, $monthOffset, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = gmtime();
  $year = 1900 + $yearOffset;
  $month= $monthOffset + 1;
  #create todays utc date format and push on list of dates to do
  $new_date= sprintf("%4d.%02.2d.%02.2d_%02d:%02d:%02d", $year,$month,$dayOfMonth,$hour,$minute,$second);

  # get username, processid, parentprocessid, machinename
  $username=getlogin();
  $parentprocessid=getppid();
  $processid=$$;    #$$ or $PID
  $machinename=`hostname`;
  $machinename =~ s/\n//g; #regular exp rm cr 

  # create logger message string
  $logger_string=sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,<%s>","\@LOGGER_MSG", $new_date,$username,$machinename,$parentprocessid,$processid,$logger_init[0],$logger_init[1],$logger_init[2],$status,$message);

  # write to logfile
  $ret_print=print $fptr "$logger_string\n";
  if($ret_print != 1)
  {
    print "ERROR: When calling logger.pl, the fileptr argument passed is not valid:<$ret_print>\n";
  }
  else
  {
    close($fptr);
  }
}

sub loggerMsgPrt($$@)
{
  # get arguments passed
  my($status,$message, @logger_init)= @_;

  # local variables
  my $logger_string;
  my($username,$parentprocessid,$processid,$machinename);
  my($second, $minute, $hour, $dayOfMonth, $monthOffset, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings, $new_date);
  my ($year,$month);

  # get date in this format 2010.09.13_19:11:17 - > utc 
  ($second, $minute, $hour, $dayOfMonth, $monthOffset, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = gmtime();
  $year = 1900 + $yearOffset;
  $month= $monthOffset + 1;
  #create todays utc date format and push on list of dates to do
  $new_date= sprintf("%4d.%02.2d.%02.2d_%02d:%02d:%02d", $year,$month,$dayOfMonth,$hour,$minute,$second);

  # get username, processid, parentprocessid, machinename
  $username=getlogin();
  $parentprocessid=getppid();
  $processid=$$;    #$$ or $PID
  $machinename=`hostname`;
  $machinename =~ s/\n//g; #regular exp rm cr 

  # create logger message string
  $logger_string=sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,<%s>","\@LOGGER_MSG", $new_date,$username,$machinename,$parentprocessid,$processid,$logger_init[0],$logger_init[1],$logger_init[2],$status,$message);

  # return logger message to calling perl code 
  return $logger_string;
}

1;

__END__
