#!/usr/bin/perl

package toolbox;
# contains a heap of various tools to make 
#  writin' perl routines go quick & easy
#

require 'timelocal.pl';

#--|----------------------------------------------
#--| toolbox.pm
#--|----------------------------------------------
#--| contains the following tools:
#--|  
#--|  getDate (delimiter)
#--|  
#--|  getTime (date_delimiter, time_delimiter)
#--|  
#--|  logWrite (logfile, log_text_string)
#--|  
#--|  crtDir (dirname) 
#--|    note: path can be included in dirname
#--|           the defailt is the current dir (or ./)
#--|  
#--|----------------------------------------------

#--|----------------------------------------------
#--| toolbox.pm file variables
#--|----------------------------------------------
my $retStatus = 0; # 0 = success

#--|----------------------------------------------
#--| getDate
#--|----------------------------------------------
sub getDate {
	$delimiter = shift;
	@now = localtime time;
	$mdy = sprintf "%02d$delimiter%02d$delimiter%04d",
	$now[4] + 1, $now[3], $now[5] + 1900;

	return ($mdy);
}

#--|----------------------------------------------
#--| getTime
#--|----------------------------------------------
sub getTime {
	$dd = shift; #date delimiter
	$td = shift; #time delimiter
	@now = localtime time;

	$mdy = sprintf "%02d$dd%02d$dd%04d$td%02d$td%02d$td%02d",
	$now[4] + 1,
	$now[3],
	$now[5] + 1900,
	$now[2],
	$now[1],
	$now[0];

	return ($mdy);
}


#--|----------------------------------------------
#--| logWrite
#--|----------------------------------------------
sub logWrite {
	if ($#_ != 1) {
		print "Error, incorrect number of params in logWrite\n";
		$retStatus = 1;
	}
	else {
		my $logfile = shift;
		my $logtext = shift;
		my $dateStr = getTime ("/", ":");
		chomp $logtext;

		#open logfile
		if (open (CURRLOGFILE, ">>$logfile")) {
			print CURRLOGFILE "---[$dateStr]------------------------\n";
			print CURRLOGFILE "$logtext\n";
			print CURRLOGFILE "------------------------------------------------\n\n";
			close CURRLOGFILE;
		}
		else {
			print "Error opening logfile [$logfile] in logWrite\n";
			$retStatus = 1;
		}
	}
	return $retStatus;
}

#--|----------------------------------------------
#--| crtDir
#--|----------------------------------------------
sub crtDir {
	if ($#_ != 0) {
		print "Error, incorrect number of params in crtDir\n";
		$retStatus = 1;
	}
	else {
		my $dirname = shift;

		if (! -d "$dirname") {
			my $ret = mkdir ("$dirname", 0777);
			if (! $ret) {
				$retStatus = 1;
			}
		}
	}
	return $retStatus;
}
1;
