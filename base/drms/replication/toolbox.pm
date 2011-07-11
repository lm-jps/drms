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

# This function takes a configuration file and a reference to a hash as input, and returns 
# a hash array (in the referenced hash) of configuration key/value pairs.
sub GetCfg
{
   my($conf) = $_[0];
   my($hashref) = $_[1];
   my(@cfgdata);
   my(%cfgraw);
   my(%cfg);
   my($rv);

   $rv = 0;

   if (open(CNFFILE, "<$conf"))
   {
      @cfgdata = <CNFFILE>;

      # Make key-value pairs of all non-ccomment lines in configuration file.
      %cfgraw = map {
         chomp;
         my($key, $val) = m/^\s*(\w+)\s*=\s*(.*)/;
         defined($key) && defined($val) ? ($key, $val) : ();
      } grep {/=/ and !/^#/} @cfgdata;

      close(CNFFILE);

      # Expand in-line variables in arguments
      %cfg = map {
         my($val) = $cfgraw{$_};
         my($var);
         my($key);
         my($sub);

         while ($val =~ /(\${.+?})/)
         {
            $var = $1;
            $key = ($var =~ /\${(.+)}/)[0];
            $sub = $cfgraw{$key};

            if (defined($var) && defined($sub))
            {
               $var = '\${' . $key . '}';
               $val =~ s/$var/$sub/g;
            }
         }

         ($_, $val);
      } keys(%cfgraw);
   }
   else
   {
      print STDERR "Unable to open configuration file '$conf'.\n";
      $rv = 1;
   }

   if ($rv == 0)
   {
      # everything is AOK - copy %cfg to referenced hash array
      my($hkey);
      my($hval);

      foreach $hkey (keys(%cfg))
      {
         $hval = $cfg{$hkey};
         $hashref->{$hkey} = $hval;
      }
   }
   
   return $rv;
}

1;
