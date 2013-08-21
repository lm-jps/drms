#!/usr/bin/perl

package toolbox;
# contains a heap of various tools to make 
#  writin' perl routines go quick & easy
#

use Fcntl ':flock';
use Getopt::Long;
use FileHandle;

my(%fpaths);

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

sub AcquireLock
{
   my($path) =$_[0];
   my($lckfh) = $_[1];
   my($gotlock);
   my($natt);
    
   if (-e $path)
   {
      $$lckfh = FileHandle->new("<$path");
      $fpaths{fileno($$lckfh)} = $path;
   }
   else
   {
      $$lckfh = FileHandle->new(">$path");
   }
   $gotlock = 0;

   $natt = 0;
   while (1)
   {
      if (flock($$lckfh, LOCK_EX|LOCK_NB)) 
      {
         $gotlock = 1;
         last;
      }
      else
      {
         if ($natt < 10)
         {
            print "Lock '$path' in use - trying again in 1 second.\n";
            sleep 1;
         }
         else
         {
            print "Couldn't acquire lock after $natt attempts; bailing.\n";
         }
      }

      $natt++;
   }

   return $gotlock;
}

sub ReleaseLock
{
   my($lckfh) = $_[0];
   my($lckfn);
   my($lckfpath);

   $lckfn = fileno($$lckfh);
   $lckfpath = $fpaths{$lckfn};

   flock($$lckfh, LOCK_UN);
   $$lckfh->close;

   if (defined($lckfpath))
   {
      chmod(0664, $lckfpath);
      delete($fpaths{$lckfn});
   }
}

# Given a list of expected arguments, GetArgs() parses the cmd-line and captures the expected-argument values.
# If an expected argument is missing, then GetArgs() returns with an error.
# argsinH -  Input: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose 
#            values are the argument type (as specified in GetOptions()).
# argsoutH - Output: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose 
#            values are the argument values as specified on the cmd-line.
#
# Returns 0 on error, 1 on success. Will return error if the requested argument name does not exist on the cmd-line.
# 
# NOTE: Each cmd-line argument name must match exactly one of the argument names in argsinH. No argument name
# can begin with '-' (GetOptions() considers that an invalid argument name).
sub GetArgs
{
   my($argsinH) = $_[0];
   my($argsoutH) = $_[1];

   my($rv) = 0;
   my(@argsA);
   my(@optlist);
   my($iarg);
   my($type);
   my($rsp);

   # Create hash array to pass to GetOptions
   @argsA = keys(%$argsinH);
   foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg};
        
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {    
            push(@optlist, "$iarg=$type");
        }
    }

   # Trick to get GetOptions() to ignore what we'd consider options. If an argument
   # name, from the cmd-line, starts with an optional space, then it matches, and 
   # GetOptions() will consider it an option and process it. It will also accept
   # things that start with '-', but in the @optlist, no argument will start with '-'.
   # If we want this to work for options too, defined as arguments that start with
   # '-', then each option in @optlist should start with '-'.
   # ( |) works for some reason too.
   # pass_through allows the skipping of unknown options (instead of causing GetOptions()
   # to fail.
   Getopt::Long::Configure("prefix_pattern=( ?)", "pass_through");

   # Returns undef if any options on cmd-line are not in @optlist. Returns 1 otherwise, even if
   # there is an item in @optlist that is not on the cmd-line.
   $rsp = GetOptions($argsoutH, @optlist);

   $rv = $rsp if (defined($rsp));

   if ($rv)
   {
      # Now check that all arguments in argsinH were actually specified on the cmd-line.
      my(@check);

      @argsA = keys(%$argsinH);
      @check = map ({(!defined($argsoutH->{$_}) ? $_ : ())} @argsA);
      $rv = ($#check < 0);

      unless ($rv)
      {
         my($msg) = join(', ', @check);
         print STDERR "Missing required argument(s) '$msg'.\n";
      }
   }

   return $rv;
}

sub GetOpts
{
    my($argsinH) = $_[0];
    my($argsoutH) = $_[1];
    
    my($rv) = 0;
    my(@argsA);
    my(@optlist);
    my($iarg);
    my($type);
    my($rsp);
    
    # Create hash array to pass to GetOptions
    @argsA = keys(%$argsinH);
    foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg}; 
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {
            push(@optlist, "$iarg=$type");            
        }
    }
    
    # A previous Configure call could mess everything up. Just always precede GetOptions()
    # with the proper configuration.
    Getopt::Long::Configure("default");
    
    $rsp = GetOptions($argsoutH, @optlist);
    
    $rv = $rsp if (defined($rsp));
    
    return $rv;
}

1;
