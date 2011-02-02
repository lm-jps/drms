#!/home/jsoc/bin/linux_x86_64/perl5.12.2 -w


# This script takes at least one argument which specifies which of three types of checkouts to perform:
#  1. A NetDRMS checkout (-net). If present, all other arguments are ignored.
#  2. A JSOC_SDP checkout (-sdp). If present, all other arugments are ignored.
#  3. A custom checkout (-custom). To perform a custom checkout, the caller must follow this argument
#     with the full path to a configuration file specifying the project directories the caller would
#     like to check-out.

# Each type of checkout contains a different subset of files that reside in the JSOC CVS module. To specify
# that subset, we maintain one "file specification" for each checkout type. A file specification is
# a list of file and directory paths relative to the CVS code-tree root. It turns out that you
# can use the CVS checkout or export command with these relative paths. So, for example, to check-out
# the sdp files, you can run "cvs checkout <relpath1> <relpath2> ... <relpathN>. This script maintains
# the file specification for each type of checkout, then performs the desired checkout or export.

# In order to determine the complete set of file names of the files that reside in a checkout file set, 
# you must download the files in that file set from the CVS repository. There is no CVS command that
# will print out all files in the repository or a subtree of the repository. 
# And it is not desirable to use the checkout command to download the files, because
# the checkout command creates additional CVS "state" files and places them in every node of the
# downloaded code tree. These intermingled state files make it difficult to isolate all the source files
# that comprise the checkout. To cope with these CVS deficiencies, when printing the list of files in a file set, 
# this script first EXPORTS all files in the JSOC module into a 
# temporary directory. When an export is performed, CVS does not introduce these extra state files.

# It is important to use this script to update the files in your working directory after the initial 
# checkout. Using the cvs update command directly can result in the download of files outside of the file set.
# And once that happens, 'make' may not work properly.

# NOTE: You cannot check-out the entire JSOC module, then delete the files not needed. If you do that, then CVS
# will think that the checkout is incomplete - cvsstatus.pl will indicate that the deleted files are 
# missing. The CVS state files, which list all files that were downloaded, including the the deleted files, 
# indicate to the CVS server that those deleted files are expected to be present in the working directory. If
# they are missing, various cvs commands will complain.

# flags:
#  -o The operation to perform, which includes:
#       checkout - The current directory must be the parent directory of the CVS working directory root.
#       export   - The current directory must be the parent directory of the CVS working directory root.
#       update   - update the set of files with changes committed to the CVS repository since the initial
#                  checkout. The current directory may be either the CVS working directory root, or its parent
#                  directory. The -f flag is ignored during an update - the checkout type is read from the
#                  configuration file. If the checkout type is sdp or net, then the file specification 
#                  for that checkout type is obtained from this script (not from the state file), since the 
#                  file specification for that checkout type may have changed since the original checkout.
#                  For a custom checkout, the file specification is re-generated. If the caller
#                  provides a -f <config file> argument, then the file specification for the proj directories
#                  is derived from <config file>. Otherwise, the proj directories file specification
#                  is obtained from the state file. Regardless, the file specification for the "core" 
#                  directories is obtained from this script.
#       tag      - tag the set of files in the CVS respository implied by the -f flag.
#       untag    - remove the tag on the  set of files in the CVS respository implied by the -f flag.
#       print    - print the set of files in the CVS repository implied by the -f flag.
#  -f The type of file set to operate on, which includes:
#       sdp (all files in the repository, aka the "full JSOC" source tree).
#       net (the set of files that compose the NetDRMS release).
#       <configuration file> (the set of files is specified by <configuration file>).
#  -r For the checkout, export, and update operations, the revision of files to download. This is a
#       CVS tag or file version number. For the tag and untag operations, the CVS tag to set or remove.


use XML::Simple;
use IO::Dir;
use File::Copy;
use File::Basename;
use File::Path qw(mkpath remove_tree);
use File::Spec;
use Fcntl ':flock';
use Cwd qw(chdir getcwd); # need to override chdir so that $ENV{'PWD'} is changed when chdir is called.
use Data::Dumper;

use constant kMakeDiv => "__MAKE__";
use constant kProjDiv => "__PROJ__";
use constant kFspecDev => "__FSPEC__";
use constant kEndDiv => "__END__";
use constant kStUnk => 0;
use constant kStMake => 1;
use constant kStProj => 2;

use constant kCoUnk => "unk";
use constant kCoNetDRMS => "net";
use constant kCoSdp => "sdp";
use constant kCoCustom => "custom";

use constant kDlCheckout => "checkout";
use constant kDlExport => "export";
use constant kDlUpdate => "update";
use constant kDlTag => "tag";
use constant kDlUntag => "untag";
use constant kDlPrint => "print";

use constant kStrproj => "proj";
use constant kStrname => "name";

# Assume a localization directory of "localization" right in the root of the CVS tree.
use constant kRootDir => "JSOC/";
use constant kLocDir => "localization/";
use constant kTmpDir => "/tmp/chkout/";
use constant kTypeFile => "dlset.txt";


my($arg);
my($cotype);
my($cfgfile);
my($cmd);
my($err);
my(@core);
my(@netonly);
my(@netco);
my(@sdbco);
my(@cstco);
my($curdir);
my($xmldata); # reference to hash array
my($dltype);
my($version);
my($stfile);
my($stcotype);
my($stfspec);

# Don't allow more than one version of this file to run concurrently to avoid race conditions.
unless (flock(DATA, LOCK_EX | LOCK_NB)) 
{
   print "$0 is already running. Exiting.\n";
   exit(1);
}

@core = qw(base/cfortran.h base/foundation.h base/jsoc.h base/jsoc_version.h base/mypng.h base/Rules.mk base/drms base/export base/libs base/sums base/util configure configproj.pl customizemake.pl moreconfigure.pl getmachtype.pl doc jsoc_update.pl make_basic.mk Makefile make_jsoc.pl README Rules.mk jsoc_sync.pl target.mk build);
@netonly = qw(config.local.template gen_init.csh gen_sumcf.csh seed_sums.c netdrms_setup.pl getuid.c proj/example proj/myproj proj/cookbook);
@sdponly = qw(base/local proj configsdp.txt customizedefs.pl config.local.sutemplate CM);

$err = 0;
$cotype = kCoSdp;
$dltype = kDlCheckout;
$version = "";

while ($arg = shift(@ARGV))
{
   if ($arg eq "-o")
   {
      # download type
      $arg = shift(@ARGV);
      if ($arg eq kDlCheckout ||
          $arg eq kDlExport ||
          $arg eq kDlUpdate ||
          $arg eq kDlTag ||
          $arg eq kDlUntag ||
          $arg eq kDlPrint)
      {
         $dltype = $arg;
      }
      else
      {
         print STDERR "Invalid download type - please choose from 'checkout', 'export', 'update', 'tag', 'untag' or 'print'.\n";
         $err = 1;
         last;
      }
   }
   elsif ($arg eq "-r")
   {
      # revision (version)
      $arg = shift(@ARGV);
      $version = $arg;
   }
   elsif ($arg eq "-f")
   {
      # file set
      $arg = shift(@ARGV);

      if ($arg eq kCoSdp)
      {
         $cotype = kCoSdp;
      }
      elsif ($arg eq kCoNetDRMS)
      {
         $cotype = kCoNetDRMS;
      }
      else
      {
         # custom - argument must be a configuration file
         if (-f $arg)
         {
            $cotype = kCoCustom;
            $cfgfile = $arg;
         }
         else
         {
            print STDERR "Invalid custom-download configuration file $arg.\n";
            $err = 1;
         }
      }
   }
}

if (!$err)
{
   if (defined($cfgfile))
   {
      # Custom checkout - if the user is performing an update, then use the file spec saved
      # in the TYPEFILE (no need to re-read a config file).
      my($xml);
      my($xmlobj) = new XML::Simple;

      # Read in the configuration file to obtain the set of project files that will reside
      # in the custom checkout set.
      if (!ReadCfg($cfgfile, \$xml) && defined($xml))
      {
         $xmldata = $xmlobj->XMLin($xml, ForceArray => 1);
      }
      else
      {
         print STDERR "Unable to read or parse configuration file $cfgfile.\n";
         $err = 1;
      }
   }

   if ($dltype eq kDlCheckout || $dltype eq kDlExport || $dltype eq kDlUpdate)
   {
      # Set the state file path.
      my($rootdir) = File::Spec->catdir(kRootDir);
      my($cdir) = File::Spec->catdir($ENV{'PWD'});

      $stfile = kLocDir . kTypeFile;

      if ($cdir !~ /$rootdir\s*$/)
      {
         # Assume that the current directory is the parent of the JSOC code tree.
         $stfile = kRootDir . $stfile
      }
   }

   if ($dltype eq kDlUpdate)
   {
      # If this is an update, obtain the checkout type from the statefile. The 
      # state file will exist at this point only for kDlUpdate.
      if (open(STFILE, "<$stfile"))
      {
         # Modify $cotype - should be determined by the first line of the state file.
         my($line);

         $line = <STFILE>;
         chomp($line);
         $stcotype = $line;
         $cotype = $stcotype;
         $line = <STFILE>;
         chomp($line);
         $stfspec = $line;

         close(STFILE);
      }
      else
      {
         print STDERR "Unable to open state file '$stfile' for reading.\n";
         $err = 1;
      }
   }

   if ($err)
   {
      # Do nothing - this will essentially cause this script to exit.
   }
   elsif ($cotype ne kCoSdp && $cotype ne kCoNetDRMS && $cotype ne kCoCustom)
   {
      print STDERR "Invalid file set identifier '$cotype'.\n";
      $err = 1;
   }
   elsif ($dltype ne kDlCheckout && $dltype ne kDlExport && $dltype ne kDlUpdate && 
          $dltype ne kDlTag && $dltype ne kDlUntag && $dltype ne kDlPrint)
   {
      print STDERR "Invalid operation '$dltype'.\n";
      $err = 1;
   }
   else
   {
      if (BuildFilespec($cotype, $dltype, $stfspec, \$xmldata, \@core, \@netonly, \@sdponly, \@filespec))
      {
         print STDERR "Unable to build filespec.\n";
         $err = 1;
      }
      else
      {
         undef($curdir);

         if ($dltype eq kDlTag || $dltype eq kDlUntag || $dltype eq kDlPrint)
         {
            # cd to tmp directory for these commands
            if (!(-d kTmpDir))
            {
               # no need to check this call, because the chdir() cmd is being checked.
               mkpath(kTmpDir);
            }
           
            $curdir = $ENV{'PWD'};
            $err = (chdir(kTmpDir) == 0);
            if ($err)
            {
               print STDERR "Unable to cd to " . kTmpDir . ".\n";
            }
         }
         elsif ($dltype eq kDlUpdate)
         {
            # If the current directory is the root directory of the CVS working directory, then
            # cd up to the parent directory (DownloadTree assumes the current directory is the
            # parent of the CVS working directory).
            my($rootdir) = File::Spec->catdir(kRootDir);
            my($cdir) = File::Spec->catdir($ENV{'PWD'});

            if ($cdir =~ /$rootdir\s*$/)
            {
               # The current directory is the CVS working directory.
               $curdir = $ENV{'PWD'};
               $err = (chdir('..') == 0);
            }
         }

         # Do a cvs checkout, export, or update into the current directory
         if (!$err)
         {
            $err = DownloadTree($cotype, $dltype, $version, \@filespec);
            if ($err)
            {
               print STDERR "Unable to $dltype CVS tree.\n";
            }
         }

         if (!$err)
         {
            # Assumes that the cdir is the one containing kRootDir
            if ($dltype eq kDlCheckout || $dltype eq kDlExport || $dltype eq kDlUpdate)
            {
               if (!(-d kRootDir . kLocDir))
               {
                  mkpath(kRootDir . kLocDir);
               }

               # save state file
               if (open(TYPEFILE, ">" . kRootDir . kLocDir . kTypeFile))
               {
                  # save check-out type                 
                  print TYPEFILE "$cotype\n";

                  # print file spec used during checkout
                  if (!$err)
                  {
                     my($fs) = join(' ', @filespec);
                     print TYPEFILE "$fs\n";
                  }

                  # Now print list of files that compose the file set.
                  if (!$err)
                  {
                     if ($dltype eq kDlUpdate)
                     {
                        # Must print each tree in file specification, since
                        # the tree rooted at kRootDir may not contain
                        # files other than the files originally downloaded
                        # from CVS (e.g., running make will create new 
                        # files).
                        foreach $subspec (@filespec)
                        {
                           if (PrintFilenames(*TYPEFILE, kRootDir . $subspec))
                           {
                              print STDERR "Unable to print file-set file names.\n";
                              $err = 1;
                              last;
                           }
                        }
                     }
                     else
                     {
                        if (PrintFilenames(*TYPEFILE, kRootDir))
                        {
                           print STDERR "Unable to print file-set file names.\n";
                           $err = 1;
                        }
                     }
                  }

                  close(TYPEFILE);
               }
               else
               {
                  print STDERR "Unable to open file " . kRootDir . kLocDir . kTypeFile . " for writing.\n";
               }
            }
            elsif ($dltype eq kDlTag || $dltype eq kDlUntag)
            {
               # Can only use the tag/untag command to tag/untag releases, 
               # which can only be done on either the sdp or netdrms checkouts types.
               if ($cotype eq kCoSdp || $cotype eq kCoNetDRMS)
               {
                  my($curdirin) = $ENV{'PWD'};
                  $err = (chdir(kTmpDir . kRootDir) == 0);
                  if ($err)
                  {
                     print STDERR "Unable to cd to " . kTmpDir . ".\n";
                  }
                  else
                  {
                     if (TagFiles($version, $dltype))
                     {
                        print STDERR "Unable to tag/untag files in file specification.\n";
                        $err = 1;
                     }
                  }

                  if (chdir($curdirin) == 0)
                  {
                     print STDERR "Unable to cd to $curdir.\n";
                     if (!$err)
                     {
                        $err = 1;
                     }
                  }
               }
               else
               {
                  print STDERR "Checkout type $cotype not compatible with tag command.\n";
                  $err = 1;
               }
            }
            elsif ($dltype eq kDlPrint)
            {
               if (PrintFilenames(*STDOUT, kTmpDir . kRootDir))
               {
                  print STDERR "Unable to print file set file names.\n";
                  $err = 1;
               }
            }
         }

         if (defined($curdir))
         {
            # This implies that a successful chdir was done previously.
            if (chdir($curdir) == 0)
            {
               print STDERR "Unable to cd to $curdir.\n";
               $err = 1;
            }
         }

         # Delete all files from temporary directory, but only if there were no errors
         if (!$err)
         {
            if (-d kTmpDir)
            {
               remove_tree(kTmpDir, {error => \my $errlist});
               if (@{$errlist})
               {
                  print STDERR "Unable to properly remove temporary subdirectory $tmpdir.\n";
                  $err = 1;
               }
            }
         }
      }
   }
}

flock(DATA, LOCK_UN);

exit($err);

sub ReadCfg
{
   my($cfgfile) = $_[0];
   my($xml) = $_[1]; # reference
   my($rv);

   if (defined($cfgfile) && -e $cfgfile)
   {
      if (open(CFGFILE, "<$cfgfile"))
      {
         my($projdone);
         my($line);

         $st = kStUnk;
         $pdiv = kProjDiv;
         $ediv = kEndDiv;

         $projdone = 0;

         while (defined($line = <CFGFILE>))
         {
            chomp($line);

            if ($line =~ /^\#/ || $line =~ /^\s*$/)
            {
               # Skip blank lines or lines beginning with # (comment lines)
               next;
            }
            elsif ($line =~ /^$pdiv/)
            {
               $st = kStProj;

               # initialize xml string variable
               $$xml = "";
               next;
            }
            elsif ($line =~ /^$ediv/)
            {
               if ($st == kStProj)
               {
                  $projdone = 1;
               }

               $st = kStUnk;

               if ($projdone)
               {
                  last;
               }

               next;
            }

            if ($st == kStProj)
            {
               # suck out xml
               $$xml = $$xml . "$line\n";
            }
         } # loop over cfg file

         close(CFGFILE);
      }
      else
      {
         print STDERR "Unable to open $cfgfile for reading.\n";
         $rv = 1;
      }
   }
   else
   {
      print STDERR "Unable to open $cfgfile for reading.\n";
      $rv = 1;
   }

   return $rv;
}

sub BuildFilespec
{
   my($cotype) = $_[0];
   my($dltype) = $_[1];
   my($stfspec) = $_[2];
   my($xmldata) = $_[3];
   my($core) = $_[4];
   my($netonly) = $_[5];
   my($sdponly) = $_[6];
   my($fsout) = $_[7];

   my($rv);
   my($strproj) = kStrproj;
   my($strname) = kStrname;

   $rv = 0;

   if ($cotype eq kCoCustom && $dltype eq kDlUpdate && (!defined($xmldata) || $#{$xmldata->{$strproj}} < 0))
   {
      # If this is an update of a custom checkout, and the user did not provide a config-file argument,
      # then use the file spec stored in the existing typefile
      if (defined($stfspec) && length($stfspec) > 0)
      {
         @$fsout = qw($stfspec);
      }
      else
      {
         print STDERR "Invalid file specification in state file.\n";
         $rv = 1;
      }
   }
   else
   {
      if ($cotype eq kCoNetDRMS)
      {
         push(@$fsout, @$core);
         push(@$fsout, @$netonly);
      }
      elsif ($cotype eq kCoSdp)
      {
         push(@$fsout, @$core);
         push(@$fsout, @$sdponly);
      }
      elsif ($cotype eq kCoCustom)
      {
         push(@$fsout, @$core);

         # Use $xmldata to populate @cstco;
         foreach $proj (@{$xmldata->{$strproj}})
         {
            push(@$fsout, $$proj->{$strname}->[0]);
         }
      }
      else
      {
         print STDERR "Unsupported checkout type $cotype.\n";
         $rv = 1;
      }
   }

   return $rv;
}

sub DownloadTree
{
   my($cotype) = $_[0];
   my($dltype) = $_[1];
   my($version) = $_[2];
   my($fspec) = $_[3];

   my($rv) = 0;
   my($curdir);
   my($callstat);
   my($cvscmd);
   my($cmd);
   my($rev);
   my(@relpaths);

   if (length($dltype) > 0)
   {
      if ($dltype eq kDlCheckout || $dltype eq kDlTag || $dltype eq kDlUntag)
      {
         $cvscmd = "checkout -AP";
      }
      elsif ($dltype eq kDlExport || $dltype eq kDlPrint)
      {
         $cvscmd = "export";
      }
      elsif ($dltype eq kDlUpdate)
      {
         # If a new directory is added to the repository AND the new directory is added to the
         # file specifications above, then the -d flag to cvs update will cause the new directory
         # to be downloaded to the client.
         $cvscmd = "update -APd";
      }
      else
      {
         print STDERR "Unsupported download type $dltype.\n";
         $rv = 1;
      }
   }
   else
   {
      # Default to a checkout.
      $cvscmd = "checkout -AP";
   }

   if (!$rv)
   {
      if (length($version) > 0)
      {
         $rev = "-r $version";
      } 
      elsif ($dltype eq kDlExport || $dltype eq kDlPrint)
      {
         # Only export requires a revision argument
         $rev = "-r HEAD";
      } 
      else
      {
         $rev = "";
      }
   }

   if (!$rv)
   {
      # Check the existence of the proper directories required for each operation.
      if ($dltype eq kDlCheckout || $dltype eq kDlExport)
      {
         # If $dltype is kDlCheckout, kDlExport,then there must not be a JSOC subdirectory in the current directory. 
         if (-e kRootDir)
         {
            print STDERR "Root directory " . kRootDir . " already exists.\n";
            $rv = 1;
         }
      }
      elsif ($dltype eq kDlTag || $dltype eq kDlUntag || $dltype eq kDlPrint)
      {
         # If $dltype is kDlTag,  kDlUntag, or kDlPrint then it is okay to delete the JSOC subdirectory 
         # because these three operations create a temporary JSOC directory.
         if (-e kTmpDir . kRootDir)
         {
            remove_tree(kTmpDir . kRootDir, {error => \my $errlist});
            if (@{$errlist})
            {
               print STDERR "Unable to properly remove temporary subdirectory $tmpdir.\n";
               $rv = 1;
            }
         }
      }
      elsif ($dltype eq kDlUpdate)
      {
         # If $dltype is kDlUpdate, then there MUST be a working directory root.
         # The current directory is the parent of the root directory (if it exists).
         if (!(-e kRootDir))
         {
            print STDERR "No CVS working directory exists in $ENV{'PWD'}.\n";
            $rv = 1;
         }
      }
      else
      {
         print STDERR "Unsupported download type $dltype.\n";
         $rv = 1;
      }
   }

   if (!$rv)
   {
      @relpaths = map({kRootDir . "$_"} @{$fspec});
      $cmd = join(' ', "cvs", $cvscmd, $rev, @relpaths);

      if (CallCVS($cmd))
      {
         print STDERR "Unable to $dltype repository files.\n";
         $rv = 1;
      }
   }

   return $rv;
}

# tag all files in cwd with tag $tag
sub TagFiles
{
   my($tag) = $_[0];
   my($dltype) = $_[1];

   my($rv) = 0;
   my(@allfiles);
   my($curdir) = $ENV{'PWD'};
   my($cmd);

   if ($dltype eq kDlTag)
   {
      # call cvs tag -c <tag> 
      $cmd = "cvs tag -c $tag ."
   } else
   {
      # call cvs tag -d <tag> 
      $cmd = "cvs tag -d $tag ."
   }
   
   if (CallCVS($cmd))
   {
      print STDERR "Unable to tag repository files.\n";
      $rv = 1;
   }

   return $rv;
}

sub PrintFilenames
{
   my($fh) = $_[0];
   my($froot) = $_[1];

   my($rv) = 0;
   my(@allfiles);

   if (GetFileList($froot, "", \@allfiles))
   {
      print STDERR "Unable to retrieve list of files rooted at $froot.\n";
      $rv = 1;
   }
   else
   {
      foreach $afile (@allfiles)
      {
         print $fh "$afile\n";
      }
   }

   return $rv;
}

# Returns a list of all files in the code tree rooted at $spec. The names of each file
# will be prefixed by $dir, unless $dir is the empty string. $listout points to the
# returned list of files.
sub GetFileList
{
   my($spec) = $_[0];
   my($dir) = $_[1]; # the directory to prepend to files when inserting into $listout
   my($listout) = $_[2];

   my($rv) = 0;
   my($prefix);

   if (length($dir) > 0)
   {
      if (substr($dir, length($dir) - 1, 1) eq "/")
      {
         $prefix = $dir;
      }
      else
      {
         $prefix = "$dir/";
      }
   } 
   else
   {
      $prefix = "";
   }

   if (!(-e $spec))
   {
      print STDERR "File $spec does not exist in $ENV{'PWD'}.\n";
      $rv = 1;
   }
   else
   {

      if (-f "$spec")
      {
         # $spec is a file - just push onto output list
         push(@{$listout}, "$prefix$spec");
      }
      elsif (-d "$spec")
      {
         my(@alltreefiles);
         my(@treefiles);
         my($curdir);

         # This is a directory, collect all files (excluding "." and "..") in the tree rooted at
         # this directory.
         $curdir = "$ENV{'PWD'}";
         chdir($spec);
         tie(my(%tree), "IO::Dir", ".");
         @alltreefiles = keys(%tree);
         @treefiles = map({$_ !~ /^\.$/ && $_ !~ /^\.\.$/ ? $_ : ()} @alltreefiles);


         # Now recursively call GetFileList() for each item in @treefiles
         foreach $childspec (@treefiles)
         {
            GetFileList($childspec, "$prefix$spec", $listout);
         }

         chdir($curdir);
      }
      else
      {
         print STDERR "File '$spec' is not a supported file type.\n";
         $rv = 1;
      }
   }

   return $rv;
}

sub CallCVS
{
   my($cmd) = $_[0];

   my($rv) = 0;
   my($callstat);

   system($cmd);
   $callstat = $?;

   if ($callstat == -1)
   {
      print STDERR "Failed to execute '$cmd'.\n";
      $rv = 1;
   } 
   elsif ($callstat & 127)
   {
      print STDERR "cvs command terminated abnormally.\n";
      $rv = 1;
   } 
   elsif ($callstat >> 8 != 0)
   {
      $callstat = $callstat >> 8;
      print STDERR "cvs command ran unsuccessfully, status == $callstat.\n";
      $rv = 1;
   }

   return $rv;
}

sub MoveFiles
{
   my($files) = $_[0];
   my($srcroot) = $_[1];
   my($tgtroot) = $_[2];

   my($rv) = 0;
   my($fullsrc);
   my($fulltgt);
   my($tgtdir);

   foreach $onefile (@{$files})
   {
      $fullsrc = "$srcroot/$onefile";
      $fulltgt = "$tgtroot/$onefile";
      $tgtdir = dirname($fulltgt);

      if (-e $fullsrc)
      {
         # Make sure the tgt directory exists.
         if (!(-d $tgtdir))
         {
            mkpath($tgtdir);
         }

         if (-d $tgtdir)
         {
            if (!move($fullsrc, $fulltgt))
            {
               print STDERR "Unable to move $fullsrc to $fulltgt; skipping.\n";
            }
         }
         else
         {
            print STDERR "Unable to make target directory $tgtdir; skipping.\n";
         }
      }
      else
      {
         print STDERR "File $fullsrc does not exist; skipping.\n";
      }
   }

   return $rv;
}

__DATA__

