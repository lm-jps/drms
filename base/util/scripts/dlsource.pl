#!/home/jsoc/bin/linux_x86_64/perl -w


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

# IGNORE COMMENTS BELOW


# In order to determine what files
# reside in a directory of the file specification, you must download the files in that directory
# from the CVS repository. There is no CVS command that will print out all files in the repository.
# Furthermore, it is not desirable to use the checkout command to download the files, because
# the checkout command creates additional CVS "state" files and places them in every node of the
# downloaded code tree. These intermingled state files make it difficult to isolate all the source files
# that comprise the checkout. 

# To cope with these CVS deficiencies, this script first EXPORTS all files in the JSOC module into a 
# temporary directory. When an export is performed, CVS does not introduce these extra state files.
# Then this script identifies all downloaded files that match the file specification, and it saves this
# list of files in memory. The script then deletes the downloaded files, which have no other purpose, and
# it then checks-out, using the CVS checkout command, all files in this list directly from the 
# repository. It must checkout these files one-by-one.  You cannot
# check-out the entire JSOC module, then delete the files not needed. If you do that, then CVS
# will think that the checkout is incomplete - cvsstatus.pl will indicate that the deleted files are 
# missing. The CVS state files, which list all files that were downloaded, including the the deleted files, 
# indicate to the CVS server that those deleted files are expected to be present in the working directory. If
# they are missing, various cvs commands will complain. If you checkout files one at a time, CVS 
# will not expect files not checked out to be present in the working directory.


use XML::Simple;
use IO::Dir;
use File::Copy;
use File::Basename;
use File::Path qw(mkpath rmtree);
use Cwd qw(chdir getcwd); # need to override chdir so that $ENV{'PWD'} is changed when chdir is called.
use Data::Dumper;

use constant kMakeDiv => "__MAKE__";
use constant kProjDiv => "__PROJ__";
use constant kEndDiv => "__END__";
use constant kStUnk => 0;
use constant kStMake => 1;
use constant kStProj => 2;

use constant kCoUnk => 0;
use constant kCoNetDRMS => 1;
use constant kCoSdp => 2;
use constant kCoCustom => 3;

use constant kDlCheckout => "checkout";
use constant kDlExport => "export";
use constant kDlUpdate => "update";

use constant kStrproj => "proj";
use constant kStrname => "name";

# Assume a localization directory of "localization" right in the root of the CVS tree.
use constant kRootDir => "JSOC/";
use constant kLocDir => "localization/";
use constant kTypeFile => "dlset.txt";


my($arg);
my($printfiles);
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

@core = qw(base/cfortran.h base/foundation.h base/jsoc.h base/jsoc_version.h base/mypng.h base/Rules.mk base/drms base/export base/libs base/sums base/util configure configproj.pl customizemake.pl moreconfigure.pl getmachtype.pl doc jsoc_update.pl make_basic.mk Makefile make_jsoc.pl README Rules.mk jsoc_sync.pl target.mk build);
@netonly = qw(config.local.template gen_init.csh gen_sumcf.csh seed_sums.c netdrms_setup.pl getuid.c proj/example proj/myproj proj/cookbook);
@sdponly = qw(base/local proj configsdp.txt customizedefs.pl config.local.sutemplate CM);

$err = 0;
$printfiles = 0;
$cotype = kCoSdp;
$dltype = kDlCheckout;
$version = "";

while ($arg = shift(@ARGV))
{
   if ($arg eq "-p")
   {
      # print files
      $printfiles = 1;
   }
   elsif ($arg eq "-d")
   {
      # download type
      $arg = shift(@ARGV);
      if ($arg eq "checkout")
      {
         $dltype = kDlCheckout;
      }
      elsif ($arg eq "export")
      {
         $dltype = kDlExport;
      }
      elsif ($arg eq "update")
      {
         $dltype = kDlUpdate;
      }
      else
      {
         print STDERR "Invalid download type - please choose from 'checkout', 'export', or 'update'.\n";
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

      if ($arg eq "sdp")
      {
         $cotype = kCoSdp;
      }
      elsif ($arg eq "net")
      {
         $cotype = kCoNetDRMS;
      }
      else
      {
         # custom - argument must be a configuration file
         if (-f $arg)
         {
            my($xml);
            my($xmlobj) = new XML::Simple;

            $cotype = kCoCustom;
            $cfgfile = $arg;

            # Read in the configuration file to obtain the set of project files that will reside
            # in the custom checkout set.
            if (!ReadCfg($cfgfile, \$xml) && defined($xml))
            {
               $xmldata = $xmlobj->XMLin($xml, ForceArray => 1);
            }
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
   if ($cotype == kCoCustom && !defined($xmldata))
   {
      print STDERR "Unable to read or parse configuration file $cfgfile.\n";
      $err = 1;
   }
   else
   {
      if (BuildFilespec($cotype, \$xmldata, \@core, \@netonly, \@sdponly, \@filespec))
      {
         print STDERR "Unable to build filespec.\n";
         $err = 1;
      }
      else
      {
         # Do a cvs checkout or export into the current directory
         if (DownloadTree($cotype, $dltype, $version, \@filespec))
         {
            print STDERR "Unable to $dltype CVS tree.\n";
            $err = 1;
         }
         else
         {
            if (!(-d kRootDir . kLocDir))
            {
               mkpath(kRootDir . kLocDir);
            }

            # save check-out type
            if (open(TYPEFILE, ">" . kRootDir . kLocDir . kTypeFile))
            {
               # Copy JUST the project directories requested. What is copied depends on what type of check-out is being
               # performed.
               if ($cotype == kCoNetDRMS)
               {
                  print TYPEFILE "net\n";
               } 
               elsif ($cotype == kCoSdp)
               {
                  print TYPEFILE "sdp\n";
               } 
               elsif ($cotype == kCoCustom)
               {
                  print TYPEFILE "custom\n";
               }
               else
               {
                  print STDERR "Unsupported checkout type $cotype.\n";
                  $err = 1;
               }

               close(TYPEFILE);
            }
            else
            {
               print STDERR "Unable to open file " . kRootDir . kLocDir . kTypeFile . " for writing.\n";
            }
         }
      }
   }
}

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
   my($xmldata) = $_[1];
   my($core) = $_[2];
   my($netonly) = $_[3];
   my($sdponly) = $_[4];
   my($fsout) = $_[5];

   my($rv);

   $rv = 0;
   if ($cotype == kCoNetDRMS)
   {
      push(@$fsout, @$core);
      push(@$fsout, @$netonly);
   }
   elsif ($cotype == kCoSdp)
   {
      push(@$fsout, @core);
      push(@$fsout, @sdponly);
   }
   elsif ($cotype == kCoCustom)
   {
      my($strproj) = kStrproj;
      my($strname) = kStrname;

      push(@$fsout, @core);

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
      if ($dltype eq "checkout")
      {
         $cvscmd = "$dltype -AP";
      }
      elsif ($dltype eq "export")
      {
         $cvscmd = "$dltype";
      }
      elsif ($dltype eq "update")
      {
         # If a new directory is added to the repository AND the new directory is added to the
         # file specifications above, then the -d flag to cvs update will cause the new directory
         # to be downloaded to the client.
         $cvscmd = "$dltype -APd";
      }
      else
      {
         print STDERR "Unsupported download type $dltype.\n";
         $rv = 1;
      }
   }
   else
   {
      $cvscmd = "checkout -AP";
   }

   if (length($version) > 0)
   {
      $rev = "-r $version";
   }
   elsif ($dltype eq "export")
   {
      # Only export requires a revision argument
      $rev = "-r HEAD";
   }
   else
   {
      $rev = "";
   }

   if (!$rv)
   {
      @relpaths = map({"JSOC/$_"} @{$fspec});
      $cmd = join(' ', "cvs", $cvscmd, $rev, @relpaths);

print "$cmd\n";
exit;

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
      $prefix = "$dir/";
   } 
   else
   {
      $prefix = "";
   }

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
