#!/usr/bin/perl -w

# Create the CVS tags that identify two sets of files: one set contains the full JSOC release head versions, 
# the other set contains the NetDRMS release head versions. Excludes a list of 

# createtags.pl [-tv] vers=<vers> drmsvers=<drmsvers>
# args
#  vers - The JSOC release version number (<major>.<minor>)
#  drmsvers - The NetDRMS release version number (<major>.<minor>)
#  -t - test; print cvs tag commands, but don't execute them
#  -v - verbose; print diagnostic information

use Cwd;
use File::Path;
use constant kRelTypeJSOC => 0;
use constant kRelTypeDRMS => 1;
use constant kRelTypeALL => 2;
use constant kBatchSize => 128;
use constant kJSOCRoot => "/tmp/$ENV{'USER'}/JSOCTags";
use constant kDRMSRoot => "/tmp/$ENV{'USER'}/DRMSTags";
use constant kCORERoot => "/tmp/$ENV{'USER'}/CoreTags";
use constant kAllFiles => "find . -name '*CVS*' -prune -o -type f -print";

my($arg);
my($maj);
my($min);
my($drmsmaj);
my($drmsmin);
my($reltype);
my($printonly);
my($showtagcmd);
my($verbose);
my($dir);
my($cmd);
my($line);
my($verstr);
my($file);
my($resp);
my(@filelist);
my($fileliststr);
my($err);

my(@exclspec);
my(%exclusions);

# list of files to exclude (if a path is given, then all files and subdirectories of files are excluded)
@exclspec = 
(
 "base/sums/scripts/dpck.pl",
 "base/sums/scripts/fixportm.pl",
 "base/sums/scripts/GRAD_BLUE_LINE.gif",
 "base/sums/scripts/SDO_Badge.gif",
 "base/sums/scripts/SDO_HSB_CCSDS_Data_Structures.gif",
 "base/sums/scripts/build_parc_file.pl",
 "base/sums/scripts/find_dir_sum_partn_alloc_dc",
 "base/sums/scripts/rsync_scr111.pl",
 "base/sums/scripts/ssh_rsync.source",
 "base/sums/scripts/sum_bad_permissions.pl",
 "base/sums/scripts/sum_start_d00_jim",
 "base/sums/scripts/sum_start_d02",
 "base/sums/scripts/sum_start_d02_auto",
 "base/sums/scripts/sum_start_dc",
 "base/sums/scripts/sum_start_n02_jim",
 "base/sums/scripts/sum_start_n02_jim_auto",
 "base/sums/scripts/sum_stop_d00_jim",
 "base/sums/scripts/sum_stop_d02",
 "base/sums/scripts/sum_stop_d02_auto",
 "base/sums/scripts/sum_stop_dc",
 "base/sums/scripts/sum_stop_n02_jim",
 "base/sums/scripts/sum_stop_n02_jim_auto",
 "base/sums/scripts/sum_tape_insert.pl", 
 "base/sums/scripts/sum_tape_insert_t50.pl",
 "base/sums/scripts/sum_tape_insert_t950.pl",
 "base/sums/scripts/sumck sumck_n02_jim",
 "base/sums/scripts/t120_reachive.pl",
 "base/sums/scripts/t120stageall.pl",
 "base/sums/scripts/t120view",
 "base/sums/scripts/t50view",
 "base/sums/scripts/t950view",
 "base/sums/scripts/tape_verify.pl",
 "base/sums/scripts/tapearc_do",
 "base/sums/scripts/tapearc_do_dcs1",
 "base/sums/scripts/tapeid.list",
 "base/sums/scripts/tapeid_t50.list",
 "base/sums/scripts/postgres/create_user.txt",
 "base/util/scripts/dsview",
 "base/util/scripts/tkdiff.tcl",
 "base/sums/scripts/test",
 "base/drms/libs/api/test"
);

while ($arg = shift(@ARGV))
{
  if ($arg =~ /drmsvers=(.+)\.(.+)/)
  {
    $drmsmaj = $1;
    $drmsmin = $2;
  }
  elsif ($arg =~ /vers=(.+)\.(.+)/)
  {
    $maj = $1;
    $min = $2;
  }
  elsif ($arg eq "-p")
  {
    # print files that would otherwise be tagged
    $printonly = 1;
  }
  elsif ($arg eq "-t")
  {
    # print tag commands, but don't execute the commands
    $showtagcmd = 1;
  }
  elsif ($arg eq "-v")
  {
    $verbose = 1;
  }
}

$err = 0;

# obtain a list of ALL files
$cmd = kAllFiles;

# first do JSOC tags
print STDOUT "***Tagging full JSOC release files***\n";

# cd to JSOC root
$dir = kJSOCRoot;
if (!(-d $dir))
{
  mkpath($dir);
}

chdir($dir);
`cvs co JSOC > /dev/null 2>&1`;
chdir("$dir/JSOC");

$reltype = kRelTypeJSOC;
$verstr = "Ver_${maj}-${min}";
@filelist = GenerateFileList($cmd, $reltype, \%exclusions, \$err);

if ($err == 0)
{
  if ($printonly)
  {
    while (defined($file = shift(@filelist)))
    {
      print STDOUT "$file\n";
    }
  }
  else
  {
    # call cvs tag in batches
    my($count);
    my($nitems) = scalar(@filelist);
  
    while ($nitems > 0 && $err == 0)
    {
      $fileliststr = "";
      $count = 0;
      while (defined($file = shift(@filelist)) && $count < kBatchSize)
      {
        $fileliststr = "$fileliststr $file";
        $count++;
      }
   
      # these tag commands affect the repository immediately (no commit is necessary)
      $err = SetTag($verstr, $fileliststr, 1, $showtagcmd);
  
      if ($err) 
      {
        print STDERR "Failure creating tag $verstr on file $fileliststr.\n";
        last;
      }

      $err = SetTag("Ver_LATEST", $fileliststr, 0, $showtagcmd);
            
      if ($err)
      {
        print STDERR "Failure deleting tag Ver_LATEST on file $fileliststr.\n";
        last;
      }

      $err = SetTag("Ver_LATEST", $fileliststr, 1, $showtagcmd);

      if ($err)
      {
        print STDERR "Failure creating tag Ver_LATEST on file $fileliststr.\n";
        last;
      }

      $nitems = scalar(@filelist);
    }
  }
  
  # remove checked-out files
  chdir($dir);
  rmtree("$dir/JSOC");
}

if ($err == 0)
{
  # then do NetDRMS tags
  print STDOUT "***Tagging NetDRMS release files***\n";

  # cd to DRMS root
  $dir = kDRMSRoot;
  if (!(-d $dir))
  {
    mkpath($dir);
  }

  chdir($dir);
  `cvs co NETDRMSONLY > /dev/null 2>&1`;
  chdir("$dir/JSOC");

  $reltype = kRelTypeDRMS;
  $verstr = "NetDRMS_Ver_${drmsmaj}-${drmsmin}";
  @filelist = GenerateFileList($cmd, $reltype, \%exclusions, \$err);

  if ($err == 0)
  {
    if ($printonly)
    {
      while (defined($file = shift(@filelist)))
      {
        print STDOUT "$file\n";
      }
    }
    else
    {
      # call cvs tag in batches
      my($count);
      my($nitems) = scalar(@filelist);
      
      while ($nitems > 0 && $err == 0)
      {
        $fileliststr = "";
        $count = 0;
        while (defined($file = shift(@filelist)) && $count < kBatchSize)
        {
          $fileliststr = "$fileliststr $file";
          $count++;
        }
   
        # these tag commands affect the repository immediately (no commit is necessary)
        $err = SetTag($verstr, $fileliststr, 1, $showtagcmd);
  
        if ($err) 
        {
          print STDERR "Failure creating tag $verstr on file $fileliststr.\n";
          last;
        }

        $err = SetTag("NetDRMS_Ver_LATEST", $fileliststr, 0, $showtagcmd);
            
        if ($err)
        {
          print STDERR "Failure deleting tag NetDRMS_Ver_LATEST on file $fileliststr.\n";
          last;
        }
        
        $err = SetTag("NetDRMS_Ver_LATEST", $fileliststr, 1, $showtagcmd);

        if ($err)
        {
          print STDERR "Failure creating tag NetDRMS_Ver_LATEST on file $fileliststr.\n";
          last;
        }

        $nitems = scalar(@filelist);
      }
    }

    # remove checked-out files
    chdir($dir);
    rmtree("$dir/JSOC");
  }
}

if ($err == 0)
{
  # then do NetDRMS tags on the CORE files

  # cd to CORE root
  $dir = kCORERoot;

  if (!(-d $dir))
  {
    mkpath($dir);
  }

  chdir($dir);
  `cvs co CORE > /dev/null 2>&1`;
  chdir("$dir/JSOC");

  # Exclusions will apply to the CORE files of the NetDRMS release only 
  SetExclusions(\@exclspec, \%exclusions);

  if ($verbose)
  {
    while (my($k, $v) = each %exclusions)
    {
      print STDOUT "key $k, value $v\n";
    }
  }

  $reltype = kRelTypeDRMS;
  $verstr = "NetDRMS_Ver_${drmsmaj}-${drmsmin}";
  @filelist = GenerateFileList($cmd, $reltype, \%exclusions, \$err);

  if ($err == 0)
  {
    if ($printonly)
    {
      while (defined($file = shift(@filelist)))
      {
        print STDOUT "$file\n";
      }
    }
    else
    {
      # call cvs tag in batches
      my($count);
      my($nitems) = scalar(@filelist);
      
      while ($nitems > 0 && $err == 0)
      {
        $fileliststr = "";
        $count = 0;
        while (defined($file = shift(@filelist)) && $count < kBatchSize)
        {
          $fileliststr = "$fileliststr $file";
          $count++;
        }
   
        # these tag commands affect the repository immediately (no commit is necessary)
        $err = SetTag($verstr, $fileliststr, 1, $showtagcmd);
  
        if ($err) 
        {
          print STDERR "Failure creating tag $verstr on file $fileliststr.\n";
          last;
        }

        $err = SetTag("NetDRMS_Ver_LATEST", $fileliststr, 0, $showtagcmd);
            
        if ($err)
        {
          print STDERR "Failure deleting tag NetDRMS_Ver_LATEST on file $fileliststr.\n";
          last;
        }
        
        $err = SetTag("NetDRMS_Ver_LATEST", $fileliststr, 1, $showtagcmd);

        if ($err)
        {
          print STDERR "Failure creating tag NetDRMS_Ver_LATEST on file $fileliststr.\n";
          last;
        }

        $nitems = scalar(@filelist);
      }
    }

    # remove checked-out files
    chdir($dir);
    rmtree("$dir/JSOC");
  }
}

exit;

sub SetExclusions
{
  my($exclusions) = $_[0];
  my($exclout) = $_[1]; # reference to exclusion hash array

  my($file);
  my($cmd) = kAllFiles;
  my($cwd);
  my($line);
  my($base);

  while (defined($file = shift(@$exclusions)))
  {
    # if directory, find all files contained within
    if (-d $file)
    {
      $base = $file;
      $cwd = getcwd();
      chdir($file);

      if (open(ALLFILESSUB, "$cmd |"))
      {
        while (defined($line = <ALLFILESSUB>))
        {
          chomp($line);
          $file = $line;

          if ($line =~ /^\.\/(.+)/)
          {
            $file = $1;
          }

          $$exclout{"$base/$file"} = "T";
        }

        close(ALLFILES);
      }
      else
      {
        print STDERR "Couldn't obtain list of all files.\n";
        $err = 1;
      }

      chdir($cwd);
    }
    else
    {
      $$exclout{"$file"} = "T";
    }
  }
}

sub ExcludeFile
{
  my($file) = $_[0];
  my($exclusions) = $_[1]; # reference to exclusion hash array
  my($exclude);

  $exclude = 0;
  
  if (defined($$exclusions{"$file"}))
  {
    $exclude = 1;
  }
  
  return $exclude;
}

sub GenerateFileList
{
  my($cmd) = $_[0];
  my($reltype) = $_[1];
  my($exclusions) = $_[2]; # reference
  my($err) = $_[3]; # reference

  my(@filelist);
  my($line);

  if (open(ALLFILES, "$cmd |"))
  {
    while (defined($line = <ALLFILES>)) 
    {
      chomp($line);

      if ($line =~ /^\.\/(.+)/)
      {
        $line = $1;
      }

      if ($reltype == kRelTypeJSOC || !ExcludeFile($line, $exclusions)) 
      {
        $file = $line;
        
        if ($line =~ /^\.\/(.+)/)
        {
          $file = $1;
        }
        
        push(@filelist, $file);
      }
    }

    close(ALLFILES);
  }
  else
  {
    print STDERR "Couldn't obtain list of all files.\n";
    $$err = 1;
  }

  return @filelist;
}

sub SetTag
{
  my($tag) = $_[0];
  my($files) = $_[1];
  my($set) = $_[2];
  my($showtagcmd) = $_[3];

  my($err);
  my($cmd);
  my($resp);
  my($code);

  if ($set == 1)
  {
    $cmd = "cvs tag -c $tag $files";
    $code = "T";
  }
  else
  {
    $cmd = "cvs tag -d $tag $files";
    $code = "D";
  }

  $err = 0;

  if ($showtagcmd)
  {
    print STDOUT "$cmd\n";
  }
  else
  {
    if (open(TAGRESP, "$cmd |"))
    {
      while (defined($resp = <TAGRESP>))
      {
        chomp($resp);
        
        if ($resp !~ /^${code}\s/) 
        {
          # failure
          $err = 1;
        }
      }

      close(TAGRESP);
    }
    else
    {
      print STDERR "Can't tag the file set $files.\n";
      $err = 1;
    }
  }

  return $err;
}
