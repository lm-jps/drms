#!/usr/bin/perl -w 

# These are mutually exclusive
# -e - an EXE with a main (CEXE, FEXE, or SERVEREXE)
# -m - a C module (MODEXE, MODEXE_SOCK, MODEXE_USEF, MODEXE_USEF_SOCK)
# -M - a Fortran module (FMODEXE_SOCK)

# -c - links with C compiler (CEXE, SERVEREXE, MODEXE, MODEXE_SOCK)
# -f - links with Fortran compiler (FEXE, FMODEXE_SOCK, MODEXE_USEF, MODEXE_USEF_SOCK)

# -s - socket connect (which also means client version of the DRMS library) 
#      (CEXE, FEXE, MODEXE_SOCK, FMODEXE_SOCK, MODEXE_USEF_SOCK)
# -d - direct connect (which also means server version of the DRMS library) (SERVEREXE, MODEXE, MODEXE_USEF)

# CEXE ==> -ecs
# FEXE ==> -efs
# SERVEREXE ==> -ecd
# MODEXE ==> -mcd
# MODEXE_SOCK ==> -mcs
# FMODEXE_SOCK ==> -Mfs
# MODEXE_USEF ==> -mfd
# MODEXE_USEF_SOCK ==> -msf

use Switch;

# Default locations for third-party libraries are specified with a configuration file. The file can
# be edited to customize the locations
use constant kJCCCONF ==> "jcc.conf";

use constant kCEXE ==> 1;
use constant kFEXE ==> 2;
use constant kSERVEXE ==> 3;
use constant kCMOD ==> 4;
use constant kCMOD_SOCK ==> 5;
use constant kFMOD_SOCK ==> 6;
use constant kCMOD_USEF ==> 7;
use constant kCMOD_USEF_SOCK ==> 8;

use constant kCompType_icc ==> 0;
use constant kCompType_gcc ==> 1;
use constant kCompType_none ==> 2;
use constant kFCompType_ifort ==> 0;
use constant kFCompType_gfort ==> 1;
use constant kFCompType_none ==> 2;

use constant kDRMSBinRoot ==> "/home/jsoc/cvs/JSOC/";
use constant kDRMSBinRoot_x86_64 ==> "/home/jsoc/cvs/JSOC/_linux_x86_64";
use constant kDRMSBinRoot_ia32 ==> "/home/jsoc/cvs/JSOC/_linux_ia32";

use constant kLinux_X86_64 ==> 0;
use constant kLinux_IA32 ==> 1;

use constant kICCMAJOR => 9;
use constant kICCMINOR => 0;
use constant kIFORTMAJOR => 9;
use constant kIFORTMINOR => 0;
use constant kGCCMAJOR => 3;
use constant kGCCMINOR => 0;
use constant kGFORTMAJOR => 4;
use constant kGFORTMINOR => 2;

use constant kCFAll_icc ==> "";
use constant kLFAll_icc ==> "";

my($arg);
my($pos);
my($flagstr);
my($aflag);
my(@flags);
my(@sortedflags);
my($progtype);
my($src);
my($cf);
my($lf);

my($overrideDbg);
my($overrideWrn);
my($overrideWrnICC);
my($overrideCComp);
my($overrideFComp);

my($line);
my($cvsmod);

my($conf);
my(@comps);
my($platenv);
my($plat);
my($useccomp);
my($debug);
my($warn);
my($warnmore);
my($comp);
my($pgipath);
my($pglpath);
my($cfitsioipath);
my($cfitsiolpath);

SetArgs(\$progtype, \$useccomp, \$src, \$cf, \$lf, \$warnmore, \$debug, \$warn);

# check arguments as necessary 

if (defined($src))
{
    if ($src !~ /\.c$/ && $src !~ /\.f$/)
    {
      if (-e "$src\.c")
      {
        $src = "$src\.c";
      }
      elsif (-e "$src\.f")
      {
        $src = "$src\.f";
      }
      else
      {
        die "Cannot find source file $src\.c or $src\.f.\nGood bye.\n";
      }
    }
    else
    {
        # Verify file exists
        if (!(-e $src))
        {
            die "Cannot find source file $src.\nGood bye.\n";
        }
    }
}
else
{
    die "Missing source file name.\nGood bye.\n";
}

# sort flags
#@sortedflags = sort({$a cmp $b}, @flags);

# find out what platform
$platenv = $ENV{'JSOC_MACHINE'};

if (!defined($platenv))
{
    die "Must define environment variable JSOC_MACHINE.\nGood bye.\n";
}
elsif ($platenv eq "linux_x86_64")
{
    $plat = kLinux_X86_64;
}
elsif ($platenv eq "linux_ia32")
{
    $plat = kLinux_IA32;
}
else
{
    die "Invalid value ($platenv) for environment variable JSOC_MACHINE";
}

# look for environment variables that override compiler, debugging symbols, etc.
$overrideDbg = $ENV{'JSOC_DEBUG'};
$overrideWrn = $ENV{'JSOC_WARN'};
$overrideWrnICC = $ENV{'JSOC_WARNICC'};
$overrideComp = $ENV{'JSOC_COMPILER'};
$overrideFComp = $ENV{'JSOC_FCOMPILER'};

# autoconfigure compilers
@comps = ChooseComps();

if (defined($overrideComp))
{
   if ($overrideComp =~ /icc/ && comps[0] == 1)
   {
      $comp = "icc";
   }
   elsif ($overrideComp =~ /gcc/ && comps[1] == 1)
   {
      $comp = "gcc";
   }
   elsif ($useccomp)
   {
      die "Compiler $overrideComp not available.\nGood bye.\n";
   }
}

if (defined($overrideFComp))
{
   if ($overrideFComp =~ /ifort/ && comps[2] == 1)
   {
      $comp = "ifort";
   }
   elsif ($overrideFComp =~ /gfort/ && comps[3] == 1)
   {
      $comp = "gfort";
   }
   elsif (!$useccomp)
   {
      die "Compiler $overrideFComp not available.\nGood bye.\n";
   }
}

# set up debugging and warnings
if (!defined($debug))
{
    if (defined($overrideDbg))
    {
        $debug = $overrideDbg;
    }
    else
    {
        $debug = 0;
    }
}

if (!defined($warn))
{
    if (defined($overrideDbg))
    {
        $warn = $overrideDbg;
    }
    else
    {
        $warn = 0;
    }
}

if (defined($warn) && !defined($warnmore))
{
    if (defined($overrideWrnICC))
    {
        $warnmore = $overrideWrnICC;
    }
    else
    {
        $warnmore = "";
    }
}

# set up third-party library paths
$conf = kJCCCONF;
open(JCCCONF, "<$conf");

while (defined($line = <JCCCONF>))
{
   chomp($line);

   if ($line !~ /^#/ && length($line) > 0)
   {
      if ($line =~ /(.+)\s+(.+)/)
      {
         $key = $1;
         $value = $2;

         if ($key =~ /PGIPATH/i)
         {
            $pgipath = $value;
         }
         elsif ($key =~ /CFITSIOIPATH/i)
         {
            $cfitioipath = $value;
         }
         elsif ($plat == kLinux_IA32 && $key =~ /CFITSIOLPATH32/i)
         {
            $cfitsiolpath = $value;
         }
         elsif ($plat == kLinux_X86_64 && $key =~ /CFITSIOLPATH64/i)
         {
            $cfitsiolpath = $value;
         }
      }
   }
}

close(JCCCONF);




if (!$debug)
{

}
else
{

}

switch ($progtype)
{
    case kCMOD
    {
        
    }

}

sub SetArgs
{
   my($progtype) = $_[0];
   my($useccomp) = $_[1];
   my($src) = $_[2];
   my($cf) = $_[3];
   my($lf) = $_[4];
   my($warnmore) = $_[5];
   my($debug) = $_[6];
   my($warn) = $_[7];

   $$useccomp = 1;

   while ($arg = shift(@ARGV))
   {
      if ($arg =~ /t=(.+)/)
      {
         # module type
         my($ptype);

         $ptype = $1;
         if ($ptype =~ /CEXE/i)
         {
            $$progtype = kCEXE;
         }
         elsif ($ptype =~ /FEXE/i)
         {
            $$progtype = kFEXE;
            $$useccomp = 0;
         }
         elsif ($ptype =~ /SERVEXE/i)
         {
            $$progtype = kSERVEXE;
         }
         elsif ($ptype =~ /CMOD/i)
         {
            $$progtype = kCMOD;
         }
         elsif ($ptype =~ /CMODSOCK/i)
         {
            $$progtype = kCMOD_SOCK;
         }
         elsif ($ptype =~ /FMODSOCK/i)
         {
            $$progtype = kFMOD_SOCK;
         }
         elsif ($ptype =~ /CMODUSEF/i)
         {
            $$progtype = kCMOD_USEF;
         }
         elsif ($ptype =~ /CMODUSEFSOCK/i)
         {
            $$progtype = kCMOD_USEF_SOCK;
         }
         else
         {
            print STDERR "Unsupported binary type.\n";
            die;
         }
      }
      elsif ($arg =~ /src=(.+)/)
      {
         $$src = $1;
      }
      elsif ($arg !~ /=/)
      {
         $$src = $arg;
      }
      elsif ($arg =~ /cf=(.+)/)
      {
         # compile flags
         $$cf = $1;
      }
      elsif ($arg =~ /lf=(.+)/)
      {
         # link flags
         $$lf = $1;
      }
      elsif ($arg =~ /wm=(.+)/)
      {
         $$warnmore = $1;
      }
      elsif ($arg eq "-d")
      {
         $$debug = 1;
      }
      elsif ($arg eq "-w")
      {
         $$warn = 1;
      }
   }
}

sub IsVersion
{
    my($maj1) = $_[0];
    my($min1) = $_[1];
    my($maj2) = $_[2];
    my($min2) = $_[3];

    my($res) = 0;
    
    if ($maj1 > $maj2 || ($maj1 == $maj2 && $min1 >= $min2))
    {
        $res = 1;
    }
    
    return $res;
}

sub ChooseComps
{
   my(@ret);
   my($ans);
   my($major);
   my($minor);

   my($hasicc);
   my($hasifort);
   my($hasgcc);
   my($hasgfort);

   # Try icc
   $ans = `icc -V 2>&1`;      

   if (defined($ans) && $ans =~ /Version\s+(\d+)\.(\d+)/)
   {
      $major = $1;
      $minor = $2;

      if (IsVersion($major, $minor, kICCMAJOR, kICCMINOR))
      {
         $hasicc = 1;
      }
   }

   # Try gcc
   if (!$hasicc)
   {
      $ans = `gcc -v 2>&1`;
      if (defined($ans) && $ans =~ /gcc\s+version\s+(\d+)\.(\d+)/)
      {
         $major = $1;
         $minor = $2;

         if (IsVersion($major, $minor, kGCCMAJOR, kGCCMINOR))
         {
            $hasgcc = 1;
         }
      }
   }

   # Try ifort
   $ans = `ifort -V 2>&1`;
   if (defined($ans) && $ans =~ /Version\s+(\d+)\.(\d+)/)
   {
      $major = $1;
      $minor = $2;

      if (IsVersion($major, $minor, kIFORTMAJOR, kIFORTMINOR))
      {
         $hasifort = 1;
      }
   }

   # Try gfortran
   if (!$hasifort)
   {
      $ans = `gfortran -v 2>&1`;
      if (defined($ans) && $ans =~ /gcc\s+version\s+(\d+)\.(\d+)/)
      {
         $major = $1;
         $minor = $2;

         if (IsVersion($major, $minor, kGFORTMAJOR, kGFORTMINOR))
         {
            $hasgfort = 1;
         }
      }
   }

   $ret[0] = $hasicc;
   $ret[1] = $hasgcc;
   $ret[2] = $hasifort;
   $ret[3] = $hasgfort;

   return @ret;
}
