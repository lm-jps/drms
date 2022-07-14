#!/usr/bin/perl -w 

# In config.local, __DEFS__ section must precede __MAKE__ section 

use constant kSTATUSOK => 0;
use constant kSTATUSCONF => 1;

use constant kUNKSECTION => 0;
use constant kDEFSSECTION => 1;
use constant kMAKESECTION => 2;

use constant kCUSTMK => "custom.mk";

my($line);
my($section);
my($status);
my($tmp);
my($beginning);
my($locdir);
my($varname);
my($mach);
my($varvalu);
my(%platmaps) = ();
my(%machmaps) = ();

$status = kSTATUSOK;
$section = kUNKSECTION;
$tmp = $ARGV[0];
$locdir = $ARGV[1];

if (open(CONFLOC, "<$tmp"))
{
   $tmp = kCUSTMK;
   if (!open(CUSTMK, ">>$locdir/$tmp"))
   {
      print STDERR "Can't open file '$tmp' for writing.\n"
   }
   else
   {
      $beginning = 1;

      while (defined($line = <CONFLOC>))
      {
         chomp($line);

         if ($line =~ /^\#/ || $line =~ /^\s*$/)
         {
            next;
         }

         if ($line =~ /__DEFS__/)
         {
            $section = kDEFSSECTION;
            next;
         }
         elsif ($line =~ /__MAKE__/)
         {
            $section = kMAKESECTION;
            next;
         }

         if ($section == kDEFSSECTION || $section == kMAKESECTION)
         {
            # Put all config.local key-value pairs in custom.mk too. Some of these will be 
            # used by the make system. There is an unaddressed problem: The config.local
            # for NetDRMS has key names that do not match the key names for an SDP
            # checkout. The SDP names match the names of the defines used in the source code,
            # but NetDRMS do not match anything. gen_init.csh maps these names to the 
            # names used by the source code.
            #
            # Currently, the only key-value pair needed by the make system is SUMS_TAPE_AVAILABLE, 
            # which in fact IS named identically in all three places (SDP config.local, NetDRMS
            # config.local, and in the source code).
            $mach = "";

            if ($line =~ /\s*(\S+)\s+(\S+)/)
            {
               $varname = $1;
               $varvalu = $2;

               if ($varname =~ /(\S+):(\S+)/)
               {
                  $varname = $1;
                  $mach = $2;
               }

               if (length($mach) == 0)
               {
                  print CUSTMK "$varname = $varvalu\n";
               }
            }
            elsif ($line =~ /\s*(\S+)\s*/)
            {
               $varname = $1;

               if ($varname =~ /(\S+):(\S+)/)
               {
                  $varname = $1;
                  $mach = $2;
               }

               if (length($mach) == 0)
               {
                  print CUSTMK "$varname =  \n";
               }
            }
            else
            {
               print STDERR "Invalid line '$line' in configuration file.\n";
               $status = kSTATUSCONF;
               last;
            }

            # deal with mach-specific
             if (length($mach) > 0)
             {
                if (SupportedPlat($mach))
                {
                   $platmaps{$mach}{$varname} = $varvalu;
                }
                else
                {
                   $machmaps{$mach}{$varname} = $varvalu;
                }
             }
         }
         else
         {
            print STDERR "Invalid configuration file format.\n";
            $status = kSTATUSCONF;
         }
      }

      # Write out machine-specific key-value pairs
      my(%map);

      foreach $mach (keys(%platmaps))
       {
           %map = %{$platmaps{$mach}};
           
           # There are two special machine 'types' - X86_64 and IA32; they refer to 
           # the two supported linux-CPU types, linux_x86_64 and linux_ia32 as
           # identified by $JSOC_MACHINE
           if ($mach =~ /x86_64/i)
           {
               print CUSTMK 'ifeq ($(JSOC_MACHINE), linux_x86_64)' . "\n";
           }
           elsif ($mach =~ /ia32/i)
           {
               print CUSTMK 'ifeq ($(JSOC_MACHINE), linux_ia32)' . "\n";
           }
           elsif ($mach =~ /ia64/i)
           {
               print CUSTMK 'ifeq ($(JSOC_MACHINE), linux_ia64)' . "\n";
           }
           elsif ($mach =~ /avx/i)
           {
               print CUSTMK 'ifeq ($(JSOC_MACHINE), linux_avx)' . "\n";
           }
           
           foreach $varname (keys(%map))
           {
               $varvalu = $map{$varname};
               print CUSTMK "$varname = $varvalu\n";
           }
           
           print CUSTMK "endif\n";
       }

      foreach $mach (keys(%machmaps))
      {
         %map = %{$machmaps{$mach}};

         # The MACH make variable is passed in as part of the make command-line:
         # make MACH='N02'
         # This will override the supported platform types.
         print CUSTMK "ifeq (\$(MACH), $mach)\n";

         foreach $varname (keys(%map))
         {
            $varvalu = $map{$varname};
            print CUSTMK "$varname = $varvalu\n";
         }

         print CUSTMK "endif\n";
      }


      close(CUSTMK);
   }

   close(CONFLOC);
}

exit($status);

sub SupportedPlat
{
   my($plat) = $_[0];

   if ($plat =~ /^x86_64$/i || $plat =~ /^ia32$/i || $plat =~ /^ia64$/i || $plat =~ /^avx$/i)
   {
      return 1;
   }
   else
   {
      return 0;
   }
}
