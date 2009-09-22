#!/usr/bin/perl -w 

use constant kSTATUSOK => 0;
use constant kSTATUSCONF => 1;

use constant kUNKSECTION => 0;
use constant kDEFSSECTION => 1;
use constant kMAKESECTION => 2;

use constant kCONFIGFILE => "config.local";
use constant kHEADER => "include/customizeddefs.h";

my($line);
my($section);
my($status);
my(@defs);
my(%defines);
my($adef);
my($defname);
my($val);
my($tmp);

$status = kSTATUSOK;

# create hash table to hold all customizable define names
@defs = qw(SERVER DRMS_LOCAL_SITE_CODE USER PASSWD DBNAME POSTGRES_ADMIN SUMS_MANAGER SUMS_MANAGER_UID SUMS_GROUP SUMLOG_BASEDIR SUMBIN_BASEDIR SUMSERVER SUMS_TAPE_AVAILABLE LOC_SUMEXP_METHFMT LOC_SUMEXP_USERFMT LOC_SUMEXP_HOSTFMT LOC_SUMEXP_PORTFMT);
foreach $adef (@defs)
{
   $defines{$adef} = 1;
}

$section = kUNKSECTION;
$tmp = kCONFIGFILE;
if (open(CONFLOC, "<$tmp"))
{
   $tmp = kHEADER;
   if (!open(HEADER, ">$tmp"))
   {
      print STDERR "Can't open file '$tmp' for writing.\n"
   }
   else
   {
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

         if ($section == kDEFSSECTION)
         {
            $val = "";

            if ($line =~ /^\s*(\S+)\s+(\S+.*)/)
            {
               $defname = $1;
               $val = $2;
            }
            elsif ($line =~ /^\s*(\S+)\s*/)
            {
               $defname = $1;
            }
            else
            {
               print STDERR "Invalid line '$line' in configuration file.\n";
               $status = kSTATUSCONF;
               last;
            }

            if (defined($defines{$defname}))
            {
               if (length($val) > 0)
               {
                  print HEADER "#define $defname $val\n";
               }
               else
               {
                  print HEADER "#define $defname\n";
               }
            }
            else
            {
               print STDERR "Invalid parameter '$defname'.\n";
               $status = kSTATUSCONF;
               last;
            }
         }
         elsif ($section == kMAKESECTION)
         {
            # Done - use customizemake.pl to create the custom.mk file from config.local
            last;
         }
         else
         {
            print STDERR "Invalid configuration file format.\n";
            $status = kSTATUSCONF;
         }
      }

      close(HEADER);
   }

   close(CONFLOC);
}

exit($status);
