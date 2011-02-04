#!/usr/bin/perl -w


# Format of the projdirs.cfg file:
#   __MAKE__
#   $(CEXESUMS):                    $(LIBSUMSAPI) $(LIBSUM) $(LIBDSTRUCT)
#   $(MODEXESUMS):                  $(LIBSUMSAPI) $(LIBSUM)
#
#   $(MODEXEDROBJ):                 CF_TGT := $(CF_TGT) -I$(SRCDIR)/proj/libs/dr
#   $(MODEXEDR) $(MODEXEDR_SOCK):   $(LIBDR)
#   __END__
#   __PROJ__
#   proj=mag 
#     subdir=pfss/apps
#     subdir=ambig/apps
#     subdir=ident/apps
#     COMPILER=icc
#   proj=limbfit
#      subdir=apps
#      JSOC_MACHINE=linux_x86_64
#   <?xml version='1.0'?>
#   <projects>
#        <proj>
#             <name>mag</name>
#             <subdirs>
#                  <subdir>pfss/apps</subdir>
#                  <subdir>ambig/apps</subdir>
#                  <subdir>ident/apps</subdir>
#             </subdirs>
#             <filters>
#                  <filter>
#                       <name>COMPILER</name>
#                       <value>icc</value>
#                  </filter>
#             </filters>
#        </proj>
#        <proj>
#             <name>limbfit</name>
#             <subdirs>
#                 <subdir>apps</subdir>
#             </subdirs>
#             <filters>
#                  <filter>
#                       <name>COMPILER</name>
#                       <value>icc</value>
#                  </filter>
#             </filters>
#        </proj>
#   </projects>
#   __END__

use XML::Simple;
use Data::Dumper;

use constant kMakeDiv => "__MAKE__";
use constant kProjDiv => "__PROJ__";
use constant kProjCfgDiv => "__PROJCFG__";
use constant kEndDiv => "__END__";
use constant kStUnk => 0;
use constant kStMake => 1;
use constant kStProj => 2;
use constant kStProjCfg => 3;
use constant kMakeFile => "make_basic.mk";
use constant kTargetFile => "target.mk";
use constant kRulesFile => "Rules.mk";
use constant kProjCfgFile => "configure";
use constant kMakeVarCOMPILER => "COMPILER";
use constant kMakeVarFCOMPILER => "FCOMPILER";
use constant kMakeVarJSOC_MACHINE => "JSOC_MACHINE";
use constant kStrproj => "proj";
use constant kStrsubdirs => "subdirs";
use constant kStrsubdir => "subdir";
use constant kStrname => "name";
use constant kStrvalue => "value";
use constant kStrfilters => "filters";
use constant kStrfilter => "filter";

my($err);
my($arg);
my($pos);
my($locdir); # localization dir
my($cfgfile); # file containing project directories needed
my($st); # current position in configuration file
my($proj);
my($subdir);
my($compiler);
my($plat);
my($key);
my($val);
my($mdiv);
my($pdiv);
my($pcdiv);
my($ediv);
my($xml);

$err = 0;

while ($arg = shift(@ARGV))
{
   if (($pos = index($arg, "-d", 0)) == 0)
   {
      $locdir = substr($arg, 2);
   }
   elsif (($pos = index($arg, "-c", 0)) == 0)
   {
      $cfgfile = substr($arg, 2);
   }
}

if (defined($cfgfile) && -e $cfgfile)
{
   if (open(CFGFILE, "<$cfgfile"))
   {
      my($makedone);
      my($projdone);
      my($projcfgdone);

      $st = kStUnk;
      $mdiv = kMakeDiv;
      $pdiv = kProjDiv;
      $pcdiv = kProjCfgDiv;
      $ediv = kEndDiv;

      $makedone = 0;
      $projdone = 0;
      $projcfgdone = 0;

      while (defined($line = <CFGFILE>))
      {
         chomp($line);

         if ($line =~ /^\#/ || $line =~ /^\s*$/)
         {
            # Skip blank lines or lines beginning with # (comment lines)
            next;
         }
         elsif ($line =~ /^$mdiv/)
         {
            $st = kStMake;
            if (!open(MKFILE, ">${locdir}/" . kMakeFile))
            {
               print STDERR "Unable to open " . kMakeFile . " for writing.\n";
               $err = 1;
               last;
            }

            next;
         }
         elsif ($line =~ /^$pdiv/)
         {
            $st = kStProj;
            if (!open(TARGETFILE, ">${locdir}/" . kTargetFile))
            {
               print STDERR "Unable to open " . kTargetFile . " for writing.\n";
               $err = 1;
               last;
            }

            print TARGETFILE "\$(PROJOBJDIR):\n\t+\@[ -d \$\@ ] || mkdir -p \$\@\n";

            if (!open(RULESFILE, ">${locdir}/" . kRulesFile))
            {
               print STDERR "Unable to open " . kRulesFile . " for writing.\n";
               $err = 1;
               last;
            }

            # initialize xml string variable
            $xml = "";
            next;
         }
         elsif ($line =~ /^$pcdiv/)
         {
            $st = kStProjCfg;
            if (!open(PROJCFGFILE, ">${locdir}/" . kProjCfgFile))
            {
               print STDERR "Unable to open " . kProjCfgFile . " for writing.\n";
               $err = 1;
               last;
            }

            next;
         }
         elsif ($line =~ /^$ediv/)
         {
            if ($st == kStMake)
            {
               $makedone = 1;
            }
            elsif ($st == kStProj)
            {
               $projdone = 1;
            }
            elsif ($st == kStProjCfg)
            {
               $projcfgdone = 1;
            }

            $st = kStUnk;

            if ($makedone && $projdone && $projcfgdone)
            {
               last;
            }

            next;
         }

         if ($st == kStMake)
         {
            # copy verbatim to make_basic.mk
            print MKFILE "$line\n";
         }
         elsif ($st == kStProj)
         {
            # suck out xml
            $xml = $xml . "$line\n";
         }
         elsif ($st == kStProjCfg)
         {
            # copy verbatim to configure
            print PROJCFGFILE "$line\n";
         }
      } # loop over cfg file

      close(CFGFILE);

      if (length($xml) > 0)
      {
         # Extract data from xml and write to target and rules files.
         my($xmlobj) = new XML::Simple;
         my($xmldata) = $xmlobj->XMLin($xml, ForceArray => 1);
         my($rulesstr);
         my($prefix);
         my($fileprefix);
         my($filesuffix);
         my($suffix);

         my($strproj) = kStrproj;
         my($strname) = kStrname;
         my($strsubdirs) = kStrsubdirs;
         my($strsubdir) = kStrsubdir;
         my($strfilters) = kStrfilters;
         my($strfilter) = kStrfilter;
         my($strvalue) = kStrvalue;

#print Dumper($xmldata);

         # If the config file has at least one project specification, then print the rules file prefix.
         if ($#{$xmldata->{$strproj}} >= 0)
         {
            my(@filedata) = <DATA>;
            my($st);
            
            $st = 0;
            foreach $dline (@filedata)
            {
               chomp($dline);

               if ($st == 0 && $dline =~ /__DATAPREFIX__/)
               {
                  $fileprefix = "";
                  $st = 1;
                  next;
               }
               elsif ($st == 1 && $dline =~ /__ENDER__/)
               {
                  $st = 2;
                  next;
               }
               elsif ($st == 2 && $dline =~ /__DATASUFFIX__/)
               {
                  $filesuffix = "";
                  $st = 3;
                  next;
               }
               elsif ($st == 3 && $dline =~ /__ENDER__/)
               {
                  $st = 4;
                  last;
               }

               if ($st == 1)
               {
                  $fileprefix = $fileprefix . "$dline\n";
               }
               elsif ($st == 3)
               {
                  $filesuffix = $filesuffix . "$dline\n";
               }
            }
         }

         if (defined($fileprefix))
         {
            print RULESFILE "${fileprefix}\n";
         }

         foreach $proj (@{$xmldata->{$strproj}})
         {
            $rulesstr = "dir     := \$(d)/$proj->{$strname}->[0]\n-include          \$(SRCDIR)/\$(dir)/Rules.mk\n";

            foreach $subdir (@{$proj->{$strsubdirs}->[0]->{$strsubdir}})
            {
               # I believe $subdir is now the actual subdirectory string.
               print TARGETFILE "\t+\@[ -d \$\@/$proj->{$strname}->[0]/$subdir ] || mkdir -p \$\@/$proj->{$strname}->[0]/$subdir\n";
            }

            # make doesn't support logical operations in ifeq conditionals (you can't do ifeq (A AND B)), 
            # so we need to write:
            #   ifeq (A)
            #      ifeq (B)
            #        <do something>
            #      endif
            #   endif

            $prefix = "";
            $suffix = "";

            foreach $filter (@{$proj->{$strfilters}->[0]->{$strfilter}})
            {
               $prefix = $prefix . "ifeq (\$($filter->{$strname}->[0]),$filter->{$strvalue}->[0])\n";
               $suffix = $suffix . "endif\n";
            }

            if (defined($prefix) && defined($suffix) && defined($rulesstr))
            {
               if (length($prefix) > 0)
               {
                  print RULESFILE $prefix;
               }

               print RULESFILE $rulesstr;

               if (length($suffix) > 0)
               {
                  print RULESFILE $suffix;
               }
            }
         } # loop over projects

         if (defined($filesuffix))
         {
            print RULESFILE "${filesuffix}\n";
         }
      }
      
      close(TARGETFILE);
      close(RULESFILE);
      close(PROJCFGFILE);
      close(MKFILE);

      if (chmod(0744, "${locdir}/" . kProjCfgFile) != 1)
      {
         print STDERR "Unable to set file permissions for ${locdir}/" . kProjCfgFile . ".\n";
         $err = 1;
      }
   }
   else
   {
      print STDERR "Unable to open configuration file $cfgfile.\n";
      $err = 1;
   }
}

exit($err);

__DATA__

__DATAPREFIX__
# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

__ENDER__

__DATASUFFIX__
dir	:= $(d)/example
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/cookbook
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/myproj
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
__ENDER__
