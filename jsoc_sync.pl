#!/usr/bin/perl -w 

# script for synchronizing your CVS working directory with the CVS JSOC module (new tree)

# must run from root of JSOC tree (not necessarily from $JSOCROOT)

# run this on each machine to be used.
#    n02 - for linux_X86_64 machines
#    n00 - for linux4 machines such as n00, phil, etc.
# 
#    n12 formerly used for AMD x86-64 can also be used instead of n02
#    lws Itaniam processors no longer supported for JSOC

# This just in:
#   The cvs update "-d" flag will cause files outside of the CVS module
#   originally checked out to be downloaded.  This is bad, because
#   if a user checks out DRMS, then does a 'cvs update -d .', the
#   user then has the JSOC-module files in their working directory.
#   BUT, as long as the "-d" flag is not used, cvs update respects
#   boundaries of the module originally checked out: so 'cvs checkout DRMS'
#   followed by 'cvs update DRMS' will not result in any new files being 
#   downloaded that are outside of the DRMS module.  But new files
#   within the DRMS module WILL be downloaded.
#
#   So, this script should use 'cvs update' not 'cvs checkout'.  checkout
#   doesn't know how to remove files from the working directory that
#   have been deleted from the repository.  Since this script uses
#   'cvs update', there is no need to look at the 'modulespec.txt' file.
#
#   This just in.  If a cvs user adds a new directory to the repository, 
#   then the only way to get that new directory is to use the "-d" flag.
#   But we can't use the "-d" flag because this causes files outside of the
#   CVS module to be downloaded.  To work around the preposterous lameness of CVS
#   first call cvs update (no "-d" flag), then call cvs checkout.  The first call
#   will add/remove/update all files that are already in the user's 
#   working directory.  The second call will add files WITHIN THE CORRECT CVS 
#   MODULE that the user doesn't have.

$LATESTREL = "Ver_LATEST";
$CVSLOG = "cvsupdate.log";

my($aidx) = 0;
my($arg);
my($pos);
my($rev) = "";
my($line);
my($cvsmod);

while ($arg = shift(@ARGV))
{
    if ($arg eq "-R")
    {
	$rev = "-r $LATESTREL";
    }
    elsif (($pos = index($arg, "-l", 0)) == 0)
    {
	$CVSLOG = substr($arg, 2);
    }

    $aidx++;
}

if (-e "suflag.txt")
{
    $cvsmod = "JSOC";
}
else
{
    $cvsmod = "DRMS";
}

# remove old log file
if (-e $CVSLOG)
{
    unlink $CVSLOG;
}

CallCVS($rev);

print "JSOC synchronization finished.\n";

sub CallCVS
{
    my($rev) = @_;
    my($updatecmd);
    my($checkoutcmd);
    my($date);

    $updatecmd = "cvs update -AP $rev \.";
    $checkoutcmd = "cvs checkout -AP $rev $cvsmod";

    #Things that didn't really work:
    #$updatecmd = "(cd ..; $updatecmd | sed 's/^/STDOUT:/') 2>&1 |";
    #$updatecmd = "(cd $parent; $updatecmd) |";

    $date = `"date"`;

    open(CVSLOG, ">>$CVSLOG");
    print CVSLOG "$date\n";
    print CVSLOG "Calling '$updatecmd'.\n";
    close(CVSLOG);
    $updatecmd = "($updatecmd) 1>>$CVSLOG 2>&1";
    print "Calling '$updatecmd'.\n";
    system($updatecmd);

    open(CVSLOG, ">>$CVSLOG");
    print CVSLOG "Calling '$checkoutcmd'.\n";
    close(CVSLOG);
    $checkoutcmd = "(cd ..; $checkoutcmd) 1>>$CVSLOG 2>&1";
    print "Calling '$checkoutcmd'.\n";
    system($checkoutcmd);
}
