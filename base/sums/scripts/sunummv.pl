eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -d -S $0 "$@"'
    if 0;

#/home/production/cvs/JSOC/base/sums/scripts/sunummv.pl
#
#Takes an sunum (aka ds_index) and a /SUMx dir and moves the data on
#that sunum into the /SUMx dir and updates the DB tables sum_main and
#sum_partn_alloc.
#
use DBI;
use Term::ReadKey;

sub usage {
  print "Moves data in sunum to the given /SUMx dir and updates the jsoc_sums DB\n";
  print "Usage: sunummv.pl [-b] [-B#] [-ffile] [sunum /SUMx]\n";
  print "       -h = help. Print usage msg.\n";
  print "       -b = batch mode. Don't have to confirm to proceed.\n";
  print "       -B = block count. Number of sunum to do single cp, mv, commit, etc. on.\n";
  print "            Default block count is $MAX_BLOCKCNT\n";
  print "       -f = file name that contains the sunums and /SUMx.\n";
  print "       sunum = If no -f, the sunum to move\n";
  print "       /SUMx = If no -f, SUMS root dir, e.g. /SUM43\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date in form 199801071442
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2._.$hour2.":".$min2.":".$sec2;
  $effdate = $year4.$mon2.$mday2.$hour2.$min2;
  return($date);
}

$label = &labeldate;
$ACNT = $#ARGV;
for($i=0; $i <= $ACNT; $i++) {
  push(@aval, $ARGV[$i]);
}
$HOSTDB = "hmidb";      #host where DB runs
$SADMINUSER = "production"; #!!TBD ck w/Art for config info on SUMSADMIN user
$MAX_BLOCKCNT = 10;	#how many sunums to do before process and commit
#$DBIN = "jsoc_sums";	#target sums DB
#!!!TEMP use 'jim' DB
$DBIN = "jim";	#target sums DB
$PORTIN = 5432;
#$PORTIN = 5434;
$INFILE = 0;
$B = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-f(.*)/) {
    $INFILE = $1;
    if(!$INFILE) { &usage; }
  }
  if (/^-b(.*)/) {
    $B = 1;
  }
  if (/^-h(.*)/) {
    &usage;
  }
}
if(!$INFILE) {
  if($#ARGV != 1) { &usage; }
  else {
    $sunum = $ARGV[0];
    $SUMD = $ARGV[1];
  }
}
else {
  if($#ARGV != -1) { &usage; }
}
if(!$INFILE) {		#sunum and /SUMx given
  $pos = index($SUMD, '/');
  if($pos != 0) {
    $SUMD = sprintf("/%s", $SUMD);
  }
  $line = "$sunum $SUMD\n";
  push(@lines, $line);
}
else {			#get sunum and dirs from file
  @lines = `cat $INFILE`;
}

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
$host = $ENV{'HOST'};
$sserv = $ENV{'SUMSERVER'};
%HofContain = ();       #hash key is container dir fr a tape rd, 
			#e.g. D423352259. Has any cp command in effect
if($user ne "$SADMINUSER") {
  print "This changes the jsoc_sums DB. You must be user $SADMINUSER to run\n";
  exit;
}
if($host ne $sserv) {
  print "You must run this on the SUMSERVER ($sserv)\n";
  exit;
}

if(!$B) {
  print "This will mv data and update the jsoc_sums DB.\nOk to proceed ('yes' to continue):";
  $ans = ReadLine(0);
  chomp($ans);
  print "\n";
  if($ans ne "yes") {
    print "Bye\n";
    exit(1);
  }
}
$logfile = "/usr/local/logs/SUM/sunummv_".$label.".log";
print "$logfile\n";
open(LOG, ">$logfile") || die "Can't open $logfile: $!\n";
print "$0 ";
print LOG "$0 ";
while($x = shift(@aval)) {
  print "$x ";
  print LOG "$x ";
}
print "\n";
print LOG "\n";
print "$label\n";
print LOG "$label\n";

  #connect to the databases
  $dbh = DBI->connect("dbi:Pg:dbname=$DBIN;host=$hostdb;port=$PORTIN ", "$user", "", {AutoCommit=>0});
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  print "Connected to DB $DBIN w/AutoCommit off\n";
  print LOG "Connected to DB $DBIN w/AutoCommit off\n";
  $blockcnt = 0;
  $end = 0;
  #@lines_sv = @lines;

  while(!$end) {
    for($blockcnt=0; $blockcnt < $MAX_BLOCKCNT; $blockcnt++) {
      #print "\n$_";
      #print LOG "\n$_";
      if(!($_ = shift(@lines))) { $end = 1; last; }
      if(/^#/ || /^\n/) { #ignore any comment or blank lines
        $blockcnt--; #don't count this
        next;
      }
      chomp;
      ($sunum, $SUMD) = split(/\s+/);
      if($blockcnt == 0) {
        $sqlloc = "select online_loc from sum_main where ds_index in ($sunum";
      }
      else {
        $sqlloc = $sqlloc.",$sunum";
      }
    }
    if($blockcnt) {
      $sqlloc = $sqlloc.")";
      print "$sqlloc\n";
    }
    else { next; }
    $sth = $dbh->prepare($sqlloc);
    if ( !defined $sth ) {
       print "Cannot prepare statement: $DBI::errstr\n";
       print LOG "Cannot prepare statement: $DBI::errstr\n";
       $dbh->disconnect();
       close(LOG);
       exit; 
    }
    # Execute the statement at the database level
    $sth->execute;

      # Fetch the rows back from the SELECT statement
      $fetch = 0;
      while ( @row = $sth->fetchrow() ) {
        $onlineloc = shift(@row);
        push(@onlinelocs, $onlineloc);
        #print "onlineloc = $onlineloc\n";
        $fetch++;
      }
      if(!$fetch) {
        print "No online_loc found in DB for the last set of sunum\n";
        print LOG "No online_loc found in DB for the last set of sunum\n";
        $dbh->disconnect();
        close(LOG);
        exit; 
      }
    while($onlineloc = shift(@onlinelocs)) {
      if(!-e $onlineloc) {
        print "fetched onlineloc = $onlineloc does not exist\n";
        print LOG "fetched onlineloc = $onlineloc does not exist\n";
        next;
        #$dbh->disconnect();
	#close(LOG);
        #exit; 
      }
print "fetched onlineloc = $onlineloc\n";
      $pos = index($onlineloc, '/');		#find first /
      if($pos != 0) {
        print "DB content error: Invalid online_loc: $onlineloc\n";
        print LOG "DB content error: Invalid online_loc: $onlineloc\n";
        $dbh->disconnect();
	close(LOG);
        exit;
      }
      $pos1 = index($onlineloc, '/', $pos+1);	#find 2nd /
      if($pos1 == -1) {
        print "DB content error: Invalid online_loc: $onlineloc\n";
        print LOG "DB content error: Invalid online_loc: $onlineloc\n";
        $dbh->disconnect();
	close(LOG);
        exit;
      }
      $pos2 = index($onlineloc, '/', $pos1+1);  #find any 3rd /
      if($pos2 == -1) {				#wd is like: /SUM44/D421680658
        $dpos = rindex($onlineloc, 'D');
        $sunum = substr($onlineloc, $dpos+1);
        push(@cpwd, "$sunum $onlineloc $SUMD");
        print "sunum loc from cpwd $sunum $onlineloc $SUMD\n";  #!!TEMP
        #$sql = sprintf("update sum_main set online_loc='%s/D%u' where ds_index=%u",
	#		$SUMD, $sunum, $sunum);
        #print "$sql\n";
        #print LOG "$sql\n";
        #$sth = $dbh->prepare($sql);
        #if ( !defined $sth ) {
        #  print "Cannot prepare statement: $DBI::errstr\n";
        #  print LOG "Cannot prepare statement: $DBI::errstr\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit; 
        #}
        ## Execute the statement at the database level
        #$sth->execute;
        #$sql = sprintf("update sum_partn_alloc set wd='%s/D%u' where ds_index=%u",
	#		$SUMD, $sunum, $sunum);
        #print "$sql\n";
        #print LOG "$sql\n";
        #$sth = $dbh->prepare($sql);
        #if ( !defined $sth ) {
        #  print "Cannot prepare statement: $DBI::errstr\n";
        #  print LOG "Cannot prepare statement: $DBI::errstr\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit; 
        #}
        # Execute the statement at the database level
        #$sth->execute;
        #$cmd = "bin/rm -rf $onlineloc";
        #print "$cmd\n\n";
        #print LOG "$cmd\n";
        #if(system($cmd)) {
        #  print "ERROR on: $cmd\n";
        #  print LOG "ERROR on: $cmd\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit;
        #}
      }
      else {					#wd is like: /SUM42/D424237920/D19954994
        #Note: This was read back from tape. There may be other dirs under e.g. /SUM42/D424237920/
        #print "For 3: $onlineloc\n";
        $containD = substr($onlineloc, $pos1+1, ($pos2-$pos1)-1);
        #print "containD = $containD\n";
        if(!grep(/$SUMD\/$containD/, @mkdirarray)) {
          push(@mkdirarray, "$SUMD/$containD");
        }
        if(!grep(/$containD/, @containarray)) {
          push(@containarray, "$containD");
        }
        $dpos = rindex($onlineloc, 'D');
        $sunum = substr($onlineloc, $dpos+1);
        push(@cpwd, "$sunum $onlineloc $SUMD/$containD");
        print "sunum loc from cpwd $sunum $onlineloc $SUMD/$containD\n";  #!!TEMP
        #$sql = sprintf("update sum_main set online_loc='%s/%s/D%u' where ds_index=%u",
	#		$SUMD, $containD, $sunum, $sunum);
        #print "$sql\n";
        #print LOG "$sql\n";
        #$sth = $dbh->prepare($sql);
        #if ( !defined $sth ) {
        #  print "Cannot prepare statement: $DBI::errstr\n";
        #  print LOG "Cannot prepare statement: $DBI::errstr\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit; 
        #}
        ## Execute the statement at the database level
        #$sth->execute;
        #$sql = sprintf("update sum_partn_alloc set wd='%s/%s/D%u' where ds_index=%u",
	#		$SUMD, $containD, $sunum, $sunum);
        #print "$sql\n";
        #print LOG "$sql\n";
        #$sth = $dbh->prepare($sql);
        #if ( !defined $sth ) {
        #  print "Cannot prepare statement: $DBI::errstr\n";
        #  print LOG "Cannot prepare statement: $DBI::errstr\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit; 
        #}
        ## Execute the statement at the database level
        #$sth->execute;
        #$cmd = "/bin/rm -rf $onlineloc";
        #print "$cmd\n\n";
        #print LOG "$cmd\n";
        #if(system($cmd)) {
        #  print "ERROR on: $cmd\n";
        #  print LOG "ERROR on: $cmd\n";
        #  $dbh->disconnect();
	#  close(LOG);
        #  exit;
        #}
      }
      #print "commit to DB\n";
      #print LOG "commit to DB\n";
      #$dbh->commit();
   }		#while($onlineloc)
   #This is the end of a $MAX_BLOCKCNT loop
   $dircnt = 0;
   if($#mkdirarray != -1) {	#some dirs to mk
     while($dir = shift(@mkdirarray)) {
       if(!grep(/$dir/, @dirsmade)) {
         push(@dirsmade, $dir);
         if(!$dircnt) {
           $cmd = "mkdir -p $dir";
           $dircnt = 1;
         } else {
           $cmd = $cmd." $dir";
         }
       }
     }
   }
   if($dircnt) {
    print "$cmd\n";
    if(system($cmd)) {
      print "ERROR on: $cmd\n";
      print LOG "ERROR on: $cmd\n";
      $dbh->disconnect();
      close(LOG);
      exit;
    }
   }
   $fcnt = 0;
   print "\@cpwd:\n@cpwd\n"; #!!TEMP

     while($x = shift(@cpwd)) {
       ($index, $wd, $dest) = split(/\s+/, $x);
       $pos = index($wd, "/D");
       $pos1 = index($wd, "/D", $pos+2);
       if($pos1 == -1) {
         $contain = "XX";	#no containing dir from a tape rd
       }
       else {
         $contain = substr($wd, $pos+1, ($pos1-$pos)-1);
       }
       if(!$HofContain{$contain}) {
         $HofContain{$contain} = "cp -rp --target-directory=$dest $wd";
       }
       else {
         $HofContain{$contain} = $HofContain{$contain}." $wd";
       }
     }
     if($HofContain{"XX"}) { 
       push(@containarray, "XX"); #must gen a $cmd for this
     }
     while($contain = shift(@containarray)) {
       $cmd =  $HofContain{$contain};
       delete($HofContain{$contain});
       print "$cmd\n";
       print LOG "$cmd\n";
       if(system($cmd)) {
         print "ERROR on: $cmd\n";
         print LOG "ERROR on: $cmd\n";
         $dbh->disconnect();
         close(LOG);
         exit;
       }
     }

   print "commit to DB\n";
   print LOG "commit to DB\n";
   $dbh->commit();
    #print "\@mkdirarray:\n@mkdirarray\n";
    print "\@dirsmade:\n@dirsmade\n";
    #print "\@containarray:\n@containarray\n";
    print "*********End of Block Count Loop**********\n";
    #@mkdirarray = ();
    #@containarray = ();
    @cpwd = ();
  }		#do next $MAX_BLOCKCNT loop

$dbh->disconnect();
close(LOG);

