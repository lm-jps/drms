eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/sumlookgroupX.pl
#
#Show how the storage is distributed (free, del pend, arch pend, etc.)
#for each SUM by group_id.
#Prints a XHTML file to stdout that s/b redirected to a saved .html for later display.
#
use DBI;

sub usage {
  print "Show storage distribution for each SUM group.\n";
  print "Usage: sumlookgroup.pl [-f] [-hdb_host] db_name (e.g. jsoc_sums)\n";
  print "       -f = full mode. Count the data read from tape that is always in group 0\n";
  print "       The default db_host is $HOSTDB\n";
  print "\nYou must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date ($effdate) in form 199801071442
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

#Return an effective_date in form 199801071442
#Arg has how many sec to add to current time to calculate eff date
sub reteffdate {
  my($xsec) = @_;
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2, $esec, $edate);
  $esec = time + $xsec;
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($esec);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2._.$hour2.":".$min2.":".$sec2;
  $edate = $year4.$mon2.$mday2.$hour2.$min2;
  return($edate);
}

sub commify {
                  local $_  = shift;
                  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
                  return $_;
             }

$HOSTDB = "hmidb";      #host where DB runs
$FULL = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
  if (/^-f(.*)/) {
    $FULL = 1;
  }
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit;
}
$totalbytesap = 0;
$totalbytes = 0;
$totalavail = 0;
$totalbyteso = 0;
$totalbytes30 = 0;
$totalbytes100 = 0;
$ss0 = 0;
$ss1 = 0;
$ss2 = 0;
$grandA = 0;
$grandT = 0;
$alternate = 1;

      $addsec = 86400 * 100;	#sec in 100 days
      $xeffdate = &reteffdate($addsec);
      $addsec = 86400 * 30;	#sec in 30 days
      $yeffdate = &reteffdate($addsec);

  #First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  $sql = "select group_id, name, sum_set from sum_arch_group order by group_id";
  $sth = $dbh->prepare($sql);
  if ( !defined $sth ) {
    print "Cannot prepare statement: $DBI::errstr\n";
    $dbh->disconnect();
    exit; 
  }
  # Execute the statement at the database level
  $sth->execute;
  # Fetch the rows back from the SELECT statement
  @row = ();
  while ( @row = $sth->fetchrow() ) {
    $group_id = shift(@row);
    $name = shift(@row);
    $sum_set = shift(@row);
    #if($name eq "N/A") { next; }
    if(!$name) { next; }
    push(@groupids, $group_id);
    push(@name, $name);
    push(@sumset, $sum_set);
  }
  push(@groupids, -1);

$ldate = &labeldate();
#output the xhtml header stuff
print <<EOF;
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
   "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1" />
    <title>sumlookgroup</title>
    <style type="text/css">
      p {
          color: maroon;
      }
        table {
                margin-left: 20px;
                margin-right: 20px;
                border: thin solid black;
                caption-side: top;
		border-collapse: collapse;
        }
        td, th {
                border: thin solid gray;
                padding: 5px;
        }
        caption {
                font-style: bold;
                padding-top: 12px;
                padding-bottom: 6px;
        }
        h4 {
                color: green;
        }
        .cellcolor {
                background-color: #fcba7a;
                background-color: lightyellow;
        }
        .totalcell {
                background-color: lightgrey;
        }
    </style>
  </head>
  <body>
  <h3 align=center>/SUM for db=jsoc_sums by group_id $ldate (Full mode=$FULL)</h3>
  <h4>Rounded down to nearest MegaByte</h4>
  <table>
        <tr>
                <th>Group</th>
                <th>DPnow</th>
                <th>DP1-30d</th>
                <th>DP31-100d</th>
                <th>DPlater</th>
                <th>AP</th>
                <th>ArchAll</th>
                <th>TapeClosed</th>
                <th>Sum-set</th>
                <th>Name</th>
                <th>GrpTotal</th>
        </tr>
EOF
#print "	/SUM for db=$DB by group_id $ldate (Full mode=$FULL)\n";

  #print "\nQuery in progress, may take awhile...\n";
  #print "	Rounded down to nearest Megabyte\n\n";
  #printf("Group %12s %12s %12s %12s %12s %15s %12s %07s %24s %16s\n", "DPnow", "DP1-30d", "DP31-100d", "DPlater", "AP", "ArchAll", "TapeClosed", "Sum_set", "Name    ", "GrpTotal   ");
  #printf("----- %12s %12s %12s %12s %12s %15s %12s %07s %24s %16s\n", "--------", "--------", "--------", "--------", "----------", "------------", "--------", "-------", "--------------------", "-------", "------------", "------------");

    #@groupids = (0,1,2,3,4,5,6,7,8,9,10,11,12,30,100,102,103,104,105,310,311,400,401,10000,-1);
    while(($group = shift(@groupids)) != -1) {
      if(!$FULL || $group==400 || $group==401) { #no Full mode for 400s
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$effdate'";
      } 
      else {  #there can be data from tape that is always in group=0
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$effdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytes = shift(@row);
        $bytes = $bytes/1048576;
        push(@bytes, $bytes);
      }

      #now get DP<=30d
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$yeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$yeffdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytes30 = shift(@row);
        $bytes30 = $bytes30/1048576;
        push(@bytes30, $bytes30);
      }

      #now get DP<=100d
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$xeffdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytes100 = shift(@row);
        $bytes100 = $bytes100/1048576;
        push(@bytes100, $bytes100);
      }
      #now get DPlater 
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date > '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date > '$xeffdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $byteso = shift(@row);
        $byteso = $byteso/1048576;
        push(@byteso, $byteso);
      }
      #now get total archived MB
      $sql = "select sum(bytes) from sum_main where storage_group=$group and archive_status='Y'";
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $totalA = shift(@row);
        $totalA = $totalA/1048576;
      }
      $grandA += $totalA;
      #now get total number of tapes
      $sql = "select count(*) from sum_tape where group_id=$group and closed=2";
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $totalT = shift(@row);
      }
      $grandT += $totalT;
      #now get AP 
      $sql = "select sum(bytes) from sum_partn_alloc where status=4 and archive_substatus=128 and group_id=$group";
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytesap = shift(@row);
        $bytesap = $bytesap/1048576;
        $totalbytesap += $bytesap;
        $bytes = shift(@bytes);
        $totalbytes += $bytes;
        $avail = shift(@avail);
        $totalavail += $avail;
        $bytes30 = shift(@bytes30);
        $totalbytes30 += $bytes30;
        $bytes100 = shift(@bytes100);
        $totalbytes100 += $bytes100;
        $byteso = shift(@byteso);
        $totalbyteso += $byteso;
        #printf("$sum %12d %12d %12d %12d\n", $avail,$bytes,$byteso,$bytesap);
        $BYTES = $bytes;
        $BYTES30 = $bytes30 - $bytes;
        if($BYTES30 < 0) { $BYTES30=0; }
        $BYTES100 = $bytes100 - $bytes30;
        if($BYTES100 < 0) { $BYTES100=0; }
        $TOTALBYTES30 = $totalbytes30 - $totalbytes;
        $TOTALBYTES100 = $totalbytes100 - $totalbytes30;
        $sum_set = shift(@sumset);
        $name = shift(@name);
        $totalgrp = $BYTES+$BYTES30+$BYTES100+$byteso+$bytesap;
        if($alternate) {
          print "  <tr class=\"cellcolor\">\n";
          $alternate = 0;
        }
        else {
          print "  <tr>\n";
          $alternate = 1;
        }
        print "    <td>$group</td>\n";
        $c = commify(int($BYTES));
        print "    <td align=\"right\">$c</td>\n";
        $c = commify(int($BYTES30));
        print "    <td align=\"right\">$c</td>\n";
        $c = commify(int($BYTES100));
        print "    <td align=\"right\">$c</td>\n";
        $c = commify(int($byteso));
        print "    <td align=\"right\">$c</td>\n";
        $c = commify(int($bytesap));
        print "    <td align=\"right\">$c</td>\n";
        $c = commify(int($totalA));
        print "    <td align=\"right\">$c</td>\n";
        print "    <td align=\"right\">$totalT</td>\n";
        print "    <td align=\"right\">$sum_set</td>\n";
        print "    <td align=\"right\">$name</td>\n";
        $c = commify(int($totalgrp));
        print "    <td align=\"right\">$c</td>\n";
        print "  </tr>\n";

        if($sum_set == 0) { $ss0 += $totalgrp; }
        elsif($sum_set == 1) { $ss1 += $totalgrp; }
        elsif($sum_set == 2) { $ss2 += $totalgrp; }
      }
    }
    print "  <tr class=\"totalcell\">\n";
    print "    <td>TOTAL:</td>\n";
    $c = commify(int($totalbytes));
    print "    <td align=\"right\">$c</td>\n";
    $c = commify(int($TOTALBYTES30));
    print "    <td align=\"right\">$c</td>\n";
    $c = commify(int($TOTALBYTES100));
    print "    <td align=\"right\">$c</td>\n";
    $c = commify(int($totalbyteso));
    print "    <td align=\"right\">$c</td>\n";
    $c = commify(int($totalbytesap));
    print "    <td align=\"right\">$c</td>\n";
    $c = commify(int($grandA));
    print "    <td align=\"right\">$c</td>\n";
    print "    <td align=\"right\">$grandT</td>\n";
    print "    <td></td>\n<td></td>\n<td></td>\n";
    print "  </tr>\n";
    print "</table>\n";

    print "<table>\n";
    print "<caption>SUM_SET (MB)</caption>\n";
    print "<tr class=\"cellcolor\">\n<td>0</td>\n";
    $c = commify(int($ss0));
    print "<td align=\"right\">$c</td>\n";
    print "</tr>\n";
    print "<tr>\n<td>1</td>\n";
    $c = commify(int($ss1));
    print "<td align=\"right\">$c</td>\n";
    print "</tr>\n";
    print "<tr class=\"cellcolor\">\n<td>2</td>\n";
    $c = commify(int($ss2));
    print "<td align=\"right\">$c</td>\n";
    print "</tr>\n";
    print "<tr class=\"totalcell\">\n<td>TOTAL:</td>\n";
    $stotal = $ss0+$ss1+$ss2;
    $c = commify(int($stotal));
    print "<td align=\"right\">$c</td>\n";
    print "</tr>\n";
    print "</table>\n";

    $totalstor = $totalbytes+$TOTALBYTES30+$TOTALBYTES100+$totalbyteso+$totalbytesap;

    #Get all storage that is read back from tape  
    $sql = "select sum(bytes) from sum_partn_alloc where group_id=0 and wd like '/%/D%/D%'";
    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    $sth->execute;
    while ( @row = $sth->fetchrow() ) {
      $bytest = shift(@row);
      $bytest = $bytest/1048576;
    }

    $p = sprintf("%02.1f", 100.0 * ($bytest/$totalstor));
    $c = commify(int($bytest));
    $d = commify(int($totalstor));
    print "<h4>Total bytes online that have been read back from tape:<br>$c = $p percent of total storage of $d<br></h4>\n";

@df = `/bin/df /SUM*`;
$f = shift(@df);		#skip title line
$total = 0;
while( $f = shift(@df)) {
($a, $b, $c, $avail) = split(/\s+/, $f);
$total += $avail;
}
$tmb = $total/1024;
$grand = $totalstor + $tmb;
    $c = commify(int($tmb));
    $d = commify(int($grand));
    print "<h4>Total free Mbytes not counted above  $c<br>Grand total SUM storage $d</h4>\n";

PartnAvail:
    print "<table>\n";
    print "<caption>Partition Assignment</caption>\n";
    print "<tr>\n<th>partn</th><th>sum_set</th></tr>\n";
    $sql = "select partn_name, pds_set_num from sum_partn_avail order by pds_set_num, partn_name";
    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    $sth->execute;
    $alternate = 1;
    while ( @row = $sth->fetchrow() ) {
      $a = shift(@row);
      $b = shift(@row);
      if($alternate) {
        $alternate = 0;
        print "<tr class=\"cellcolor\">\n";
      }
      else {
        $alternate = 1;
        print "<tr>\n";
      }
      print "<td>$a</td>\n";
      print "<td align=\"right\">$b</td>\n";
      print "</tr>";
    }
    print "</table>\n";
    print "</body>\n"; #!!TEMP
    print "</html>\n";

$dbh->disconnect();
