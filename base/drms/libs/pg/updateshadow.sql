-- This code block gets placed into an anonymous perl 
-- subroutine, so we cannot declare global subroutines.
-- Instead, you have to create anonymous subroutines.
CREATE OR REPLACE FUNCTION public.updateshadow() RETURNS trigger AS $updateshadow$    
    use strict;
    use warnings;
    use constant kShadowSuffix = '_shadow';
    
    my($fGetPkeys);
    my($fShadowExists);
    my($fIsGroupNew);
    my($fUpdateShadow);
    my($fInsertIntoShadow);
    my($fWasGroupDeleted);
    my($fDeleteFromShadow);
    
    # Anonymous-function definitions
    $fGetPkeys = sub {my($ns, $tab, $statusR) = @_;
                    my(@pkeys);
                    my($sname);
                    my($stmnt);
                    my($pkeysstr);
                    my(@draft);
                    
                    $sname = lc("$ns\.$tab");
                    $stmnt = "SELECT primary_idx FROM $ns\.drms_series WHERE lower(seriesname) = '$sname'";
                    
                    # seriesname is THE prime-key, so this query can return at most one row.
                    $rv = spi_exec_query($stmnt, 1);
                    
                    if ($rv->{status} == 'SPI_OK_SELECT')
                    {
                        $pkeysstr = $rv->{rows}[0]->{primary_idx};
                        
                        # Parse out prime-key keyword names.
                        @draft = split(',', $pkeystr);
                        @pkeys = map({lc(($_ =~ /\s*(\S+)\s*/)[0])} @draft);
                        
                        $$statusR = 0;                        
                    }
                    else
                    {
                        $$statusR = 1;
                    }
                    
                    return @pkeys;
                    };
    
    $fShadowExists = sub {
                        my($ns, $tab, $statusR) = @_;
                        my($rv);
                        my($nrows);
                        
                        $nrows = 0;
                        $stmnt = "SELECT * FROM pg_tables WHERE schemaname ILIKE '$ns' AND tablename ILIKE '$tab'";
                        $rv = spi_exec_query($stmnt, 1);
                        
                        if ($rv->{status} == 'SPI_OK_SELECT')
                        {
                            $nrows = $rv->{processed};
                            $$statusR = 0;   
                        }
                        else
                        {
                            $$statusR = 1;
                        }
                        
                        return ($nrows == 1);
                        };
                        
    $fIsGroupNew = sub {
                        my($recno, $ns, $tab, $pkeynamesR, $statusR) = @_;
                        my($sname);
                        my($stmnt);
                        my($ikey);
                        my($rv);
                        my($nvers);
                        my($newGroup);
                        
                        $newGroup = 0;
                        
                        $sname = lc("$ns\.$tab");
                        
                        $stmnt = "SELECT count(*) FROM $sname AS T1, (SELECT ";
                        
                        # Loop through prime-key keywords.
                        $ikey = 0;
                        foreach my $key (@{$pkeynamesR})
                        {
                            if ($ikey > 0)
                            {
                                $stmnt = "$stmnt, ";
                            }
                            
                            $stmnt = "$stmnt$key AS p$ikey";
                            $ikey++;
                        }
                        
                        $stmnt = "$stmnt FROM $sname WHERE recnum = $recno) AS T2 WHERE ";
                        $ikey = 0;
                        foreach my $key (@{$pkeynamesR})
                        {
                            if ($ikey > 0)
                            {
                                $stmnt = "$stmnt AND ";
                            }
                        
                            $stmnt = "T1\.$key = T2\.p$ikey";
                        }
                        
                        # Execute the query - we expect to get one row / one column. The value
                        # is the number of versions of the DRMS record just inserted into the series.
                        $rv = spi_exec_query($stmnt, 1);
                        
                        if ($rv->{status} == 'SPI_OK_SELECT' && $rv->{processed} == 1)
                        {
                            $nvers = $rv->{rows}[0]->{count};
                            
                            if ($nvers < 1)
                            {
                                # Something went wrong - this implies there are no records whose prime-key
                                # matches the record that just got inserted (but there should be at least one
                                # such record - the one that just got inserted).
                                $$statusR = 1;
                            }
                            else
                            {
                                # If there is only one record with this prime-key value, then that was
                                # the record that just got inserted, and the insertion created a 
                                # new group, which required that a new row be added to the shadow table.
                                $newGroup = ($nvers == 1);
                                $$statusR = 0;
                            }
                        }
                        else
                        {
                            $$statusR = 1;
                        }
                        
                        return $newGroup;
                    };
                    
    # This function cannot use recnum to find a series record since this function may be called when a record
    # is deleted from the series table, and if that is the case, then the series table will
    # no longer contain a row with the deleted records recnum.
    $fUpdateShadow = sub {
                            my($ns, $tab, $primekeysR, $primekeyvalsH, $recno, $inserted, $statusR) = @_;
                            my($sname);
                            my($stmnt);
                            my($keylist);
                            my($maxrec);
                            
                            # SELECT max(T1.recnum) FROM <series> WHERE <pkey1> = <pkeyval1> AND <pkey2> = <pkeyval2> ...
                             
                            $sname = lc("$ns\.$tab");
                            $shadow = $sname . &kShadowSuffix;
                            
                            # Loop through prime-key keywords.
                            $ikey = 0;
                            $keylist = "";
                            foreach my $key (@{$pkeynamesR})
                            {   
                                if ($ikey > 0)
                                {
                                    $keylist3 = "$keylist AND ";
                                }
                            
                                $keylist = "${keylist}$key = " . $primekeyvalsH->{$key};
                                $ikey++;
                            }
                            
                            $stmnt = "SELECT max(recnum) FROM $sname WHERE $keylist";
                            
                            # Execute the query. There will only be one record. 
                            $rv = spi_exec_query($stmnt, 1);
                        
                            if ($rv->{status} == 'SPI_OK_SELECT' && $rv->{processed} == 1)
                            {
                                $maxrec = $rv->{rows}[0]->{max};
                            }
                            else
                            {
                                $$statusR = 1;
                            }

                            if (defined($maxrec))
                            {
                                if ($inserted)
                                {
                                    if ($maxrec == $recno)
                                    {
                                        # The record just inserted into series is the newest version. Update the 
                                        # corresponding records shadow-table record, using the prime-key values to 
                                        # locate the shadow-table record.
                                        #
                                        # UPDATE <shadow> AS T1 SET recnum = <recnum>, nrecords = T1.nrecords + 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ...
                                
                                        $stmnt = "UPDATE $shadow SET recnum = $recno, nrecords = T1.nrecords + 1 WHERE $keylist";
                                
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt, 1);
                                
                                        if ($rv->{status} != 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $$statusR = 1;
                                        }
                                    }
                                }
                                else
                                {
                                    if ($maxrec > $recno)
                                    {
                                        # An obsolete version of the DRMS record was deleted. Update the nrecords column only.
                                        # UPDATE <shadow> SET nrecords = T1.nrecords - 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ...
                                    
                                        $stmnt = "UPDATE $shadow SET nrecords = T1.nrecords - 1 WHERE $keylist";
                                        
                                        
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt, 1);
                                
                                        if ($rv->{status} != 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $$statusR = 1;
                                        }
                                    }
                                    elseif ($maxrec < $recno)
                                    {
                                        # The current version of the DRMS record was deleted. Update both the nrecords column and the recnum column.
                                        # UPDATE <shadow> SET recnum = <maxrec>, nrecords = T1.nrecords - 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ... 
                                        
                                        $stmnt = "UPDATE $shadow SET recnum = $maxrec, nrecords = T1.nrecords - 1 WHERE $keylist";
                                                                                
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt, 1);
                                
                                        if ($rv->{status} != 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $$statusR = 1;
                                        }
                                    }
                                    else
                                    {
                                        # Cannot happen - duplicate recnums are not allowed.
                                        $$statusR = 1;
                                    }
                                }
                            }
                        };
                        
    # INSERT INTO <shadow> (pkey1, pkey2, ..., kShadowColRecnum, kShadowColNrecs) SELECT pkey1, pkey2, ..., recnum, 1 FROM <series> WHERE recnum = <recnum> 
    $fInsertIntoShadow = sub {
                                my($ns, $tab, $pkeynamesR, $recno, $statusR) = @_;
                                my($sname);
                                my($shadow);
                                my($stmnt);
                                my($ikey);
                                my($keylist);
                                
                                
                                $sname = lc("$ns\.$tab");
                                $shadow = $sname . &kShadowSuffix;
                                
                                $stmnt = "INSERT INTO $shadow (";
                                
                                # Loop through prime-key keywords.
                                $ikey = 0;
                                foreach my $key (@{$pkeynamesR})
                                {
                                    $keylist = "$key, ";
                                    $ikey++;
                                }
                               
                                $stmnt = "${stmnt}${keylist}recnum, nrecords) SELECT $keylist, recnum, 1 FROM $sname WHERE recnum = $recno";
                                
                                # Inserting a single record.
                                $rv = spi_exec_query($stmnt, 1);
                        
                                if ($rv->{status} != 'SPI_OK_INSERT' || $rv->{processed} != 1)
                                {
                                    $$statusR = 1;
                                }
                            };
                            
    # SELECT count(*) FROM <series> WHERE <pkey1> = <pkey1val> AND <pkey2> = T2.<pkey2val> AND ...; 
    $fWasGroupDeleted = sub {
                                my($ns, $tab, $pkeynamesR, $primekeyvalsH, $recno, $statusR) = @_;
                                my($sname);
                                my($stmnt);
                                my($ikey);
                                my($keylist);
                                my($rv);
    
                                $rv = -1;
                                $sname = lc("$ns\.$tab");
                                
                                # Loop through prime-key keywords.
                                $ikey = 0;
                                $keylist = "";
                                foreach my $key (@{$pkeynamesR})
                                {   
                                    if ($ikey > 0)
                                    {
                                        $keylist = "$keylist AND ";
                                    }

                                    $keylist = "${keylist}$key = " . $primekeyvalsH->{$key};
                                    $ikey++;
                                }

                                $stmnt = "SELECT count(*) FROM $sname WHERE $keylist";
                                
                                if ($rv->{status} == 'SPI_OK_SELECT' && $rv->{processed} == 1)
                                {
                                    # The group was deleted if there are no more records
                                    $rv = ($rv->{rows}[0]->{count} == 0);
                                }
                                else
                                {
                                    $$statusR = 1;
                                }
                                
                                return $rv;
                            };

    # DELETE FROM <shadow> WHERE <pkey1> = <pkey1val> AND <pkey2> = <pkey2val> ...*/
    $fDeleteFromShadow = sub {
                                my($ns, $tab, $pkeynamesR, $primekeyvalsH, $statusR) = @_;
                                my($sname);
                                my($shadow);
                                my($stmnt);
                                my($ikey);
                                my($keylist);
                                my($rv);

                                $rv = -1;
                                $sname = lc("$ns\.$tab");
                                $shadow = $sname . &kShadowSuffix;

                                # Loop through prime-key keywords.
                                $ikey = 0;
                                $keylist = "";
                                foreach my $key (@{$pkeynamesR})
                                {
                                    if ($ikey > 0)
                                    {
                                        $keylist = "$keylist AND ";
                                    }

                                    $keylist = "${keylist}$key = " . $primekeyvalsH->{$key};
                                    $ikey++;
                                }

                                $stmnt = "DELETE FROM $shadow WHERE $keylist";

                                if ($rv->{status} != 'SPI_OK_DELETE' || $rv->{processed} != 1)
                                {
                                    $$statusR = 1;
                                }
                        };                        

    my($shadowexists);    
    my($inserted);
    my($datarec); # This contains the string 'new' or 'old' - to refer to either the 
                  # inserted (new) record, or the deleted (old) record where data record
                  # values are needed. If a record was inserted, then the data record
                  # values that are needed reside in the 'new' record. If a record was 
                  # deleted, then the data record values that are needed reside in the 'old'
                  # record.
    my($recnum);
    my(@primekeys);
    my($primekeyvalsH);
    my($isnew);
    my($istat);

    # This function should be a no-op if there is no shadow table.
    $shadowexists = &{$fShadowExists}($_TD->{table_schema}, $_TD->{table_name}, \$istat);
    
    if ($istat)
    {
        elog(ERROR, "Cannot check for existence of shadow table.");
    }
    
    if ($shadowexists)
    {
        # Support row insertions and deletions only.
        if ($_TD->{event} eq 'INSERT')
        {
            $inserted = 1;
            $datarec = 'new';
        }
        elsif ($_TD->{event} eq 'DELETE')
        {
            $inserted = 0;
            $datarec = 'old';
        }
        else
        {
            elog(ERROR, "Unsupported trigger event " . $_TD->{event} . "\.");
        }
    
        # The record inserted is the 'new' record, and the record deleted is the 'old' record.
        $recnum = $_TD->{$datarec}{recnum};
        
        # Query dbase to get a list of prime-key keyword names.
        @primekeys = &{$fGetPkeys}($_TD->{table_schema}, $_TD->{table_name}, \$istat);
        
        if (!istat)
        {
            elog(ERROR, "Cannot obtain the names of the prime-key keywords.");
        }
        
        # Insert prime-key value into $primkeyvalsH.
        $primekeyvalsH = {};
        foreach my $pkey (@primekeys)
        {
            $primekeyvalsH->{$pkey} = $_TD->{$datarec}{$pkey};
        }
            
        if ($inserted)
        {
            # We're updating the shadow table because one or more rows was inserted into the original series table.
            $isnew = &{$fIsGroupNew}($recnum, $_TD->{table_schema}, $_TD->{table_name}, \@primekeys, \$istat);
            
            if ($istat)
            {
                elog(ERROR, "Failure in IsGroupNew().");
            }
            
            if ($isnew == 0)
            {
                # Need to update an existing record in the shadow table.
                &{$fUpdateShadow}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, $recnum, 1, \$istat);
                
                if ($istat)
                {
                    elog(ERROR, "Failure calling UpdateShadow() for record insertion.");
                }
            }
            else if ($isnew == 1)
            {
                # Need to add a new record to the shadow table for the new group just created in the series table.
                # shadow-table structure - pkey1, pkey2, ..., recnum, nrecords
                &{$fInsertIntoShadow}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $recnum, \$istat);
                
                if ($istat)
                {
                    elog(ERROR, "Failure in InsertIntoShadow().");
                }
            }
            else
            {
                # Unexpected value for isnew.
                elog(ERROR, "IsGroupNew() returned an unexpected value.");
            }
        }
        else
        {
            # We're updating the shadow table because one or more rows was deleted from the original series table.
            $wasdel = &{$fWasGroupDeleted}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, $recnum, \$istat);
            
            if ($istat)
            {
                elog(ERROR, "Failure in WasGroupDeleted().");
            }
            
            if ($wasdel)
            {
                # The last DRMS record was deleted - delete the corresponding group from the shadow table.
                ${$fDeleteFromShadow}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, $recnum, \$istat);
                
                if ($istat)
                {
                    elog(ERROR, "Failure in DeleteFromShadow().");
                }
            }
            else
            {
                # One version of a DRMS record was deleted. May need to update the corresponding groups record in the shadow table (update nrecords and recnum). If the version deleted was an obsolete version, then no change to the recnum in the shadow table is needed.
                &{$fUpdateShadow}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, $recnum, 0, \$istat);
                
                if ($istat)
                {
                    elog(ERROR, "Failure calling UpdateShadow() for record deletion.");
                }
            }
        }
    }
    
    return NULL;
$updateshadow$ LANGUAGE plperl;

-- This script is a template; must substitute in the name of the table
-- CREATE TRIGGER updateshadowtrig AFTER INSERT OR DELETE on <TABLE>
--    FOR EACH ROW EXECUTE PROCEDURE updateshadow();