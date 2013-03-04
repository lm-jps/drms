-- This code block gets placed into an anonymous perl 
-- subroutine, so we cannot declare global subroutines.
-- Instead, you have to create anonymous subroutines.
CREATE OR REPLACE FUNCTION public.updateshadow() RETURNS trigger AS $updateshadow$    
    use strict;
    use warnings;
    use constant kShadowSuffix => '_shadow';
    
    my($fGetPkeys);
    my($fCheckKeyType);
    my($fShadowExists);
    my($fCleanKey);
    my($fIsGroupNew);
    my($fUpdateShadow);
    my($fInsertIntoShadow);
    my($fWasGroupDeleted);
    my($fDeleteFromShadow);
    
    # Anonymous-function definitions
    $fCheckKeyType = sub {
                            # If keyword has a data type of string, then append a '*'
                            # to the end of the keyword name. We need to mark 
                            # such keywords because the SQL that queries keyword
                            # values needs to treat string keywords differently 
                            # from other types of keywords. For string keywords
                            # their values must be enclosed in single quotes.
                            my($ns, $seriesname, $keyname, $statusR) = @_;
                            my($stmnt);
                            my($errmsg);
                            my($finalkeyname);
                            my($rv);
                            
                            if ($$statusR)
                            {
                                return ();
                            }
                            
                            $finalkeyname = lc(($keyname =~ /\s*(\S+)\s*/)[0]);
                            
                            $stmnt = "SELECT type FROM $ns.drms_keyword WHERE lower(seriesname) = '$seriesname' AND lower(keywordname) = '$keyname'";
                            
                            $rv = spi_exec_query($stmnt, 1);
                    
                            if ($rv->{status} eq 'SPI_OK_SELECT' && $rv->{processed} == 1)
                            {
                                if ($rv->{rows}[0]->{type} eq "string")
                                {
                                    # lib DRMS maps the string stored in the type field to an enum by comparing, 
                                    # without regard to case, the string value to the strings in drms_type_names 
                                    # (using the function drms_str2type()). I just hard-coded the string "string"
                                    # here.
                                    $finalkeyname = "$finalkeyname\*"
                                }
                                
                                $$statusR = 0;
                            }
                            else
                            {
                                $errmsg = "Bad db query: $stmnt.";
                                elog(WARNING, $errmsg);
                                $$statusR = 1;
                            }

                            return $finalkeyname;
                        };
    
    $fGetPkeys = sub {
                    my($ns, $tab, $statusR) = @_;
                    my(@pkeys);
                    my($sname);
                    my($stmnt);
                    my($pkeysstr);
                    my(@draft);
                    my($cleankey);
                    my($mangled);
                    my($errmsg);
                    my($rv);
                    
                    $sname = lc("$ns\.$tab");
                    $stmnt = "SELECT primary_idx FROM $ns\.drms_series WHERE lower(seriesname) = '$sname'";
                    
                    # seriesname is THE prime-key, so this query can return at most one row.
                    $rv = spi_exec_query($stmnt, 1);
                    
                    if ($rv->{status} eq 'SPI_OK_SELECT')
                    {
                        $pkeysstr = $rv->{rows}[0]->{primary_idx};
                        
                        # Parse out prime-key keyword names.
                        @draft = split(',', $pkeysstr);
                        
                        foreach my $key (@draft)
                        {
                            $cleankey = lc(($key =~ /\s*(\S+)\s*/)[0]);
                            $mangled = &{$fCheckKeyType}($ns, $sname, $cleankey, $statusR);
                            push(@pkeys, $mangled);
                        }
                    }
                    else
                    {
                        $errmsg = "Bad db query: $stmnt.";
                        elog(WARNING, $errmsg);
                        $$statusR = 1;
                    }
                    
                    return @pkeys;
                    };
    
    $fShadowExists = sub {
                        my($ns, $tab, $statusR) = @_;
                        my($nrows);
                        my($stmnt);
                        my($errmsg);
                        my($rv);
                        
                        $nrows = 0;
                        $stmnt = "SELECT * FROM pg_tables WHERE schemaname ILIKE '$ns' AND tablename ILIKE '$tab'";
                        $rv = spi_exec_query($stmnt, 1);
                        
                        if ($rv->{status} eq 'SPI_OK_SELECT')
                        {
                            $nrows = $rv->{processed};
                            $$statusR = 0;   
                        }
                        else
                        {
                            $errmsg = "Bad db query: $stmnt";
                            elog(WARNING, $errmsg);
                            $$statusR = 1;
                        }
                        
                        return ($nrows == 1);
                        };
                        
    $fCleanKey = sub {
                        my($keyin) = @_;
                                
                        return ($keyin =~ /([^\*]+)(\*)?$/)[0];
                    };
                        
    $fIsGroupNew = sub {
                        # Because of the way that PG first inserts all records into the series
                        # table for a given statement, THEN it calls the trigger function 
                        # once per record inserted, we cannot use information in the series table
                        # to distinguish between the inserted records. If the insert statement
                        # causes three records to be inserted into the series table, then 
                        # the trigger function will be called three times, but during each 
                        # invocation, the series table will have had the three records already
                        # inserted. So, between the three calls to the trigger function,
                        # nothing will change in the series table.
                        #
                        # To determine if a group is new, we have to determine what group
                        # (prime-key value) the inserted record will belong to, and then 
                        # check the shadow table to see if that group exists. If not, then
                        # IsGroupNew() should return true. Otherwise, it should return false.
                        
                        
                        my($recno, $ns, $tab, $pkeynamesR, $primekeyvalsH, $statusR) = @_;
                        my($sname);
                        my($shadow);
                        my($stmnt);
                        my($ikey);
                        my($cleankey);
                        my($newGroup);
                        my($errmsg);
                        my($rv);
                        
                        $newGroup = 0;
                        
                        $sname = lc("$ns\.$tab");
                        $shadow = $sname . &kShadowSuffix;
                        
                        $stmnt = "SELECT count(*) FROM $shadow WHERE ";
                        $ikey = 0;
                        foreach my $key (@{$pkeynamesR})
                        {
                            $cleankey = &{$fCleanKey}($key);
                            
                            if ($ikey > 0)
                            {
                                $stmnt = "$stmnt AND ";
                            }
                        
                            if ($key =~ /\*$/)
                            {
                                $stmnt = "${stmnt}$cleankey = \'" . $primekeyvalsH->{$cleankey} . "\'";
                            }
                            else
                            {
                                $stmnt = "${stmnt}$cleankey = " . $primekeyvalsH->{$cleankey};
                            }
                                
                            $ikey++;
                        }

                        # Execute the query - we expect to get one row / one column, the count of the
                        # number of records in the shadow table whose prime-key value matches the prime-key
                        # value of the record just inserted into the series table. The answer is either
                        # 0 (which implies that we need to create a new group) or 1 (which implies
                        # that group already exists).
                        
                        $rv = spi_exec_query($stmnt, 1);
                        
                        if ($rv->{status} eq 'SPI_OK_SELECT' && $rv->{processed} == 1)
                        {
                            $newGroup = ($rv->{rows}[0]->{count} == 0);                        }
                        else
                        {
                            $errmsg = "Bad db query: $stmnt.";
                            elog(WARNING, $errmsg);
                            $$statusR = 1;
                        }
                        
                        return $newGroup;
                    };

    $fUpdateShadow = sub {
                            my($ns, $tab, $pkeynamesR, $primekeyvalsH, $recno, $inserted, $statusR) = @_;
                            my($sname);
                            my($stmnt);
                            my($keylist);
                            my($cleankey);
                            my($maxrec);
                            my($shadow);
                            my($ikey);
                            my($errmsg);
                            my($rv);
                            
                            # SELECT max(recnum) FROM <series> WHERE <pkey1> = <pkeyval1> AND <pkey2> = <pkeyval2> ...
                             
                            $sname = lc("$ns\.$tab");
                            $shadow = $sname . &kShadowSuffix;
                            
                            # Loop through prime-key keywords.
                            $ikey = 0;
                            $keylist = "";
                            foreach my $key (@{$pkeynamesR})
                            {
                                $cleankey = &{$fCleanKey}($key);
                                
                                if ($ikey > 0)
                                {
                                    $keylist = "$keylist AND ";
                                }
                            
                                if ($key =~ /\*$/)
                                {
                                    $keylist = "${keylist}$cleankey = \'" . $primekeyvalsH->{$cleankey} . "\'";
                                }
                                else
                                {
                                    $keylist = "${keylist}$cleankey = " . $primekeyvalsH->{$cleankey};
                                }
                                    
                                $ikey++;
                            }
                            
                            $stmnt = "SELECT max(recnum) FROM $sname WHERE $keylist";
                            
                            # Execute the query. There will only be one record. 
                            $rv = spi_exec_query($stmnt, 1);
                        
                            if ($rv->{status} eq 'SPI_OK_SELECT' && $rv->{processed} == 1)
                            {
                                $maxrec = $rv->{rows}[0]->{max};
                            }
                            else
                            {
                                $errmsg = "Bad update db statement: $stmnt";
                                elog(WARNING, $errmsg);
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
                                        # UPDATE <shadow> SET recnum = <recnum>, nrecords = nrecords + 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ...
                                
                                        $stmnt = "UPDATE $shadow SET recnum = $recno, nrecords = nrecords + 1 WHERE $keylist";
                                
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt);
                                
                                        if ($rv->{status} ne 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $errmsg = "Bad update db statement: $stmnt";
                                            elog(WARNING, $errmsg);
                                            $$statusR = 1;
                                        }
                                    }
                                    elsif ($maxrec > $recno)
                                    {
                                        # The record just inserted into series is an obsolete version. This
                                        # cannot happen if the record was inserted via DRMS. But it could 
                                        # happen if somebody inserts a record by some other means. If this happens
                                        # then increment the nrecords column.
                                        # 
                                        # UPDATE <shadow> SET nrecords = nrecords + 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ...
                                        
                                        $stmnt = "UPDATE $shadow SET nrecords = nrecords + 1 WHERE $keylist";
                                
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt);
                                
                                        if ($rv->{status} ne 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $errmsg = "Bad update db statement: $stmnt.";
                                            elog(WARNING, $errmsg);
                                            $$statusR = 1;
                                        }
                                    }
                                    else
                                    {
                                        $errmsg = "What happened to the record just inserted?";
                                        elog(WARNING, $errmsg);
                                        $$statusR = 1;
                                    }
                                }
                                else
                                {
                                    if ($maxrec > $recno)
                                    {
                                        # An obsolete version of the DRMS record was deleted. Update the nrecords column only.
                                        # UPDATE <shadow> SET nrecords = nrecords - 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ...
                                    
                                        $stmnt = "UPDATE $shadow SET nrecords = nrecords - 1 WHERE $keylist";
                                        
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt);
                                
                                        if ($rv->{status} ne 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $errmsg = "Bad update db statement: $stmnt.";
                                            elog(WARNING, $errmsg);
                                            $$statusR = 1;
                                        }
                                    }
                                    elsif ($maxrec < $recno)
                                    {
                                        # The current version of the DRMS record was deleted. Update both the nrecords column and the recnum column.
                                        # UPDATE <shadow> SET recnum = <maxrec>, nrecords = nrecords - 1 WHERE <pkey1>=<pkey1val> AND <pkey2>=<pkey2val> AND ... 
                                        
                                        $stmnt = "UPDATE $shadow SET recnum = $maxrec, nrecords = nrecords - 1 WHERE $keylist";
                                                                                
                                        # Updating a single record.
                                        $rv = spi_exec_query($stmnt);
                                
                                        if ($rv->{status} ne 'SPI_OK_UPDATE' || $rv->{processed} != 1)
                                        {
                                            $errmsg = "Bad update db statement: $stmnt.";
                                            elog(WARNING, $errmsg);
                                            $$statusR = 1;
                                        }
                                    }
                                    else
                                    {
                                        # Cannot happen - duplicate recnums are not allowed.
                                        $errmsg = "Was there a duplicate recnum? Impossible.";
                                        elog(WARNING, $errmsg);
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
                                my($keylist);
                                my($cleankey);
                                my($errmsg);
                                my($rv);
                                
                                $sname = lc("$ns\.$tab");
                                $shadow = $sname . &kShadowSuffix;
                                
                                $stmnt = "INSERT INTO $shadow (";
                                
                                # Loop through prime-key keywords.
                                $keylist = "";
                                foreach my $key (@{$pkeynamesR})
                                {
                                    $cleankey = &{$fCleanKey}($key);
                                    $keylist = "$keylist$cleankey, ";
                                }
                               
                                $stmnt = "${stmnt}${keylist}recnum, nrecords) SELECT ${keylist}recnum, 1 FROM $sname WHERE recnum = $recno";
                                
                                # Inserting a single record.
                                $rv = spi_exec_query($stmnt);
                        
                                if ($rv->{status} ne 'SPI_OK_INSERT' || $rv->{processed} != 1)
                                {
                                    $errmsg = "Bad db insert statement: $stmnt.";
                                    elog(ERROR, $errmsg);
                                    $$statusR = 1;
                                }
                            };
                            
    # SELECT count(*) FROM <series> WHERE <pkey1> = <pkey1val> AND <pkey2> = <pkey2val> AND ...; 
    $fWasGroupDeleted = sub {
                                my($ns, $tab, $pkeynamesR, $primekeyvalsH, $recno, $statusR) = @_;
                                my($sname);
                                my($stmnt);
                                my($ikey);
                                my($keylist);
                                my($cleankey);
                                my($errmsg);
                                my($rv);
                                my($wasdel);
    
                                $rv = -1;
                                $sname = lc("$ns\.$tab");
                                
                                # Loop through prime-key keywords.
                                $ikey = 0;
                                $keylist = "";
                                foreach my $key (@{$pkeynamesR})
                                {
                                    $cleankey = &{$fCleanKey}($key);
                                    
                                    if ($ikey > 0)
                                    {
                                        $keylist = "$keylist AND ";
                                    }

                                    if ($key =~ /\*$/)
                                    {
                                        $keylist = "${keylist}$cleankey = \'" . $primekeyvalsH->{$cleankey} . "\'";
                                    }
                                    else
                                    {
                                        $keylist = "${keylist}$cleankey = " . $primekeyvalsH->{$cleankey};
                                    }
                                    
                                    $ikey++;
                                }

                                $stmnt = "SELECT count(*) FROM $sname WHERE $keylist";
                                
                                $rv = spi_exec_query($stmnt, 1);
                                if ($rv->{status} eq 'SPI_OK_SELECT' && $rv->{processed} == 1)
                                {
                                    # The group was deleted if there are no more records
                                    $wasdel = ($rv->{rows}[0]->{count} == 0);
                                }
                                else
                                {
                                    $errmsg = "Bad select statement $stmnt";
                                    elog(WARNING, $errmsg);
                                    $$statusR = 1;
                                }

                                return $wasdel;
                            };

    # This function may be a no-op. This will happen if more than one record was deleted from the series
    # table, such that the last record deleted caused the deletion of a group, and a previous deletion
    # was in the same group. In that case, the first time the trigger function is called, the group record
    # will be deleted from the shadow table. This happens because all series records are deleted before
    # the trigger function is called (and then it is called for each series record that got deleted). 
    # The second and subsequent calls of the trigger function will not result in the deletion of any records
    # from the shadow table, since the record has already been removed. For these calls, $rv->{processed} will
    # be 0 since the DELETE statement does not delete anything.
    #    
    # DELETE FROM <shadow> WHERE <pkey1> = <pkey1val> AND <pkey2> = <pkey2val> ...*/
    $fDeleteFromShadow = sub {
                                my($ns, $tab, $pkeynamesR, $primekeyvalsH, $statusR) = @_;
                                my($sname);
                                my($shadow);
                                my($stmnt);
                                my($ikey);
                                my($keylist);
                                my($cleankey);
                                my($errmsg);
                                my($rv);

                                $rv = -1;
                                $sname = lc("$ns\.$tab");
                                $shadow = $sname . &kShadowSuffix;

                                # Loop through prime-key keywords.
                                $ikey = 0;
                                $keylist = "";
                                foreach my $key (@{$pkeynamesR})
                                {
                                    $cleankey = &{$fCleanKey}($key);
                                    
                                    if ($ikey > 0)
                                    {
                                        $keylist = "$keylist AND ";
                                    }
                                    
                                    if ($key =~ /\*$/)
                                    {
                                        $keylist = "${keylist}$cleankey = \'" . $primekeyvalsH->{$cleankey} . "\'";
                                    }
                                    else
                                    {
                                        $keylist = "${keylist}$cleankey = " . $primekeyvalsH->{$cleankey};
                                    }

                                    $ikey++;
                                }
                                
                                $stmnt = "DELETE FROM $shadow WHERE $keylist";
                                
                                $rv = spi_exec_query($stmnt);
                                # Will not delete any record if the record was previously deleted.
                                if ($rv->{status} ne 'SPI_OK_DELETE' || ($rv->{processed} != 1 && $rv->{processed} != 0))
                                {
                                    $errmsg = "Bad delete db statement: $stmnt.";
                                    elog(WARNING, $errmsg);
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
    my($cleankey);
    my($primekeyvalsH);
    my($isnew);
    my($wasdel);
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
        
        if ($istat)
        {
            elog(ERROR, "Cannot obtain the names of the prime-key keywords.");
        }
        
        # Insert prime-key value into $primkeyvalsH.
        $primekeyvalsH = {};
        foreach my $pkey (@primekeys)
        {
            $cleankey = &{$fCleanKey}($pkey);
            $primekeyvalsH->{$cleankey} = $_TD->{$datarec}{$cleankey};
        }
            
        if ($inserted)
        {
            # We're updating the shadow table because one or more rows was inserted into the original series table.
            $isnew = &{$fIsGroupNew}($recnum, $_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, \$istat);
            
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
            elsif ($isnew == 1)
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
                &{$fDeleteFromShadow}($_TD->{table_schema}, $_TD->{table_name}, \@primekeys, $primekeyvalsH, \$istat);
                
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
    
    return;
$updateshadow$ LANGUAGE plperlu;

-- This script is a template; must substitute in the name of the table
-- CREATE TRIGGER updateshadowtrig AFTER INSERT OR DELETE on <TABLE>
--    FOR EACH ROW EXECUTE PROCEDURE updateshadow();