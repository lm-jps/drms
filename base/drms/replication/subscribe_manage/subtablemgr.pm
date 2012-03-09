#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use strict;
use warnings;

use DBI;
use DBD::Pg;
use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg AcquireLock ReleaseLock);

package SubTableMgr;

##### Return Codes #####
use constant kRetSuccess       => 0;
use constant kRetLock          => 1;
use constant kRetInvalidArg    => 2;
use constant kRetDBConn        => 3;
use constant kRetBadQuery      => 4;
use constant kRetIO            => 5;
use constant kRetTable         => 6;
use constant kRetTableRepl     => 7;
use constant kRetTableAdd      => 8;
use constant kRetTableCreate   => 9;
use constant kRetAlreadyExists => 10;

##### Table Columns #####
my(@headersCfg) = qw(node sitedir);
my(@pkeyCfg) = qw(node);
my(@headersLst) = qw(node series);
my(@pkeyLst) = qw(node series);

use constant kLstNode   => 0;
use constant kLstSeries => 1;
use constant kCfgNode => 0;
use constant kCfgSiteDir => 1;

sub new
{
    my($clname) = shift;
    my($self) = 
    {
        _lockfh => undef,
        _cfgtable => undef,
        _lsttable => undef,
        _dbh => undef,
        _err => undef
    };
    
    # Acquire lock 
    my($lockfh);
    
    $self->{_err} = &kRetSuccess;
    
    # Returns 1 if lock was acquired.
    if (toolbox::AcquireLock(shift, \$lockfh))
    {
        my($cfgtable) = shift;
        my($lsttable) = shift;
        
        if (defined($cfgtable) && defined($lsttable))
        {
            # Connect to db (all ops will require db access).
            $self->{_dbh} = new DBConn(@_);
            
            if (defined($self->{_dbh}))
            {
                # Create table objects.
                $self->{_cfgtable} = new CfgTable($self->{_dbh}, $cfgtable, \@headersCfg, \@pkeyCfg);
                
                if (!defined($self->{_cfgtable}))
                {
                    print STDERR "Unable to create CFG table object.\n";
                    $self->{_err} = &kRetTable;
                }
                else
                {
                    $self->{_lsttable} = new LstTable($self->{_dbh}, $lsttable, \@headersLst, \@pkeyLst);
                    
                    if (!defined($self->{_lsttable}))
                    {
                        print STDERR "Unable to create LST table object.\n";
                        $self->{_err} = &kRetTable;
                    }
                }
            }
            else
            {
                print STDERR "Unable to connect to the database.\n";
                $self->{_err} = &kRetDBConn;
            }
        }
        else
        {
            print STDERR "Missing required argument (CFG table name or LST table name).\n";
            $self->{_err} = &kRetInvalidArg;
        }
    }
    else
    {
        print STDERR "Unable to acquire lock.\n";
        $self->{_err} = &kRetLock;
    }
    
    if (!defined($self->{_err}))
    {
        $self->{_lockfh} = $lockfh;
    }
    
    bless($self, $clname);
    return $self;
}

# Replace the contents of the node's lst table records
# with the contents of a lst file.
sub Replace
{
    my($self) = shift;
    my($node) = shift;
    my($lstfile) = shift;
    
    # Should already be connected to the db.
    if (!defined($self->{_dbh}))
    {
        $self->{_err} = &kRetDBConn;
    }
    else
    {
        # Put the contents of $lstfile into the lst table.
        if (defined($self->{_lsttable}))
        {
            if ($self->{_lsttable}->Replace($node, $lstfile, &kLstNode, &kLstSeries))
            {
                $self->{_err} = &kRetTableRepl;
            }
        }
        else
        {
            $self->{_err} = &kRetTable;
        }
    }
}

# Add a row for node to the cfg table. Optionally, insert rows for this node
# into the lst table.
sub Add
{
    # $lstfile may be undefined (optional)
    my($self) = shift;
    my($node) = shift;
    my($sitedir) = shift;
    my($lstfile) = shift;
    my($rsp);
    
    # Connect to the db if necessary.
    if (!defined($self->{_dbh}))
    {
        $self->{_err} = &kRetDBConn;
    }
    else
    {
        if (defined($self->{_cfgtable}))
        {
            $rsp = $self->{_cfgtable}->Add($node, $sitedir);
            if ($rsp == 1)
            {
                $self->{_err} = &kRetAlreadyExists;
            }
            elsif ($rsp == 2)
            {
                $self->{_err} = &kRetBadQuery;
            }
        }
        else
        {
            $self->{_err} = &kRetTable;
        }
        
        if (defined($lstfile))
        {            
            if (defined($self->{_lsttable}))
            {
                if ($self->{_lsttable}->Replace($node, $lstfile, &kLstNode, &kLstSeries))
                {
                    $self->{_err} = &kRetTableRepl;
                }
            }
            else
            {
                $self->{_err} = &kRetTable;
            }
        }
    }
}

sub Create
{
    my($self) = shift;
    my($cfgfile) = shift; # optional
    my($stmnt);
    my($lstexists);
    my($cfgexists);
    
    if (defined($self->{_cfgtable}))
    {
        $cfgexists = $self->{_cfgtable}->Exists();
        
        unless (defined($cfgexists))
        {
            $self->{_err} = &kRetTable;
        }
        elsif (!$cfgexists)
        {
            if ($self->{_cfgtable}->Create())
            {
                $self->{_err} = &kRetTableCreate;
            }
        }
        else
        {
            print STDERR "Table " . $self->{_cfgtable}->GetName() . " already exists; skipping creation.\n";
        }
    }
    else
    {
        $self->{_err} = &kRetTable;
    }
    
    # If the lst table doesn't exist, create it.
    if (defined($self->{_lsttable}))
    {
        $lstexists = $self->{_lsttable}->Exists();
        
        unless (defined($lstexists))
        {
            $self->{_err} = &kRetTable;
        }
        elsif (!$lstexists)
        {
            if ($self->{_lsttable}->Create())
            {
                $self->{_err} = &kRetTableCreate;
            }
        }
        else
        {
            print STDERR "Table " . $self->{_lsttable}->GetName() . " already exists; skipping creation.\n";
        }
    }
    else
    {
        $self->{_err} = &kRetTable;
    }
    
    if (defined($cfgfile))
    {
        if (-e $cfgfile && open(CFG, "<$cfgfile"))
        {
            my(@content) = <CFG>;
            
            # Populate the CFG table from the first column of $cfgfile.
            if ($self->{_cfgtable}->Ingest(\@content))
            {
                # Bad content.
                print STDERR "cfg file $cfgfile has improper format.\n";
                $self->{_err} = &kRetTable;
            }
            else
            {
                # Populate the LST table from the series lists in the lst files in the second column of $cfgfile.
                if ($self->{_lsttable}->Ingest(\@content, &kLstNode, &kLstSeries))
                {
                    # Bad content.
                    print STDERR "cfg file $cfgfile has improper format.\n";
                    $self->{_err} = &kRetTable;
                }
            }
            
            close(CFG);
        }
        else
        {
            print STDERR "Unable to read cfg file $cfgfile.\n";
            $self->{_err} = &kRetIO;
        }   
    }
}

# Accessor functions
sub GetErr
{
    my($self) = shift;
    
    return $self->{_err};
}

sub DESTROY
{
    my($self) = shift;
    
    # Destroy tables.
    
    # Commit/Rollback db changes.
    if (defined($self->{_dbh}))
    {
        unless ($self->{_err})
        {
            $self->{_dbh}->Commit()
        }
        else
        {
            $self->{_dbh}->Rollback();
        }
        
        $self->{_dbh}->Disconnect();
        $self->{_dbh} = undef;
    }
}

package DBConn;

sub new
{
    my($clname) = shift;
    my($self) = 
    {
        _dbh    => undef,
        _dbname => shift,
        _dbhost => shift,
        _dbport => shift,
        _dbuser => shift
    };
    
    bless($self, $clname);
    
    # Connect to db.
    if ($self->Connect())
    {
        # Failure.
        $self = undef;
    }
    
    return $self;
}

# Returns 0 upon success, 1 otherwise.
sub Connect
{
    my($self) = shift;
    my($dsn) = "dbi:Pg:dbname=$self->{_dbname};host=$self->{_dbhost};port=$self->{_dbport}";
    
    $self->{_dbh} = DBI->connect($dsn, $self->{_dbuser}, '', { AutoCommit => 0 });
    
    return 0 if (defined($self->{_dbh}));
    return 1;
}

sub Disconnect
{
    my($self) = shift;
    
    if (defined($self->{_dbh}))
    {
        $self->{_dbh}->disconnect();
    }
}

# If a second argument is provided, then the caller expects the db to return rows. Otherwise, 
# the caller has provided a cmd-like query that returns no rows.
# Returns 0 upon success, 1 otherwise.
sub ExeQuery
{
    my($self) = shift;
    my($stmnt) = shift;
    my($rsp) = shift; # Reference to rows to return
    
    my($dbh);
    my($rows);
    my($rv) = 0;
    
    $dbh = $self->{_dbh};
        
    if (defined($rsp))
    {
        # DB should return 0 or more rows.
        $rows = $dbh->selectall_arrayref($stmnt, undef);
    }
    else
    {
        # DB should not return any rows.
        $dbh->do($stmnt);
    }
    
    if (defined($dbh->err))
    {
        print STDERR "Bad db query: $stmnt\n";
        $rv = 1;
    }
    else
    {
        $$rsp = $rows;
    }
    
    return $rv;
}

sub Commit
{
    my($self) = shift;
    
    if (defined($self->{_dbh}))
    {
        $self->{_dbh}->commit();
    }
}

sub Rollback
{
    my($self) = shift;
    
    if (defined($self->{_dbh}))
    {
        $self->{_dbh}->rollback();
    }
}

sub DESTROY
{
    my($self) = shift;
    
    $self->{_dbh} = undef;
}

package Table;

# Args - dbh      - DB object.
#      - name     - Name of the table.
#      - [ cols ] - A list of table columns. 
#      - [ pkey ] - A list of columns that compose the prime key. 
sub new
{
    my($clname) = shift;
    my($self) =
    {
        _dbh => shift,
        _name => shift,
        _cols => [],
        _pkey => [],
        _exists => undef
    };
    
    my($acol);
    my($cols) = shift;
    my($pkey) = shift;
    
    if (defined(@$cols))
    {
        foreach $acol (@$cols)
        {
            push(@{$self->{_cols}}, $acol);
        }
    }
    
    if (defined(@$pkey))
    {
        foreach $acol (@$pkey)
        {
            push(@{$self->{_pkey}}, $acol);
        }
    }
    
    # Gotta bless $self before calling Exists().
    bless($self, $clname);
    
    my($exists) = $self->Exists();
    if (!defined($exists))
    {
        # Some problem calling Exists().
        $self = undef;
    }
    else
    {
        $self->{_exists} = $exists;
    }
    
    return $self;
}

# Returns 1 if the Table exists, 0 if not, undef on bad query error.
# This is an internal function, not part of the API.
sub Exists
{
    my($self) = shift;
    my($ns);
    my($tab);
    my($rv) = 0;
    my($stmnt);
    my($rows);
    
    ($ns, $tab) = ($self->{_name} =~ /(.+)\.(.+)/);
    
    $stmnt = "SELECT * FROM information_schema.tables WHERE table_schema = '$ns' AND table_name = '$tab'";
    if ($self->{_dbh}->ExeQuery($stmnt, \$rows))
    {
        # Bad query.
        $rv = undef;
    }
    else
    {
        # Good query.
        my(@rowsdp) = @$rows;
        if ($#rowsdp == 0)
        {
            $rv = 1;
        }
        else
        {
            $rv = 0;
        }
    }
    
    return $rv;
}

# Returns 0 on success, 1 on failure.
sub Create
{
    my($self) = shift;
    my($stmnt);
    my($colstr);
    my($pkeystr);
    my($acol);
    my($apkey);
    my($first);
    my($pkeylst);
    my(@pkeys);
    my($rv);
    
    $rv = 0;
    
    $colstr = "";
    $pkeystr = "";
    
    if (!$self->{_exists})
    {
        $colstr = "(";
        $first = 1;
        foreach $acol (@{$self->{_cols}})
        {
            if (!$first)
            {
                $colstr = $colstr . ", ";
            }
            else
            {
                $first = 0;
            }
            $colstr = $colstr . "$acol text NOT NULL";
        }
        
        $first = 1;
        foreach $acol (@{$self->{_pkey}})
        {
            if (!$first)
            {
                $pkeystr = $pkeystr . ", ";
            }
            else
            {
                $first = 0;
            }
            
            $pkeystr = $pkeystr . "$acol";
        }
        
        
        @pkeys = @{$self->{_pkey}};
        
        if ($#pkeys >= 0)
        {
            $colstr = $colstr . ", ";
            $colstr = $colstr . "PRIMARY KEY ($pkeystr)";
        }
        
        $colstr = $colstr . ")";
        
        $stmnt = "CREATE TABLE $self->{_name} $colstr";
        
        if ($self->{_dbh}->ExeQuery($stmnt))
        {
            $rv = 1;
        }
        
        if ($rv == 0)
        {
            $self->{_exists} = 1;
        }
    }
    
    return $rv;
}

sub Replace
{
    # Noop - done by child classes.
    my($self) = shift;
    
    return 0;
}

sub GetName
{
    my($self) = shift;
    return $self->{_name};
}

package LstTable;
use base 'Table';

sub new
{
    my($clname) = shift;
    
    # $clname is the class name (e.g., LstTable). So we're telling Perl to invoke the new function in LstTable's parent chain (e.g., Table).
    my($self) = $clname->SUPER::new(@_);
    
    bless($self, $clname);
    return $self;
}

# Returns 0 on success, 1 on failure.
sub Replace
{
    my($self) = shift;
    my($node) = shift;
    my($lstfile) = shift;
    my($nodecol) = shift;
    my($seriescol) = shift;
    my($stmnt);
    my(@content);
    my($line);
    my($rv);
    
    $rv = 0;
    
    # Open the lst file.
    if (open(LST, "<$lstfile"))
    {
        @content = <LST>;
        
        # We're going to modify the lst table - ensure it exists first.        
        if ($self->{_exists})
        {
            # Delete previous records (if they exist - this query should succeed regardless).
            $stmnt = "DELETE FROM $self->{_name} WHERE $self->{_cols}->[$nodecol] = '$node'";
            unless ($self->{_dbh}->ExeQuery($stmnt))
            {
                # Insert new records.
                foreach $line (@content)
                {
                    chomp($line);
                    if ($line =~ /\S+/)
                    {
                        $stmnt = "INSERT INTO $self->{_name} ($self->{_cols}->[$nodecol], $self->{_cols}->[$seriescol]) VALUES ('$node', '$line')";
                        if ($self->{_dbh}->ExeQuery($stmnt))
                        {
                            $rv = 1;
                            last;
                        }
                        else
                        {
                            print "Inserted record ('$node', '$line') into $self->{_name} for node $node.\n";
                        }
                    }
                }
            }
            else
            {
                # Bad query.
                $rv = 1;
            }
        }
        else
        {
            # Lst table doesn't exist.
            print STDERR "Lst table $self->{_name} does not exist.\n";
            $rv = 1;
        }
        
        close(LST);
    }
    else
    {
        print STDERR "Unable to open $lstfile for reading.\n";
        $rv = 1;
    }
    
    return $rv;
}

sub ProcessCfg
{
    my($self) = shift;
    my($line) = shift;
    my($nodecol) = shift;
    my($seriescol) = shift;
    my($node);
    my($lstfile);
    my($rsp);
    
    chomp($line);
    
    if ($line !~ /\w/)
    {
        # Ignore 'empty' lines.  
    }
    elsif ($line =~ /\s*(\S+)\s*(\S+)\s*/)
    {
        $node = $1;
        $lstfile = $2;
        
        # Extract node from $node.
        if ($node =~ /.+\/(\w+)/)
        {
            $node = $1;
            
            $rsp = $self->Replace($node, $lstfile, $nodecol, $seriescol);
            
            if ($rsp)
            {
                print STDERR "Unable to insert into lst table rows for node $node.\n";
            }
        }
        else
        {
            print STDERR "Invalid config file line $line.\n";
        }
    }
    else
    {
        # Ignore bad lines.
        print STDERR "Invalid config file line $line.\n";
    }
}

# Lines are like "/solarport/pgsql/slon_logs/live/site_logs//sao        /solarport/pgsql/slon_logs/live/etc//sao.lst"
sub Ingest
{
    my($self) = shift;
    my($content) = shift;
    my($nodecol) = shift;
    my($seriescol) = shift;
    my($line);
    
    # For each file in the second column, read series-list contents and
    # ingest into lst table.
    # $content contains the contents of the slon_parser.cfg
    print "Adding records to $self->{_name}.\n";
    
    map({ $self->ProcessCfg($_, $nodecol, $seriescol) } @$content);
    
    return 0;
}

# Configuration object - one for each node
package CfgTable;
use base 'Table';

sub new
{
    my($clname) = shift;
 
    # $clname is the class name (e.g., CfgTable). So we're telling Perl to invoke the new function in CfgTable's parent chain (e.g., Table).
    my($self) = $clname->SUPER::new(@_);
    
    bless($self, $clname);
    return $self;
}

# Returns 0 on success, 1 if a row already exists for this node, 2 if a bad db query 
# was issued.
sub Add
{
    my($self) = shift;
    my($node) = shift;
    my($sitedir) = shift;
    my($stmnt);
    my($rows);
    my($rv);
    
    $rv = 0;
    
    # Check for previous existence of node in the configuration table.
    $stmnt = "SELECT count(*) FROM $self->{_name} WHERE $self->{_cols}->[0] = '$node'"; 
    
    unless ($self->{_dbh}->ExeQuery($stmnt, \$rows))
    {
        # Good query.
        my(@rowsdp) = @$rows;
        if ($#rowsdp == 0)
        {   
            if ($rows->[0]->[0] > 0)
            {
                # Record for this node already exists.
                print STDERR "A record for node '$node' already exists in $self->{_name}.\n";
                $rv = 1;
            }
            else
            {
                # Node does not already have a record in the configuration table.
                $stmnt = "INSERT INTO $self->{_name} ($self->{_cols}->[0], $self->{_cols}->[1]) VALUES ('$node', '$sitedir')";
                if ($self->{_dbh}->ExeQuery($stmnt))
                {
                    $rv = 2;
                }
            }
        }
        else
        {
            print STDERR "Unexpected response from db.\n";
            $rv = 2;
        }
    }
    else
    {
        # Bad query;
        $rv = 2;
    }
    
    return $rv;
}

sub ProcessCfg
{
    my($self) = shift;
    my($line) = shift;
    my($node);
    my($sitedir);
    my($rsp);
    
    chomp($line);

    if ($line !~ /\w/)
    {
        # Ignore 'empty' lines.  
    }
    elsif ($line =~ /\s*(\S+)\s*\S+\s*/)
    {
        $sitedir = $1;
        
        # Get node name.
        if ($sitedir =~ /.+\/(\w+)/)
        {
            $node = $1;
            
            # Insert into db table.
            $rsp = $self->Add($node, $sitedir);
            
            if ($rsp == 0)
            {
                print "Inserted record ('$node', '$sitedir') into $self->{_name} for node $node.\n";
            }
            elsif ($rsp == 1)
            {
                print STDERR "Duplicate entry for node $node; skipping insert for this node.\n";
            }
            
            # Bad db query message will print elsewhere.
        }
        else
        {
            print STDERR "Invalid config file line $line.\n";
        }
    }
    else
    {
        # Ignore bad lines.
        print STDERR "Invalid config file line $line.\n";
    }
    
}

# Lines are like "/solarport/pgsql/slon_logs/live/site_logs//sao        /solarport/pgsql/slon_logs/live/etc//sao.lst"
sub Ingest
{
    my($self) = shift;
    my($content) = shift;
    my($line);
    
    # For each file in the second column, read series-list contents and
    # ingest into lst table.
    # $content contains the contents of the slon_parser.cfg
    print "Adding records to $self->{_name}.\n";
    
    map({ $self->ProcessCfg($_) } @$content);
    
    return 0;
}
1;
