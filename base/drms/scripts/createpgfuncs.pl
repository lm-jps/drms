#!/usr/bin/perl -w

use File::Basename;

# read in the dbname
my($dbname);
my($wdir) = dirname(__FILE__);

if (scalar(@ARGV) != 1)
{
    print STDERR "A single argument, the name of the DRMS database, is expected.\n";
    exit(1);
}

$dbname = $ARGV[0];


# If the the types or functions already exist, remove them first.
# since functions depend on types, dropping the type will drop
# the function, IFF, the CASCADE flag is used
# These might error out if the type doesn't already exist, but it is 
# safe to ignore the error message (the redirection redirects stderr
# to stdout, which then gets dropped).
`psql -c "DROP TYPE drmskw CASCADE" $dbname 2>&1`;
`psql -c "DROP TYPE drmssg CASCADE" $dbname 2>&1`;
`psql -c "DROP TYPE rep_item CASCADE" $dbname 2>&1`;
`psql -c "DROP TYPE drmsseries CASCADE" $dbname 2>&1`;
`psql -c "DROP TYPE drmssession CASCADE" $dbname 2>&1`;

# Now create the types/functions
`psql -f $wdir/create_type_drmskw.sql $dbname`;
`psql -f $wdir/drms_keyword.sql $dbname`;
`psql -f $wdir/create_type_drmssg.sql $dbname`;
`psql -f $wdir/drms_segment.sql $dbname`;
`psql -f $wdir/create_type_rep_item.sql $dbname`;
`psql -f $wdir/drms_replicated.sql $dbname`;
`psql -f $wdir/create_type_drmsseries.sql $dbname`;
`psql -f $wdir/drms_series.sql $dbname`;
`psql -f $wdir/create_type_drmssession.sql $dbname`;
`psql -f $wdir/drms_session.sql $dbname`;
