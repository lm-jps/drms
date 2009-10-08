#!/usr/bin/perl -w

if (-d "scripts")
{
    chdir("scripts");
}

# If the the types or functions already exist, remove them first.
# since functions depend on types, dropping the type will drop
# the function, IFF, the CASCADE flag is used
# These might error out if the type doesn't already exist, but it is 
# safe to ignore the error message (the redirection redirects stderr
# to stdout, which then gets dropped).
`psql -c "DROP TYPE drmskw CASCADE" 2>&1`;
`psql -c "DROP TYPE rep_item CASCADE" 2>&1`;
`psql -c "DROP TYPE drmsseries CASCADE" 2>&1`;
`psql -c "DROP TYPE drmssession CASCADE" 2>&1`;

# Now create the types/functions
`psql -f create_type_drmskw.sql`;
`psql -f drms_keyword.sql`;
`psql -f create_type_rep_item.sql`;
`psql -f drms_replicated.sql`;
`psql -f create_type_drmsseries.sql`;
`psql -f drms_series.sql`;
`psql -f create_type_drmssession.sql`;
`psql -f drms_session.sql`;
