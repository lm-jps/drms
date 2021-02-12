#include "jsoc_main.h"
#include "drms_types.h"
#include <sys/file.h>

char *module_name = "dscp";

typedef enum
{
   kDSCPErrSuccess     = 0,
   kDSCPErrBadArgs     = 1,
   kDSCPErrCantOpenRec = 2
} DSCPError_t;

#define kRecSetIn      "rsin"
#define kDSOut         "dsout"
#define ARG_SOURCE     "source"
#define ARG_DEST       "dest"
#define ARG_KEYS       "keys"
#define kUndef         "undef"
#define kMaxChunkSize  8192
#define kGb            1073741824

ModuleArgs_t module_args[] =
{
   {ARG_STRING, kRecSetIn,   kUndef,  "Input record-set specification"},
   {ARG_STRING, kDSOut,      kUndef,  "Output data series"},
   {ARG_STRING, ARG_SOURCE,  kUndef,  "Input record-set specification"},
   {ARG_STRING, ARG_DEST,    kUndef,  "Output data series"},
   {ARG_STRINGS, ARG_KEYS,   kUndef,  "List of keywords whose values should be copied"},
   {ARG_END}
};

static int SeriesExist(DRMS_Env_t *env, const char *rsspec, char ***names, int *nseries, int *ostat)
{
    int rv = 0;
    char *allvers = NULL;
    char **sets = NULL;
    DRMS_RecordSetType_t *settypes = NULL;
    char **snames = NULL;
    char **filts = NULL;
    int nsets = 0;
    int istat;

    if ((istat = drms_record_parserecsetspec(rsspec, &allvers, &sets, &settypes, &snames, &filts, &nsets, NULL)) == DRMS_SUCCESS)
    {
        int iseries;

        for (iseries = 0; iseries < nsets; iseries++)
        {
            rv = drms_series_exists(env, snames[iseries], &istat);
            if (istat != DRMS_SUCCESS)
            {
                fprintf(stderr, "Problems checking for series '%s' existence.\n", snames[iseries]);
                rv = 0;
                break;
            }
            else if (rv == 0)
            {
                break;
            }
        }
    }
    else
    {
        fprintf(stderr, "dscp FAILURE: invalid record-set specification %s.\n", rsspec);
        rv = 0;
    }

    if (istat == DRMS_SUCCESS)
    {
        int iseries;

        if (nseries)
        {
            *nseries = nsets;
        }

        if (names)
        {
            *names = (char **)malloc(nsets * sizeof(char *));

            for (iseries = 0; iseries < nsets; iseries++)
            {
                (*names)[iseries] = strdup(snames[iseries]);
            }
        }
    }

    drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);

    if (ostat)
    {
        *ostat = istat;
    }

    return rv;
}

/* returns kMaxChunkSize on error, otherwise it returns an estimated good-chunk size (the
 * number of records whose SUs comprise a GB of storage). */
static int CalcChunkSize(DRMS_Env_t *env, const char *sname)
{
    int rv = 0;

    /* Open latest record to get file sizes. Use this as a (poor) estimate of
     * the file size of each segment. Use an SQL query, because this is the only
     * way to get the 'last' record in a series without knowing anything about
     * the series (i.e., the number of prime keys). */
    int drms_stat = DRMS_SUCCESS;
    DRMS_Record_t *template = NULL;
    char query[2048];
    int iinfo;
    long long sunum = -1;
    SUM_info_t **info = NULL;
    DB_Binary_Result_t *res = NULL;


    rv = kMaxChunkSize;

    /* check to see if the input series has segments */
    template = drms_template_record(env, sname, &drms_stat);
    if (template && drms_stat == DRMS_SUCCESS)
    {
        if (drms_record_numsegments(template) > 0)
        {
            /* series-name case matters not */
            snprintf(query, sizeof(query), "SELECT sunum FROM %s WHERE recnum = (SELECT max(recnum) FROM %s)", sname, sname);
            res = drms_query_bin(env->session, query);

            if (res)
            {
                if (res->num_rows == 1 && res->num_cols == 1)
                {
                    sunum = db_binary_field_getint(res, 0, 0);

                    if (sunum >= 0)
                    {
                        info = (SUM_info_t **)calloc(1, sizeof(SUM_info_t *));

                        if (info)
                        {
                            /* Get file-size info from SUMS. */
                            if (drms_getsuinfo(env, &sunum, 1, info) == DRMS_SUCCESS)
                            {
                                if (*(info[0]->online_loc) != '\0')
                                {
                                    rv =  (int)((double)kGb / info[0]->bytes);
                                    if (rv < 1)
                                    {
                                        rv = 1;
                                    }
                                }
                                else
                                {
                                    /* Not online or invalid SUNUM. Give up and use maximum chunk size. */
                                }
                            }

                            for (iinfo = 0; iinfo < 1; iinfo++)
                            {
                                if (info[iinfo])
                                {
                                    free(info[iinfo]);
                                    info[iinfo] = NULL;
                                }
                            }

                            free(info);
                            info = NULL;
                        }
                    }
                }

                db_free_binary_result(res);
                res = NULL;
            }
            else
            {
                rv = kMaxChunkSize;
            }
        }
    }

    return rv;
}

static int ProcessRecord(DRMS_Record_t *recin, DRMS_Record_t *recout)
{
    HIterator_t *iter = NULL;
    char infile[DRMS_MAXPATHLEN];
    char outfile[DRMS_MAXPATHLEN];
    DRMS_Segment_t *segin = NULL;
    DRMS_Segment_t *segout = NULL;
    DRMS_Link_t *linkin = NULL;
    DRMS_Record_t *lrec = NULL;
    int istat = DRMS_SUCCESS;
    int rv = 0;

    /* copy keywords to output records */
    if (drms_copykeys(recout, recin, 1, kDRMS_KeyClass_Explicit))
    {
        rv = 1;
        fprintf(stderr, "failure copying DRMS keywords\n");
    }

    if (rv == 0)
    {
        /* copy segment files */
        while ((segin = drms_record_nextseg(recin, &iter, 0)) != NULL)
        {
            /* Get output segment */
            segout = drms_segment_lookup(recout, segin->info->name);

            if (segout)
            {
                if (recin->sunum != -1LL)
                {
                    if (segin->info->type == segout->info->type)
                    {
                        /* Since the segment data types are the same, we can copy the source SUMS file directly into the
                         * newly created SU. */
                        *infile = '\0';
                        *outfile = '\0';

                        /* skip input records that have no SU associated with them */
                        drms_segment_filename(segin, infile);

                        if (segout->info->protocol == DRMS_GENERIC)
                        {
                            char *filename = NULL;
                            filename = rindex(infile, '/');
                            if (filename)
                            {
                                filename++;
                            }
                            else
                            {
                                filename = infile;
                            }

                            CHECKSNPRINTF(snprintf(segout->filename, DRMS_MAXSEGFILENAME, "%s", filename), DRMS_MAXSEGFILENAME);
                            drms_segment_filename(segout, outfile);
                        }
                        else
                        {
                            drms_segment_filename(segout, outfile);
                        }

                        if (*infile != '\0' && *outfile != '\0')
                        {
                            struct stat statBuf;

                            if (stat(infile, &statBuf) == -1)
                            {
                                /* input segment file does not exist - this is not an error condition; segment files do not
                                 * necessarily need to exist */
                                 fprintf(stderr, "input segment file %s does not exist; skipping to next input segment\n", infile);
                                 continue;
                            }

                            if (copyfile(infile, outfile) != 0)
                            {
                                fprintf(stderr, "failure copying file '%s' to '%s'.\n", infile, outfile);
                                rv = 1;
                                break;
                            }
                        }
                    }
                    else
                    {
                        /* The input and output segments are of different data types. We cannot directly copy the source SUMS file to the target.
                         * Instead, we need to read the source file with drms_segment_read(). */
                        if (segin->info->protocol == DRMS_GENERIC || segout->info->protocol == DRMS_GENERIC)
                        {
                            /* If the input file is of a generic segment, and the output file is of a non-generic segment, or if the output
                             * file is of a generic segment and the input file is of a non-generic segment, then we must fail. We cannot
                             * convert to or from a generic segment. */
                            fprintf(stderr, "Unable to convert to or from a generic segment.\n");
                            rv = 1;
                            break;
                        }
                        else
                        {
                            DRMS_Array_t *data = NULL;

                            /* drms_segment_read() always returns an array in 'physical units' (or bscale/bzero are 1/0). It
                             * is possible that drms_segment_read() could set data->israw to 1, but in that case,
                             * bscale == 1 and bzero == 0, so whether israw is 0 or not is irrelevant. The data are in
                             * physical units. */

                            /* Do NOT convert to the output data type with drms_segment_read(). If conversion happens now,
                             * then files with binary double data that are to be converted to integer data will be integer
                             * truncated. Read data as-is, and let drms_segment_write() do the inverse-scaling if needed.
                             * drms_segment_write() will preserve as much precision as possible. */
                            data = drms_segment_read(segin, segin->info->type, &istat);
                            if (!data || istat != DRMS_SUCCESS)
                            {
                                fprintf(stderr, "Unable to read input segment file.\n");
                                rv = 1;
                                if (data)
                                {
                                    drms_free_array(data);
                                    data = NULL;
                                }
                                break;
                            }

                            /* Force inverse-scaling. If we don't, then input floating-point values will get TRUNCATED to int
                             * values (integer truncation). */
                            data->israw = 0;

                            /* If israw == 0, then bzero and bscale are ignored by all code, except for drms_segment_write(),
                             * which will do inverse-scaling if the output segment data type is an integer.*/
                            data->bzero = segout->bzero;
                            data->bscale = segout->bscale;

                            istat = drms_segment_write(segout, data, 0);

                            drms_free_array(data);
                            data = NULL;

                            if (istat != DRMS_SUCCESS)
                            {
                                fprintf(stderr, "Unable to write output segment file.\n");
                                rv = 1;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (iter)
        {
            hiter_destroy(&iter);
        }

        /* If recin is linked to a record in another series, then
         * copy that link to recout, if recout has a same-named link. */
        while ((linkin = drms_record_nextlink(recin, &iter)) != NULL)
        {
            /* If the output record has a link whose name matches the current input
             * record's link ...*/
            if (hcon_lookup_lower(&recout->links, linkin->info->name))
            {
                /* Obtain record linked-to from recin, if such a link exists. */
                lrec = drms_link_follow(recin, linkin->info->name, &istat);

                if (istat == DRMS_SUCCESS && lrec)
                {
                    if (drms_link_set(linkin->info->name, recout, lrec) != DRMS_SUCCESS)
                    {
                        fprintf(stderr, "Failure setting output record's link '%s'.\n", linkin->info->name);
                        rv = 1;
                        break;
                    }
                }
            }
        }

        if (iter)
        {
            hiter_destroy(&iter);
        }
    }
    else
    {
        fprintf(stderr, "copy keys failure.\n");
        rv = 1;
    }

    return rv;
}

static int copy_columns(DRMS_Env_t *env, const char *source_series, const char *dest_series, DRMS_Record_t *source_template, HContainer_t *columns, const char *record_set, long long num_records)
{
    /* first try: assume the column names and data types match exactly when compared, regardless of declared order */
    char *source_schema = NULL;
    char *source_table = NULL;
    char *dest_schema = NULL;
    char *dest_table = NULL;
    int exact_match = 0;
    int subset_of = 0;
    int specify_columns = 0;
    char cmd[2048];
    DB_Binary_Result_t *res = NULL;
    int num_columns = 0;
    char *column_list = NULL;
    char *where = NULL;
    char *series_name = NULL;
    char *pkwhere = NULL;
    char *npkwhere = NULL;
    int filter;
    int mixed;
    int allvers;
    HContainer_t *firstlast = NULL;
    HContainer_t *pkwhereNFL = NULL;
    int recnumq;
    char *query = NULL;
    long long limit;
    int err;


    err = 1;

    if (get_namespace(source_series, &source_schema, &source_table) || get_namespace(dest_series, &dest_schema, &dest_table))
    {
        fprintf(stderr, "unable to parse source and/or destination series name(s)\n");
    }
    else
    {
        strtolower(source_schema);
        strtolower(source_table);
        strtolower(dest_schema);
        strtolower(dest_table);

        if (!columns)
        {
            /* check for exact match of column name/data type between tables (subtract intersection from union), but only if
             * the user has not specified the set of columns to copy */
            snprintf(cmd, sizeof(cmd), "SELECT column_name, data_type, series FROM\n(SELECT column_name, data_type FROM\n((SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' UNION SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s') EXCEPT\n(SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' INTERSECT SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s')) AS FOO) AS BAR\nJOIN (SELECT column_name, data_type, table_schema || E'.' || table_name AS series FROM information_schema.columns) AS BART USING (column_name, data_type) WHERE BART.series IN ('%s.%s', '%s.%s')", source_schema, source_table, dest_schema, dest_table, source_schema, source_table, dest_schema, dest_table, source_schema, source_table, dest_schema, dest_table);

            if ((res = drms_query_bin(env->session, cmd)) != NULL)
            {
                if (res->num_rows == 0 && res->num_cols == 3)
                {
                    /* the tables match - columns match in name and data type, but the declared order of columns may differ, so must provide column list */
                    exact_match = 1;
                }

                db_free_binary_result(res);
                res = NULL;
            }

            if (!exact_match)
            {
                /* check to see if the set of columns in the source table is a subset of the columns in the destination table; do not
                 * check column data types - let insert fail if auto-casting fails; provide a column list in case the declared
                 * column order differs between tables (intersection equals the source's set of column names); check only if not
                 * specified the set of columns to copy */
                snprintf(cmd, sizeof(cmd), "SELECT column_name FROM\n((SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' )\nEXCEPT\n(SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' INTERSECT SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s')) AS BART", source_schema, source_table, source_schema, source_table, dest_schema, dest_table);

                if ((res = drms_query_bin(env->session, cmd)) != NULL)
                {
                    if (res->num_rows == 0 && res->num_cols == 1)
                    {
                        subset_of = 1;
                    }

                    db_free_binary_result(res);
                    res = NULL;
                }
            }

            if (!exact_match && !subset_of)
            {
                /* we might be here simply because column names do not match - however, the declared order of data types MIGHT match OR
                 * they might be cast-able to match on insert; the best way to 'check' for this is to simply try it (we don't want to
                 * check the data type of each column and figure out what PG is going to do when it tries to cast); do not provide
                 * a column list since we already know the column names do not match */
                if (drms_recordset_query(env, record_set, &where, &pkwhere, &npkwhere, &series_name, &filter, &mixed, &allvers, &firstlast, &pkwhereNFL, &recnumq) == DRMS_SUCCESS)
                {
                    /* `query` select columns from `source_table`*/
                    limit = num_records + 1; /* force limit to be all records (plus 1 to avoid truncation error) */
                    query = drms_query_string(env, source_template->seriesinfo->seriesname, where, pkwhere, npkwhere, filter, mixed, DRMS_QUERY_ALL, NULL, NULL, allvers, firstlast, pkwhereNFL, recnumq, 1, 1, &limit);

                    if (query)
                    {
                        snprintf(cmd, sizeof(cmd), "INSERT INTO %s.%s %s", dest_schema, dest_table, query);

                        if (!drms_dms_quiet(env->session, NULL, cmd))
                        {
                            err = 0;
                        }
                        else
                        {
                            /* if this query fails, it might have hosed the transaction (a mismatch in data types does this);
                             * re-start the transaction */
                            drms_server_end_transaction(env, 0, 0);
                            drms_server_begin_transaction(env);
                        }

                        free(query);
                    }
                 }
            }
            else
            {
                /* use a column list */
                specify_columns = 1;
            }
        }
        else
        {
            /* the user provided a set of column names to copy; the names must exist in both the source and destination tables,
             * otherwise, this is an error; we know the columns in `columns` exist in the source table because we checked that
             * when creating the container of columns, but we do not know if they exist in the destination series; do not
             * bother checking the destination table because even though the columns might exist, we do not know if their
             * data types match - just try to copy, and if it fails, then the calling function will handle the copy with
             * a slower method */

             /* use `columns` as the column list */
             specify_columns = 1;
        }


        if (specify_columns)
        {
            if (columns)
            {
                /* reads link columns too */
                column_list = drms_db_columns(source_template, NULL, columns, NULL, &num_columns, NULL);
            }
            else
            {
                /* the user did not specify a column list, but we need to do this for the set operation needed;
                 * drms_field_list WILL select all db columns */
                column_list = drms_field_list(source_template, 1, &num_columns);
            }

            if (column_list)
            {
                if (drms_recordset_query(env, record_set, &where, &pkwhere, &npkwhere, &series_name, &filter, &mixed, &allvers, &firstlast, &pkwhereNFL, &recnumq) == DRMS_SUCCESS)
                {
                    /* `query` select columns from `source_table`*/
                    limit = num_records + 1; /* force limit to be all records (plus 1 to avoid truncation error) */
                    if (columns)
                    {
                        query = drms_query_string(env, source_template->seriesinfo->seriesname, where, pkwhere, npkwhere, filter, mixed, DRMS_QUERY_PARTIAL, NULL, (char *)columns, allvers, firstlast, pkwhereNFL, recnumq, 1, 1, &limit);
                    }
                    else
                    {
                        query = drms_query_string(env, source_template->seriesinfo->seriesname, where, pkwhere, npkwhere, filter, mixed, DRMS_QUERY_ALL, NULL, NULL, allvers, firstlast, pkwhereNFL, recnumq, 1, 1, &limit);
                    }

                    if (query)
                    {
                        snprintf(cmd, sizeof(cmd), "INSERT INTO %s.%s (%s) %s", dest_schema, dest_table, column_list, query);

                        /* in the case where the user provided a list of columns, we do not know if the destination table has
                         * those columns, or if it does if they have compatible data types, so drop all error messages that could
                         * occur in that case, and re-start the transaction on failure */
                        if (columns)
                        {
                            if (!drms_dms_quiet(env->session, NULL, cmd))
                            {
                                err = 0;
                            }
                            else
                            {
                                drms_server_end_transaction(env, 0, 0);
                                drms_server_begin_transaction(env);
                            }
                        }
                        else
                        {
                            if (!drms_dms(env->session, NULL, cmd))
                            {
                                err = 0;
                            }
                        }

                        free(query);
                    }
                }

                free(column_list);
                column_list = NULL;
            }
        }
    }

    if (where)
    {
        free(where);
        where = NULL;
    }

    if (pkwhere)
    {
        free(pkwhere);
        pkwhere = NULL;
    }
    if (npkwhere)
    {
        free(npkwhere);
        npkwhere = NULL;
    }

    if (series_name)
    {
        free(series_name);
        series_name = NULL;
    }

    if (firstlast)
    {
        hcon_destroy(&firstlast);
    }

    if (pkwhereNFL)
    {
        hcon_destroy(&pkwhereNFL);
    }

    if (source_schema)
    {
        free(source_schema);
    }

    if (source_table)
    {
        free(source_table);
    }

    if (dest_schema)
    {
        free(dest_schema);
    }

    if (dest_table)
    {
        free(dest_table);
    }

    return err;
}

int insert_keyword(DRMS_Record_t *template, const char *keyword, HContainer_t **keys_hcon, LinkedList_t **keys_list)
{
    DRMS_Keyword_t *template_keyword = NULL;
    int err = 0;

    if (!*keys_hcon)
    {
        *keys_hcon = hcon_create(sizeof(DRMS_Keyword_t *), DRMS_MAXKEYNAMELEN, NULL, NULL, NULL, NULL, 0);
    }

    if (!*keys_list)
    {
        *keys_list = list_llcreate(DRMS_MAXKEYNAMELEN, NULL);
    }

    if (*keys_hcon && *keys_list)
    {
        template_keyword = drms_keyword_lookup(template, keyword, 0);
        if (template_keyword)
        {
            /* valid keyword */
            hcon_insert_lower(*keys_hcon, template_keyword->info->name, &template_keyword);
            list_llinserttail(*keys_list, template_keyword->info->name);
        }
        else
        {
            fprintf(stderr, "WARNING: keyword %s is not a valid source keyword", keyword);
        }
    }
    else
    {
        fprintf(stderr, "[ insert_keyword ] out of memory\n");
        err = 1;
    }

    return err;
}


int DoIt(void)
{
    DSCPError_t rv = kDSCPErrSuccess;
    int status;

    const char *rsspecin = NULL;
    const char *series_in = NULL;
    const char *series_out = NULL;
    int num_keys = 0;
    char **keys_strings = NULL;
    HContainer_t *keys_hcon = NULL;
    LinkedList_t *keys_list = NULL;
    int index_key;
    char **seriesin = NULL;
    int nseriesin = 0;
    DRMS_Record_t *source_template = NULL;

    rsspecin = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);

    if (strcasecmp(rsspecin, kUndef) == 0)
    {
        rsspecin = cmdparams_get_str(&cmdparams, ARG_SOURCE, NULL);
    }

    series_out = cmdparams_get_str(&cmdparams, kDSOut, NULL);

    if (strcasecmp(series_out, kUndef) == 0)
    {
        series_out = cmdparams_get_str(&cmdparams, ARG_DEST, NULL);
    }

    /* If both rsspecin and dsout are missing, use positional arguments. */
    if (strcasecmp(rsspecin, kUndef) == 0 && strcasecmp(series_out, kUndef) == 0)
    {
        /* cmdparams counts the executable as the first argument! */
        if (cmdparams_numargs(&cmdparams) >= 3)
        {
            rsspecin = cmdparams_getarg(&cmdparams, 1);
            series_out = cmdparams_getarg(&cmdparams, 2);
        }
        else
        {
            rv = kDSCPErrBadArgs;
        }
    }

    if (rv == kDSCPErrSuccess)
    {
        num_keys = cmdparams_get_strarr(&cmdparams, ARG_KEYS, &keys_strings, NULL);

        if (num_keys == 1 && strncmp(keys_strings[0], kUndef, sizeof(kUndef)) == 0)
        {
            /* the user did not provide the ARGS_KEYS argument */
            num_keys = 0;
        }
    }

    if (rv == kDSCPErrSuccess)
    {
        if (strcasecmp(rsspecin, kUndef) == 0 || strcasecmp(series_out, kUndef) == 0)
        {
            rv = kDSCPErrBadArgs;
        }
    }

    if (rv == kDSCPErrSuccess)
    {
        /* Make sure input and output series exist. */
        int exists;

        rv = kDSCPErrBadArgs;
        exists = SeriesExist(drms_env, rsspecin, &seriesin, &nseriesin, &status);
        if (status)
        {
            fprintf(stderr, "LibDRMS error  %d.\n", status);
        }
        else if (exists)
        {
            source_template = drms_template_record(drms_env, seriesin[0], &status);

            if (status != DRMS_SUCCESS || source_template == NULL)
            {
                fprintf(stderr, "unable to retrieve template record, status %d\n", status);
            }
            else
            {
                exists = SeriesExist(drms_env, series_out, NULL, NULL, &status);
                if (status)
                {
                    fprintf(stderr, "LibDRMS error  %d.\n", status);
                }
                else
                {
                    rv = kDSCPErrSuccess;
                    series_in = source_template->seriesinfo->seriesname;
                }
            }
        }
    }

    if (rv == kDSCPErrSuccess)
    {
        /* Operate on record chunks. */
        DRMS_RecordSet_t *rsin = NULL;
        DRMS_RecordSet_t *rsout = NULL;
        DRMS_RecordSet_t *rsfinal = NULL;
        int chunksize = 0;
        int chunksize_for_iter = 0;
        int newchunk = 0;
        DRMS_RecChunking_t cstat;
        DRMS_Record_t *recin = NULL;
        DRMS_Record_t *recout = NULL;
        int irec;
        int ichunk;
        int total_records;
        int nrecs;
        int copy_keywords = 1;


        /* First obtain number of records in record-set. Must use drms_count_records() to determine
        * exactly how many records exist. drms_open_recordset() cannot determine that. */
        nrecs = drms_count_records(drms_env, rsspecin, &status);

        if (status == DRMS_SUCCESS && nrecs > 0)
        {
            if (num_keys > 0)
            {
                for (index_key = 0; index_key < num_keys; index_key++)
                {
                    insert_keyword(source_template, keys_strings[index_key], &keys_hcon, &keys_list);
                }
            }

            if (drms_record_numsegments(source_template) == 0)
            {
                /* might be able to get away with copying from source series db table to dest series db table; if this
                 * succeeds, then there is nothing else to do
                 */
                copy_keywords = (copy_columns(drms_env, series_in, series_out, source_template, keys_hcon, rsspecin, nrecs) != 0);
            }

            if (copy_keywords)
            {
                /* Determine good chunk size (based on first input series). */
                chunksize = CalcChunkSize(drms_env, series_in);

                if (chunksize < drms_recordset_getchunksize())
                {
                    /* minimum of default chunk size */
                    chunksize = drms_recordset_getchunksize();
                }
                else
                {
                    if (drms_recordset_setchunksize(chunksize))
                    {
                        fprintf(stderr, "Unable to set record-set chunk size of %d; using default.\n", chunksize);
                        chunksize = drms_recordset_getchunksize();
                    }
                }

                rsin = drms_open_records2(drms_env, rsspecin, keys_list, 1, 0, 0, &status);
                if (status || !rsin)
                {
                    fprintf(stderr, "Invalid record-set specification %s.\n", rsspecin);
                    rv = kDSCPErrBadArgs;
                }
                else
                {

                    /* Stage the records. The retrieval is actually deferred until the drms_recordset_fetchnext()
                    * call opens a new record chunk. */
                    drms_sortandstage_records(rsin, 1, 0, NULL);

                    /* try copying keywords OUTSIDE of the loop that copies the SUs; this will only work if either: 1., the
                     * the data types and names of input columns and output columns match exactly, regardless of column order,
                     * or 2., the data types of the input columns and output columns match exactly when compared in their
                     * original orders (the first column of the input table is compared to the first column of the output table, ...);
                     *
                     * if this attempt succeeds, then there is no attempt to copy DRMS keywords; if this fails, then
                     * drms_copykeys() is called (which might succeed since DRMS will cast/convert data types if necessary -
                     * DRMS sorts the keywords in both series by rank before attempting to copy);
                     */

                    ichunk = -1;
                    irec = 0;
                    total_records = 0;

                    while ((recin = drms_recordset_fetchnext(drms_env, rsin, &status, &cstat, &newchunk)) != NULL)
                    {
                        if (status != DRMS_SUCCESS)
                        {
                            fprintf(stderr, "Unable to fetch next input record.\n");
                            rv = kDSCPErrCantOpenRec;
                            break;
                        }

                        if (newchunk)
                        {
                            /* Create a chunk of output records. */
                            if (rsin->cursor->lastrec >= 0)
                            {
                                /* this is the last chunk, so lastrec is the index of the last record in the last chunk;
                                 * lastrec + 1 is the number of records in the last chunk */
                                chunksize_for_iter = rsin->cursor->lastrec + 1;
                            }
                            else
                            {
                                chunksize_for_iter = chunksize;
                            }

                            if (rsfinal && rsfinal->records)
                            {
                                drms_close_records(rsfinal, DRMS_INSERT_RECORD);
                            }

                            /* Create a record-set that contains only "good" output records. Records for which
                             * there was some kind of failure do not get put into rsfinal. */
                            rsfinal = calloc(1, sizeof(DRMS_RecordSet_t));
                            rsfinal->records = calloc(chunksize_for_iter, sizeof(DRMS_Record_t *));
                            rsfinal->env = drms_env;

                            /* Close old chunk of output records (if one exists). */
                            if (rsout)
                            {
                                /* All "good" records will have been saved in rsfinal, commit those, and
                                 * free all rsout records. */
                                drms_close_records(rsout, DRMS_FREE_RECORD);
                                rsout = NULL;
                            }

                            rsout = drms_create_records(drms_env, chunksize_for_iter, series_out, DRMS_PERMANENT, &status);

                            if (status || !rsout || rsout->n != chunksize_for_iter)
                            {
                                fprintf(stderr, "Failure creating output records.\n");
                                break;
                            }

                            if (ichunk == -1)
                            {
                                /* first chunk (first time through loop) */
                                ichunk = 0;
                            }
                            else
                            {
                                ichunk++;
                            }

                            irec = 0;
                        }

                        recout = drms_recordset_fetchnext(drms_env, rsout, &status, NULL, NULL);
                        if (status != DRMS_SUCCESS)
                        {
                            fprintf(stderr, "Unable to fetch next output record.\n");
                            rv = kDSCPErrCantOpenRec;
                            break;
                        }

                        status = ProcessRecord(recin, recout);

                        if (status != 0)
                        {
                            fprintf(stderr, "Failure processing record.\n");
                            break;
                        }
                        else
                        {
                            /* Successfully processed record - save output record in rsfinal. */
                            rsfinal->records[irec] = recout;
                            rsfinal->n++;

                            /* Renounce ownership (if this isn't done, the calls to drms_close_records(rsout) and
                            * drms_close_records(rsfinal) will attempt to free the same record.). */
                            rsout->records[irec] = NULL;

                            total_records++;
                        }

                        irec++;
                    }

                    /* free remaining output records not freed in while loop. */
                    if (rsout)
                    {
                        drms_close_records(rsout, DRMS_FREE_RECORD);
                        rsout = NULL;
                    }

                    if (rsfinal)
                    {
                        drms_close_records(rsfinal, DRMS_INSERT_RECORD);
                        rsfinal = NULL;
                    }
                }
            }
        }
        else
        {
            fprintf(stderr, "No records to process.\n");
        }

        /* Close input records. */
        drms_close_records(rsin, DRMS_FREE_RECORD);
    }

    list_llfree(&keys_list);
    hcon_destroy(&keys_hcon);

    if (seriesin)
    {
        int iseries;

        for (iseries = 0; iseries < nseriesin; iseries++)
        {
            if (seriesin[iseries])
            {
                free(seriesin[iseries]);
            }
        }

        free(seriesin);
    }

    return rv;
}
