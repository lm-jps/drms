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
#define kUndef         "undef"
#define kMaxChunkSize  8192
#define kGb            1073741824

ModuleArgs_t module_args[] =
{
   {ARG_STRING, kRecSetIn, kUndef,  "Input record-set specification."},
   {ARG_STRING, kDSOut,    kUndef,  "Output data series."},
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

static int copy_columns(DRMS_Env_t *env, const char *source_series, const char *dest_series, DRMS_Record_t *source_template, const char *record_set, long long num_records)
{
    /* first try: assume the column names and data types match exactly when compared, regardless of declared order */
    char *source_schema = NULL;
    char *source_table = NULL;
    char *dest_schema = NULL;
    char *dest_table = NULL;
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

        /* compare names and types */
        snprintf(cmd, sizeof(cmd), "SELECT column_name, data_type, series FROM\n(SELECT column_name, data_type FROM\n((SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' UNION SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s') EXCEPT\n(SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' INTERSECT SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s')) AS FOO) AS BAR\nJOIN (SELECT column_name, data_type, table_schema || E'.' || table_name AS series FROM information_schema.columns) AS BART USING (column_name, data_type) WHERE BART.series IN ('%s.%s', '%s.%s')", source_schema, source_table, dest_schema, dest_table, source_schema, source_table, dest_schema, dest_table, source_schema, source_table, dest_schema, dest_table);

        res = drms_query_bin(env->session, cmd);

        if (res)
        {
            if (res->num_rows == 0 && res->num_cols == 3)
            {
                /* the tables match, but the order of the columns may differ so specify column list */
                /* this is the same column list that is explicit in `query` */
                column_list = drms_field_list(source_template, 1, &num_columns);
                if (column_list)
                {
                    if (drms_recordset_query(env, record_set, &where, &pkwhere, &npkwhere, &series_name, &filter, &mixed, &allvers, &firstlast, &pkwhereNFL, &recnumq) == DRMS_SUCCESS)
                    {
                        /* `query` select columns from `source_table`*/
                        limit = num_records + 1; /* force limit to be all records (plus 1 to avoid truncation error) */
                        query = drms_query_string(env, source_template->seriesinfo->seriesname, where, pkwhere, npkwhere, filter, mixed, DRMS_QUERY_ALL, NULL, NULL, allvers, firstlast, pkwhereNFL, recnumq, 1, 1, &limit);
                        if (query)
                        {
                            snprintf(cmd, sizeof(cmd), "INSERT INTO %s.%s (%s) %s", dest_schema, dest_table, column_list, query);

                            if (!drms_dms(env->session, NULL, cmd))
                            {
                                err = 0;
                            }

                            free(query);
                        }
                    }

                    free(column_list);
                }
            }

            db_free_binary_result(res);
        }

        if (err)
        {
            /* second try: assume the data types match exactly when compared in declared order; try an insert without specifying
             * column lists
             */
            if (drms_recordset_query(env, record_set, &where, &pkwhere, &npkwhere, &series_name, &filter, &mixed, &allvers, &firstlast, &pkwhereNFL, &recnumq) == DRMS_SUCCESS)
            {
                 /* `query` select columns from `source_table`*/
                 limit = num_records + 1; /* force limit to be all records (plus 1 to avoid truncation error) */
                 query = drms_query_string(env, source_template->seriesinfo->seriesname, where, pkwhere, npkwhere, filter, mixed, DRMS_QUERY_ALL, NULL, NULL, allvers, firstlast, pkwhereNFL, recnumq, 1, 1, &limit);
                 if (query)
                 {
                     snprintf(cmd, sizeof(cmd), "INSERT INTO %s.%s %s", dest_schema, dest_table, query);

                     if (!drms_dms(env->session, NULL, cmd))
                     {
                         err = 0;
                     }

                     free(query);
                 }
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

int DoIt(void)
{
    DSCPError_t rv = kDSCPErrSuccess;
    int status;

    const char *rsspecin = NULL;
    const char *series_in = NULL;
    const char *series_out = NULL;
    char **seriesin = NULL;
    int nseriesin = 0;
    DRMS_Record_t *source_template = NULL;

    rsspecin = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
    series_out = cmdparams_get_str(&cmdparams, kDSOut, NULL);

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

    if (strcasecmp(rsspecin, kUndef) == 0 || strcasecmp(series_out, kUndef) == 0)
    {
        rv = kDSCPErrBadArgs;
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
        int newchunk = 0;
        DRMS_RecChunking_t cstat;
        DRMS_Record_t *recin = NULL;
        DRMS_Record_t *recout = NULL;
        int irec;
        int nrecs;
        int copy_keywords = 1;


        /* First obtain number of records in record-set. Must use drms_count_records() to determine
        * exactly how many records exist. drms_open_recordset() cannot determine that. */
        nrecs = drms_count_records(drms_env, rsspecin, &status);

        if (status == DRMS_SUCCESS && nrecs > 0)
        {
            if (drms_record_numsegments(source_template) == 0)
            {
                /* might be able to get away with copying from source series db table to dest series db table; if this
                 * succeeds, then there is nothing else to do
                 */
                copy_keywords = (copy_columns(drms_env, series_in, series_out, source_template, rsspecin, nrecs) != 0);
            }

            if (copy_keywords)
            {
                rsin = drms_open_recordset(drms_env, rsspecin, &status);
                if (status || !rsin)
                {
                    fprintf(stderr, "Invalid record-set specification %s.\n", rsspecin);
                    rv = kDSCPErrBadArgs;
                }
                else
                {
                    /* Determine good chunk size (based on first input series). */
                    chunksize = CalcChunkSize(drms_env, series_in);

                    if (chunksize < drms_recordset_getchunksize())
                    {
                        /* we are going to allocate `chunksize` output records each iteration, so we need to make sure that
                        * chunksize == drms_recordset_getchunksize() */
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

                    /* adjust chunksize - if nrecs < chunksize, then there are fewer records in the
                    * record-set than exist in a chunk. Make the chunksize = nrecs
                    */
                    if (chunksize >= nrecs)
                    {
                        chunksize = nrecs;
                        if (drms_recordset_setchunksize(chunksize))
                        {
                            fprintf(stderr, "Unable to set record-set chunk size of %d; using default.\n", chunksize);
                            chunksize = drms_recordset_getchunksize();
                        }
                    }

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


                    /* Create a record-set that contains only "good" output records. Records for which
                    * there was some kind of failure do not get put into rsfinal. */
                    rsfinal = malloc(sizeof(DRMS_RecordSet_t));
                    memset(rsfinal, 0, sizeof(DRMS_RecordSet_t));
                    irec = 0;

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
                            /* Close old chunk of output records (if one exists). */
                            if (rsout)
                            {
                                /* All "good" records will have been saved in rsfinal, so we can
                                * free all rsout records. */
                                drms_close_records(rsout, DRMS_FREE_RECORD);
                                rsout = NULL;
                            }

                            /* Create a chunk of output records. */
                            rsout = drms_create_records(drms_env, chunksize, series_out, DRMS_PERMANENT, &status);
                            if (status || !rsout || rsout->n != chunksize)
                            {
                                fprintf(stderr, "Failure creating output records.\n");
                                break;
                            }

                            /* rsfinal will own all records in rsout, so the records in rsfinal are not detached from
                            * the environment, and must point to the environment for drms_close_records() to
                            * know how to free them from the environment record cache. */
                            rsfinal->env = rsout->env;

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
                            drms_merge_record(rsfinal, recout);
                            XASSERT(rsout->records[irec] == recout);

                            /* Renounce ownership (if this isn't done, the calls to drms_close_records(rsout) and
                            * drms_close_records(rsfinal) will attempt to free the same record.). */
                            rsout->records[irec] = NULL;
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
