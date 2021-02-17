/* */

#include "jsmn.h"
#include "jsoc_main.h"
#include "tasrw.h"

char *module_name = "rawingest";

#define kMaxJSONTokens 65536 /* A very large JSON string. */
#define kRecChunkSz 128

#define kArgNotSpec "not specified"
#define kArgSeries "series"
#define kArgSegment "segment"
#define kArgTest "t"
#define kArgSetDate "d"

enum RIStatus_enum
{
    kErrNone = 0,
    kErrNoMemory,
    kErrInvalidJSON,
    kErrHugeJSON,
    kErrIngest
};

typedef enum RIStatus_enum RIStatus_t;

static int gNextTokIndex = 0;

ModuleArgs_t module_args[] =
{
    {ARG_STRING,
        kArgSeries,
        NULL,
        "The series to which records are being added.",
        NULL},

    {ARG_STRING,
        kArgSegment,
        NULL,
        "The segment that will contain the data files being ingested.",
        NULL},

    {ARG_STRING,
        kArgTest,
        kArgNotSpec,
        "Test argument - if set, then this will be the path to a file containing json for testing.",
        NULL},

    {ARG_FLAG,
        kArgSetDate,
        NULL,
        "If set, then this flag will cause the DATE keyword, if present, to be updated in every record created.",
        NULL},

    {ARG_END}
};

static int GetNextTokIndex()
{
   return gNextTokIndex;
}

static void SetNextTokIndex(int index)
{
   gNextTokIndex = index;
}

static int GetKeyAndValue(DRMS_Record_t *orec, const char *json, jsmntok_t *tokens, int ntoks, int itok, char **key, DRMS_Value_t **val, int *nchild)
{
    jsmntok_t *keytok = NULL;
    jsmntok_t *valtok = NULL;
    size_t len;
    char keyname[DRMS_MAXKEYNAMELEN];
    char *keyval = NULL;
    DRMS_Keyword_t *drmskey = NULL;
    DRMS_Type_t type;
    int consumed;
    int istat;

    keytok = &(tokens[itok]);
    valtok = &(tokens[itok + 1]);
    if ((keytok->start == 0 && keytok->end == 0) || itok == ntoks - 1)
    {
        /* We know there are no more tokens to process, if, when we're iterating through them, we encounter
         * a nulled-out token struct. The jsmn parser sets the token struct's start and end fields to -1 when it
         * first allocates the token, so if these fields are still 0, then the token has not been allocated. */

    }

    if (keytok->type != JSMN_STRING)
    {
        /* All keys must be strings. */
        return 1;
    }

    if (valtok->type == JSMN_ARRAY)
    {
        /* Should really support this at some point. */
    }
    else if (valtok->type == JSMN_OBJECT)
    {
        /* The value is an object whose key is a prime-key value - we don't need the value. We need to recurse
         * into the object until we get a primitive or string value. But we do need the property name, which
         * is actualy a prime-key keyword value. */

        /* Extract string value - the prime-key keyword VALUE. */
        len = keytok->end - keytok->start;
        keyval = calloc(1, len + 1);

        if (*keyval)
        {
            return 1;
        }

        memcpy(keyval, &json[keytok->start], len);
        keyval[len] = '\0';
        *key = keyval;

        *val = NULL;
        *nchild = valtok->size;
    }
    else
    {
        /* Extract string value - the prime-key keyword NAME. */
        len = keytok->end - keytok->start;
        memcpy(keyname, &json[keytok->start], len);
        keyname[len] = '\0';
        *key = strdup(keyname);

        if (!key)
        {
            return 1;
        }

        /* Get the data type from the DRMS keyword - do not follow link! We cannot set linked keywords. */

        /* Create a DRMS_Value_t. */
        len = valtok->end - valtok->start;
        keyval = calloc(1, len + 1);

        if (!keyval)
        {
            return 1;
        }

        memcpy(keyval, &json[valtok->start], len);
        keyval[len] = '\0';

        /* It MAY be the case that the property value is a DRMS keyword value, although that isn't always true.
         * If it is true, then use the DRMS keyword data type to create a DRMS_Value_t containing the
         * keyword value in its designed data type. Otherwise, create a DRMS_Value_t containing the
         * value as a string, which is the original formatin the JSON */
        drmskey = drms_keyword_lookup(orec, keyname, 0);

        if (drmskey)
        {
            type = drms_keyword_gettype(drmskey);
        }
        else
        {
            type = DRMS_TYPE_STRING;
        }

        *val = calloc(1, sizeof(DRMS_Value_t));

        if (!*val)
        {
            return 1;
        }

        (*val)->type = type;

        if (type == DRMS_TYPE_CHAR ||
            type == DRMS_TYPE_SHORT ||
            type == DRMS_TYPE_INT ||
            type == DRMS_TYPE_LONGLONG)
        {
           (*val)->value.longlong_val = drms_types_strtoll(keyval, type, &consumed, &istat);
           if (istat)
           {
              return 1;
           }
        }
        else
        {
           drms_strval(type, &((*val)->value), keyval);
        }

        *nchild = 0; /* The property value is a primitive - no objects. */
    }

    return 0;
}

static int GetNextKeyAndValue(DRMS_Record_t *orec, const char *json, jsmntok_t *tokens, int ntoks, char **key, DRMS_Value_t **val, int *nchild)
{
    int itok = GetNextTokIndex();
    int ret = 0;

    ret = GetKeyAndValue(orec, json, tokens, ntoks, itok, key, val, nchild);
    if (ret == 0)
    {
        SetNextTokIndex(itok + 2);
    }

    return ret;
}

static int IngestRawFile(DRMS_Record_t *orec, const char *segname, const char *infile)
{
    DRMS_Segment_t *seg = NULL;
    struct stat stBuf;
    char outfile[DRMS_MAXPATHLEN] = {0};
    char key[DRMS_MAXKEYNAMELEN];
    int istat;
    int rv;

    rv = 0;

    /* Make sure the file to ingest really exists. */
    if (stat(infile, &stBuf) == -1)
    {
        rv = 1;
    }
    else
    {
        /* Get the path to the SU dir.  */
        seg = drms_segment_lookup(orec, segname);

        if (!seg)
        {
            rv = 1;
        }
        else
        {
            if (seg->info->protocol == DRMS_GENERIC)
            {
                /* If the output segment has a protocol of generic, then use the base file
                 * name of the input file as the base file name of the output file. */
                const char *filename = NULL;
                filename = rindex(infile, '/');

                if (filename)
                {
                    filename++;
                }
                else
                {
                    filename = infile;
                }

                CHECKSNPRINTF(snprintf(seg->filename, DRMS_MAXSEGFILENAME, "%s", filename), DRMS_MAXSEGFILENAME);
                drms_segment_filename(seg, outfile);
            }
            else if (seg->info->protocol == DRMS_BINARY ||
                     seg->info->protocol == DRMS_BINZIP ||
                     seg->info->protocol == DRMS_FITZ ||
                     seg->info->protocol == DRMS_FITS)
            {
                int naxis;
                int iaxis;
                TASRW_FilePtr_t fptr = NULL;
                TASRW_FILE_PTR_INFO fpinfo;
                int fileCreated = 0;

                /* Use the name that lib DRMS derives. */
                drms_segment_filename(seg, outfile);

                /* Must fetch axis lengths from the actual FITS file. */
                fptr = fitsrw_getfptr(0, infile, 0, &istat, &fileCreated);

                XASSERT(!fileCreated);

                if (fptr && !istat)
                {
                   if (fitsrw_getfpinfo_ext(fptr, (TASRW_FilePtrInfo_t)&fpinfo))
                   {
                      fprintf(stderr, "Unable to retrieve file info.\n");
                      rv = 1;
                   }

                }
                else
                {
                   fprintf(stderr, "Unable to read file %s.\n", infile);
                   rv = 1;
                }

                if (fptr)
                {
                   fitsrw_closefptr(0, fptr);
                }

                if (rv == 0)
                {
                    /* Need to set for protocols DRMS_BINARY, DRMS_BINZIP, DRMS_FITZ, DRMS_FITS. Reject DRMS_TAS
                     * for now - I don't want to deal with it. */
                    snprintf(key, sizeof(key), "%s_bzero", seg->info->name);

                    if (fpinfo.bitpix == 8)
                    {
                        /* For CHAR data, we store our image files with a bzero of -128 so that
                         * we can store unsigned char data in FITS, which does not support
                         * unsigned char data. When we read the image data, we apply the
                         * bzero value, which converts the data to unsigned char data. But then
                         * we need to adjust the bzero value that DRMS has. It should be 0, not
                         * -128 (when bscale is 1). */
                        drms_setkey_double(seg->record, key, 128 * fpinfo.bscale + fpinfo.bzero);
                    }
                    else
                    {
                        drms_setkey_double(seg->record, key, fpinfo.bzero);
                    }

                    snprintf(key, sizeof(key), "%s_bscale", seg->info->name);
                    drms_setkey_double(seg->record, key, fpinfo.bscale);

                    if (seg->info->protocol == DRMS_FITS)
                    {
                        if (seg->info->scope == DRMS_VARDIM)
                        {
                            /* Must fetch axis lengths from the actual FITS file. */
                            naxis = fpinfo.naxis;

                            /* Use the input array's axis dimensionality and lengths for the output file */
                            seg->info->naxis = naxis;

                            for (iaxis = 0; iaxis < naxis; iaxis++)
                            {
                                seg->axis[iaxis] = fpinfo.naxes[iaxis];
                            }
                        }
                        else
                        {
                            /* I think that seg->axis is already correctly set for non DRMS_VARDIM. */
                        }
                    }
                    else
                    {
                        /* I think that seg->axis is irrelevant for the other protocols, but I'm not sure.  */
                    }
                }
            }
            else
            {
                fprintf(stderr, "Unsupported segment protocol %d.\n", seg->info->protocol);
                rv = 1;
            }
        }
    }

    if (rv == 0)
    {
        if (*outfile)
        {
            if (copyfile(infile, outfile) != 0)
            {
                fprintf(stderr, "failure copying file '%s' to '%s'.\n", infile, outfile);
                rv = 1;
            }
        }
    }

    return rv;
}

/* Processes a single json object. The current token must be the key of a js property. */
int SetKeyValues(DRMS_Env_t *env,
                 const char *series,
                 const char *segname,
                 DRMS_RecordSet_t **chunk,
                 DRMS_RecordSet_t *final,
                 char **pkeys,
                 int npkeys,
                 HContainer_t *pkeyvals,
                 int pklev,
                 const char *json,
                 jsmntok_t *tokens,
                 int ntoks,
                 int ntoproc,
                 int *nprocessed,
                 int * hasnpk,
                 int *irec,
                 int setdate)
{
    char *key = NULL;
    DRMS_Value_t *val = NULL;
    DRMS_Record_t *orec = NULL;
    DRMS_Value_t pkeyval;
    DRMS_Value_t *ppkeyval = NULL;
    int nproc;
    int nchild;
    int istat;
    int nelem;
    int ielem;
    int subnchild;
    HIterator_t *hit = NULL;
    int isubirec;
    int rv;

    rv = 0;

    *nprocessed = 0;
    *hasnpk = 0;
    nproc = 0;
    orec = (*chunk)->records[*irec];

    while (nproc != ntoproc)
    {
        /* If the next json value is an object, then val will be NULL, but nchild
         * will contain the number of tokens within the object (including the key). */
        if (GetNextKeyAndValue(orec, json, tokens, ntoks, &key, &val, &nchild))
        {
            rv = 1;
            break;
        }

        if (!key)
        {
            /* No more tokens to process. */
            break;
        }

        if (!val)
        {
            /* key is a prime-key keyword value. */
            DRMS_Keyword_t *drmskey = NULL;
            DRMS_Type_t keytype;
            int consumed;
            int subnproc;
            int subhasnpk;
            const char *pkeyname = NULL;
            char pkeynamelc[DRMS_MAXKEYNAMELEN];
            DRMS_RecordSet_t *inrs = NULL;
            DRMS_Record_t *inrec = NULL;
            char *spec = NULL;
            char *stmp = NULL;
            size_t szspec = 256;
            DRMS_Segment_t *segin = NULL;
            DRMS_Link_t *linkin = NULL;
            DRMS_Record_t *lrec = NULL;
            char infile[DRMS_MAXPATHLEN];


            /* Since the keys property contains an object, val will be NULL when key is "keys". */
            if (strcasecmp(key, "keys") == 0)
            {
                *hasnpk = 1;

                /* If we're processing a keys object, then we've seen all the prime-key keyword values
                 * for the current record. Now we can look up the original record and copy its keyword
                 * values into the output record.  Unfortunatley,
                 * DRMS doesn't provide a way to search for DRMS records that exist in memory, so we have to instead
                 * go to the db every time we want to find a record by prime-key. DO THIS BEFORE
                 * SETTING THE KEYWORD VALUES PROVIDED IN THE JSON STRING. */
                hit = hiter_create(pkeyvals);
                if (!hit)
                {
                    rv = 1;
                    break;
                }

                spec = calloc(1, szspec * sizeof(char));

                if (!spec)
                {
                    rv = 1;
                    break;
                }

                spec = base_strcatalloc(spec, series, &szspec);

                while ((ppkeyval = hiter_extgetnext(hit, &pkeyname)) != NULL)
                {
                    spec = base_strcatalloc(spec, "[", &szspec);
                    stmp = drms2string(ppkeyval->type, &(ppkeyval->value), &istat);
                    if (istat)
                    {
                        rv = 1;
                        break;
                    }

                    spec = base_strcatalloc(spec, stmp, &szspec);
                    free(stmp);
                    spec = base_strcatalloc(spec, "]", &szspec);

                    /* We can also set the prime-key keyword values in the output record at this time. */
                    drms_setkey_p(orec, pkeyname, ppkeyval);
                }

                hiter_destroy(&hit);

                if (istat)
                {
                    rv = 1;
                    break;
                }

                inrs = drms_open_records(env, spec, &istat);
                free(spec);

                inrec = NULL;
                if (!istat && inrs && inrs->n == 1)
                {
                    inrec = inrs->records[0];

                    if (drms_copykeys(orec, inrec, 0, kDRMS_KeyClass_Explicit))
                    {
                        rv = 1;
                        break;
                    }

                    /* If the user has requested that the DATE keyword be set, do that now. */
                    if (setdate)
                    {
                        drms_keyword_setdate(orec);
                    }
                }

                if (inrec)
                {
                    /* We must also copy the segments other that the one whose name matches the name in the series variable. */
                    while ((segin = drms_record_nextseg(inrec, &hit, 0)) != NULL)
                    {
                        if (segin->info->islink || strcasecmp(segin->info->name, segname) == 0)
                        {
                            /* Skip :
                             * 1. If the input segment is a link - these are handled below.
                             * 2. If the segment is the one whose data file is being copied by this program. */
                            continue;
                        }

                        if (segin->record->sunum != -1LL)
                        {
                            /* Believe it or not, drms_segment_filename() will STAGE the relevant SU too! We need
                             * to do that before trying to ingest the original SUMS files. */
                            drms_segment_filename(segin, infile);

                            if (IngestRawFile(orec, segin->info->name, infile) != 0)
                            {
                                rv = 1;
                                break;
                            }
                        }
                    }

                    if (rv)
                    {
                        break;
                    }

                    hiter_destroy(&hit);

                    /* Finally, we must copy links from the input segment to other segments TO orec. */
                    while ((linkin = drms_record_nextlink(inrec, &hit)) != NULL)
                    {
                        /* If the output record has a link whose name matches the current input
                         * record's link ...*/
                        if (hcon_lookup_lower(&orec->links, linkin->info->name))
                        {
                            /* Obtain record linked-to from recin, if such a link exists. */
                            lrec = drms_link_follow(inrec, linkin->info->name, &istat);

                            if (istat == DRMS_SUCCESS && lrec)
                            {
                                if (drms_link_set(linkin->info->name, orec, lrec) != DRMS_SUCCESS)
                                {
                                    fprintf(stderr, "Failure setting output record's link '%s'.\n", linkin->info->name);
                                    rv = 1;
                                    break;
                                }
                            }
                        }
                    }

                    hiter_destroy(&hit);
                }

                drms_close_records(inrs, DRMS_FREE_RECORD);

                if (rv)
                {
                    break;
                }

                /* Now we can overwrite the values of the keyword in orec that were specified in the json string. */
                nelem = nchild / 2;
                for (ielem = 0; ielem < nelem; ielem++)
                {
                    GetNextKeyAndValue(orec, json, tokens, ntoks, &key, &val, &subnchild);
                    drms_setkey_p(orec, key, val);
                }

                /* We processed the "keys" token, and the object (containing keyword values) - so
                 * that is only 2 tokens. */
                nproc += 2;
            }
            else
            {
                /* Set the value of this prime key in the pkeyvals container. */
                drmskey = drms_keyword_lookup(orec, pkeys[pklev], 0);

                if (!drmskey)
                {
                    rv = 1;
                    break;
                }

                keytype = drms_keyword_gettype(drmskey);
                pkeyval.type = keytype;

                if (keytype == DRMS_TYPE_CHAR ||
                    keytype == DRMS_TYPE_SHORT ||
                    keytype == DRMS_TYPE_INT ||
                    keytype == DRMS_TYPE_LONGLONG)
                {
                   pkeyval.value.longlong_val = drms_types_strtoll(key, keytype, &consumed, &istat);
                   if (istat)
                   {
                      rv = 1;
                      break;
                   }
                }
                else
                {
                   drms_strval(keytype, &(pkeyval.value), key);
                }

                /* AHH! If an element already exists in a hash container, then you cannot insert the same element again. So,
                 * if the element already exists, delete it. */

                snprintf(pkeynamelc, sizeof(pkeynamelc), "%s", pkeys[pklev]);
                strtolower(pkeynamelc);

                /* If the element does not already exist, then hcon_remove() is a no-op. */
                hcon_remove(pkeyvals, pkeynamelc);
                hcon_insert_lower(pkeyvals, pkeys[pklev], &pkeyval);

                if (SetKeyValues(env, series, segname, chunk, final, pkeys, npkeys, pkeyvals, pklev + 1, json, tokens, ntoks, nchild, &subnproc, &subhasnpk, irec, setdate))
                {
                    rv = 1;
                    break;
                }

                nproc += 2;

                /* We completely processed an object. This means that we're done with a DRMS record
                 * and we are moving on to the next one. */

                if (subhasnpk)
                {
                    (*irec)++;

                    if (*irec > (*chunk)->n - 1)
                    {
                        int nrecs = (*chunk)->n;

                        /* Merge into final. */
                        for (isubirec = 0; isubirec < nrecs; isubirec++)
                        {
                            drms_merge_record(final, (*chunk)->records[isubirec]);
                            (*chunk)->records[isubirec] = NULL;
                        }

                        /* final now owns the records *chunk used to own. */
                        drms_close_records(*chunk, DRMS_FREE_RECORD);
                        *chunk = NULL;

                        /* Get a new chunk */
                        *irec = 0;

                        if (nproc == ntoproc)
                        {
                           /* No need to create a new record chunk - we're done. */
                           break;
                        }

                        *chunk = drms_create_records(env, kRecChunkSz, series, DRMS_PERMANENT, &istat);
                    }

                    orec = (*chunk)->records[*irec];
                }
            }
        }
        else
        {
            /* The current property is either a "keys" object, or a "file" string primitive. */
            *hasnpk = 1;

            if (strcasecmp(key, "file") == 0)
            {
                /* Ingest the file. */
                if (IngestRawFile(orec, segname, val->value.string_val) != 0)
                {
                    rv = 1;
                    break;
                }

                nproc += 2;
            }
            else
            {
                /* Error. Currently, only the "file" property has a non-object primitive for a value and
                 * is processed in this block of code.  The series keyword values are processed above. */

            }
        }
    }

    if (nprocessed)
    {
        *nprocessed = nproc;
    }

    if (key)
    {
        free(key);
    }

    if (val)
    {
        if (val->type == DRMS_TYPE_STRING)
        {
            free(val->value.string_val);
        }

        free(val);
    }

    return rv;
}

int DoIt(void)
{
    /* Read json from stdin. */
    FILE *strm = NULL;
    char line[LINE_MAX];
    char *json = NULL;
    size_t szjson = 2048;
    jsmn_parser parser;
    jsmntok_t *tokens = NULL;
    size_t szjstokens = 512;
    int res;
    const char *series = NULL;
    const char *segname = NULL;
    const char *testfile = NULL;
    int setdate;
    int istat;
    RIStatus_t rv;

    rv = kErrNone;

    json = calloc(1, szjson);

    if (json)
    {
        series = cmdparams_get_str(&cmdparams, kArgSeries, &istat);
        segname = cmdparams_get_str(&cmdparams, kArgSegment, &istat);
        testfile = cmdparams_get_str(&cmdparams, kArgTest, &istat);
        setdate = cmdparams_isflagset(&cmdparams, kArgSetDate);

        if (strcmp(testfile, kArgNotSpec) != 0)
        {
            strm = fopen(testfile, "r");
        }
        else
        {
            strm = stdin;
        }

        while (fgets(line, LINE_MAX, strm) != NULL)
        {
            /* The calling script must pipe-in a json object that contains the prime-key
             * values, keyword values, and path to the file to ingest. We need to read in
             * all of the json string before we can process it. */
            json = base_strcatalloc(json, line, &szjson);
        }

        if (strlen(json) > 0)
        {
            tokens = calloc(1, sizeof(jsmntok_t) * szjstokens);

            if (tokens)
            {
                while (1)
                {
                    jsmn_init(&parser);
                    res = jsmn_parse(&parser, json, tokens, szjstokens);

                    if (res == JSMN_ERROR_NOMEM)
                    {
                        /* This means that there were too few tokens provided to the parser. We want to try again with more
                         * tokens. To avoid an endless loop, we need to set a limit on the number of tokens, and
                         * fail with an appropriate "your json string is too large" message. */
                        if (szjstokens > kMaxJSONTokens)
                        {
                            fprintf(stderr, "Whoa! Try providing a smaller JSON string.\n");
                            rv = kErrHugeJSON;
                            break;
                        }
                        else
                        {
                            szjstokens *= 2;
                            tokens = realloc(tokens, sizeof(jsmntok_t) * szjstokens);
                        }
                    }
                    else if (res == JSMN_ERROR_INVAL)
                    {
                        /* Syntactically incorrect json; bail! */
                        fprintf(stderr, "Invalid json.\n");
                        rv = kErrInvalidJSON;
                        break;
                    }
                    else if (res == JSMN_ERROR_PART)
                    {
                        fprintf(stderr, "The json provided was incomplete.\n");
                        rv = kErrInvalidJSON;
                        break;
                    }
                    else
                    {
                        XASSERT(res == JSMN_SUCCESS);
                        break;
                    }
                }

                if (rv == kErrNone)
                {
                    /* OK, we got tokens. Need to traverse the tree. tokens[0] will be the overall object, so
                     * skip that and start with tokens[1]. */


                    /* First key will be the value of the first prime key, and its value will be the
                     * object whose first key will be the value of the second prime key. Etc. The first
                     * object of the last prime-key value will contain the keywords values to set. Each
                     * keyword value is a key-value pair, where the key is the DRMS keyword name, and the value
                     * is a string representation of the DRMS keyword value. */

                    /* Call SetKeyValues() once for the entire json object. */
                    HContainer_t *pkeyvals = NULL;
                    int nproc;
                    int hasnpk;
                    DRMS_RecordSet_t *chunk = NULL;
                    DRMS_RecordSet_t *final = NULL;
                    int irec;
                    int isubirec;
                    int npkeys;
                    char **pkeys = NULL;

                    pkeys = drms_series_createpkeyarray(drms_env, series, &npkeys, &istat);
                    pkeyvals = hcon_create(sizeof(DRMS_Value_t), DRMS_MAXKEYNAMELEN, NULL, NULL, NULL, NULL, 0);

                    if (pkeyvals)
                    {
                        irec = 0;

                        /* Create batches of records. */
                        final = malloc(sizeof(DRMS_RecordSet_t));
                        memset(final, 0, sizeof(DRMS_RecordSet_t));

                        chunk = drms_create_records(drms_env, kRecChunkSz, series, DRMS_PERMANENT, &istat);

                        /* final will contain the cached records that chunk owns. */
                        final->env = chunk->env;

                        SetNextTokIndex(1); /* Skip the top-level object - it isn't of the same format as the rest. */
                        if (SetKeyValues(drms_env, series, segname, &chunk, final, pkeys, npkeys, pkeyvals, 0, json, tokens, tokens[0].size, tokens[0].size, &nproc, &hasnpk, &irec, setdate))
                        {
                            /* Problem setting keyword values or ingesting SUMS files. */
                            rv = kErrIngest;
                        }
                        else
                        {
                            /* final now owns the records *chunk used to own. */
                            if (chunk)
                            {
                                /* There might be a partially used chunk of records. Move those records into the final record-set.
                                 * (there might be no records to merge, in which case this loop is a no-op). */
                                for (isubirec = 0; isubirec < irec; isubirec++)
                                {
                                    drms_merge_record(final, chunk->records[isubirec]);
                                    chunk->records[isubirec] = NULL;
                                }

                                irec = 0;
                                drms_close_records(chunk, DRMS_FREE_RECORD);
                            }

                            /* Save the output records. */
                            drms_close_records(final, DRMS_INSERT_RECORD);
                        }
                    }
                    else
                    {
                        rv = kErrNoMemory;
                    }

                    drms_series_destroypkeyarray(&pkeys, npkeys);
                }
            }
            else
            {
                rv = kErrNoMemory;
            }
        }
    }
    else
    {
        rv = kErrNoMemory;
    }

    if (tokens)
    {
        free(tokens);
        tokens = NULL;
    }

    if (json)
    {
        free(json);
        json = NULL;
    }

    return rv;
}
