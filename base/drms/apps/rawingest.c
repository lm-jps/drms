/* */

#include "jsmn.h"
#include "jsoc_main.h"

char *module_name = "rawingest";

#define kMaxJSONTokens 65536 /* A very large JSON string. */
#define kRecChunkSz 128

#define kArgTest "t"
#define kArgNotSpec "not specified"
#define kArgSeries "series"
#define kArgSegment "segment"

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
        drms_strval(type, &((*val)->value), keyval);
        
        *nchild = 0; /* The property value is a primitive - no objects. */
    }
    
    return 0;
}

int GetNextKeyAndValue(DRMS_Record_t *orec, const char *json, jsmntok_t *tokens, int ntoks, char **key, DRMS_Value_t **val, int *nchild)
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
         else
         {
            /* Use the name that lib DRMS derives. */
            drms_segment_filename(seg, outfile);
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

#if 0
{
   <harpnum> : 
   {
      <t_rec> :
      {
         "keys" : 
         {
            "key1" : <val1>,
            "key2" : <val2>
         },
         "file" : <path>
      }
   }
}
#endif

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
                 int *irec)
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
            int subnproc;
            int subhasnpk;
            const char *pkeyname = NULL;
            
            
            /* Since the keys property contains an object, val will be NULL when key is "keys". */
            if (strcasecmp(key, "keys") == 0)
            {
                *hasnpk = 1;
                
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
                drms_strval(keytype, &(pkeyval.value), key);
                hcon_insert_lower(pkeyvals, pkeys[pklev], &pkeyval);
                
                if (SetKeyValues(env, series, segname, chunk, final, pkeys, npkeys, pkeyvals, pklev + 1, json, tokens, ntoks, nchild, &subnproc, &subhasnpk, irec))
                {
                    rv = 1;
                    break;
                }
                
                nproc += 2;
                
                /* We completely processed an object. This means that we're done with a DRMS record
                 * and we are moving on to the next one. */
                
                if (subhasnpk)
                {
                    /* loop through prime keys, setting all values for this record. */
                    HIterator_t *hit = NULL;
                    hit = hiter_create(pkeyvals);
                    
                    while ((ppkeyval = hiter_extgetnext(hit, &pkeyname)) != NULL)
                    {
                        drms_setkey_p(orec, pkeyname, ppkeyval);
                    }
                    
                    (*irec)++;
                    
                    if (*irec > (*chunk)->n - 1)
                    {
                        int nrecs = *irec + 1;
                        int isubirec;
                        
                        /* Merge into final. */
                        for (isubirec = 0; isubirec < nrecs; isubirec++)
                        {
                            drms_merge_record(final, (*chunk)->records[isubirec]);
                            (*chunk)->records[isubirec] = NULL;
                        }
                        
                        /* final now owns the records *chunk used to own. */
                        drms_close_records(*chunk, DRMS_FREE_RECORD);
                        
                        /* Get a new chunk */
                        *chunk = drms_create_records(env, kRecChunkSz, series, DRMS_PERMANENT, &istat);
                        *irec = 0;
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
    int istat;
    RIStatus_t rv;
    
    rv = kErrNone;
    
    json = calloc(1, szjson);
    
    if (json)
    {
        series = cmdparams_get_str(&cmdparams, kArgSeries, &istat);
        segname = cmdparams_get_str(&cmdparams, kArgSegment, &istat);
        testfile = cmdparams_get_str(&cmdparams, kArgTest, &istat);
        
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
                        SetNextTokIndex(1); /* Skip the top-level object - it isn't of the same format as the rest. */
                        if (SetKeyValues(drms_env, series, segname, &chunk, final, pkeys, npkeys, pkeyvals, 0, json, tokens, tokens[0].size, tokens[0].size, &nproc, &hasnpk, &irec))
                        {
                           /* Problem setting keyword values or ingesting SUMS files. */
                           rv = kErrIngest;
                        }
                        else
                        {
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
    
    return 1;
    exit(rv);
}

#if 0
nelem = (tokens[0]->size) / 2; /* This could be wrong - might need to use the size field
                                * to calculate the number of elements. */

for (ielem = 0; ielem < (tokens[itok]->size) / 2; ielem++)
{   
    for (ipkey = 0; ipkey < npkeys; ipkey++)
    {
        GetKeyAndValue(tokens, 0, &key, &val);
        drms_setkey_p(orec, pkey[ikey], stringval(key));
        
        
    }
}





itok = 1;
while (something)
{
    for (ipkey = 0; ipkey < npkeys; ipkey++)
    {
        GetKeyAndValue(tokens, itok, &key, &val);
        itok += 2;
        drms_setkey_p(orec, pkey[ikey], stringval(key));
    }
    
    /* Now get the DRMS keyword value key-value pairs. */
    if (val->type == JSMN_OBJECT)
    {
        nelem = (val->size) / 2; /* The values come in key-value pairs. */
        
        for (ipkey = 0; ipkey < nelem; ipkey++)
        {
            GetNextKeyAndValue(tokens, &key, &val);
            itok += 2;
            if (val->type == JSMN_PRIMITIVE || val->type == JSMN_STRING)
            {
                /* DRMS keyword values are ints, floats, strings, etc. key is the 
                 * DRMS-keyword name, and val is the keyword's value (a string
                 * representation of an int, float, string, etc.) */
                GetTokenStrData(tokens[], );
                drms_setkey_p(orec, );
                
            }
            else
            {
                fprintf(stderr, "Invalid input - expecting a primitive or string value.\n");                                    
            }
        }
    }
    else
    {
        fprintf(stderr, "Invalid input - expecting a DRMS key-value json object.\n");
        
    }
}

int length = key.end - key.start;
#endif
