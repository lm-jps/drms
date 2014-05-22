/* This module parses the provided keyword specification. If parsing succeeds (the keyword specification is syntactically correct),
 * then this module prints a text description of all keyword data that can be input directly into the DRMS database. By default, 
 * the output is JSON and has this format:
 * {
 *     "cgem.Lorentz":
 *     {
 *         "cgem.lorentz":[{"bkeys1":"text"},{"bkeyi1":"int4"},{"bkeyi2":"int4"}],
 *         "cgem.drms_keyword":
           [
               {
                   "BKEYS1":
                   {
                       "seriesname":"cgem.Lorentz",
                       "keywordname":"BKEYS1",
                       "linkname":"",
                       "targetkeyw":"",
                       "type":"string",
                       "defaultval":"",
                       "format":"%s",
                       "unit":"none",
                       "islink":"0",
                       "isconstant":"0",
                       "persegment":"196608",
                       "description":"Comma-separated list of NOAA ARs matching this HARP"
                   }
               },
               {
                   "BKEYI1":
                   {
                       "seriesname":"cgem.Lorentz",
                       "keywordname":"BKEYI1",
                       "linkname":"",
                       "targetkeyw":"",
                       "type":"int",
                       "defaultval":"-2147483648",
                       "format":"%d",
                       "unit":"none",
                       "islink":"0",
                       "isconstant":"0",
                       "persegment":"65536",
                       "description":"NOAA AR number that best matches this HARP"
                   }
               },
               {
                   "BKEYI2":
                   {
                       "seriesname":"cgem.Lorentz",
                       "keywordname":"BKEYI2",
                       "linkname":"",
                       "targetkeyw":"",
                       "type":"int",
                       "defaultval":"-2147483648",
                       "format":"%d",
                       "unit":"none",
                       "islink":"0",
                       "isconstant":"0",
                       "persegment":"131072",
                       "description":"Number of NOAA ARs that match this HARP (0 allowed)"
                   }
               }
           ]
 *     }
 * }
 *
 * The keyword desciption must be complete, e.g., if a keyword description requires 8 fields, then all 8 must be provided, even if only
 * 3 of the fields are being modified. */

#include "jsoc_main.h"
#include "json.h"


char *module_name = "drms_parsekeys";

typedef enum
{
    PKSTAT_SUCCESS = 0,
    PKSTAT_FILEIO,
    PKSTAT_NOMEM,
    PKSTAT_CANTMODSERIES,
    PKSTAT_BADKEYDESC,
    PKSTAT_NOTIMPL
} PKSTAT_t;

#define SERIES "series"
#define SPEC   "spec"
#define DOIT   "d"

ModuleArgs_t module_args[] =
{
    {ARG_STRING, SERIES,  NULL,   "The series containing the keywords to modify."},
    {ARG_STRING, SPEC,    NULL,   "The keyword specification(s). This can be a JSD keyword description, or a path to a file that contains JSD keyword descriptions, one per line."},
    {ARG_FLAG, DOIT, NULL, "Commit the changes to the DRMS database."},
    {ARG_END}
};


static void sprintDefVal(DRMS_Keyword_t *key, char *defvalout, size_t size)
{
    char defval[DRMS_DEFVAL_MAXLEN];
    
    if (defvalout)
    {
        if (key->info->type == DRMS_TYPE_TIME)
        {
            drms_sprintfval(defval, key->info->type, &key->value, 1);
        }
        else
        {
            /* We want to store the default value as a text string, with canonical formatting.
             * The conversion used (in drms_record.c) when populating DRMS_Keyword_ts is:
             *   char -> none (first char in stored text string is used)
             *   short -> strtol(..., NULL, 0)
             *   int -> strtol(..., NULL, 0)
             *   long long -> strtoll(..., NULL, 0)
             *   float -> atof
             *   double -> atof
             *   time -> sscan_time
             *   string -> none (copy_string)
             */
            drms_sprintfval(defval, key->info->type, &key->value, 0);

            
            /* If the resulting string is quoted, strip the quotes. */
            if (key->info->type == DRMS_TYPE_STRING)
            {
                size_t slen = strlen(defval);
                int ich;
                
                if (slen > 1)
                {
                    if ((defval[0] == '\'' && defval[slen - 1] == '\'') ||
                        (defval[0] == '\"' && defval[slen - 1] == '\"'))
                    {
                        ich = 0;
                        while(ich < slen - 2)
                        {
                            defval[ich] = defval[ich + 1];
                            ich++;
                        }
                        
                        defval[ich] = '\0';
                    }
                }
            }
        }
        
        snprintf(defvalout, size, "%s", defval);
    }
}

void escapePercent(const char *unescaped, char *escaped, size_t size)
{
    const char *pCh = NULL;
    char *pEscaped = NULL;
    
    for (pCh = unescaped, pEscaped = escaped; pCh && *pCh && pEscaped - escaped < size; pCh++)
    {
        if (*pCh == '%')
        {
            *pEscaped++ = '%';
            *pEscaped++ = '%';
        }
        else
        {
            *pEscaped++ = *pCh;
        }
    }
    
    *pEscaped = '\0';
}

int DoIt(void)
{
    PKSTAT_t rv = PKSTAT_SUCCESS;
    int drmsstat = DRMS_SUCCESS;
    
    const char *series = NULL;
    const char *spec = NULL;
    char serieslc[DRMS_MAXSERIESNAMELEN];
    struct stat stBuf;
    FILE *fptr = NULL;
    char *specin = NULL;
    json_t *root = NULL;
    
    series = cmdparams_get_str(&cmdparams, SERIES, NULL);
    spec = cmdparams_get_str(&cmdparams, SPEC, NULL);
    
    root = json_new_object();

    if (!stat(spec, &stBuf) && S_ISREG(stBuf.st_mode))
    {
        /* This is a file with keyword JSD descriptions, one keyword per line. */
        char rbuf[512];
        size_t nbytes;
        size_t ntot = 0;
        size_t szspec;
        
        fptr = fopen(spec, "r");
        if (fptr)
        {
            szspec = 1024;
            specin = calloc(1, szspec);
            
            if (specin)
            {
                while ((nbytes = fread(rbuf, sizeof(char), sizeof(rbuf) - 1, fptr)) > 0)
                {
                    rbuf[nbytes] = '\0';
                    ntot += nbytes;
                    specin = base_strcatalloc(specin, rbuf, &szspec);
                }
                
                if (ntot != stBuf.st_size)
                {
                    fprintf(stderr, "Error reading file %s.\n", spec);
                    rv = PKSTAT_FILEIO;
                }
            }
            else
            {
                rv = PKSTAT_NOMEM;
            }
            
            fclose(fptr);
        }
        else
        {
            fprintf(stderr, "Unable to open file %s for reading.\n", spec);
            rv = PKSTAT_FILEIO;
        }
    }
    else
    {
        /* This is a parseable string containing one or more newline-separated keyword JSD descriptions. */
        specin = strdup(spec);
    }

    if (rv == PKSTAT_SUCCESS)
    {
        HContainer_t *keys = NULL;
        DRMS_Record_t *seriesTempl = NULL;
        int nkeys;
        
        seriesTempl = drms_template_record(drms_env, series, &drmsstat);
        
        if (!seriesTempl || drmsstat != DRMS_SUCCESS)
        {
            rv = PKSTAT_CANTMODSERIES;
            fprintf(stderr, "Series '%s'does not exist.\n", series);
        }
        
        if (rv == PKSTAT_SUCCESS)
        {
            keys = drms_parse_keyworddesc(drms_env, specin, &drmsstat);
            if (!keys || drmsstat != DRMS_SUCCESS)
            {
                rv = PKSTAT_BADKEYDESC;
                fprintf(stderr, "Unable to parse keyword description. Please fix syntactic errors are re-try.\n");
            }
        }
        
        if (rv == PKSTAT_SUCCESS)
        {
            /* Set the pointer from the key struct to the containing record. */
            HIterator_t *hit = NULL;
            DRMS_Keyword_t *key = NULL;
            DRMS_Keyword_t *origKey = NULL;
            char defval[DRMS_DEFVAL_MAXLEN];
            json_t *seriesObj = NULL;
            json_t *stableArr = NULL;
            json_t *ktableArr = NULL;
            json_t *ktableKeyObj = NULL;
            json_t *jObj = NULL;
            char *namespace = NULL;
            char *table = NULL;
            char drmskeyTable[DRMS_MAXSERIESNAMELEN];
            char keylc[DRMS_MAXKEYNAMELEN];
            char numBuf[64];
            char formatBuf[128];
            int kwflags;
            
            hit = hiter_create(keys);
            
            if (hit)
            {
                nkeys = 0;
                
                snprintf(serieslc, sizeof(serieslc), "%s", series);
                strtolower(serieslc);
                
                get_namespace(serieslc, &namespace, &table);
                snprintf(drmskeyTable, sizeof(drmskeyTable), "%s.%s",namespace, DRMS_MASTER_KEYWORD_TABLE);
                free(namespace);
                namespace = NULL;
                free(table);
                table = NULL;
                

                // printf("%s:\n", series);
                seriesObj = json_new_object();
                json_insert_pair_into_object(root, series, seriesObj);
                
                stableArr = json_new_array();
                json_insert_pair_into_object(seriesObj, serieslc, stableArr);
                
                ktableArr = json_new_array();
                json_insert_pair_into_object(seriesObj, drmskeyTable, ktableArr);

                // printf("\t%s:\n", serieslc);
                
                while((key = (DRMS_Keyword_t *)hiter_getnext(hit)) != NULL)
                {
                    /* First, ensure that the keyword to modify exists. */
                    if (!(origKey = drms_keyword_lookup(seriesTempl, key->info->name, 0)))
                    {
                        fprintf(stderr, "Unable to parse description for keyword '%s'; it does not exist. Skipping.\n", key->info->name);
                        continue;
                    }
                    
                    nkeys++;
                    
                    snprintf(keylc, sizeof(keylc), "%s", key->info->name);
                    strtolower(keylc);

                    /* Print out the series-table information in a text form that can be used directly in SQL. The 
                     * only relevant bit of information in this table is the data type of the column. */
                    // printf("%s\n", db_type_string(drms2dbtype(key->info->type)));
                    jObj = json_new_object();
                    json_insert_pair_into_object(jObj, keylc, json_new_string(db_type_string(drms2dbtype(key->info->type))));
                    json_insert_child(stableArr, jObj);
                    
                    /* Print out the <ns>.drms_keyword information. */
                    ktableKeyObj = json_new_object();
                    jObj = json_new_object();
                    json_insert_pair_into_object(jObj, key->info->name, ktableKeyObj);
                    json_insert_child(ktableArr, jObj);

                    // printf("seriesname: %s\n", series);
                    json_insert_pair_into_object(ktableKeyObj, "seriesname", json_new_string(series));
                    
                    // printf("keywordname: %s\n", key->info->name);
                    json_insert_pair_into_object(ktableKeyObj, "keywordname", json_new_string(key->info->name));
                    
                    // printf("linkname: %s\n", key->info->linkname);
                    json_insert_pair_into_object(ktableKeyObj, "linkname", json_new_string(key->info->linkname));
                    
                    // printf("targetkeyw: %s\n", key->info->target_key);
                    json_insert_pair_into_object(ktableKeyObj, "targetkeyw", json_new_string(key->info->target_key));
                    
                    // printf("type: %s\n", drms_type2str(key->info->type)); /* Use the DRMS type string. */
                    json_insert_pair_into_object(ktableKeyObj, "type", json_new_string(drms_type2str(key->info->type)));
                    
                    sprintDefVal(key, defval, sizeof(defval));
                    // printf("defaultval: %s\n", defval);
                    json_insert_pair_into_object(ktableKeyObj, "defaultval", json_new_string(defval));
                    
                    // printf("format: %s\n", key->info->format);
                    /* Must escape '%' chars. */
                    escapePercent(key->info->format, formatBuf, sizeof(formatBuf));
                    json_insert_pair_into_object(ktableKeyObj, "format", json_new_string(formatBuf));
                    
                    // printf("unit: %s\n", key->info->unit);
                    json_insert_pair_into_object(ktableKeyObj, "unit", json_new_string(key->info->unit));
                    
                    // printf("islink: %d\n", key->info->islink);
                    snprintf(numBuf, sizeof(numBuf), "%d", key->info->islink);
                    json_insert_pair_into_object(ktableKeyObj, "islink", json_new_string(numBuf));
                    
                    // printf("isconstant: %d\n", (int)key->info->recscope);
                    snprintf(numBuf, sizeof(numBuf), "%d", (int)key->info->recscope);
                    json_insert_pair_into_object(ktableKeyObj, "isconstant", json_new_string(numBuf));
                    
                    // printf("persegment: %d\n", key->info->kwflags);
                    /* Must OR this value with the rank of the keyword (which is stored in the upper 16 bits of the kwflags field). 
                     * Ignore the rank that drms_parser assigned to this keyword (it will take the max rank of the current set of
                     * keywords, and add 1). */
                    kwflags = (origKey->info->kwflags & 0xFFFF0000) | (key->info->kwflags & 0x0000FFFF);
                    snprintf(numBuf, sizeof(numBuf), "%d", kwflags);
                    json_insert_pair_into_object(ktableKeyObj, "persegment", json_new_string(numBuf));
                    
                    // printf("description: %s\n", key->info->description);
                    json_insert_pair_into_object(ktableKeyObj, "description", json_new_string(key->info->description));
                }
                
                hiter_destroy(&hit);
            }
            else
            {
                rv  = PKSTAT_NOMEM;
            }
        }
    }
    
    if (rv == PKSTAT_SUCCESS)
    {
        char *jsonStr = NULL;
        
        json_tree_to_string(root, &jsonStr);
        printf(jsonStr);
        free(jsonStr);
        printf("\n");
    }
    
    if (root)
    {
        json_free_value(&root);
    }
    
    if (specin)
    {
        free(specin);
    }
    
    return rv;
}
