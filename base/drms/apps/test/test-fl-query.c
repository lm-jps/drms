#include "jsoc_main.h"
#include "drms_types.h"

char *module_name = "DRMS_QUERY_FL_TEST";

typedef enum
{
   kDQFLErr_Success = 0,
   kDQFLErr_Error = 1
} DQFLError_t;

#define kRecSetIn      "recsin"

ModuleArgs_t module_args[] =
{
     {ARG_STRING, kRecSetIn, "",  "Input data series."},
     {ARG_END}
};

int DoIt(void)
{
    int status = DRMS_SUCCESS;
    const char *rsin = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
    int ncols;
    int nrows; 
    int icol;
    int irow;
    DRMS_Record_t *template = NULL;
    char *keyBuf = NULL;
    size_t szKeyBuf;
    DRMS_Array_t *data = NULL;
    DQFLError_t rv = kDQFLErr_Success;

    char *allvers = NULL; /* If 'y', then don't do a 'group by' on the primekey value.
                           * The rationale for this is to allow users to get all versions
                           * of the requested DRMS records */
    char **sets = NULL;
    DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
    char **snames = NULL;
    char **filts = NULL;
    int nsets = 0;
    DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */

    if (drms_record_parserecsetspec(rsin, &allvers, &sets, &settypes, &snames, &filts, &nsets, &rsinfo) != DRMS_SUCCESS)
    {
        rv = kDQFLErr_Error;
    }
    else
    {
        /* Ignore all record subsets after the first one. */
        template = drms_template_record(drms_env, snames[0], &status);

        drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);

        if (!template || status != DRMS_SUCCESS)
        {
            rv = kDQFLErr_Error;
        }
        else
        {
            /* Use some random keywords. Got to make a comma-separated list from the container of keywords (which 
             * will then be converted back into a container in lib DRMS). */
            HIterator_t *last = NULL;
            DRMS_Keyword_t *key = NULL;

            szKeyBuf = 1024;
            keyBuf = calloc(1, szKeyBuf);
            XASSERT(keyBuf);

            /* 3 keywords max. */
            int start = ceil(hcon_size(&template->keywords) * 0.15); /* Choose this as the first keyword. */
            int skip = 2; /* Number of keywords skipped between keywords chosen. */
            int nKeys;
            int skipCount;
            
            if (start + skip * 3 > hcon_size(&template->keywords))
            {
                start = hcon_size(&template->keywords);
                skip = 0;
                nKeys = 1;
            }
            else
            {
                nKeys = 3;
            }

            skipCount = 0;
            while (((key = drms_record_nextkey(template, &last, 0)) != NULL) && nKeys > 0)
            {
                if (start > 1)
                {
                    start--;
                }
                else
                {
                    if (skipCount == 0)
                    {
                        /* drms_record_nextkey() does not work with contant keywords. Just skip those. */
                        if (!drms_keyword_isconstant(key))
                        {                        
                            keyBuf = base_strcatalloc(keyBuf, key->info->name, &szKeyBuf); XASSERT(keyBuf);

                            if (nKeys > 1)
                            {
                                keyBuf = base_strcatalloc(keyBuf, ", ", &szKeyBuf); XASSERT(keyBuf);
                            }
                        }

                        nKeys--;
                        
                        skipCount = skip;
                    }
                    else
                    {
                        skipCount--;
                    }
                }
            }

            if (last)
            {
                hiter_destroy(&last);
            }

            /* Coerce to DRMS_TYPE_DOUBLE - who care what type we use. */
            data = drms_record_getvector(drms_env, rsin, keyBuf, DRMS_TYPE_STRING, 1, &status);

            if (data && status == DRMS_SUCCESS)
            {
                ncols = data->axis[0]; /* keywords */
                nrows = data->axis[1]; /* records */
        
                /* column-major ordering - so data->data[0] is the first column. */
                for (irow = 0; irow < nrows; irow++)
                {
                    for (icol = 0; icol < ncols; icol++)                    
                    {
                        printf("%s ", ((char **)data->data)[icol * nrows + irow]);
                    }
                    
                    printf("\n");
                }
            }
            else
            {
                rv = kDQFLErr_Error;
            }
        }
    }

   return rv;
}
