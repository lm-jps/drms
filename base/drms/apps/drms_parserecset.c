/* Parse a DRMS record-set specification into components (series name, filters, etc.). This module returns results in a JSON object:
 *
 * { 
 *    "spec"      : "hmi.MEharp_720s[][2014.5.12/10m][! 1=1 !]{field},hmi.M_720s[2014.5.12/10m]",
 *    "atfile"    : false,
 *    "hasfilts"  : true,
 *    "nsubsets"  : 2,
 *    "subsets"   :
 *    [
 *        {
 *            "spec"       : "hmi.MEharp_720s[][2014.5.12/10m][! 1=1 !]{field}"
 *            "settype"    : "drms"
 *            "seriesname" : "hmi.MEharp_720s",
 *            "seriesns"   : "hmi",
 *            "seriestab"  : "meharp_720s",
 *            "filter"     : "[][2014.5.12/10m]",
 *            "segments"   : "{field}"
 *            "autobang"   : true,
 *        },
 *        {
 *            "spec"       : "hmi.M_720s[2014.5.12/10m]"
 *            "settype"    : "drms"
 *            "seriesname" : "hmi.M_720s",
 *            "seriesns"   : "hmi",
 *            "seriestab"  : "m_720s",
 *            "filter"     : "[2014.5.12/10m]",
 *            "autobang"   : false,
 *        }
 *    ],
 *    "errMsg" : null
 * }
 */



#include "drms.h"
#include "jsoc_main.h"
#include "json.h"
#include "qDecoder.h"


char *module_name = "drms_parserecset";

typedef enum
{
    PRSSTAT_SUCCESS = 0,
    PRSSTAT_MISSINGARG,
    PRSSTAT_NOMEM,
    PRSSTAT_CANTPARSE,
    PRSSTAT_JSONELEM
} PRSSTAT_t;

#define SPEC          "spec"
#define NOT_SPECIFIED "NOT_SPECIFIED"

ModuleArgs_t module_args[] =
{
    {ARG_STRING, SPEC,    NOT_SPECIFIED,   "The record-set specification to be parsed into parts."},
    {ARG_END}
};


int DoIt(void)
{
    PRSSTAT_t rv = PRSSTAT_SUCCESS;
    
    const char *specArg = NULL;
    char *spec = NULL;
    char *allvers = NULL;
    char **sets = NULL;
    DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
    char **snames = NULL;
    char **filts = NULL;
    char **segs = NULL;
    int nsets = 0;
    DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
    
    const char *errMsg = NULL;
    json_t *root = NULL;
    json_t *subsets = NULL;
    json_t *elem = NULL;

    Q_ENTRY *req = NULL;
    
    char *escapedStr = NULL;

    root = json_new_object();
    
    if (root)
    {
        specArg = cmdparams_get_str(&cmdparams, SPEC, NULL);
        if (strcmp(specArg, NOT_SPECIFIED) == 0)
        {
           /* spec was not provided on the cmd-line. Assume this program was invoked in response to an HTTP GET request. 
            * If this is true, then the environment variable QUERY_STRING will contain the spec argument.
            */
           req = qCgiRequestParseQueries(NULL, NULL);
           if (req)
           {
              char *value = NULL;

              value = (char *)qEntryGetStr(req, SPEC);
              if (value)
              {
                 spec = strdup(value);
                 if (!spec)
                 {
                    rv = PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create spec string.";
                 }
              }
              else
              {
                 rv = PRSSTAT_MISSINGARG;
                 errMsg = "Missing required argument 'spec'.";
              }
           }
        }
        else
        {
           spec = strdup(specArg);
           if (!spec)
           {
              rv = PRSSTAT_NOMEM;
              errMsg = "Not enough memory to create spec string.";
           }
        }

        if (rv == PRSSTAT_SUCCESS)
        {
           rv = (drms_record_parserecsetspec_plussegs(spec, &allvers, &sets, &settypes, &snames, &filts, &segs, &nsets, &rsinfo) == 0) ? PRSSTAT_SUCCESS : PRSSTAT_CANTPARSE;
           errMsg = "Unable to parse specification.";
        }
        
        if (rv == PRSSTAT_SUCCESS)
        {
            /* json_insert_pair_into_object() will fail if json_new_...() fails. */
            escapedStr = json_escape(spec);
            if (!escapedStr)
            {
                rv = PRSSTAT_NOMEM;
                errMsg = "Not enough memory to create spec string.";
            }
            
            if (rv == PRSSTAT_SUCCESS)
            {
                rv = (json_insert_pair_into_object(root, "spec", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                free(escapedStr);
            }
            
            if (rv == PRSSTAT_SUCCESS)
            {
                /* True implies that the specification is an "at file" specification. */
                rv = (json_insert_pair_into_object(root, "atfile", ((rsinfo & kAtFile) != 0) ? json_new_true() : json_new_false()) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                errMsg = "Unable to create atfile property.";
            }
            
            if (rv == PRSSTAT_SUCCESS)
            {
                /* True implies that the specification contains at least one filter. */
                rv = (json_insert_pair_into_object(root, "hasfilts", ((rsinfo & kFilters) != 0) ? json_new_true() : json_new_false())) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                errMsg = "Unable to create hasfilts property.";
            }
            
            if (rv == PRSSTAT_SUCCESS)
            {
                /* json_new_number() takes a string - why? */
                char numBuf[64];
                
                snprintf(numBuf, sizeof(numBuf), "%d", nsets);
                rv = (json_insert_pair_into_object(root, "nsubsets", json_new_number(numBuf)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                errMsg = "Unable to create nsubsets property.";
            }
            
            if (rv == PRSSTAT_SUCCESS)
            {
                subsets = json_new_array();
            }
            
            if (subsets)
            {
                int iSet;
                char *ns = NULL;
                char *tab = NULL;
                
                for (iSet = 0; iSet < nsets; iSet++)
                {
                    if (rv == PRSSTAT_SUCCESS)
                    {
                        rv = (elem = json_new_object()) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                        errMsg = "Not enough memory to create JSON subset object.";
                    }
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (escapedStr = json_escape(sets[iSet])) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create spec string.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (json_insert_pair_into_object(elem, "spec", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create spec property in subset object.";
                    free(escapedStr);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (escapedStr = json_escape(drms_type_recsetnames[settypes[iSet]])) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create set-type string.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (json_insert_pair_into_object(elem, "settype", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create settype property in subset object.";
                    free(escapedStr);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (escapedStr = json_escape(snames[iSet])) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create series-name string.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (json_insert_pair_into_object(elem, "seriesname", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create seriesname property in subset object.";
                    free(escapedStr);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (!base_nsAndTab(snames[iSet], &ns, &tab)) ? PRSSTAT_SUCCESS : PRSSTAT_CANTPARSE;
                    errMsg = "Invalid series name.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    strtolower(ns);
                    rv = (escapedStr = json_escape(ns)) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create series-namespace string.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (json_insert_pair_into_object(elem, "seriesns", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create seriesns property in subset object.";
                    free(escapedStr);
                    free(ns);
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    strtolower(tab);
                    rv = (escapedStr = json_escape(tab)) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                    errMsg = "Not enough memory to create series-table string.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (json_insert_pair_into_object(elem, "seriestab", json_new_string(escapedStr)) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create seriestab property in subset object.";
                    free(escapedStr);
                    free(tab);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    if (filts[iSet])
                    {
                        rv = (escapedStr = json_escape(filts[iSet])) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                        errMsg = "Not enough memory to create filter string.";
                    }
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (json_insert_pair_into_object(elem, "filter", (escapedStr != NULL) ? json_new_string(escapedStr) : json_new_null()) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create filter property in subset object.";
                    free(escapedStr);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    if (segs[iSet])
                    {
                        rv = (escapedStr = json_escape(segs[iSet])) ? PRSSTAT_SUCCESS : PRSSTAT_NOMEM;
                        errMsg = "Not enough memory to create segment-list string.";                          
                    }

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (json_insert_pair_into_object(elem, "segments", (escapedStr != NULL) ? json_new_string(escapedStr) : json_new_null()) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create segments property in subset object.";
                    free(escapedStr);

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }

                    rv = (json_insert_pair_into_object(elem, "autobang", (allvers[iSet] == 'Y' ||  allvers[iSet] == 'y') ? json_new_true() : json_new_false()) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to create autobang property in subset object.";

                    if (rv != PRSSTAT_SUCCESS)
                    {
                        break;
                    }
                    
                    rv = (json_insert_child(subsets, elem) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Unable to insert a set into the subsets array.";
                    
                    if (rv != PRSSTAT_SUCCESS)
                    {    
                        break;
                    }
                } // loop over subsets
                
                if (rv == PRSSTAT_SUCCESS)
                {
                    rv = (json_insert_pair_into_object(root, "subsets", subsets) == JSON_OK) ? PRSSTAT_SUCCESS : PRSSTAT_JSONELEM;
                    errMsg = "Not enough memory to create JSON subsets array.";
                }
            }
            else
            {
                rv = PRSSTAT_NOMEM;
                errMsg = "Not enough memory to create JSON subsets array.";
            }
        }

        free(spec);
        spec = NULL;
        
        drms_record_freerecsetspecarr_plussegs(&allvers, &sets, &settypes, &snames, &filts, &segs, nsets);
    }
    else
    {
        rv = PRSSTAT_NOMEM;
        errMsg = "Not enough memory to create JSON root object.";
    }
    
    /* Since this is always run as a CGI, always return 0, but provide an error message if there was an error. */
    if (rv != PRSSTAT_SUCCESS)
    {
        if (errMsg)
        {
            fprintf(stderr, "%s\n", errMsg);
        }
    
        json_free_value(&root);
        root = json_new_object();
        
        if (root)
        {
            escapedStr = json_escape(errMsg);
            if (escapedStr)
            {
                if (json_insert_pair_into_object(root, "errMsg", json_new_string(escapedStr)) != JSON_OK)
                {
                    fprintf(stderr, "%s\n", "Unable to insert errMsg into JSON root object");
                }

                free(escapedStr);
            }
            else
            {
                fprintf(stderr, "%s\n", "Not enough memory to create error-message string.");
            }
        }
        else
        {
            fprintf(stderr, "%s\n", "Not enough memory to create JSON root object.");
        }
    }
    else
    {
        if (json_insert_pair_into_object(root, "errMsg", json_new_null()) != JSON_OK)
        {
            fprintf(stderr, "Unable to insert errMsg into JSON root object");
        }
    }
    
    char *jsonStr = NULL;
    
    json_tree_to_string(root, &jsonStr);
    json_free_value(&root);
    printf(jsonStr);
    free(jsonStr);
    printf("\n");

    return 0;
}
