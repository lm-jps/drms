#include "jsoc_main.h"

char *module_name = "jsoc_export_clone";

typedef enum
{
    kExpCloneErr_Success = 0,
    kExpCloneErr_Argument,
    kExpCloneErr_UnknownSeries,
    kExpCloneErr_OutOfMemory,
    kExpCloneErr_NoTemplate,
    kExpCloneErr_CantCreateProto,
    kExpCloneErr_CantCreateSeries,
    kExpCloneErr_CantParseKeyDesc,
    kExpCloneErr_LibDRMS,
    kExpCloneErr_CantFollowLink
} ExpError_t;

#define kArgSeriesIn   "dsin"
#define kArgSeriesOut  "dsout"
#define kArgRetention  "ret"
#define kArgArchive    "arch"
#define kNotSpec       "NOTSPECIFIED"

/* Possibly new keywords for series being created. */
#define kKeyReqID      "RequestID"
#define kKeyHistory    "HISTORY"
#define kKeyComment    "COMMENT"
#define kKeySource     "SOURCE"

ModuleArgs_t module_args[] =
{
    {ARG_STRING,  kArgSeriesIn,  NULL,       "Input series name."},
    {ARG_STRING,  kArgSeriesOut, kNotSpec,   "(Optional) Output series name."},
    {ARG_INT,     kArgRetention, "10",       "(Optional) Output-series' SU retention."},
    {ARG_STRING,  kArgArchive,   "-1",       "(Optional) Output-series' SU archive flag."},
    {ARG_END,     NULL,          NULL,       NULL}
};

static DRMS_Record_t *CopySeriesTemplate(DRMS_Env_t *env, const char *in, ExpError_t *status)
{
    ExpError_t err = kExpCloneErr_Success;
    int drmsstat = DRMS_SUCCESS;
    
    DRMS_Record_t *proto = NULL;
    DRMS_Record_t *template = NULL; 
    
    /* Ensure series exists. */
    if (!drms_series_exists(env, in, &drmsstat) || drmsstat)
    {
        fprintf(stderr, "Input series '%s' does not exist.\n", in);
        err = kExpCloneErr_UnknownSeries;
    }
    else
    {
        /* Get the read-only input series template record. */
        template = drms_template_record(env, in, &drmsstat);
        
        if (!template || drmsstat)
        {
            fprintf(stderr, "Unable to obtain template record for series '%s'.\n", in);
            err = kExpCloneErr_NoTemplate;
        }
        else
        {
            /* drms_create_recproto() SHALLOW-COPIES the keyword->info structs! */
            proto = drms_create_recproto(template, &drmsstat);
        }
        
        if (!proto || drmsstat)
        {
            fprintf(stderr, "Unable to obtain record prototype for series '%s'.\n", in);
            err = kExpCloneErr_CantCreateProto;
        }
    }
    
    if (status)
    {
        *status = err;
    }
    
    return proto;
}

static ExpError_t AddAKey(const char *keyname, 
                          DRMS_Record_t *prototype, 
                          const char *desc,
                          int intprime,
                          int extprime,
                          int implicit,
                          int rank)
{
    ExpError_t rv = kExpCloneErr_Success;
    int drmsstat = DRMS_SUCCESS;
    
    if (!drms_keyword_lookup(prototype, keyname, 0))
    {
        DRMS_Keyword_t *tKey = NULL;
        DRMS_Keyword_t finalkey;
        HContainer_t *keys = NULL;
        HIterator_t *hit = NULL;
        
        keys = drms_parse_keyworddesc(prototype->env, desc, &drmsstat);
        if (!keys || drmsstat)
        {
            fprintf(stderr, "Failed to parse keyword description '%s'.\n", desc);
            rv = kExpCloneErr_CantParseKeyDesc;
        }
        else if (hcon_size(keys) == 1)
        {
            /* Set the pointer from the key struct to the containing record. */
            hit = hiter_create(keys);
            
            if (hit)
            {
                tKey = (DRMS_Keyword_t *)hiter_getnext(hit);
                tKey->record = prototype;
                
                /* Set the keyword's rank. */
                tKey->info->rank = rank; /* 0-based */
                tKey->info->kwflags |= (rank + 1) << 16; /* 1-based - does directly into db. */
                
                /* Set 'prime' flags. */
                if (intprime)
                {
                    drms_keyword_setintprime(tKey);
                }
                
                if (extprime)
                {
                    drms_keyword_setextprime(tKey);
                }

                if (implicit)
                {
                   drms_keyword_setimplicit(tKey);
                }
                
                /* Put the key into the prototype's keyword container. But first copy the keyword info struct to 
                 * a new struct. When we free the keys container, this will cause tKey->info 
                 * to be freed (keys was set up with a deep-free function - see drms_free_template_keyword_struct). 
                 * Unlike the case for finalkey.value, finalkey.info will NOT be copied into prototype->keywords. */
                finalkey.record = tKey->record;
                finalkey.info = malloc(sizeof(DRMS_KeywordInfo_t));
                *(finalkey.info) = *(tKey->info);
                finalkey.value = tKey->value;
                
                /* hcon_insert_lower() will COPY finalkey.value.string_val because drms_copy_keyword_struct() 
                 * was used as the deep_copy function when prototype->keywords was set up. It WILL NOT 
                 * copy finalkey.info though. */
                hcon_insert_lower(&prototype->keywords, keyname, &finalkey);
                
                if (intprime)
                {
                    /* When setting up pointer to prime key, must use the key in prototype->keywords. */
                    tKey = (DRMS_Keyword_t *)hcon_lookup_lower(&prototype->keywords, keyname);
                    if (tKey)
                    {
                        prototype->seriesinfo->pidx_keywords[prototype->seriesinfo->pidx_num++] = tKey;
                    }
                    else
                    {
                        rv = kExpCloneErr_LibDRMS;
                    }
                }
                
                hiter_destroy(&hit);
            }
        }
        else
        {
            /* Error */
            fprintf(stderr, "Failed to parse keyword description '%s'.\n", desc);
            rv = kExpCloneErr_CantParseKeyDesc;
        }
        
        if (keys)
        {
            /* Free the keys container. This will deep-free any string values, AND it will free the info
             * struct. */
            hcon_destroy(&keys);
        }
    }
    
    return rv;
}

int DoIt(void) 
{
    ExpError_t err = kExpCloneErr_Success;
    
    int drmsstat = DRMS_SUCCESS;
    char *seriesin = NULL;
    const char *seriesout = NULL;
    char *name = NULL;
    int retention = -1;
    int archive = -1;
    DRMS_Record_t *copy = NULL;
    DRMS_Segment_t *seg = NULL;
    HIterator_t *lastseg = NULL;
    int hirank = -1;
    int exists = 0;
    
    /* seriesin is the input series. */
    seriesin = strdup(cmdparams_get_str(&cmdparams, kArgSeriesIn, NULL));
    
    /* seriesout is the name of the series to create. */
    seriesout = cmdparams_get_str(&cmdparams, kArgSeriesOut, NULL);
    
    if (strcmp(seriesout, kNotSpec) == 0)
    {
        /* If name is not specified on the cmd-line, then default to concatenating 
         * seriesin and "_mod". */
        size_t nsize = strlen(seriesin) + 16;
        name = calloc(nsize, 1);
        name = base_strcatalloc(name, seriesin, &nsize);
        name = base_strcatalloc(name, "_mod", &nsize);
    }
    else
    {
        name = strdup(seriesout);
    }
    
    exists = drms_series_exists(drms_env, name, &drmsstat);
    
    if (drmsstat && drmsstat != DRMS_ERROR_UNKNOWNSERIES)
    {
        fprintf(stderr, "Unable to check for series '%s' existence; bailing out.\n", name);
        err = kExpCloneErr_LibDRMS;
    }
    else if (!exists)
    {
        /* retention is name's jsd retention value. */
        retention = cmdparams_get_int(&cmdparams, kArgRetention, NULL);
        
        /* archive is name's jsd archive value. */
        archive = cmdparams_get_int(&cmdparams, kArgArchive, NULL);
        
        /* Get a COPY of the input series template record. */
        copy = CopySeriesTemplate(drms_env, seriesin, &err);
        
        if (!copy || err)
        {
            fprintf(stderr, "Unable to copy template record for series '%s'.\n", seriesin);
            if (!err)
            {
                err = kExpCloneErr_CantCreateProto;
            }
        }
        else
        {
            /* copy has has a copy of seriesin's seriesinfo, which has the name of the original series, 
             * not the output series. Overwrite copy's seriesinfo's name with the the output series' name. */
            snprintf(copy->seriesinfo->seriesname, sizeof(copy->seriesinfo->seriesname), "%s", name);
            
            /* unitsize will match the unitsize of the input series. */
            /* tapegroup will match the tapegroup of the input series, but will not
             * matter, since archive == -1. */
            copy->seriesinfo->archive = archive;
            copy->seriesinfo->retention = retention;
            
            hirank = drms_series_gethighestkeyrank(drms_env, seriesin, &drmsstat);
            if (drmsstat || hirank == -1)
            {
                hirank = 0;
            }
            
            /* Add prime keyword RequestID, if it doesn't already exist. */
            err = AddAKey(kKeyReqID, 
                          copy, 
                          "Keyword:RequestID, string, variable, record, \"Invalid RequestID\", %s, NA, \"The export request identifier, if this record was inserted while an export was being processed.\"", 
                          1, 
                          1, 
                          0, 
                          1 + hirank++);
            
            /* Add keywords HISTORY and COMMENT, if they don't exist. */
            if (err == kExpCloneErr_Success)
            {
                err = AddAKey(kKeyHistory, 
                              copy, 
                              "Keyword:HISTORY, string, variable, record, \"No history\", %s, NA, \"The processing history of the data.\"", 
                              0, 
                              0, 
                              0, 
                              1 + hirank++);
            }
            
            if (err == kExpCloneErr_Success)
            {
                err = AddAKey(kKeyComment, 
                              copy, 
                              "Keyword:COMMENT, string, variable, record, \"No comment\", %s, NA, \"Commentary on the data processing.\"", 
                              0, 
                              0, 
                              0, 
                              1 + hirank++);
            }
            
            if (err == kExpCloneErr_Success)
            {
                err = AddAKey(kKeySource,
                              copy,
                              "Keyword:SOURCE, string, variable, record, \"No source\", %s, NA, \"Input record record-set specification.\"",
                              0,
                              0, 
                              0, 
                              1 + hirank++);
            }

            /* If the first input FITS data segment does not have a VARDIM segment scope, then make it so. */
            if (err == kExpCloneErr_Success)
            {
                DRMS_Link_t *link = NULL;
                DRMS_Record_t *tRec = NULL;
                DRMS_Segment_t *tSeg = NULL;
                char oSegName[DRMS_MAXSEGNAMELEN];
                int segnum = 0;
                char segkeybuf[512];
                char segkeyname[DRMS_MAXKEYNAMELEN];
                
                /* We're examine template segments. If a segment is linked to another series, then 
                 * we can't use drms_segment_lookup() to find the target link, because the target
                 * for a linked segment is not set if the segment is a template. Instead, we have
                 * to obtain the link structure (the template segment has a field, seg->info->linkname, 
                 * which identifies the name of the link pointing to the target series). The link
                 * structure has a field, link->info->target_series, which identifies the series
                 * that contains the actual link-target segment. The name of the segment in the target 
                 * series is specified in the source segment structure, in the seg->info->target_seg
                 * field. */
                while ((seg = drms_record_nextseg(copy, &lastseg, 0)))
                {
                    /* The segments will have been sorted by increasing segnum. */
                    if (seg->info->islink)
                    {
                        /* If this segment was originally a linked segment, then we need to replace it
                         * with the target of the link. */
                        link = hcon_lookup_lower(&copy->links, seg->info->linkname);
                        
                        if (!link)
                        {
                            fprintf(stderr, "Unable to obtain link %s.\n", seg->info->linkname);
                            err = kExpCloneErr_CantFollowLink;
                            break;
                        }
                        else
                        {
                            tRec = drms_template_record(drms_env, link->info->target_series, &drmsstat);
                            
                            if (drmsstat != DRMS_SUCCESS || !tRec)
                            {
                                fprintf(stderr, "Unable to obtain template record for series %s.\n", link->info->target_series);
                                err = kExpCloneErr_LibDRMS;
                                break;
                            }
                            else
                            {
                                tSeg = drms_segment_lookup(tRec, seg->info->target_seg);
                                
                                if (!tSeg)
                                {
                                    fprintf(stderr, "Unable to follow link to target segment.\n");
                                    err = kExpCloneErr_CantFollowLink;
                                    break;
                                }

                                /* Need to use the name of the segment in the original series, not the name in the 
                                 * target series, so save the original name before overwriting. */
                                snprintf(oSegName, sizeof(oSegName), "%s", seg->info->name);

                                /* Need to save the original segnum as well, since tSeg->info->segnum is for the 
                                * target series, not the source series. */
                                segnum = seg->info->segnum;
                                
                                /* Copy - must deep copy the info struct, since it will be freed during shutdown. */
                                if (seg->info)
                                {
                                    free(seg->info);
                                }
                                
                                *seg = *tSeg;
                                seg->info = malloc(sizeof(DRMS_SegmentInfo_t));
                                
                                if (!seg->info)
                                {
                                    err = kExpCloneErr_OutOfMemory;
                                    break;
                                }
                                else
                                {
                                    *seg->info = *tSeg->info;
                                    
                                    /* Need to set record field to point to copy; seg->info points to the original 
                                     * series seg->info struct. */
                                    seg->record = copy;
                                    
                                    /* Copy the saved original segment's info to the new series' segment. */
                                    snprintf(seg->info->name, sizeof(seg->info->name), "%s", oSegName);
                                    seg->info->segnum = segnum;
                                }
                            }
                        }

                        /* Have to add bzero, bscale, and cparm keywords keyowrds for each output segment. These 
                         * keywords do not exist in the original series, since these segments are linked to 
                         * a target series. */
                        if (err == kExpCloneErr_Success)
                        {
                           snprintf(segkeyname, sizeof(segkeyname), "%s_bzero", seg->info->name);
                           snprintf(segkeybuf, sizeof(segkeybuf), "Keyword:%s, double, variable, record, %f, %%g, none, \"\"", segkeyname, seg->bzero);
                           err = AddAKey(segkeyname,
                                         copy,
                                         segkeybuf, 
                                         0,
                                         0,
                                         1,
                                         1 + hirank++);
                        }

                        if (err == kExpCloneErr_Success)
                        {
                           snprintf(segkeyname, sizeof(segkeyname), "%s_bscale", seg->info->name);
                           snprintf(segkeybuf, sizeof(segkeybuf), "Keyword:%s, double, variable, record, %f, %%g, none, \"\"", segkeyname, seg->bscale);
                           err = AddAKey(segkeyname,
                                         copy,
                                         segkeybuf, 
                                         0,
                                         0,
                                         1,
                                         1 + hirank++);
                        }

                        if (err == kExpCloneErr_Success)
                        {
                           snprintf(segkeyname, sizeof(segkeyname), "cparms_sg%03d", seg->info->segnum);
                           snprintf(segkeybuf, sizeof(segkeybuf), "Keyword:%s, string, variable, record, \"%s\", %%s, none, \"\"", segkeyname, seg->cparms);
                           err = AddAKey(segkeyname,
                                         copy,
                                         segkeybuf, 
                                         0,
                                         0,
                                         1,
                                         1 + hirank++);
                        }

                    }
                        
                    if (seg->info->protocol == DRMS_FITS)
                    {
                        if (seg->info->scope != DRMS_VARDIM)
                        {
                            seg->info->scope = DRMS_VARDIM;
                            memset(seg->axis, 0, sizeof(seg->axis));
                        }
                    }  
                }
                
                if (lastseg)
                {
                    hiter_destroy(&lastseg);
                }
                
                /* If the segment contains integer data, then the bzero and bscale values of the original series will suffice, 
                 * and that is what seg contains. If they are float data, then bzero and bscale are ignored. */
            }

            if (err == kExpCloneErr_Success)
            {
                DRMS_Keyword_t *key = NULL;
                DRMS_Link_t *link = NULL;
                DRMS_Record_t *tRec = NULL;
                DRMS_Keyword_t *tKey = NULL;
                HIterator_t *lastkey = NULL;
                char oKeyName[DRMS_MAXKEYNAMELEN];
                int rank = -1;
                
                /* If a keyword is linked to another series, we want to make that keyword a non-linked one. */
                while ((key = drms_record_nextkey(copy, &lastkey, 0)))
                {
                    /* The segments will have been sorted by increasing segnum. */
                    if (key->info->islink)
                    {
                        /* If this segment was originally a linked segment, then we need to replace it                                      
                         * with the target of the link. */
                        link = hcon_lookup_lower(&copy->links, key->info->linkname);
                        
                        if (!link)
                        {
                            fprintf(stderr, "Unable to obtain link %s.\n", key->info->linkname);
                            err = kExpCloneErr_CantFollowLink;
                            break;
                        }
                        else
                        {
                            tRec = drms_template_record(drms_env, link->info->target_series, &drmsstat);
                            
                            if (drmsstat != DRMS_SUCCESS || !tRec)
                            {
                                fprintf(stderr, "Unable to obtain template record for series %s.\n", link->info->target_series);
                                err = kExpCloneErr_LibDRMS;
                                break;
                            }
                            else
                            {
                                tKey = drms_keyword_lookup(tRec, key->info->target_key, 0);
                                
                                if (!tKey)
                                {
                                    fprintf(stderr, "Unable to follow link to target keyword.\n");
                                    err = kExpCloneErr_CantFollowLink;
                                    break;
                                }
                                
                                /* Need to use the name of the keyword in the original series, not the name in the                          
                                 * target series, so save the original name before overwriting. */
                                snprintf(oKeyName, sizeof(oKeyName), "%s", key->info->name);
                                
                                /* Need to save the original keyword rank as well, since tKey->info->rank is for the                            
                                 * target series, not the source series. */
                                rank = key->info->rank;
                                
                                /* Copy - must deep copy the info struct, since it will be freed during shutdown. Must
                                 * also copy the key->value value, if the keyword is a string, since the original
                                 * string will also be freed during shutdown. Both frees happen in drms_free_template_keyword_struct()
                                 * operating on the keywords in aia.lev1. */
                                if (key->info->type == DRMS_TYPE_STRING && key->value.string_val)
                                {
                                    free(key->value.string_val);
                                    key->value.string_val = NULL;
                                }
                                
                                if (key->info)
                                {
                                    free(key->info);
                                }
                                
                                *key = *tKey;
                                key->info = malloc(sizeof(DRMS_KeywordInfo_t));
                                
                                if (!key->info)
                                {
                                    err = kExpCloneErr_OutOfMemory;
                                    break;
                                }
                                else
                                {
                                    *key->info = *tKey->info;
                                    
                                    /* Need to set record field to point to copy; seg->info points to the original                          
                                     * series seg->info struct. */
                                    key->record = copy;
                                    
                                    /* Copy the saved original segment's info to the new series' segment. */
                                    snprintf(key->info->name, sizeof(key->info->name), "%s", oKeyName);
                                    key->info->rank = rank;
                                }
                                
                                if (key->info->type == DRMS_TYPE_STRING)
                                {
                                    key->value.string_val = strdup(tKey->value.string_val);
                                }
                            }
                        }
                    }
                } /* while */
                
                if (lastkey)
                {
                    hiter_destroy(&lastkey);
                }
            }
            
            if (err == kExpCloneErr_Success)
            {
                /* Free all links - we don't want the created series having links. */
                (copy->links).deep_free = (void (*)(const void *)) drms_free_template_link_struct;
                hcon_free(&copy->links);
            }

            if (err == kExpCloneErr_Success)
            {
                /* drms_create_series_fromprototype() will first copy keywords with drms_copy_keyword_struct().
                 * This latter function shallow-copies each keyword's info struct. */
                if (drms_create_series_fromprototype(&copy, name, 0))
                {
                    err = kExpCloneErr_CantCreateSeries;
                }
            }
        }
    }
 
    /* copy is freed by drms_create_series_fromprototype(), if it is called. */
    if (copy)
    {
        /* Does not free the segment info structs (which is good). */
        drms_destroy_recproto(&copy);
    }
    
    if (name)
    {
        free(name);
    }
    
    if (seriesin)
    {
        free(seriesin);
    }
    
    return err;
}
