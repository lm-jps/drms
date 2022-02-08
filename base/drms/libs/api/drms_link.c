//#define DEBUG

#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"

/*********************8* Prototypes for local functions. *********************/
/* Bind a dynamic link to a specific data record, by finding the
   record with the highest record number among those with the primary
   index values given in the link.  NOTICE: This function is just a no-op
   when applied to a simple link. */
static int drms_link_resolve(DRMS_Link_t *link);
/* Find all records pointed to by the link. For a static link this is a single
   record. For a dynamic link this is the set of records with primary index
   values given in the link.  */
static int drms_link_resolveall(DRMS_Link_t *link, int *n, long long **recnums);



void drms_free_template_link_struct(DRMS_Link_t *link)
{
  if (link->info->type == DYNAMIC_LINK) {
    for (int i = 0; i < link->info->pidx_num; i++)
      free(link->info->pidx_name[i]);
  }
  free(link->info);
}

/* Deep free of a link structure. */
void drms_free_link_struct(DRMS_Link_t *link)
{
  int i;

  /* Free strings associated with dynamic links. */
  if (link->info->type == DYNAMIC_LINK)
  {
    for (i=0; i<link->info->pidx_num; i++)
    {
      if (link->info->pidx_type[i] == DRMS_TYPE_STRING &&
	  link->pidx_value[i].string_val != NULL)
      {
	free(link->pidx_value[i].string_val);
      }
    }
  }
}

/* Deep copy a link structure. */
void drms_copy_link_struct(DRMS_Link_t *dst, DRMS_Link_t *src)
{
  int i;

  /* Copy main struct. */
  memcpy(dst, src, sizeof(DRMS_Link_t));

  /* Copy strings associated with dynamic links. */
  if (src->info->type == DYNAMIC_LINK)
  {
    for (i=0; i<src->info->pidx_num; i++)
    {
      if (src->info->pidx_type[i] == DRMS_TYPE_STRING &&
	  src->pidx_value[i].string_val != NULL)
      {
	copy_string(&dst->pidx_value[i].string_val,
		    src->pidx_value[i].string_val);
      }
    }
  }
}

/* target must have no existing links, since this function fills in the link container */
HContainer_t *drms_create_link_prototypes(DRMS_Record_t *target,
					  DRMS_Record_t *source,
					  int *status)
{
    HContainer_t *ret = NULL;
    DRMS_Link_t *tLink = NULL;
    DRMS_Link_t *sLink = NULL;

    XASSERT(target != NULL && target->links.num_total == 0 && source != NULL);

    if (target != NULL && target->links.num_total == 0 && source != NULL)
    {
        *status = DRMS_SUCCESS;
        HIterator_t hit;

        hiter_new_sort(&hit, &(source->links), drms_link_ranksort);

        while ((sLink = hiter_getnext(&hit)) != NULL)
        {
            if (sLink->info && strlen(sLink->info->name) > 0)
            {
                tLink = hcon_allocslot_lower(&(target->links), sLink->info->name);
                XASSERT(tLink);
                memset(tLink, 0, sizeof(DRMS_Link_t));
                tLink->info = malloc(sizeof(DRMS_LinkInfo_t));
                XASSERT(tLink->info);
                memset(tLink->info, 0, sizeof(DRMS_LinkInfo_t));

                if (tLink && tLink->info)
                {
                    /* record */
                    tLink->record = target;

                    memcpy(tLink->pidx_value,
                           sLink->pidx_value,
                           DRMS_MAXPRIMIDX * sizeof(DRMS_Type_Value_t));

                    /* link info + pidx_value */
                    memcpy(tLink->info, sLink->info, sizeof(DRMS_LinkInfo_t));

                    int idx = 0;
                    for (; idx < tLink->info->pidx_num; idx++)
                    {
                        tLink->info->pidx_name[idx] = strdup(sLink->info->pidx_name[idx]);

                        // copy_string(&(tLink->info->pidx_name[idx]), sLink->info->pidx_name[idx]);
                        if (tLink->info->pidx_type[idx] == DRMS_TYPE_STRING &&
                            sLink->pidx_value[idx].string_val != NULL)
                        {
                            copy_string(&(tLink->pidx_value[idx].string_val),
                                        sLink->pidx_value[idx].string_val);
                        }
                    }

                    tLink->recnum = sLink->recnum;
                    tLink->isset = sLink->isset;
                    /* Do not set tLink->wasFollowed. That is reserved for links that are not detached. */
                }
                else
                {
                    *status = DRMS_ERROR_OUTOFMEMORY;
                }
            }
            else
            {
                *status = DRMS_ERROR_INVALIDLINK;
            }
        }

        hiter_free(&hit);

        if (*status == DRMS_SUCCESS)
        {
            ret = &(target->links);
        }
    }
    else
    {
        *status = DRMS_ERROR_INVALIDRECORD;
    }

    return ret;
}

/* Set link to point to target series record with record number "recnum". */
int drms_setlink_static(DRMS_Record_t *rec, const char *linkname, long long recnum)
{
  DRMS_Link_t *link;

  if ( (link = hcon_lookup_lower(&rec->links,linkname)) == NULL )
    return DRMS_ERROR_UNKNOWNLINK;

  if (link->info->type==STATIC_LINK)
  {
    link->recnum = recnum;
    //    rec->link_dirty = 1;
    return DRMS_SUCCESS;
  }
  else
    return DRMS_ERROR_INVALIDLINKTYPE;
}

int drms_setlink_dynamic(DRMS_Record_t *rec, const char *linkname,
			 DRMS_Type_t *types, DRMS_Type_Value_t *values)
{
  int i;
  DRMS_Link_t *link;

  if ( (link = hcon_lookup_lower(&rec->links,linkname)) == NULL )
    return DRMS_ERROR_UNKNOWNLINK;

  if (link->info->type==DYNAMIC_LINK)
  {
    for (i=0; i<link->info->pidx_num; i++)
    {
      if (types[i] != link->info->pidx_type[i])
	return DRMS_ERROR_INVALIDLINKTYPE;
    }
    for (i=0; i<link->info->pidx_num; i++)
    {
      drms_copy_drms2drms(types[i], &link->pidx_value[i], &values[i]);
    }
    link->isset = 1;
    return DRMS_SUCCESS;
  }
  else
    return DRMS_ERROR_INVALIDLINKTYPE;
}

int drms_link_set(const char *linkname, DRMS_Record_t *baserec, DRMS_Record_t *supplementingrec)
{
   int status;
   DRMS_Link_t *link = NULL;

   if ((link = hcon_lookup_lower(&baserec->links, linkname)) == NULL)
   {
      status = DRMS_ERROR_UNKNOWNLINK;
   }
   else
   {
      if (link->info->type == DYNAMIC_LINK)
      {
         DRMS_Type_t pidxtypes[DRMS_MAXPRIMIDX];
         DRMS_Type_Value_t pidxvalues[DRMS_MAXPRIMIDX] = {0};
         int npidx = supplementingrec->seriesinfo->pidx_num;
         int ikey;
         DRMS_Keyword_t *pkey;
         DRMS_Keyword_t *keyinstance ;

         memset(pidxtypes, 0, sizeof(DRMS_Type_t) * DRMS_MAXPRIMIDX);

         for (ikey = 0; ikey < npidx; ikey++)
         {
            pkey = supplementingrec->seriesinfo->pidx_keywords[ikey];
            pidxtypes[ikey] = pkey->info->type;

            /* pkey is a template keyword, so its value is meaningless. You need to
             * get the value from supplementingrec. */
            keyinstance = drms_keyword_lookup(supplementingrec, pkey->info->name, 0);

            if (!keyinstance)
            {
               status = DRMS_ERROR_INVALIDKEYWORD;
            }

            pidxvalues[ikey] = keyinstance->value;
         }

         status = drms_setlink_dynamic(baserec, linkname, pidxtypes, pidxvalues);
      }
      else if (link->info->type == STATIC_LINK)
      {
         status = drms_setlink_static(baserec, linkname, supplementingrec->recnum);
      }
      else
      {
         status = DRMS_ERROR_INVALIDDATA;
      }
   }

   return status;
}

/* Return the record pointed to by a named link in the given record. */
DRMS_Record_t *drms_link_follow(DRMS_Record_t *rec, const char *linkname, int *status)
{
    DRMS_Link_t *link = NULL;
    char hashkey[DRMS_MAXHASHKEYLEN];
    DRMS_Record_t *linkedRec = NULL;

    if ( (link = hcon_lookup_lower(&rec->links,linkname)) == NULL )
    {
        if (status)
            *status = DRMS_ERROR_UNKNOWNLINK;
        return NULL;
    }
    if ((link->info->type == STATIC_LINK && link->recnum==-1) ||
        (link->info->type == DYNAMIC_LINK && !link->isset))
    {
        if (status)
            *status = DRMS_ERROR_LINKNOTSET;
        return NULL;
    }
    if (drms_link_resolve(link))
    {
        if (status)
            *status = DRMS_ERROR_BADLINK;
        return NULL;
    }

    /* Do not call drms_retrieve_record() if the linked record has already been cached.
     * A refcount is the number of handles to an object. All the handles
     * will be freed in a well-written program. When you follow a link to a target record
     * from a parent record, you do create a reference to the linked record, but only
     * the first time the link is followed. You do not create additional references
     * to the linked record each time the link is followed. It is possible to call drms_link_follow()
     * on the same original record multiple times. In this case, there is really only a
     * single handle to the linked record (which is the original record), so the refcount
     * should only be 1 regardless how many times drms_link_follow() is called.
     *
     * The refcount should be incremented beyond 1 in only one case: when OPENING (e.g., drms_open_records())
     * a (linked) record. Everytime a caller opens a record, the caller creates a handle
     * to that record that the caller will eventually free. The increments due to
     * drms_open_records() will balance the decrements due to drms_close_records(). But
     * there is no such call to drms_close_records() after a call to drms_link_follow().
     * If this function were to call drms_retrieve_record() every time it was called,
     * the refcount would never decrement back to zero, and the record would never be
     * freed from the record cache.
     */
    drms_make_hashkey(hashkey, link->info->target_series, link->recnum);

    if ((linkedRec = hcon_lookup(&rec->env->record_cache, hashkey)) != NULL)
    {
        /* Do not increase refcount on linked record. */
        if (status)
        {
            *status = DRMS_SUCCESS;
        }

        if (link->wasFollowed)
        {
            /* The caller has previously called drms_link_follow() on the same original record.
             * No new handle (via original rec) to the link record will be created, so do not
             * increment the refcount on the linked record. */
        }
        else
        {
           /* The caller has never called drms_link_follow() on this original record, but the
            * linked record is in the cache, so drms_open_records() must have been called on
            * that linked record. In this case, we are creating a handle to the linked record
            * (via original rec), so we do need to increment the refcount on the linked record.*/
           ++linkedRec->refcount;
           link->wasFollowed = 1;
        }

        return linkedRec;
    }
    else
    {
        /* Set refcount on linked record to 1. */
        XASSERT(!link->wasFollowed); /* Do not follow links more than once (for any given original
                                      * record). */
        link->wasFollowed = 1;
        /* will set refcount to 1 since the child record has not been cached yet */
        return drms_retrieve_record(rec->env, link->info->target_series, link->recnum, NULL, status);
    }
}

/* recursively follows all links for a record set;
 * returns 'link_map' that maps the child record hash to child linked record */
 /* the record set argument need not be a first record set (i.e., one that can have drms_close_records() called on ); to indicate
  * this, the argument is named `record_list` and is a LinkedList_t
  *
  * `template_record` is the series to which all records in `record_list` belong */
LinkedList_t *drms_link_follow_recordset(DRMS_Env_t *env, DRMS_Record_t *template_record, LinkedList_t *record_list, const char *link, HContainer_t *link_map, int *status)
{
    DRMS_Link_t *template_link = NULL;
    DRMS_Record_t *child_template_record = NULL;
    ListNode_t *list_node = NULL;
    DRMS_Link_t *drms_link = NULL;
    char parent_hash_key[DRMS_MAXHASHKEYLEN] = {0};
    char child_hash_key[DRMS_MAXHASHKEYLEN] = {0};
    DRMS_Record_t **child_drms_record_ptr = NULL;
    DRMS_Record_t *child_drms_record = NULL;
    DRMS_Record_t *drms_record = NULL;
    int drms_status = DRMS_SUCCESS;
    int loop_status = DRMS_SUCCESS;
    HContainer_t *link_hash_map = NULL;
    HContainer_t *link_map_retrieved = NULL;
    HIterator_t hit;
    LinkedList_t *linked_records = NULL;
    const char *child_hash_key_retrieved = NULL;

    XASSERT(template_record && record_list);

    template_link = hcon_lookup_lower(&template_record->links, link);
    XASSERT(template_link);

    child_template_record = drms_template_record(env, template_link->info->target_series, &drms_status);
    XASSERT(child_template_record);

    list_llreset(record_list);
    while ((list_node = list_llnext(record_list)) != NULL)
    {
        XASSERT(list_node->data);
        drms_record = *(DRMS_Record_t **)list_node->data;

        if ((drms_link = hcon_lookup_lower(&drms_record->links, link)) == NULL)
        {
            loop_status = DRMS_ERROR_UNKNOWNLINK;
            break;
        }

        if ((drms_link->info->type == STATIC_LINK && drms_link->recnum == -1) || (drms_link->info->type == DYNAMIC_LINK && !drms_link->isset))
        {
            loop_status = DRMS_ERROR_LINKNOTSET;
            break;
        }

        /* must have called drms_link_getpidx() before drms_link_resolve() */
        if (drms_link_resolve(drms_link))
        {
            loop_status = DRMS_ERROR_BADLINK;
            break;
        }

        drms_make_hashkey(parent_hash_key, drms_record->seriesinfo->seriesname, drms_record->recnum);
        drms_make_hashkey(child_hash_key, drms_link->info->target_series, drms_link->recnum);

        if ((child_drms_record = hcon_lookup(&env->record_cache, child_hash_key)) != NULL)
        {
            /* Do not increase refcount on linked record. */
            if (drms_link->wasFollowed)
            {
                /* The caller has previously called drms_link_follow() on the same original record.
                 * No new handle (via original rec) to the link record will be created, so do not
                 * increment the refcount on the linked record. */
            }
            else
            {
               /* The caller has never called drms_link_follow() on this original record, but the
                * linked record is in the cache, so drms_open_records() must have been called on
                * that linked record. In this case, we are creating a handle to the linked record
                * (via original rec), so we do need to increment the refcount on the linked record.*/
               child_drms_record->refcount++;
               drms_link->wasFollowed = 1;
            }

            /* key is the hash_key of the input (parent) record, val is pointer to linked record (because we do not
             * know if the linked record is cached or not) */
            if (!hcon_member_lower(link_map, child_hash_key))
            {
                /* no duplicate linked records */
                hcon_insert(link_map, child_hash_key, &child_drms_record);
                list_llinserttail(linked_records, &child_drms_record);
            }
        }
        else
        {
            if (link_hash_map == NULL)
            {
                link_hash_map = hcon_create(DRMS_MAXHASHKEYLEN, DRMS_MAXHASHKEYLEN, NULL, NULL, NULL, NULL, 0);
            }

            /* Set refcount on linked record to 1. */
            XASSERT(!drms_link->wasFollowed); /* Do not follow links more than once (for any given original
                                          * record). */
            drms_link->wasFollowed = 1;
            /* need to store the target series and recnum - below we will call retrieve_records en masse, inserting
             * the results into link_map */
            if (!hcon_member_lower(link_map, child_hash_key))
            {
                /* no duplicate linked records */
                hcon_insert(link_hash_map, child_hash_key, child_hash_key);
            }
        }
    }

    if (link_hash_map != NULL)
    {
        /* iterate through records_to_retrieve (link_map: child_hash_key --> child_drms_record) */
        /* always caches linked record since we only download complete linked records (penultimate arg, 1,
         * ensures that link info is downloaded from the DB) */
        /* since all child records originated from a single link, they all belong to the same series
         *
         * no duplicate linked records */
        link_map_retrieved = drms_retrieve_linked_recordset(env, child_template_record, link_hash_map, 1, &loop_status);

        if (loop_status == DRMS_SUCCESS)
        {
            hiter_new(&hit, link_map_retrieved);
            while ((child_drms_record_ptr = (DRMS_Record_t **)hiter_extgetnext(&hit, &child_hash_key_retrieved)) != NULL)
            {
                child_drms_record = *child_drms_record_ptr;
                hcon_insert(link_map, child_hash_key_retrieved, &child_drms_record);

                /* no duplicate linked records */
                list_llinserttail(linked_records, &child_drms_record);
            }

            hiter_free(&hit);
        }

        hcon_destroy(&link_map_retrieved);
    }

    hcon_destroy(&link_hash_map);

    if (status)
    {
        *status = loop_status;
    }

    /* caller iterates through parent record set, and uses link_map to obtain handle to linked record */
    return linked_records;
}

/* Recolve dynamic links by selecting the DB ROW with highest recnum
   matching the value of the primary index given in the link structure. */
static int drms_link_resolve(DRMS_Link_t *link)
{
    int i, n;
    long long maxrecnum, *recnums;

    /* This is either a static link or a dynamic one that has already
    been resolved. In either case there is nothing to do. */
    if (link->info->type == STATIC_LINK || link->recnum >= 0)
    {
        return 0;
    }
    else
    {
        if (drms_link_resolveall(link, &n, &recnums))
        {
            return 1;
        }

        /* Find the candidate with the highest record number. */
        maxrecnum = recnums[0];
        for(i=1; i<n; i++)
        {
            if (recnums[i]>maxrecnum)
            {
                maxrecnum = recnums[i];
            }
        }

        link->recnum = maxrecnum;
        free(recnums);
        return 0;
    }
}


/* Resolve a link to all matching DB ROWS in the child series. A static link only has one match. */
static int drms_link_resolveall(DRMS_Link_t *link, int *n, long long **recnums)
{
  char query[DRMS_MAXQUERYLEN+DRMS_MAXPRIMIDX*DRMS_MAXKEYNAMELEN], *p;
  DB_Binary_Result_t  *qres;
  DRMS_Env_t *env;
  int i;
  void *argin[DRMS_MAXPRIMIDX];
  DB_Type_t intype[DRMS_MAXPRIMIDX];
  char *table;

  /* This is either a static link or a dynamic one that has already
     been resolved. In either case there is nothing to do. */
  if (link->info->type == STATIC_LINK )
  {
    *n = 1;
    *recnums = malloc(*n*sizeof(long long));
    XASSERT(*recnums);
    (*recnums)[0] = link->recnum;
    return 0;
  }
  else
  {
    XASSERT(link->info->pidx_num>0);
    env = link->record->env; /* Go up parent chain to get env. */

    /* This link is an unresolved dynamic link. It points to one or more
       records with a certain value for the primary index. Resolve it now by
       executing a query to the database that finds the record matching the
       primary index value with the highest record number. */

    /* Build query string. */
    table = strdup(link->info->target_series);
    strtolower(table);
    p = query;
    p += sprintf(p, "select recnum from %s where ", table);
    free(table);
    p += sprintf(p, "%s=?", link->info->pidx_name[0]);
    for (i=1; i<link->info->pidx_num; i++)
    {
      p += sprintf(p, " and %s=?", link->info->pidx_name[i]);
    }
    *p = 0;

    /* Collect pidx values and types */
    for (i=0; i<link->info->pidx_num; i++)
    {
      intype[i] = drms2dbtype(link->info->pidx_type[i]);
      argin[i] = drms_addr(link->info->pidx_type[i], &link->pidx_value[i]);
    }

#ifdef DEBUG
    printf("query string in drms_link_resolve = '%s'\n",query);
#endif

    /* Retrieve all records matching the primary index value. */
    qres = drms_query_bin_array(env->session, query, link->info->pidx_num,
				intype, argin);
    if (qres==NULL || qres->num_rows <= 0)
      return 1;

    /* Find the candidate with the highest record number. */
    *n = qres->num_rows;
    *recnums = malloc(*n*sizeof(long long));
    XASSERT(*recnums);
    for(i=0; i<(int)qres->num_rows; i++)
    {
      (*recnums)[i] = db_binary_field_getlonglong(qres,i,0);
    }
#ifdef DEBUG
    printf("Link resolved to series='%s', recnum=%lld\n",link->info->target_series,
	   link->recnum);
#endif
    db_free_binary_result(qres);
    return 0;
  }
}

void drms_link_print(DRMS_Link_t *link)
{
	drms_link_fprint(stdout, link);
}

void drms_link_fprint(FILE *keyfile, DRMS_Link_t *link)
{
  const int fieldwidth=13;
  int i;

  fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "Name", link->info->name);
  fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "Target series", link->info->target_series);
  fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "Description", link->info->description);
  if (link->info->type == STATIC_LINK)
  {
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "Type", "STATIC_LINK");
  }
  else
  {
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "Type", "DYNAMIC_LINK");
    fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "Pidx_num", link->info->pidx_num);
    for(i=0;i<link->info->pidx_num; i++)
    {
      fprintf(keyfile, "\t%-*s%1d :\t%s\n", fieldwidth-2, "Pidx_name", i,
	     link->info->pidx_name[i]);
      fprintf(keyfile, "\t%-*s%1d :\t%s\n", fieldwidth-2, "Pidx_type", i,
	     drms_type2str(link->info->pidx_type[i]));
      fprintf(keyfile, "\t%-*s%1d :\t", fieldwidth-2, "Pidx_value", i);
      drms_fprintfval(keyfile, link->info->pidx_type[i], &link->pidx_value[i]);
      fprintf(keyfile, "\n");
    }
  }
  fprintf(keyfile, "\t%-*s:\t%lld\n", fieldwidth, "Recnum", link->recnum);
}

/*
   Build the link part of a dataset template by
   using the query result holding a list of
   (linkname, target_seriesname, type, description)
   tuples to initialize the array of link descriptors.
*/
int drms_template_links(DRMS_Record_t *template)
{
   int i,j, status = DRMS_NO_ERROR;
   DRMS_Env_t *env;
   char buf[DRMS_MAXLINKNAMELEN], query[DRMS_MAXQUERYLEN];
   DRMS_Link_t *link;
   DB_Binary_Result_t *qres;
   int rank;

   env = template->env;

   /* Initialize container structures for links. */
   hcon_init(&template->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN,
             (void (*)(const void *)) drms_free_link_struct,
             (void (*)(const void *, const void *)) drms_copy_link_struct);

   /* Get link definitions from database and add to template. */
   char *namespace = ns(template->seriesinfo->seriesname);
   char *lcseries = strdup(template->seriesinfo->seriesname);

   if (!lcseries)
   {
      status = DRMS_ERROR_OUTOFMEMORY;
      goto bailout;
   }

   strtolower(lcseries);

   sprintf(query, "select linkname, target_seriesname, type, description "
           "from %s.%s where lower(seriesname) = '%s' order by linkname",
           namespace, DRMS_MASTER_LINK_TABLE, lcseries);
   free(lcseries);
   free(namespace);
   if ((qres = drms_query_bin(env->session, query)) == NULL)
   {
      printf("Failed to retrieve link definitions for series %s.\n",
             template->seriesinfo->seriesname);
      return DRMS_ERROR_QUERYFAILED;
   }

   if (qres->num_rows>0 && qres->num_cols != 4 )
   {
      status = DRMS_ERROR_BADFIELDCOUNT;
      goto bailout;
   }

   rank = 0;
   for (i = 0; i<(int)qres->num_rows; i++)
   {
      /* Allocate space for new structure in hashed container. */
      db_binary_field_getstr(qres, i, 0, sizeof(buf), buf);
      link = hcon_allocslot_lower(&template->links, buf);
      memset(link,0,sizeof(DRMS_Link_t));
      link->info = malloc(sizeof(DRMS_LinkInfo_t));
      XASSERT(link->info);
      memset(link->info,0,sizeof(DRMS_LinkInfo_t));
      /* Copy field values from query result. */
      link->record = template;
      strcpy(link->info->name, buf);
      db_binary_field_getstr(qres, i, 1, sizeof(link->info->target_series), link->info->target_series);
      db_binary_field_getstr(qres, i, 2, sizeof(buf), buf);
      if (!strcmp(buf,"static"))
        link->info->type = STATIC_LINK;
      else if (!strcmp(buf,"dynamic"))
        link->info->type = DYNAMIC_LINK;
      else
      {
         fprintf(stderr,"ERROR: '%s' is not a valid link type.\n",buf);
         goto bailout;
      }
      db_binary_field_getstr(qres, i, 3, DRMS_MAXCOMMENTLEN, link->info->description);
#ifdef DEBUG
      printf("Link %d: name='%s', target_series='%s', type=%d, description=%s\n",
             i, link->name, link->target_series, link->type, link->info->description);
#endif
      link->info->pidx_num = -1; /* Mark as not-yet-initialized. */

      /* For now, just rank according to alphanumeric ordering of the link name. There is no field in the
       * drms_link tables that can appropriately store the rank. However, we could use the "version" field
       * in the drms_series table, since it isn't being used. But for now, just rank alphanumerially. */
      link->info->rank = rank++;

      for(j=0; j<DRMS_MAXPRIMIDX; j++)
      {
         memset(&link->pidx_value[j],0,sizeof(DRMS_Type_Value_t));
      }
      link->isset = 0;
      link->wasFollowed = 0;
      link->recnum = -1;
   }
   db_free_binary_result(qres);
   return DRMS_SUCCESS;

 bailout:
   db_free_binary_result(qres);
   return status;
}



/* For each link in the record: Get the names and types for the
   primary index keywords of the target series if it is a dynamic
   link.*/
int drms_link_getpidx(DRMS_Record_t *rec)
{
    int i, status;
    DRMS_Keyword_t *key;
    DRMS_Record_t *template;
    DRMS_Link_t *link;
    HIterator_t hit;

    status = DRMS_SUCCESS;

    hiter_new(&hit, &rec->links); /* Iterator for link container. */
    while ((link = (DRMS_Link_t *)hiter_getnext(&hit)))
    {
        /*if (link->info->type == DYNAMIC_LINK && link->info->pidx_num == -1) */ /* ISS */
        if (link->info->type == DYNAMIC_LINK && link->info->pidx_num == -1)
        {
            template = drms_template_record(link->record->env, link->info->target_series, &status);
            if (template == NULL)
            {
                fprintf(stderr,"ERROR: Couldn't get template for series '%s'.\ndrms_template_record returned status=%d\n", link->info->target_series, status);
                return status;
            }

            link->info->pidx_num = template->seriesinfo->pidx_num;
            for (i = 0; i < link->info->pidx_num; i++)
            {
                key = template->seriesinfo->pidx_keywords[i];
                link->info->pidx_type[i] = key->info->type;
                copy_string(&link->info->pidx_name[i], key->info->name);
            }
        }
    }

    hiter_free(&hit);

    return status;
}
