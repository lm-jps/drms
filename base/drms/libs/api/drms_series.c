//#define DEBUG
#if defined(__linux__) && __linux__
#define _GNU_SOURCE
#endif
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"

/* 
extract namespace from series name. 
return error if no '.' present
*/ 
int get_namespace(const char *seriesname, char **namespace, char **shortname) {
  if (strchr(seriesname, '.')) {
    const char *p = seriesname;
    while (*p != '.') {
      p++;
    }
    *namespace = ns(seriesname);
    if (shortname) {
      *shortname = strdup(p+1);
    }
    return 0;
  } else {
    return 1;
  }
}

int drms_series_exists(DRMS_Env_t *drmsEnv, const char *sname, int *status)
{
   int ret = 0;

   if (sname != NULL && *sname != '\0')
   {
      drms_template_record(drmsEnv, sname, status);

      if (*status == DRMS_ERROR_UNKNOWNSERIES)
      {
	 ret = 0;
      }
      else if (*status != DRMS_SUCCESS)
      {
	 fprintf(stderr, "DRMS Error calling drms_series_exists.\n");
      }
      else
      {
	 ret = 1;
      }
   }

   return ret;
}

/* Create/update the database tables and entries for a new/existing series.
   The series is created from the information in the series template record
   given in the argument "template".
   If update=1 an existing series is updated. If update=0 a new series is 
   created. If update=0 and a series of the same name already exists an
   error code is returned.
*/
int drms_insert_series(DRMS_Session_t *session, int update, 
		       DRMS_Record_t *template, int perms)
{
  int i, len=0, segnum;
  char *pidx_buf=0, *dbidx_buf=0, scopestr[100], *axisstr=0;
  DRMS_SeriesInfo_t *si;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  DRMS_Link_t *link;
  HIterator_t hit;
  char *linktype,*p,*q;
  char dyn[]="dynamic";
  char stat[]="static";
  char defval[2048]={0};
  char *createstmt=0, *series_lower=0, *namespace=0;
  DB_Text_Result_t *qres;

  XASSERT(createstmt = malloc(30000));
  drms_link_getpidx(template); /* Make sure links have pidx's set. */
  si = template->seriesinfo;
  // extract namespace from series name. default to 'public'
  if (get_namespace(si->seriesname, &namespace, &series_lower)) {
    fprintf(stderr, "Invalid seriesname: namespace missing\n");
    goto failure;
  }
  strtolower(namespace);
  /* series_lower does not contain namespace information. This is
     needed to create index */
  strtolower(series_lower);

  // namespace must exists
  sprintf(createstmt, "select * from pg_namespace where nspname = '%s'", namespace);
  if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
    if (qres->num_rows == 0) {
      fprintf(stderr, "Namespace %s does not exist.\n", namespace);
      goto failure;
    }
    db_free_text_result(qres);
  }

  // dbuser must have create privilege to namespace
  // this does not work when called from client module because there
  // is no dbuser information.
  if (session->db_direct) {
    sprintf(createstmt, "select has_schema_privilege('%s', '%s', 'create')", 
	    session->db_handle->dbuser,
	    namespace);
    if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
      if (qres->num_rows > 0) {
	if (qres->field[0][0][0] == 'f') {
	  fprintf(stderr, "dbuser %s does not have create privilege in namespace %s.\n",
		  session->db_handle->dbuser, namespace);
	  goto failure;
	}
      } else {
	fprintf(stderr, "Failed: %s\n", createstmt);
	goto failure;
      }
    } else {
      fprintf(stderr, "Failed: %s\n", createstmt);
      goto failure;
    }      
    db_free_text_result(qres);
  }

  sprintf(createstmt, "set search_path to %s", namespace);
  if(drms_dms(session, NULL, createstmt)) {
    fprintf(stderr, "Failed: %s\n", createstmt);
    goto failure;
  }

  if (si->pidx_num==0)
  {
    XASSERT(pidx_buf = malloc(2));
    *pidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->pidx_num; i++)
      len += strlen((si->pidx_keywords[i])->info->name) + 3;
    XASSERT((pidx_buf = malloc(len+1)));
    memset(pidx_buf,0,len+1);
    p = pidx_buf;
    p += sprintf(p,"%s",(si->pidx_keywords[0])->info->name);
    for (i=1; i<si->pidx_num; i++)
      p += sprintf(p,", %s",(si->pidx_keywords[i])->info->name);
  }

  if (si->dbidx_num <= 0)
  {
    XASSERT(dbidx_buf = malloc(2));
    *dbidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->dbidx_num; i++)
      len += strlen((si->dbidx_keywords[i])->info->name) + 3;
    XASSERT((dbidx_buf = malloc(len+1)));
    memset(dbidx_buf,0,len+1);
    p = dbidx_buf;
    p += sprintf(p,"%s",(si->dbidx_keywords[0])->info->name);
    for (i=1; i<si->dbidx_num; i++)
      p += sprintf(p,", %s",(si->dbidx_keywords[i])->info->name);
  }

  if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_SERIES_TABLE
		"(seriesname, description, author, owner, unitsize, archive,"
		"retention, tapegroup, version, primary_idx, dbidx, created) values (?,?,?,?,"
		"?,?,?,?,?,?,?,LOCALTIMESTAMP(0))", -1,
		DB_STRING, si->seriesname, DB_STRING, si->description, 
		DB_STRING, si->author, DB_STRING, si->owner,
		DB_INT4, si->unitsize, DB_INT4, si->archive, 
		DB_INT4, si->retention, DB_INT4, si->tapegroup, 
		DB_STRING, si->version,
		DB_STRING, pidx_buf,
		DB_STRING, dbidx_buf))
    goto failure;
  
  p = createstmt;
  /* Fixed fields. */
  p += sprintf(p,"create table %s (",series_lower); 
  p += sprintf(p,"recnum bigint not null"); 
  p += sprintf(p,", sunum bigint"); 
  p += sprintf(p,", slotnum integer"); 
  p += sprintf(p,", sessionid bigint"); 
  p += sprintf(p,", sessionns text");

  /* Link fields. */
  hiter_new(&hit, &template->links); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      linktype = stat;
      p += sprintf(p,", ln_%s bigint",link->info->name);
    }
    else  /* Oh crap! A dynamic link... */
    {
      linktype = dyn;
      if (link->info->pidx_num) {
	p += sprintf(p,", ln_%s_isset %s default 0", link->info->name,  
		     db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
      {
	p += sprintf(p,", ln_%s_%s %s",link->info->name, link->info->pidx_name[i], 
		     db_type_string(drms2dbtype(link->info->pidx_type[i])));
      }
    }
    
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_LINK_TABLE
		  "(seriesname, linkname, target_seriesname, type, "
		  "description) values (?,?,?,?,?)", -1, 
		  DB_STRING, si->seriesname, DB_STRING, link->info->name,
		  DB_STRING, link->info->target_series, DB_STRING, linktype,
		  DB_STRING, link->info->description))
      goto failure;

  }
  /* Keyword fields. */
  hiter_new(&hit, &template->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      if (key->info->type==DRMS_TYPE_STRING)
      {
	p += sprintf(p,", %s %s",key->info->name,
		     db_stringtype_maxlen(4000));
      }
      else
      {
	p += sprintf(p,", %s %s",key->info->name,
		     db_type_string(drms2dbtype(key->info->type)));
      }
    }
#ifdef DEBUG
    printf("keyword '%s'\n",key->info->name);
#endif

    /* key->info->per_segment overload: This will work on <= drms_series.c:1.23 because
     * in that version of DRMS, the version of drms_parser.c will not have the 
     * code that overloads key->info->per_segment.
     */
    if (drms_keyword_getperseg(key))
    {
      len = strlen(key->info->name);
      if (strcmp(key->info->name+len-4,"_000"))
	continue;
      key->info->name[len-4] = 0;
#ifdef DEBUG
      printf("Inserting per-segment keyword %s\n",key->info->name);
#endif
    }

    if (key->info->type == DRMS_TYPE_TIME)
    {
       XASSERT((drms_sprintfval_format(defval, key->info->type, 
				       &key->value, 
				       key->info->unit, 
				       0) < DRMS_DEFVAL_MAXLEN));
    }
    else
    {
       XASSERT((drms_sprintfval_format(defval, key->info->type, 
				       &key->value, 
				       key->info->format, 
				       0) < DRMS_DEFVAL_MAXLEN));
    }

    /* The persegment column used to be either 0 or 1 and it said whether the keyword
     * was a segment-specific column or not.  But starting with series version 2.1, 
     * the persegment column was overloaded to hold all the keyword flags (including
     * per_seg).  So, the following code works on both < 2.1 series and >= 2.1 series.
     */
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_KEYWORD_TABLE
		  "(seriesname, keywordname, linkname, targetkeyw, type, "
		  "defaultval, format, unit, description, islink, "
		  "isconstant, persegment) values (?,?,?,?,?,?,?,?,?,?,?,?)",
		  -1,
		  DB_STRING, key->record->seriesinfo->seriesname, 
		  DB_STRING, key->info->name, DB_STRING, key->info->linkname, 
		  DB_STRING, key->info->target_key, 
		  DB_STRING, drms_type2str(key->info->type), DB_STRING, defval, 
		  DB_STRING, key->info->format, DB_STRING, key->info->unit,
		  DB_STRING, key->info->description,
		  DB_INT4, key->info->islink,DB_INT4, key->info->recscope, 
		  DB_INT4, key->info->kwflags))
      goto failure;

    /* key->info->per_segment overload: This will work on <= drms_series.c:1.23 because
     * in that version of DRMS, the version of drms_parser.c will not have the 
     * code that overloads key->info->per_segment.
     */
    if (drms_keyword_getperseg(key))
      key->info->name[len-4] = '_';

  }
  /* Segment fields. */
  hiter_new(&hit, &template->segments); /* Iterator for segment container. */
  segnum = 0;
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    switch(seg->info->scope)
    {
    case DRMS_CONSTANT:
      strcpy(scopestr,"constant");
      break;
    case DRMS_VARIABLE:
      strcpy(scopestr,"variable");
      break;
    case DRMS_VARDIM:
      strcpy(scopestr,"vardim");
      break;
    default:
      printf("ERROR: Invalid value of scope (%d).\n", (int)seg->info->scope);
      goto failure;
    }
    if (seg->info->naxis < 0 || seg->info->naxis>DRMS_MAXRANK)
    {
      printf("ERROR: Invalid value of rank (%d).\n",seg->info->naxis);
      goto failure;
    }
    else
    {
      XASSERT(axisstr = malloc(2*seg->info->naxis*20+1));
      axisstr[0] = 0;
      q = axisstr;
      if (seg->info->naxis>0)
      {	
	q += sprintf(q,"%d",seg->axis[0]);
	for (i=1; i<seg->info->naxis; i++)
	  q+=sprintf(q,", %d",seg->axis[i]);
	if (seg->info->protocol == DRMS_TAS)
	for (i=0; i<seg->info->naxis; i++)
	  q+=sprintf(q,", %d",seg->blocksize[i]);	  
      }
    }
    
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_SEGMENT_TABLE
		  "(seriesname, segmentname, segnum, scope, type,"
		  " naxis, axis, unit, protocol, description, islink, linkname, targetseg)"
		  " values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
		  -1,
		  DB_STRING, seg->record->seriesinfo->seriesname,
		  DB_STRING, seg->info->name, DB_INT4, seg->info->segnum, 
		  DB_STRING, scopestr, 
		  DB_STRING, drms_type2str(seg->info->type), 
		  DB_INT4, seg->info->naxis,  DB_STRING, axisstr,
		  DB_STRING, seg->info->unit, 
		  DB_STRING, drms_prot2str(seg->info->protocol),
		  DB_STRING, seg->info->description,
		  DB_INT4, seg->info->islink,
	          DB_STRING, seg->info->linkname, 
		  DB_STRING, seg->info->target_seg))
    {
      free(axisstr);
      goto failure;
    }
    free(axisstr);

    /* All segments have an assciated file. */
    p += sprintf(p,", sg_%03d_file text", segnum);

    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
      {
	p += sprintf(p,", sg_%03d_axis%03d integer",segnum,i);
      }
    }

    segnum++;
  }
  p += sprintf(p,", primary key(recnum))");
#ifdef DEBUG
  printf("statement = '%s'\n",createstmt);
#endif

  /* Create the main table for the series. */
  if(drms_dms(session, NULL, createstmt))
    goto failure;

  /* 
     Backward compatibility: don't create composite index unless there
     is no DBIndex in jsd. 
     Since we are increasing the max number of prime keys allowed,
     make sure no composite index with > 5 keyword is made.
  */
  if (si->dbidx_num == -1 && si->pidx_num>0 && si->pidx_num <= 5)
  {
    /* Build an index of the primary index columns. */
    p = createstmt;
    p += sprintf(p,"create index %s_prime_idx on %s ( %s )",
		 series_lower, series_lower, pidx_buf);
    if(drms_dms(session, NULL, createstmt))
      goto failure;
  }

  if (si->dbidx_num > 0) {
    p = createstmt;
    for (i = 0; i < si->dbidx_num; i++) {
      char *dbidx_name = (si->dbidx_keywords[i])->info->name;
      p += sprintf(p,"create index %s_%s on %s ( %s );",
		   series_lower, dbidx_name, series_lower, dbidx_name);
    }
    if(drms_dms(session, NULL, createstmt))
      goto failure;
  }

  /* Create sequence for generating record numbers for the new series. */
  if (drms_sequence_create(session, template->seriesinfo->seriesname))
    goto failure;

  /* default to readable by public */
  p = createstmt;
  p += sprintf(p,"grant select on %s to public;",series_lower);
  p += sprintf(p,"grant select on %s_seq to public;",series_lower);
  p += sprintf(p,"grant delete on %s to sumsadmin;",series_lower);
  if(drms_dms(session, NULL, createstmt))
    goto failure;

  /* default permission for owner of the namespace */
  sprintf(createstmt, "select owner from admin.ns where name = '%s'", namespace);
  if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
    if (qres->num_rows > 0) {
      char *nsowner = qres->field[0][0];
      p = createstmt;
      p += sprintf(p, "grant select, insert, update, delete on %s to %s;", series_lower, nsowner);
      p += sprintf(p, "grant update on %s_seq to %s;", series_lower, nsowner);
      if(drms_dms(session, NULL, createstmt))
	goto failure;
    } else {
      fprintf(stderr, "Failed: %s\n", createstmt);
      goto failure;
    }
  } else {
    fprintf(stderr, "Failed: %s\n", createstmt);
    goto failure;
  }      
  db_free_text_result(qres);

  /* The following is not well defined, hence leftout for now */
  if (0 && perms)
  {
    char permstr[30];
    p = permstr;
    if (perms & DB_PRIV_SELECT)
      p += sprintf(p,"select");
    if (perms & DB_PRIV_INSERT)
    {
      if (p!=permstr)
	p += sprintf(p,", ");
      p += sprintf(p,"insert");
    }
    if (perms & DB_PRIV_UPDATE)
    {
      if (p!=permstr)
	p += sprintf(p,", ");
      p += sprintf(p,"update");
    }
    sprintf(createstmt,"grant %s on %s to jsoc",permstr,series_lower);
#ifdef DEBUG
    printf("Setting permisions on table with '%s'\n",createstmt);
#endif
    /* Give table privileges to public. */
    if(drms_dms(session, NULL, createstmt))
      goto failure;

    sprintf(createstmt,"grant %s on %s_seq to jsoc",permstr,series_lower);
#ifdef DEBUG
    printf("Setting permisions on table with '%s'\n",createstmt);
#endif
    /* Give table privileges to public. */
    if(drms_dms(session, NULL, createstmt))
      goto failure;
    
    if (perms & DB_PRIV_SELECT)
    {
      sprintf(createstmt,"grant select on %s to jsoc_reader",series_lower);
      /* Give table privileges to public. */
      if(drms_dms(session, NULL, createstmt))
	goto failure;
      sprintf(createstmt,"grant select on %s_seq to jsoc_reader",series_lower);
      /* Give table privileges to public. */
      if(drms_dms(session, NULL, createstmt))
	goto failure;
    }
  }
  free(namespace);
  free(series_lower);
  free(pidx_buf);
  free(createstmt);
  return 0;
 failure:
  fprintf(stderr,"drms_insert_series(): failed to insert series %s.\n",
	  template->seriesinfo->seriesname);
  free(namespace);
  free(series_lower);
  free(pidx_buf);
  free(createstmt);
  return 1;
}



int drms_delete_series(DRMS_Env_t *env, char *series, int cascade)
{
  char query[1024], *series_lower, *namespace;
  DB_Binary_Result_t *qres;
  DRMS_Session_t *session;
  int drmsstatus = DRMS_SUCCESS;
  DRMS_Array_t *array = NULL;

  session = env->session;
  sprintf(query,"select seriesname from %s() where seriesname ~~* '%s'",
	  DRMS_MASTER_SERIES_TABLE, series);
#ifdef DEBUG
  printf("drms_delete_series: query = %s\n",query);
#endif
  if ((qres = drms_query_bin(session, query)) == NULL)
  {
    printf("Query failed. Statement was: %s\n", query);
    return 1;
  }
#ifdef DEBUG
  db_print_binary_result(qres);
#endif
  if (qres->num_rows==1)
  {
   
     /* Fetch an array of unique SUNUMs - the prime-key logic gets applied first. */
     array = drms_record_getvector(env, 
                                   series, 
                                   "sunum", 
                                   DRMS_TYPE_LONGLONG, 
                                   1, 
                                   &drmsstatus);

     if (!drmsstatus && array && array->naxis == 2 && array->axis[0] == 1 && array->axis[1] > 0)
     {
        series_lower = strdup(series);
        /* series_lower is fully qualified, i.e., it contains namespace */
        strtolower(series_lower);
        get_namespace(series, &namespace, NULL);
        if (cascade) {
           sprintf(query,"drop table %s",series_lower);
           if (drms_dms(session,NULL,query))
             goto bailout;
           if (drms_sequence_drop(session, series_lower))
             goto bailout;
        }
        sprintf(query, "set search_path to %s", namespace);
        if (drms_dms(session,NULL,query)) {
           fprintf(stderr, "Failed: %s\n", query);
           goto bailout;
        }
        sprintf(query,"delete from %s where seriesname ~~* '%s'",
                DRMS_MASTER_LINK_TABLE,series);
        if (drms_dms(session,NULL,query))
          goto bailout;
        sprintf(query,"delete from %s where seriesname ~~* '%s'",
                DRMS_MASTER_KEYWORD_TABLE, series);
        if (drms_dms(session,NULL,query))
          goto bailout;
        sprintf(query,"delete from %s where seriesname ~~* '%s'",
                DRMS_MASTER_SEGMENT_TABLE, series);
        if (drms_dms(session,NULL,query))
          goto bailout;
        sprintf(query,"delete from %s where seriesname ~~* '%s'",
                DRMS_MASTER_SERIES_TABLE,series);
        if (drms_dms(session,NULL,query))
          goto bailout;

        /* If the DRMS deletions occurred without a hitch, then delete the 
         * SUMS files from SUMS. */
        /* If this is a sock-module, must pass the vector SUNUM by SUNUM 
         * to drms_server. */
        drms_dropseries(env, series, array);

        /* Both DRMS servers and clients have a series_cache (one item per
           each series in all of DRMS). So, always remove the deleted series
           from this cache, whether or not this is a server. */
        hcon_remove(&env->series_cache,series_lower);
     }
     else
     {
        fprintf(stderr, "Couldn't create vector of sunum keywords.\n");
        goto bailout;
     }
  }
  else if (qres->num_rows>1)
  {
    fprintf(stderr,"TOO MANY ROWS RETURNED IN DRMS_DELETE_SERIES\n"); 
    /* This should never happen since seriesname is a unique index on 
       the DRMS series table. */
    return 1;
  }
  else {
    /* The series doesn't exist. */
    fprintf(stderr, "Series '%s' does not exist\n", series);
    return DRMS_ERROR_UNKNOWNSERIES;
  }
  db_free_binary_result(qres);
  
  free(series_lower);
  free(namespace);
  if (array)
  {
     drms_free_array(array);
  }
  return 0;
 bailout:
  fprintf(stderr,"drms_delete_series(): failed to delete series %s\n", series); 
  free(series_lower);
  free(namespace);
  if (array)
  {
     drms_free_array(array);
  }
  return 1;
}

static int drms_series_keynamesort(const void *first, const void *second)
{
   if (first && second)
   {
      const char *rFirst = *((const char **)first);
      const char *rSecond = *((const char **)second);

      if (!rFirst && !rSecond)
      {
	 return 0;
      }
      else if (!rFirst)
      {
	 return 1;
      }
      else if (!rSecond)
      {
	 return -1;
      }
      else
      {
	 return strcmp(rFirst, rSecond);
      }
   }

   return 0;
}

static int drms_series_intpkeysmatch(DRMS_Record_t *recTemp1, 
				     char **pkArray1, 
				     int nPKeys1, 
				     DRMS_Record_t *recTemp2, 
				     char **pkArray2, 
				     int nPKeys2)
{
   int ret = 0;

   if (nPKeys1 == nPKeys2)
   {
      /* sort each series' keys */
      qsort(pkArray1, nPKeys1, sizeof(char *), drms_series_keynamesort);
      qsort(pkArray2, nPKeys2, sizeof(char *), drms_series_keynamesort);
      
      DRMS_Keyword_t *key1 = NULL;
      DRMS_Keyword_t *key2 = NULL;
      
      int i = 0;
      for (; i < nPKeys1; i++)
      {
	 if (strcmp(pkArray1[i], pkArray2[i]) == 0)
	 {
	    key1 = hcon_lookup_lower(&(recTemp1->keywords), pkArray1[i]);
	    key2 = hcon_lookup_lower(&(recTemp2->keywords), pkArray1[i]);
	    
	    XASSERT(key1 != NULL && key2 != NULL);
	    if (key1 != NULL && key2 != NULL)
	    {
	       if (key1->info->type != key2->info->type ||
		   key1->info->recscope != key2->info->recscope ||
                   drms_keyword_getperseg(key1) != drms_keyword_getperseg(key2))
	       {
		  break;
	       }
	    }
	    else
	    {
	       break;
	    }
	 }
	 else
	 {
	    break;
	 }
      } /* for */
      
      if (i == nPKeys1)
      {
	 ret = 1;
      }
   }

   return ret;
}

/* Returns 0 on error. */
/* Returns 1 if keys2 is equal to keys1. */
static int drms_series_pkeysmatch(DRMS_Env_t *drmsEnv, 
				  const char *series1, 
				  const char *series2, 
				  int *status)
{
   int ret = 0;

   int nPKeys1 = 0;
   int nPKeys2 = 0;

   DRMS_Record_t *recTemp1 = drms_template_record(drmsEnv, series1, status);
   DRMS_Record_t *recTemp2 = NULL;

   if (*status == DRMS_SUCCESS)
   {
      recTemp2 = drms_template_record(drmsEnv, series2, status);
   }

   if (*status == DRMS_SUCCESS)
   {
      char **pkArray1 = drms_series_createrealpkeyarray(drmsEnv, series1, &nPKeys1, status);
      char **pkArray2 = NULL;
      
      if (*status == DRMS_SUCCESS)
      {
	 pkArray2 = drms_series_createrealpkeyarray(drmsEnv, series2, &nPKeys2, status);
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 ret = drms_series_intpkeysmatch(recTemp1, pkArray1, nPKeys1, recTemp2, pkArray2, nPKeys2);

	 if (!ret)
	 {
	    fprintf(stdout, 
		    "Series %s prime key does not match series %s prime key.\n",
		    series1,
		    series2);
	 }
      }

      if (pkArray1)
      {
	 drms_series_destroypkeyarray(&pkArray1, nPKeys1);
      }

      if (pkArray2)
      {
	 drms_series_destroypkeyarray(&pkArray2, nPKeys2);
      }
   }
   
   return ret;
}

static int drms_series_intcreatematchsegs(DRMS_Env_t *drmsEnv, 
					  const char *series, 
					  DRMS_Record_t *recTempl,
					  HContainer_t *matchSegs, 
					  int *status)
{
   int nMatch = 0;

   DRMS_Record_t *seriesRec = drms_template_record(drmsEnv, series, status);

   if (*status ==  DRMS_SUCCESS)
   {
      HContainer_t *s1SegCont = &(seriesRec->segments);
      HContainer_t *s2SegCont = &(recTempl->segments);
      
      if (s1SegCont && s2SegCont)
      {
	 HIterator_t *s1Hit = hiter_create(s1SegCont);
	 HIterator_t *s2Hit = hiter_create(s2SegCont);
	 
	 if (s1Hit && s2Hit)
	 {
	    DRMS_Segment_t *s1Seg = NULL;
	    DRMS_Segment_t *s2Seg = NULL;
	    
	    while ((s1Seg = (DRMS_Segment_t *)hiter_getnext(s1Hit)) != NULL)
	    {
	       if ((s2Seg = 
		    (DRMS_Segment_t *)hcon_lookup_lower(s2SegCont, s1Seg->info->name)) != NULL)
	       {
		  /* Must check for segment equivalence. */
		  if (drms_segment_segsmatch(s1Seg, s2Seg))
		  {
		     nMatch++;
		     
		     if (nMatch == 1)
		     {
			hcon_init(matchSegs, 
				  DRMS_MAXSEGNAMELEN, 
				  DRMS_MAXSEGNAMELEN, 
				  NULL, 
				  NULL);
		     }
		     
		     char *newSeg = (char *)hcon_allocslot(matchSegs, s1Seg->info->name);
		     if (newSeg != NULL)
		     {
			strncpy(newSeg, s1Seg->info->name, DRMS_MAXSEGNAMELEN);
			newSeg[DRMS_MAXSEGNAMELEN - 1] = '\0';
		     }
		     else
		     {
			nMatch = -1;
			break;
		     }
		  }
	       }
	    }
	 }
	 
	 if (s1Hit)
	 {
	    hiter_destroy(&s1Hit);
	 }
	 
	 if (s2Hit)
	 {
	    hiter_destroy(&s2Hit);
	 }
      }
   }

   return nMatch;
}

/* Fills in matchSegs with pointers to template segments. */
static int drms_series_creatematchsegs(DRMS_Env_t *drmsEnv, 
				       const char *series1, 
				       const char *series2, 
				       HContainer_t *matchSegs, 
				       int *status)
{
   int nMatch = 0;

   if (matchSegs)
   {
      DRMS_Record_t *s2RecTempl = drms_template_record(drmsEnv, series2, status);   
     
      if (*status == DRMS_SUCCESS)
      {
	 nMatch = drms_series_intcreatematchsegs(drmsEnv,
						 series1,
						 s2RecTempl,
						 matchSegs,
						 status);

	 if (nMatch == 0)
	 {
	    fprintf(stdout, "Series %s and %s have no matching segments.\n", series1, series2);
	 }
      }
   }
   else
   {
      fprintf(stderr, "Must provide HContainer_t to CreateMatchingSegs\n");
   }

   return nMatch;
}

/* Slotted keywords associated with prime index keywords ARE prime 
 * keyword from the user's perspective.  So, put those in the array
 * returned.
 */
static char **drms_series_intcreatepkeyarray(DRMS_Record_t *recTempl, 
					     int *nPKeys,
					     DRMS_PrimeKeyType_t pktype,
					     int *status)
{
   char **ret = NULL;

   if (recTempl != NULL)
   {
      int nKeys = recTempl->seriesinfo->pidx_num;
      int iKey = 0;
      
      ret = (char **)malloc(sizeof(char *) * nKeys);
      
      if (ret != NULL)
      {
	 while (iKey < nKeys)
	 {
	    DRMS_Keyword_t *pkey = recTempl->seriesinfo->pidx_keywords[iKey];

	    if (drms_keyword_isindex(pkey) && pktype == kPkeysDRMSExternal)
	    {
	       /* Use slotted keyword */
	       pkey = drms_keyword_slotfromindex(pkey);
	       ret[iKey] = strdup(pkey->info->name);
	    }
	    else
	    {
	       ret[iKey] = strdup(pkey->info->name);
	    }
	    iKey++;
	 }
	 
	 *nPKeys = nKeys;
      }
      else
      {
	 *status = DRMS_ERROR_OUTOFMEMORY;
      }
   }
   else
   {
      *status = DRMS_ERROR_INVALIDDATA;
   }

   return ret;
}

/* INTERNAL only! */
char **drms_series_createrealpkeyarray(DRMS_Env_t *env, 
				       const char *seriesName, 
				       int *nPKeys,
				       int *status)
{
     char **ret = NULL;
     int stat = 0;

     DRMS_Record_t *template = drms_template_record(env, seriesName, &stat);

     if (template != NULL && stat == DRMS_SUCCESS)
     {
	ret = drms_series_intcreatepkeyarray(template, nPKeys, kPkeysDRMSInternal, &stat);
     }

     if (status)
     {
	*status = stat;
     }

     return ret;
}

/* External */
char **drms_series_createpkeyarray(DRMS_Env_t *env, 
				       const char *seriesName, 
				       int *nPKeys,
				       int *status)
{
     char **ret = NULL;
     int stat = 0;

     DRMS_Record_t *template = drms_template_record(env, seriesName, &stat);

     if (template != NULL && stat == DRMS_SUCCESS)
     {
	ret = drms_series_intcreatepkeyarray(template, 
					     nPKeys, 
					     kPkeysDRMSExternal, 
					     &stat);
     }

     if (status)
     {
	*status = stat;
     }

     return ret;
}

void drms_series_destroypkeyarray(char ***pkeys, int nElements)
{
     int iElement = 0;
     char **array = *pkeys;

     while (iElement < nElements)
     {
	  if (array[iElement] != NULL)
	  {
	       free(array[iElement]);
	  }

	  iElement++;
     }

     free(array);
     *pkeys = NULL;
}

/* For modules like arithtool */
int drms_series_checkseriescompat(DRMS_Env_t *drmsEnv,
				  const char *series1, 
				  const char *series2, 
				  HContainer_t *matchSegs,
				  int *status)
{
   int ret = 0;
   int nMatch = 0;

   /* Ensure prime keywords match exactly. */   
   if (drms_series_pkeysmatch(drmsEnv, series1, series2, status) && *status == DRMS_SUCCESS)
   {
      /* Create a list of matching segments, if they exist. */
      nMatch = drms_series_creatematchsegs(drmsEnv, series1, series2, matchSegs, status);

      if (nMatch == 0)
      {
	 fprintf(stdout, "Series %s and %s have no matching segments.\n", series1, series2);
      }
   }

   if (*status == DRMS_SUCCESS)
   {
      ret = nMatch > 0;
   }
   
   return ret;
}

/* For modules like regrid */
/* Caller must specify a segment in the series to check. A segment is really specified 
 * by not only a series and segment name, but also by a primaryIndex, so the caller 
 * must specify a set of keyword names. */

/* recTempl contains the segment information that the caller wants to write. It can 
 * be a prototype record or template. */
/* The difference between this function and the previous one is that recTempl may 
 * refer to a record in a series that hasn't been created yet. */
int drms_series_checkrecordcompat(DRMS_Env_t *drmsEnv,
				  const char *series,
				  DRMS_Record_t *recTempl,
				  HContainer_t *matchSegs,
				  int *status)
{
   int ret = 0;
   int nMatch = 0;
   DRMS_Record_t *seriesTempl = NULL;
   int nSeriesPKeys = 0;
   int nPKeys = 0;
   char **seriesPKArray = NULL;
   char **pkArray = NULL;
   
   seriesPKArray = drms_series_createrealpkeyarray(drmsEnv, 
						   series, 
						   &nSeriesPKeys, 
						   status);
   if (*status == DRMS_SUCCESS)
   {
      pkArray = drms_series_intcreatepkeyarray(recTempl,
					       &nPKeys,
					       kPkeysDRMSInternal,
					       status);
      
      if (*status == DRMS_SUCCESS)
      {
	 seriesTempl = drms_template_record(drmsEnv, series, status);

	 if (*status == DRMS_SUCCESS)
	 {
	    if (drms_series_intpkeysmatch(seriesTempl, 
					  seriesPKArray, 
					  nSeriesPKeys, 
					  recTempl,
					  pkArray, 
					  nPKeys))
	    {
	       /* Now check for acceptable segments */
	       nMatch = drms_series_intcreatematchsegs(drmsEnv, 
						       series, 
						       recTempl, 
						       matchSegs, 
						       status);

	       if (nMatch == 0)
	       {
		  fprintf(stdout, 
			  "No series %s segment matches a series %s segment.\n",
			  recTempl->seriesinfo->seriesname,
			  series);  
	       }
	    }
	    else
	    {
	       fprintf(stdout, 
		       "Series %s prime key does not match series %s prime key.\n",
		       recTempl->seriesinfo->seriesname,
		       series);
	    }
	 }
      }
   }

   if (*status == DRMS_SUCCESS)
   {
      ret = nMatch > 0;
   }

   if (seriesPKArray)
   {
      drms_series_destroypkeyarray(&seriesPKArray, nSeriesPKeys);
   }

   if (pkArray)
   {
      drms_series_destroypkeyarray(&pkArray, nPKeys);
   }
   
   return ret;
}

int drms_series_checkkeycompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Keyword_t *keys,
			       int nKeys,
			       int *status)
{
   int ret = 0;
   
   DRMS_Record_t *recTempl = drms_template_record(drmsEnv, series, status);
   if (*status == DRMS_SUCCESS)
   {
      int iKey = 0;
      ret = 1;
      for (; iKey < nKeys; iKey++)
      {
	 DRMS_Keyword_t *oneKey = &(keys[iKey]);
	 DRMS_Keyword_t *sKey = drms_keyword_lookup(recTempl, 
						    oneKey->info->name, 
						    0);

	 if (sKey)
	 {
	    if (!drms_keyword_keysmatch(oneKey, sKey))
	    {
	       ret = 0;
	       break;
	    }
	 }
	 else
	 {
	    ret = 0;
	    break;
	 }
      }
   }
   
   return ret;
}

int drms_series_checksegcompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Segment_t *segs,
			       int nSegs,
			       int *status)
{
   int ret = 0;
   
   DRMS_Record_t *recTempl = drms_template_record(drmsEnv, series, status);
   if (*status == DRMS_SUCCESS)
   {
      int iSeg = 0;
      ret = 1;
      for (; iSeg < nSegs; iSeg++)
      {
	 DRMS_Segment_t *oneSeg = &(segs[iSeg]);
	 DRMS_Segment_t *sSeg = drms_segment_lookup(recTempl, oneSeg->info->name);

	 if (sSeg)
	 {
	    if (!drms_segment_segsmatch(oneSeg, sSeg))
	    {
	       ret = 0;
	       break;
	    }
	 }
	 else
	 {
	    ret = 0;
	    break;
	 }
      }
   }

   return ret;
}

/* Returns true iff si >= v.first && si <= v.last */
int drms_series_isvers(DRMS_SeriesInfo_t *si, DRMS_SeriesVersion_t *v)
{
   long long smajor;
   long long sminor;
   long long vmajor;
   long long vminor;

   int ok = 1;

   if (*(si->version) == '\0')
   {
      ok = 0;
   }
   else if (sscanf(si->version, "%lld.%lld", &smajor, &sminor) == 2)
   {
      if (*(v->first) != '\0')
      {
	 /* Series must be GTE to first */
	 if (sscanf(v->first, "%lld.%lld", &vmajor, &vminor) == 2)
	 {
	    if (smajor < vmajor || (smajor == vmajor && sminor < vminor))
	    {
	       ok = 0;
	    }
	 }
	 else
	 {
	    fprintf(stderr, "Invalid series version '%s'.\n", v->first);
	    ok = 0;
	 }
      }

      if (ok && *(v->last) != '\0')
      {
	 /* Series must be LTE to last */
	 if (sscanf(v->last, "%lld.%lld", &vmajor, &vminor) == 2)
	 {
	    if (smajor > vmajor || (smajor == vmajor && sminor > vminor))
	    {
	       ok = 0;
	    }
	 }
	 else
	 {
	     fprintf(stderr, "Invalid series version '%s'.\n", v->last);
	     ok = 0;
	 }
      }
   }
   else
   {
      fprintf(stderr, "Invalid series version '%s'.\n", si->version);
      ok = 0;
   }

   return ok;
}
