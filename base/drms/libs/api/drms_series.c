//#define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "atoinc.h"

#define kTableOfCountsNS   "public"
#define kTableOfCountsTab  "recordcounts"
#define kTableOfCountsColSeries "seriesname"
#define kTableOfCountsColNRecs "nrecords"
#define kShadowSuffix "_shadow"
#define kShadowColNRecs "nrecords"
#define kShadowColRecnum "recnum"

#if (defined TOC && TOC)
static int TocExists(DRMS_Env_t *env, int *status)
{
   int istat = DRMS_SUCCESS;
   int tabexists = 0;

   tabexists = drms_query_tabexists(env->session, kTableOfCountsNS, kTableOfCountsTab, &istat);

   if (status)
   {
      *status = istat;
   }

   return tabexists;
}
#endif

static int ShadowExists(DRMS_Env_t *env, const char *series, int *status)
{
   int istat = DRMS_SUCCESS;
   int tabexists = 0;
   char *namespace = NULL;
   char *table = NULL;
   char shadowtable[DRMS_MAXSERIESNAMELEN];

   if (!get_namespace(series, &namespace, &table))
   {
      snprintf(shadowtable, sizeof(shadowtable), "%s%s", table, kShadowSuffix);
      tabexists = drms_query_tabexists(env->session, namespace, shadowtable, &istat);

      free(namespace);
      free(table);
   }
   else
   {
      istat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = istat;
   }

   return tabexists;
}

/* Determine if 'series' is present in ns.tab */
static int IsSeriesPresent(DRMS_Env_t *env, const char *ns, const char *tab, const char *series, int *status)
{
   char *query = NULL;
   size_t stsz = 8192;
   int istat = DRMS_SUCCESS;
   int ans = -1;
   DB_Text_Result_t *tres = NULL;
   char *lcseries = strdup(series);

   if (lcseries)
   {
      strtolower(lcseries);

      query = malloc(stsz);
      if (query)
      {
         *query = '\0';

         query = base_strcatalloc(query, "SELECT seriesname from ", &stsz);
         query = base_strcatalloc(query, ns, &stsz);
         query = base_strcatalloc(query, ".", &stsz);
         query = base_strcatalloc(query, tab, &stsz);
         query = base_strcatalloc(query, " WHERE lower(seriesname) = '", &stsz);
         query = base_strcatalloc(query, lcseries, &stsz);
         query = base_strcatalloc(query, "'", &stsz);

         tres = drms_query_txt(env->session, query);
      
         if (tres)
         {
            if (tres->num_rows == 1)
            {
               ans = 1;
            }
            else if (tres->num_rows == 0)
            {
               ans = 0;
            }
            else
            {
               istat = DRMS_ERROR_BADDBQUERY;
            }

            db_free_text_result(tres);
            tres = NULL;
         }
         else
         {
            fprintf(stderr, "Failed: %s\n", query);
            istat = DRMS_ERROR_BADDBQUERY;
         }

         free(query);
         query = NULL;
      }
      else
      {
         istat = DRMS_ERROR_OUTOFMEMORY;
      }

      free(lcseries);
      lcseries = NULL;
   }
   else
   {
      istat = DRMS_ERROR_OUTOFMEMORY;
   }
   
   if (status)
   {
      *status = istat;
   }

   return ans;
}

static void DetermineDefval(DRMS_Keyword_t *key, char *defvalout)
{
    int pret = 0;
    TIME interval;
    int internal;
    char defval[DRMS_DEFVAL_MAXLEN];
    
    if (defvalout)
    {
        if (key->info->type == DRMS_TYPE_TIME)
        {
            interval = atoinc(key->info->unit);
            internal = (interval > 0);
            
            pret = drms_sprintfval(defval, 
                                   key->info->type, 
                                   &key->value, 
                                   internal);
            XASSERT(pret < DRMS_DEFVAL_MAXLEN);
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
            pret = drms_sprintfval(defval, 
                                   key->info->type,
                                   &key->value,	 
                                   0);
            XASSERT(pret < DRMS_DEFVAL_MAXLEN);
            
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
        
        snprintf(defvalout, DRMS_DEFVAL_MAXLEN, "%s", defval);
    }
}
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
  char defval[DRMS_DEFVAL_MAXLEN]={0};
  char *createstmt=0, *series_lower=0, *namespace=0;
  DB_Text_Result_t *qres;

  createstmt = malloc(30000);
  XASSERT(createstmt);

  /* Make sure links have pidx's set. */
  if (drms_link_getpidx(template) != DRMS_SUCCESS)
  {
     goto failure;
  }

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
    pidx_buf = malloc(2);
    XASSERT(pidx_buf);
    *pidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->pidx_num; i++)
      len += strlen((si->pidx_keywords[i])->info->name) + 3;
    pidx_buf = malloc(len+1);
    XASSERT(pidx_buf);
    memset(pidx_buf,0,len+1);
    p = pidx_buf;
    p += sprintf(p,"%s",(si->pidx_keywords[0])->info->name);
    for (i=1; i<si->pidx_num; i++)
      p += sprintf(p,", %s",(si->pidx_keywords[i])->info->name);
  }

  if (si->dbidx_num <= 0)
  {
    dbidx_buf = malloc(2);
    XASSERT(dbidx_buf);
    *dbidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->dbidx_num; i++)
      len += strlen((si->dbidx_keywords[i])->info->name) + 3;
    dbidx_buf = malloc(len+1);
    XASSERT(dbidx_buf);
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
  hiter_new_sort(&hit, &template->links, drms_link_ranksort); /* Iterator for link container. */
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
    {
        hiter_free(&hit);
        goto failure;
    }
  }
    
  hiter_free(&hit);
    
  /* Keyword fields. */
  hiter_new_sort(&hit, &template->keywords, drms_keyword_ranksort); /* Iterator for keyword container. */
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
      
      DetermineDefval(key, defval);

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
		  DB_INT4, key->info->islink,DB_INT4, key->info->recscope, /* stored in the isconstant column of
                                                                            * drms_keyword. */
		  DB_INT4, key->info->kwflags))
    {
        hiter_free(&hit);
        goto failure;
    }

    /* key->info->per_segment overload: This will work on <= drms_series.c:1.23 because
     * in that version of DRMS, the version of drms_parser.c will not have the 
     * code that overloads key->info->per_segment.
     */
    if (drms_keyword_getperseg(key))
      key->info->name[len-4] = '_';

  }
    
  hiter_free(&hit);
    
  /* Segment fields. */
  hiter_new_sort(&hit, &template->segments, drms_segment_ranksort); /* Iterator for segment container. */
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
      hiter_free(&hit);
      goto failure;
    }
    if (seg->info->naxis < 0 || seg->info->naxis>DRMS_MAXRANK)
    {
      printf("ERROR: Invalid value of rank (%d).\n",seg->info->naxis);
      hiter_free(&hit);
      goto failure;
    }
    else
    {
      axisstr = malloc(2*seg->info->naxis*20+1);
      XASSERT(axisstr);
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
      hiter_free(&hit);
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
    
  hiter_free(&hit);
    
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
  free(dbidx_buf);
  free(createstmt);
  return 0;
 failure:
  fprintf(stderr,"drms_insert_series(): failed to insert series %s.\n",
	  template->seriesinfo->seriesname);
  free(namespace);
  free(series_lower);
  free(pidx_buf);
  free(dbidx_buf);
  free(createstmt);
  return 1;
}

static int RankReverseSort(const void *he1, const void *he2)
{
    DRMS_Keyword_t *k1 = (DRMS_Keyword_t *)hcon_getval(*((HContainerElement_t **)he1));
    DRMS_Keyword_t *k2 = (DRMS_Keyword_t *)hcon_getval(*((HContainerElement_t **)he2));
    
    XASSERT(k1 && k2);
    
    return (k1->info->rank < k2->info->rank) ? 1 : (k1->info->rank > k2->info->rank ? -1 : 0);
}
/* Returns -1 if the series does not have ranked keywords (with valid ranks), 
 * otherwise returns the 0-based rank. */
int drms_series_gethighestkeyrank(DRMS_Env_t *env, const char *series, int *status)
{
    int drmsstat = DRMS_SUCCESS;
    DRMS_Record_t *rec = NULL;
    HContainer_t *keys = NULL;
    DRMS_Keyword_t *key = NULL;
    HIterator_t hit;
    int rv = -1; /* The series does not have rankings. */
    
    rec = drms_template_record(env, series, &drmsstat);
    
    if (!rec || drmsstat)
    {
        if (!drmsstat)
        {
            drmsstat = DRMS_ERROR_UNKNOWNSERIES;
        }
        
        fprintf(stderr, "Unable to obtain template record for series %s; error %d.\n", series, drmsstat);
    }
    else
    {
        keys = &rec->keywords;
        if (hcon_size(keys) > 0)
        {
            hiter_new_sort(&hit, keys, RankReverseSort);
            key = (DRMS_Keyword_t *)hiter_getnext(&hit);
            if (!key)
            {
                fprintf(stderr, "Missing keyword.\n");
                drmsstat = DRMS_ERROR_UNKNOWNKEYWORD;
            }
            else
            {
                rv = key->info->rank;
            }
            
            hiter_free(&hit);
        }
    }
    
    if (status)
    {
        *status = drmsstat;
    }
    
    return rv;
}

/* This function will take as input a jsd keyword specification, and a series
 * name, and it will modify the series' db information to add the keywords listed
 * in the spec. 
 *
 * For now, the function will not add keys that are members of the prime-key set.
 * In fact, with this function, there is no way to specify that this key is 
 * a member of the prime-key set.
 *
 * sql will contain the SQL needed to add the columns to the series table, as well
 * as add the rows to the <ns>.drms_keyword table, IFF the series is a member of a
 * Slony replication-set. Otherwise, sql will contain NULL. These SQL statements must 
 * be executed on the Slony master. To do this, run a script that calls a DRMS module
 * that calls this function. The module should return to the script the SQL.
 */
int drms_addkeys_toseries(DRMS_Env_t *env, const char *series, const char *spec, char **sql)
{
    int drmsstat = DRMS_SUCCESS;
    DRMS_Record_t *seriestemp = NULL;
    HContainer_t *keys = NULL;
    int hirank = -1;
    HIterator_t *hit = NULL;
    DRMS_Keyword_t *key = NULL;
    int isrepl = 0; /* 1 if the series is under slony repication. */
    char defval[DRMS_DEFVAL_MAXLEN];
    DRMS_Session_t *session = env->session;
    char numbuf[32];
    size_t szalterbuf;
    char *tblactionbuf = NULL;
    size_t szbuf;
    char *sqlbuf = NULL;
    int first;
    int skipkey;
    int len;
    char *ns = NULL;
    char *tab = NULL;
    int nkeys;
    
    *sql = NULL;
    
    if (series && *series && spec && *spec)
    {
        seriestemp = drms_template_record(env, series, &drmsstat);
        
        if (seriestemp && drmsstat == DRMS_SUCCESS)
        {
            isrepl = drms_series_isreplicated(env, series);
            
            /* Parse spec with drms_parser code. This will return a container of DRMS_Keyword_t structs. */
            keys = drms_parse_keyworddesc(env, spec, &drmsstat);
            
            if (keys && !drmsstat && hcon_size(keys) >= 1)
            {
                /* hirank is a 0-based rank. */
                hirank = drms_series_gethighestkeyrank(env, series, &drmsstat);
                if (drmsstat)
                {
                    hirank = -1;
                    drmsstat = DRMS_SUCCESS;
                }
                
                if (isrepl) 
                {
                    szbuf = 1024;
                    sqlbuf = calloc(1, szbuf);
                    
                    if (!sqlbuf)
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                    else
                    {
                        szalterbuf = 512;
                        tblactionbuf = calloc(1, szalterbuf);
                        
                        if (!tblactionbuf)
                        {
                            drmsstat = DRMS_ERROR_OUTOFMEMORY;
                        }
                    }
                }
                
                if (drmsstat == DRMS_SUCCESS)
                {
                    if (get_namespace(series, &ns, &tab))
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
                
                if (drmsstat == DRMS_SUCCESS)
                {
                    /* Set the pointer from the key struct to the containing record. */
                    hit = hiter_create(keys);
                    
                    if (hit)
                    {
                        first = 1;
                        nkeys = 0;
                        while((key = (DRMS_Keyword_t *)hiter_getnext(hit)) != NULL)
                        {
                            /* First, ensure that the keyword to add does not already exist. */
                            if (drms_keyword_lookup(seriestemp, key->info->name, 0))
                            {
                                fprintf(stderr, "Cannot add keyword %s, skipping.\n", key->info->name);
                                continue;
                            }
                            
                            nkeys++;
                            
                            key->record = seriestemp;
                            
                            /* Set the keyword's rank. */
                            
                            /* Determine if this is an old series where the keyword rank was not inlcuded in the kwflags 
                             * column of drms_keyword. If so, do not set the rank bits of the kwflags column. Otherwise, 
                             * determine the rank of the highest-ranked keyword, and add one to that rank before setting
                             * the kwflags column value. REMEMBER: the rank in the DRMS_Keyword_t struct is 0-based, but 
                             * the rank in the kwflags column is 1-based. */
                            if (hirank >= 0)
                            {
                                key->info->rank = hirank; /* 0-based */
                                key->info->kwflags |= (hirank + 1) << 16; /* 1-based - goes directly into db. */
                                hirank++;
                            }
                            else
                            {
                                key->info->rank = -1;
                                key->info->kwflags &= 0x0000FFFF;
                            }
                            
                            DetermineDefval(key, defval);
                            
                            /* Create the actual SQL. */
                            
                            /* ALTER TABLE action statement */
                            if (first)
                            {
                                first = 0;
                            }
                            else
                            {
                                tblactionbuf = base_strcatalloc(tblactionbuf, ", ", &szalterbuf);
                            }

                            tblactionbuf = base_strcatalloc(tblactionbuf, "ADD COLUMN ", &szalterbuf);
                            tblactionbuf = base_strcatalloc(tblactionbuf, key->info->name, &szalterbuf);
                            
                            if (!key->info->islink && !drms_keyword_isconstant(key))
                            {
                                tblactionbuf = base_strcatalloc(tblactionbuf, " ", &szalterbuf);
                                if (key->info->type == DRMS_TYPE_STRING)
                                {
                                    tblactionbuf = base_strcatalloc(tblactionbuf, db_stringtype_maxlen(4000), &szalterbuf);
                                }
                                else
                                {
                                    tblactionbuf = base_strcatalloc(tblactionbuf, db_type_string(drms2dbtype(key->info->type)), &szalterbuf);
                                }
                            }
                            
                            skipkey = 0;
                            if (drms_keyword_getperseg(key))
                            {
                                len = strlen(key->info->name);
                                if (strcmp(key->info->name + len - 4, "_000"))
                                {
                                    skipkey = 1;
                                }
                                else
                                {
                                    key->info->name[len - 4] = 0;
                                    skipkey = 0;
                                }
                            }
                            
                            if (isrepl)
                            {                                
                                /* ns.drms_keyword insert statement */
                                /* We add only a single row for a family of per-segment keywords. */
                                                                
                                if (!skipkey)
                                {
                                    sqlbuf = base_strcatalloc(sqlbuf, "INSERT INTO ", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ns, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ".", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, DRMS_MASTER_KEYWORD_TABLE, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "(seriesname, keywordname, linkname, targetkeyw, type, defaultval, format, unit, description, islink, isconstant, persegment) VALUES (", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "'", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->record->seriesinfo->seriesname, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->name, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->linkname, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->target_key, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, drms_type2str(key->info->type), &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, defval, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->format, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->unit, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->description, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "',", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->islink ? "1" : "0", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ",", &szbuf);
                                    snprintf(numbuf, sizeof(numbuf), "%d", (int)key->info->recscope);
                                    sqlbuf = base_strcatalloc(sqlbuf, numbuf, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ",", &szbuf);
                                    snprintf(numbuf, sizeof(numbuf), "%d", key->info->kwflags);
                                    sqlbuf = base_strcatalloc(sqlbuf, numbuf, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ")\n", &szbuf);
                                }
                            }
                            else
                            {
                                *sql = NULL;
                                
                                if (!skipkey)
                                {
                                    if (drms_dmsv(session, 
                                                  NULL, 
                                                  "INSERT INTO " DRMS_MASTER_KEYWORD_TABLE
                                                  "(seriesname, keywordname, linkname, targetkeyw, type, "
                                                  "defaultval, format, unit, description, islink, "
                                                  "isconstant, persegment) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                                  -1,
                                                  DB_STRING, key->record->seriesinfo->seriesname, 
                                                  DB_STRING, key->info->name, 
                                                  DB_STRING, key->info->linkname, 
                                                  DB_STRING, key->info->target_key, 
                                                  DB_STRING, drms_type2str(key->info->type), 
                                                  DB_STRING, defval, 
                                                  DB_STRING, key->info->format, 
                                                  DB_STRING, key->info->unit,
                                                  DB_STRING, key->info->description,
                                                  DB_INT4, key->info->islink,
                                                  DB_INT4, key->info->recscope, /* stored in the isconstant column of
                                                                                 * drms_keyword. */
                                                  DB_INT4, key->info->kwflags))
                                    {
                                        drmsstat = DRMS_ERROR_BADDBQUERY;
                                    }
                                }
                            }
                        } /* end while loop */
                        
                        if (drmsstat == DRMS_SUCCESS && nkeys > 0)
                        {
                            tblactionbuf = base_strcatalloc(tblactionbuf, "\n", &szalterbuf);
                            
                            szalterbuf = 2048;
                            char *alterbuf = calloc(1, szalterbuf);
                            
                            if (alterbuf)
                            {
                                alterbuf = base_strcatalloc(alterbuf, "ALTER TABLE ", &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, series, &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, " ", &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, tblactionbuf, &szalterbuf);
                                
                                if (isrepl)
                                {
                                    /* If the table is being replicated, then we just concatenate the 
                                     * SQL that adds columns to the series table to the sqlbuf. */
                                    sqlbuf = base_strcatalloc(sqlbuf, alterbuf, &szbuf);
                                }
                                else
                                {
                                    /* If the table isn't being replicated, then we can go ahead and submit 
                                     * the SQL query that adds columns to the series table. */
                                    if (drms_dms(session, NULL, alterbuf))
                                    {
                                        drmsstat = DRMS_ERROR_BADDBQUERY;
                                    }
                                }
                                
                                free(alterbuf);
                                alterbuf = NULL;
                            }
                        }
                        
                        hiter_destroy(&hit);
                    }
                    else
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
            }
            else
            {
                fprintf(stderr, "Failed to parse keyword specification '%s'.\n", spec);
                drmsstat = DRMS_ERROR_BADJSD;
            }
            
            if (keys)
            {
                /* Free the keys container. This will deep-free any string values, but it will not free the info             
                 * struct. */
                hcon_destroy(&keys);
            }    
        }
        else
        {
            fprintf(stderr, "Unable to obtain template record for series %s.\n", series);
            drmsstat = DRMS_ERROR_UNKNOWNSERIES;
        }
    }
    else
    {
        drmsstat = DRMS_ERROR_INVALIDDATA;
    }
    
    if (drmsstat == DRMS_SUCCESS)
    {
        if (isrepl)
        {
            drmsstat = DRMS_ERROR_CANTMODPUBSERIES;
            
            if (sqlbuf && strlen(sqlbuf) > 0)
            {
                *sql = sqlbuf;
                sqlbuf = NULL;
            }
            else
            {
                drmsstat = DRMS_ERROR_BADJSD;
            }
        }
    }
    
    if (tblactionbuf)
    {
        free(tblactionbuf);
    }
    
    if (sqlbuf)
    {
        free(sqlbuf);
    }
    
    if (ns)
    {
        free(ns);
    }
    
    if (tab)
    {
        free(tab);
    }
    
    return drmsstat;
}

/* cascade - I think this means to delete the PostgreSQL record-table and sequence
 * objects, and the SUs that the record table refers to. cascade == 0 when called 
 * from modify_series, but cascade == 1 when called from delete_series. 
 * modify_series copies the record-table and sequence to a new table and sequence
 * for the series. These copied records still refer to original SUs (so you
 * don't want to delete them). 
 * keepsums - If this is set, then the call to SUMS to delete the storage units
 * belonging to the series is not made. */
int drms_delete_series(DRMS_Env_t *env, const char *series, int cascade, int keepsums)
{
  char query[1024], *series_lower = NULL, *namespace = NULL;
  DB_Binary_Result_t *qres;
  DRMS_Session_t *session;
  int drmsstatus = DRMS_SUCCESS;
  DRMS_Array_t *array = NULL;
  int repl = 0;
  int retstat = -1;

  if (!env->session->db_direct && !env->selfstart)
  {
     fprintf(stderr, "Can't delete series if using drms_server. Please use a direct-connect modules, or a self-starting socket-connect module.\n");
     goto bailout;
  }

  series_lower = strdup(series);
  /* series_lower is fully qualified, i.e., it contains namespace */
  strtolower(series_lower);

  /*Check to see if the series exists first */
  if (!drms_series_exists(env, series_lower, &drmsstatus))
  {
     fprintf(stderr, "The series '%s' does not exist.  Please enter a valid series name.\n", series);
     goto bailout;
  }

  if (!drms_series_candeleterecord(env, series_lower))
  {
     fprintf(stderr, "Permission failure - cannot delete series '%s'.\n", series);
     goto bailout;
  }

  /* Don't delete the series if it is being slony-replicated (for now).
   * Eventually, we may want to allow deletion of such series under certain
   * circumstances. */
  if ((repl = drms_series_isreplicated(env, series)) == 0)
  {
     session = env->session;
     sprintf(query,"select seriesname from %s() where lower(seriesname) = '%s'",
             DRMS_MASTER_SERIES_TABLE, series_lower);
#ifdef DEBUG
     printf("drms_delete_series: query = %s\n",query);
#endif
     if ((qres = drms_query_bin(session, query)) == NULL)
     {
        printf("Query failed. Statement was: %s\n", query);
        goto bailout;
     }
#ifdef DEBUG
     db_print_binary_result(qres);
#endif
     if (qres->num_rows==1)
     {
        DB_Binary_Result_t *suqres = NULL;

        get_namespace(series, &namespace, NULL);

        if (!keepsums)
        {
           /* If we are planning on deleting SUs, but in fact there are 
            * no SUs to delete because the series has no segments, then
            * we are essentially keeping SUs (and not passing a vector
            * of SUs to delete to SUMS). */
           snprintf(query, sizeof(query), "SELECT segmentname FROM %s.%s where lower(seriesname) = '%s'", namespace, DRMS_MASTER_SEGMENT_TABLE, series_lower);

           if ((suqres = drms_query_bin(session, query)) == NULL)
           {
              fprintf(stderr, "Query failed. Statement was: %s\n", query);
              free(namespace);
              goto bailout;
           }

           if (suqres->num_rows == 0)
           {
              keepsums = 1;              
           }

           db_free_binary_result(suqres);
           suqres = NULL;
        }

        if (cascade && !keepsums) 
        {
           int irow;
           int nsunums;
           long long val;
           int axis[2];
           long long *llarr = NULL;

           /* Fetch an array of unique SUNUMs from the series table -
              the prime-key logic is NOT applied. */
           snprintf(query, sizeof(query), "SELECT DISTINCT sunum FROM %s ORDER BY sunum", series);

           /* Even on a very large table (100 M records), this query should return within 
            * a couple of minutes - if that isn't acceptable, then we can 
            * limit the code to series with segments. */
           if ((suqres = drms_query_bin(session, query)) == NULL)
           {
              fprintf(stderr, "Query failed. Statement was: %s\n", query);
              goto bailout;
           }

           nsunums = 0;

           if (suqres->num_rows > 0)
           {
              llarr = (long long *)malloc(sizeof(long long) * suqres->num_rows);

              for (irow = 0; irow < suqres->num_rows; irow++)
              {
                 val = db_binary_field_getlonglong(suqres, irow, 0);
                 if (val >= 0)
                 {
                    llarr[nsunums++] = val;
                 }
              }

              /* Stuff into a DRMS_Array_t */
              axis[0] = 1;
              axis[1] = nsunums;
              array = drms_array_create(DRMS_TYPE_LONGLONG, 2, axis, llarr, &drmsstatus);
           }

           if (nsunums == 0)
           {
              keepsums = 1;
           }

           db_free_binary_result(suqres);
           suqres = NULL;
        }

        /* This if statement just checks to make sure that if we are deleting SUs, then
         * we properly created the array of SUs that will be sent to SUMS for deletion. */
        if (keepsums || (!drmsstatus && array && array->naxis == 2 && array->axis[0] == 1))
        {
           if (cascade && !keepsums) {
              if (array->axis[1] > 0) {
                 /* Delete the SUMS files from SUMS. */
                 /* If this is a sock-module, must pass the vector SUNUM by SUNUM 
                  * to drms_server (drms_dropseries handles the sock-module case). */
                 if (drms_dropseries(env, series, array))
                 {
                    fprintf(stderr, "Unable to drop SUNUMS; failure calling SUMS.\n");
                    goto bailout;
                 }
              }
           }

           if (cascade) {
              sprintf(query,"drop table %s",series_lower);
              if (env->verbose)
              {
                 fprintf(stdout, "drms_delete_series(): %s\n", query);
              }

              if (drms_dms(session,NULL,query))
                goto bailout;
              if (drms_sequence_drop(session, series_lower))
                goto bailout;
           }
           sprintf(query, "set search_path to %s", namespace);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query)) {
              fprintf(stderr, "Failed: %s\n", query);
              goto bailout;
           }
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_LINK_TABLE,series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_KEYWORD_TABLE, series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_SEGMENT_TABLE, series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_SERIES_TABLE,series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;

           /* Both DRMS servers and clients have a series_cache (one item per
              each series in all of DRMS). So, always remove the deleted series
              from this cache, whether or not this is a server. */

           /* Since we are now caching series on-demand, this series may not be in the
            * series_cache, but hcon_remove handles this fine. */

           hcon_remove(&env->series_cache,series_lower);
        }
        else
        {
           fprintf(stderr, "Couldn't create vector of sunum keywords.\n");
           goto bailout;
        }

        free(namespace);
        namespace = NULL;
     }
     else if (qres->num_rows>1)
     {
        fprintf(stderr,"TOO MANY ROWS RETURNED IN DRMS_DELETE_SERIES\n"); 
        /* This should never happen since seriesname is a unique index on 
           the DRMS series table. */
        goto bailout;
     }
     else {
        /* The series doesn't exist. */
        fprintf(stderr, "Series '%s' does not exist\n", series);
        retstat = DRMS_ERROR_UNKNOWNSERIES;
        goto bailout;
     }
     db_free_binary_result(qres);
  
     if (array)
     {
        drms_free_array(array);
     }
  }
  else if (repl == -1)
  {
     /* There was a dbase query failure which killed the current dbase transaction; must quit 
      * program. */
     goto bailout;
  }
  else
  {
     fprintf(stderr, "Unable to delete series registered for replication.\n");
  }

  free(series_lower);

  return 0;
 bailout:
  fprintf(stderr,"drms_delete_series(): failed to delete series %s\n", series);
  if (series_lower)
  {
     free(series_lower);
  }

  if (namespace)
  {
     free(namespace);
  }

  if (array)
  {
     drms_free_array(array);
  }

  if (retstat != -1)
  {
     return retstat;
  }
  else
  {
     return 1;
  }
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

static int CanCallDrmsReplicated(DRMS_Env_t *env)
{
   int tabexists = 0;
   int status = DRMS_SUCCESS;

   tabexists = drms_query_tabexists(env->session, "_jsoc", "sl_table", &status);

   if (status == DRMS_ERROR_BADDBQUERY)
   {
      fprintf(stderr, "Unable to check database for the existence of _jsoc.sl_table.\n");
      return 0;
   }

   return (!tabexists || drms_series_hastableprivs(env, "_jsoc", "sl_table", "SELECT"));
}

/* returns:
 *   -2  Can't call drms_replicated(). This does NOT destroy the dbase transaction.
 *   -1  The query failed - when this happens, this aborts the dbase transaction, 
 *       so you need to fail and rollback the dbase.
 *    0  Not replicated
 *    1  Replicated
 */
int drms_series_isreplicated(DRMS_Env_t *env, const char *series)
{
   int ans = 0;
   char query[1024];
   DB_Binary_Result_t *qres = NULL;

   /* First, check for presence of drms_replicated(). If you don't do this and 
    * drms_replicated() doesn't exist and you try to use it, the entire transaction
    * is hosed, and the error message you get from that isn't helpful. */
   sprintf(query,
           "select routine_name from information_schema.routines where routine_name like '%s'",
           DRMS_REPLICATED_SERIES_TABLE);

   
   if ((qres = drms_query_bin(env->session, query)) == NULL)
   {
      printf("Query failed. Statement was: %s\n", query);
      ans = -1;
   }
   else
   {
      if (qres->num_rows == 1)
      {
         /* drms_replicated() exists */
         char *nspace = NULL;
         char *relname = NULL;

         /* Before calling drms_repicated(), check to see if the user has permissions to do so. */
         if (!CanCallDrmsReplicated(env))
         {
            fprintf(stderr, "You do not have permission to call database function '%s'. Please have an administrator grant you permission before proceeding.\n", DRMS_REPLICATED_SERIES_TABLE);
            ans = -2;
         }
         else
         {
            db_free_binary_result(qres);

            get_namespace(series, &nspace, &relname);

            sprintf(query, 
                    "select tab_id from %s() where tab_nspname ~~* '%s' and tab_relname ~~* '%s'",
                    DRMS_REPLICATED_SERIES_TABLE, 
                    nspace,
                    relname);

            if (nspace)
            {
               free(nspace);
            }

            if (relname)
            {
               free(relname);
            }

            if ((qres = drms_query_bin(env->session, query)) == NULL)
            {
               printf("Query failed. Statement was: %s\n", query);
               ans = -1;
            }
            else
            {
               if (qres->num_rows == 1)
               {
                  ans = 1;
               }

               db_free_binary_result(qres);
            }
         }    
      }
      else
      {
         /* drms_replicated() function doesn't exist - not safe to continue. */
         ans = -1;
      }
   }

   return ans;
}

int GetTableOID(DRMS_Env_t *env, const char *ns, const char *table, char **oid)
{
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;
   int err = 0;

   snprintf(query, sizeof(query), "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^(%s)$' AND n.nspname ~ '^(%s)$' ORDER BY 2, 3", table, ns);
   
   if (!oid)
   {
      fprintf(stderr, "Missing required argument 'oid'.\n");
      err = 1;
   }
   else if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = DRMS_ERROR_QUERYFAILED;
   }
   else
   {
      if (qres->num_rows != 1)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = 1;
      }
      else
      {
         /* row 1, column 1 */
         char ioid[8];
         *oid = malloc(sizeof(char) * 64);

         /* qres will think OID is of type string, but it is not. It is a 32-bit big-endian number.
          * So, must convert the four bytes into a 32-bit number (swapping bytes if the machine is 
          * a little-endian machine) */
         memcpy(ioid, qres->column->data, 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
         db_byteswap(DB_INT4, 1, ioid);
#endif

         snprintf(*oid, 64, "%d", *((int *)ioid));
         db_free_binary_result(qres);
      }
   }

   return err;
}

int GetColumnNames(DRMS_Env_t *env, const char *oid, char **colnames)
{
   int err = 0;
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;

   snprintf(query, sizeof(query), "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) as defval, a.attnotnull FROM pg_catalog.pg_attribute a WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", oid);

   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = 1;
   }
   else
   {
      if (qres->num_cols != 4)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = 1;
      }
      else
      {
         char *list = NULL;
         int irow;
         size_t strsize = DRMS_MAXQUERYLEN;
         char colname[512];

         if (colnames)
         {
            list = malloc(sizeof(char) * strsize);
            memset(list, 0, sizeof(char) * strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  list = base_strcatalloc(list, ",", &strsize);
               }

               /* row irow + 1, column 1 */
               db_binary_field_getstr(qres, irow, 0, sizeof(colname), colname);
               list = base_strcatalloc(list, colname, &strsize);
            }

            *colnames = list;
            list = NULL;
         }

         db_free_binary_result(qres);
      }
   }
   
   return err;
}

int drms_series_summaryexists(DRMS_Env_t *env, const char *series, int *status)
{
#if (defined TOC && TOC)
   int istat = DRMS_SUCCESS;
   int tocexists = -1;
   int shadowexists = -1;
   int summexists = -1;

   tocexists = TocExists(env, &istat);

   if (istat == DRMS_SUCCESS)
   {
      if (tocexists)
      {
         int present = IsSeriesPresent(env, kTableOfCountsNS, kTableOfCountsTab, series, &istat);
         
         if (istat == DRMS_SUCCESS)
         {
            summexists = present ? 1 : 0;
         }
      }
      else
      {
         summexists = 0;
      }
   }

   if (istat == DRMS_SUCCESS)
   {
      if (!summexists)
      {
         shadowexists = ShadowExists(env, series, &istat);

         if (istat == DRMS_SUCCESS)
         {
            summexists = shadowexists ? 1 : 0;
         }
         else
         {
            summexists = -1; /* since we don't know if it really exists. */
         }
      }
   }

   if (status)
   {
      *status = istat;
   }

   return summexists;
#else

   int istat = DRMS_SUCCESS;
   int shadowexists = -1;
   int summexists = -1;

   shadowexists = ShadowExists(env, series, &istat);

   if (istat == DRMS_SUCCESS)
   {
      summexists = shadowexists ? 1 : 0;
   }
   else
   {
      summexists = -1; /* since we don't know if it really exists. */
   }

   if (status)
   {
      *status = istat;
   }

   return summexists;

#endif
}

int drms_series_canupdatesummaries(DRMS_Env_t *env, const char *series, int *status)
{
   /* This is just a stub that is going into a version of our code tree that cannot 
    * create/update the summary tables. The idea is to get all users to update to this
    * version of lib DRMS that contains this function so that when I later add 
    * code that CAN make the TOC and shadow tables, that we don't accidentally get 
    * users with older code attempting to add records to series that have associated 
    * summary tables (The old code does not update the summary tables, and if we were
    * to allow the old code to add records to a series table that has associated summary tables
    * then the series table would get out of sync with the summary tables). */
   return 0;
}
