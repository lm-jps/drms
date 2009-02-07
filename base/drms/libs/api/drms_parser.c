// #define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "ctype.h"
#include "xmem.h"

/* Utility functions. */
static int getstring(char **inn, char *out, int maxlen);
static int getvalstring(char **inn, char *out, int maxlen);
/*
static int getdouble(char **in, double *val);
static int getfloat(char **in, float *val); 
*/
static int getint(char **in, int *val);
static inline int prefixmatch(char *token, const char *pattern);
static int gettoken(char **in, char *copy, int maxlen);
static int getvaltoken(char **in, DRMS_Type_t type, char *copy, int maxlen);

/* Main parsing functions. */
int getkeyword(char **line);
static int getnextline(char **start);
static int parse_seriesinfo(char *desc, DRMS_Record_t *template);
static int parse_segments(char *desc, DRMS_Record_t *template, HContainer_t *cparmkeys);
static int parse_segment(char **in, DRMS_Record_t *template, int segnum, HContainer_t *cparmkeys);
static int parse_keyword(char **in, 
			 DRMS_Record_t *ds, 
			 HContainer_t *slotted,
			 HContainer_t *indexkws);
static int parse_links(char *desc, DRMS_Record_t *template);
static int parse_link(char **in, DRMS_Record_t *template);
static int parse_primaryindex(char *desc, DRMS_Record_t *template);
static int parse_dbindex(char *desc, DRMS_Record_t *template);

/* This macro advances the character pointer argument past all 
   whitespace or until it points to end-of-string (0). */
#define SKIPWS(p) {while(*p && ISBLANK(*p)) { if(*p=='\n') {lineno++;} ++p;}}
#define ISBLANK(c) (c==' ' || c=='\t' || c=='\n' || c=='\r')
#define TRY(__code__) {if ((__code__)) return 1;}

static int lineno;

static void FreeCparmKey(const void *v)
{
   /* v is a pointer to DRMS_Keyword_t * */
   void **vv = (void **)v;
   if (vv && *vv)
   {
      DRMS_Keyword_t *key = (DRMS_Keyword_t *)(*vv);
      drms_free_keyword_struct(key);
      free(key);
   }
}

DRMS_Record_t *drms_parse_description(DRMS_Env_t *env, char *desc)
{
  DRMS_Record_t *template;

  XASSERT(template = calloc(1, sizeof(DRMS_Record_t)));
  XASSERT(template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));
  template->env = env;
  template->init = 1;
  template->recnum = 0;
  template->sunum = -1;
  template->sessionid = 0;
  template->sessionns = NULL;
  template->su = NULL;
  /* Initialize container structure. */
  hcon_init(&template->segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_segment_struct, 
	    (void (*)(const void *, const void *)) drms_copy_segment_struct);
  /* Initialize container structures for links. */
  hcon_init(&template->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_link_struct, 
	    (void (*)(const void *, const void *)) drms_copy_link_struct);
  /* Initialize container structure. */
  hcon_init(&template->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_keyword_struct, 
	    (void (*)(const void *, const void *)) drms_copy_keyword_struct);

  /* IMPORTANT: parse_keywords() can modify the list of primary keywords, 
  * so initialize here. */
  template->seriesinfo->pidx_num = 0;

  template->seriesinfo->dbidx_num = 0;

  /* Possibly creating cparms_sgXXX keywords from information in the segment descriptions. 
   * Must save that information during parse_segments() and use in 
   * parse_keywords() */
  HContainer_t *cparmkeys = hcon_create(sizeof(DRMS_Keyword_t *),
					DRMS_MAXKEYNAMELEN,
					FreeCparmKey,
					NULL,
					NULL,
					NULL,
					0);

  lineno = 0;
  if (parse_seriesinfo(desc, template))
  {
    fprintf(stderr,"Failed to parse series info.\n");
    goto bailout;
  }
  // Discard "Owner" in jsd file, fill it with the dbuser
  if (env->session->db_direct) {
    strcpy(template->seriesinfo->owner, env->session->db_handle->dbuser);
  }

  lineno = 0;
  if (parse_segments(desc, template, cparmkeys))
  {
    fprintf(stderr,"Failed to parse segment info.\n");
    goto bailout;
  }

  if (template->seriesinfo->unitsize < 1 && (template->segments).num_total > 0)
  {
     fprintf(stderr, 
             "The series unit size must be at least 1, but it is %d.\n",
             template->seriesinfo->unitsize);
     goto bailout;
  }


  lineno = 0;
  if (parse_links(desc, template))
  {
    fprintf(stderr,"Failed to parse links info.\n");
    goto bailout;
  }

  lineno = 0;
  if (parse_keywords(desc, template, cparmkeys))
  {
    fprintf(stderr,"Failed to parse keywords info.\n");
    goto bailout; 
  }

  lineno = 0;
  /* Ensure that slotted keywords are NOT DRMS-prime */
  if (parse_primaryindex(desc, template))
  {
    fprintf(stderr,"Failed to parse series info.\n");
    goto bailout;
  }

  if (parse_dbindex(desc, template))
  {
    fprintf(stderr,"Failed to parse series info.\n");
    goto bailout;
  }

  if (cparmkeys)
  {
     hcon_destroy(&cparmkeys);
  }

#ifdef DEBUG
  printf("SERIES TEMPLATE ASSEMBLED FROM SERIES DEFINITION:\n");
  drms_print_record(template);
#endif
  return template; /* Return series template. */

 bailout:
  hcon_free(&template->segments);
  hcon_free(&template->links);
  hcon_free(&template->keywords);
  free(template);
  if (cparmkeys)
  {
     hcon_destroy(&cparmkeys);
  }
  return NULL;
}

      



static int parse_seriesinfo (char *desc, DRMS_Record_t *template) {
  int len;
  char *start, *p, *q;
  /* Parse the description line by line, filling out the template struct. */
  start = desc;
  len = getnextline (&start);
  while (*start) {
    p = start;
    SKIPWS(p);
    q = p;
				/* Scan past keyword followed by ':'. */
    if (getkeyword (&q)) return 1;
	/* Branch on keyword and insert appropriate information into struct. */
    if (prefixmatch (p, "Seriesname"))
      TRY(getstring (&q, template->seriesinfo->seriesname, DRMS_MAXSERIESNAMELEN) <= 0)
    if (prefixmatch (p, "Version"))
      TRY(getstring (&q, template->seriesinfo->version, DRMS_MAXSERIESVERSION) <= 0)
    else if (prefixmatch (p, "Description"))
      TRY(getstring (&q, template->seriesinfo->description, DRMS_MAXCOMMENTLEN) < 0)
    else if (prefixmatch (p, "Owner"))
      TRY(getstring (&q, template->seriesinfo->owner, DRMS_MAXOWNERLEN) <= 0)
    else if (prefixmatch (p, "Author"))
      TRY(getstring (&q, template->seriesinfo->author, DRMS_MAXCOMMENTLEN) <= 0)
    else if (prefixmatch (p, "Archive"))
      TRY(getint (&q, &(template->seriesinfo->archive)))
    else if (prefixmatch (p, "Unitsize"))
    {
       TRY(getint (&q, &(template->seriesinfo->unitsize)))
    }
    else if (prefixmatch (p, "Tapegroup"))
      TRY(getint (&q, &(template->seriesinfo->tapegroup)))
    else if (prefixmatch (p, "Retention"))
      TRY(getint (&q, &(template->seriesinfo->retention)))

    start += len + 1;
    len = getnextline (&start);
  }

  if (template->seriesinfo->archive != -1 && 
      template->seriesinfo->archive !=  0 && 
      template->seriesinfo->archive !=  1)
  {
     fprintf(stderr, "WARNING: Invalid archive value '%d' - setting to 0.\n", template->seriesinfo->archive);
     template->seriesinfo->archive = 0;
  }

  //  /* Force series name to be all lower case. */
  //  strtolower(template->seriesinfo->seriesname);

  /* If version isn't specified, then assume the current version. This version will be used 
   * in downstream parsing code to switch between code branches.
   */
  snprintf(template->seriesinfo->version, DRMS_MAXSERIESVERSION, "%s", drms_series_getvers());
  

#ifdef DEBUG 
  printf("Seriesname = '%s'\n",template->seriesinfo->seriesname);
  printf("Description = '%s'\n",template->seriesinfo->description);
  printf("Owner = '%s'\n",template->seriesinfo->owner);
  printf("Author = '%s'\n",template->seriesinfo->author);
  printf("Archive = %d\n",template->seriesinfo->archive);
  printf("Unitsize = %d\n",template->seriesinfo->unitsize);
  printf("Tapegroup = %d\n",template->seriesinfo->tapegroup);
  printf("Retention = %d\n",template->seriesinfo->retention);
#endif

  return 0;
}


static int parse_segments (char *desc, DRMS_Record_t *template, HContainer_t *cparmkeys) {
  int len, segnum;
  char *start, *p, *q;

  /* Parse the description line by line, filling 
     out the template struct. */
  start = desc;
  len = getnextline(&start);
  segnum = 0;
  while(*start)
  {
    p = start;
    SKIPWS(p);
    q = p;
    if (getkeyword(&q))
      return 1;
    
    if (prefixmatch(p,"Data:"))
    {      
      if (parse_segment(&q, template, segnum, cparmkeys))
	return 1;
      ++segnum;
    }
    start += len+1;
    len = getnextline(&start);
  }
  return 0;
}



static int parse_segment(char **in, DRMS_Record_t *template, int segnum, HContainer_t *cparmkeys)
{
  int i;
  char *p,*q;
  char name[DRMS_MAXSEGNAMELEN]={0}, scope[DRMS_MAXNAMELEN]={0};
  char type[DRMS_MAXNAMELEN]={0}, naxis[24]={0}, axis[24]={0}, protocol[DRMS_MAXNAMELEN]={0};
  char cparms[DRMS_MAXCPARMS]={0};
  char unit[DRMS_MAXUNITLEN]={0};
  char bzero[64]; /* for TAS, default scale */
  char bscale[64]; /* for TAS, default scale */
  DRMS_Segment_t *seg;

  /* This function contains code that is conditional on series version. */
  DRMS_SeriesVersion_t vers2_0 = {"2.0", ""};
  DRMS_SeriesVersion_t vers2_1 = {"2.1", ""};

  p = q = *in;
  SKIPWS(p);
  q = p;


  /* Collect tokens */
  if (gettoken(&q,name,sizeof(name)) <= 0) goto failure;
  XASSERT((seg = hcon_allocslot_lower(&template->segments, name)));
  memset(seg,0,sizeof(DRMS_Segment_t));
  XASSERT(seg->info = malloc(sizeof(DRMS_SegmentInfo_t)));
  memset(seg->info,0,sizeof(DRMS_SegmentInfo_t));
  seg->record = template;
  /* Name */
  strcpy(seg->info->name, name);
  /* Number */
  seg->info->segnum = segnum;

  if (gettoken(&q,scope,sizeof(scope)) <= 0) goto failure;
  if ( !strcasecmp(scope,"link") ) {
    /* Link segment */
    seg->info->islink = 1;
    seg->info->scope= DRMS_VARIABLE;
    seg->info->type = DRMS_TYPE_INT;
    seg->info->protocol = DRMS_GENERIC;
    if(gettoken(&q,seg->info->linkname,sizeof(seg->info->linkname)) <= 0)     goto failure;
    if(gettoken(&q,seg->info->target_seg,sizeof(seg->info->target_seg)) <= 0) goto failure;
    /* Naxis */      
    if (gettoken(&q,naxis,sizeof(naxis)) <= 0) goto failure;
    seg->info->naxis = atoi(naxis);
    /* Axis */
    for (i=0; i<seg->info->naxis; i++)
      {
	if (gettoken(&q,axis,sizeof(axis)) <= 0) goto failure;
	seg->axis[i] = atoi(axis);
      }

    if (getstring(&q,seg->info->description,sizeof(seg->info->description))<0) goto failure;
  } else {
    /* Simple segment */
    seg->info->islink = 0;
    /* Scope */
    if (!strcmp(scope, "constant"))
      seg->info->scope = DRMS_CONSTANT;
    else if (!strcmp(scope, "variable"))
      seg->info->scope = DRMS_VARIABLE;
    else if (!strcmp(scope, "vardim"))
      seg->info->scope = DRMS_VARDIM;
    else goto failure;

    /* Type */
    if (gettoken(&q,type,sizeof(type)) <= 0) goto failure;
    seg->info->type  = drms_str2type(type);
    /* Naxis */      
    if (gettoken(&q,naxis,sizeof(naxis)) <= 0) goto failure;
    seg->info->naxis = atoi(naxis);
    /* Axis */
    for (i=0; i<seg->info->naxis; i++)
      {
	if (gettoken(&q,axis,sizeof(axis)) <= 0) goto failure;
	seg->axis[i] = atoi(axis);
      }

    if (gettoken(&q,unit,sizeof(unit)) < 0) goto failure;
    strcpy(seg->info->unit, unit);
    if (gettoken(&q,protocol,sizeof(protocol)) <= 0) goto failure;
    seg->info->protocol = drms_str2prot(protocol);

    if (!drms_series_isvers(template->seriesinfo, &vers2_0))
    {
       /* .jsd is pre-version 2.0 */
       if (seg->info->protocol == DRMS_TAS)
       {
          /* Tile sizes */
          for (i=0; i<seg->info->naxis; i++)
          {
             if (gettoken(&q,axis,sizeof(axis)) <= 0) goto failure;
             seg->blocksize[i] = atoi(axis);
          }
       }
    }
    else
    {
       /* .jsd is version 2.0 or greater */
       if ((seg->info->protocol == DRMS_FITS || 
            seg->info->protocol == DRMS_FITZ || 
            seg->info->protocol == DRMS_TAS))
       {
          /* Create a cparms_sgXXX keyword for each segment.  It will save 
           * the compression-parameters string. */

          /* If this is TAS, then right after the tas identifier and before the comment 
           * should come a single string  that is the FITSIO compression string 
           * (eg., "compress Rice 128,192"). 
           */
          char buf[DRMS_MAXKEYNAMELEN];

          /* cparms can be empty */
          if (seg->info->protocol != DRMS_FITZ)
          {
             if (getvaltoken(&q, DRMS_TYPE_STRING, cparms, sizeof(cparms)) < 0) goto failure;
          }

          snprintf(buf, sizeof(buf), "cparms_sg%03d", segnum);

          DRMS_Keyword_t *cpkey = calloc(1, sizeof(DRMS_Keyword_t));
          XASSERT(cpkey);
          int chused = 0;

          XASSERT(cpkey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
          memset(cpkey->info, 0, sizeof(DRMS_KeywordInfo_t));
          snprintf(cpkey->info->name, DRMS_MAXKEYNAMELEN, "%s", buf);
          cpkey->record = template;
          drms_keyword_unsetperseg(cpkey); /* don't do this, otherwise the _000 gets stripped off */
          cpkey->info->islink = 0;
          cpkey->info->linkname[0] = 0;
          cpkey->info->target_key[0] = 0;
          cpkey->info->type = DRMS_TYPE_STRING;

          /* By default, cpkey->value.string_val is NULL - set it to the empty string */
          if (seg->info->protocol != DRMS_FITZ)
          {
             drms_sscanf_str("", NULL, &cpkey->value);
          }
          else
          {
             /* For FITZ, just use defult tile-compression */
             drms_sscanf_str("compress Rice", NULL, &cpkey->value);
          }

          if (strlen(cparms) > 0 && strcasecmp(cparms, "none"))
          {
             /* drms_sscanf has a bug - the string cannot contain commas.  But
              * there are many places in the code that rely upon this bad behavior.
              * So, don't use sscanf here - do something special for strings. */
             chused = drms_sscanf_str(cparms, NULL, &cpkey->value);
             if (chused < 0)
               goto failure;
          }

          snprintf(cpkey->info->format, DRMS_MAXFORMATLEN, "%s", "%s");
          snprintf(cpkey->info->unit, DRMS_MAXUNITLEN, "%s", "none");
          cpkey->info->recscope = kRecScopeType_Variable;
          snprintf(cpkey->info->description, DRMS_MAXCOMMENTLEN, "%s", "");
          drms_keyword_unsetintprime(cpkey);
          drms_keyword_unsetextprime(cpkey);
          drms_keyword_setimplicit(cpkey);

          if (cparmkeys)
          {
             hcon_insert(cparmkeys, buf, &cpkey);
          }
       } /* FITS, FITZ, or TAS */
    } /* 2.0 */
    
    if (drms_series_isvers(template->seriesinfo, &vers2_1))
    {
       if (seg->info->protocol == DRMS_TAS || 
           seg->info->protocol == DRMS_FITS ||
           seg->info->protocol == DRMS_FITZ ||
           seg->info->protocol == DRMS_BINARY ||
           seg->info->protocol == DRMS_BINZIP)
       {
          DRMS_Value_t vholder;

          /* Must create bzero and bscale keywords for all TAS and FITS files. */
          if (gettoken(&q, bzero, sizeof(bzero)) <= 0) goto failure;
          if (gettoken(&q, bscale, sizeof(bscale)) <= 0) goto failure;

          char buf[DRMS_MAXKEYNAMELEN];
          snprintf(buf, sizeof(buf), "%s_bzero", name);

          DRMS_Keyword_t *sckey = calloc(1, sizeof(DRMS_Keyword_t));
          XASSERT(sckey);

          XASSERT(sckey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
          memset(sckey->info, 0, sizeof(DRMS_KeywordInfo_t));
          snprintf(sckey->info->name, DRMS_MAXKEYNAMELEN, "%s", buf);
          sckey->record = template;
          drms_keyword_unsetperseg(sckey);
          sckey->info->islink = 0;
          sckey->info->linkname[0] = 0;
          sckey->info->target_key[0] = 0;
          sckey->info->type = DRMS_TYPE_DOUBLE;
          memset(&vholder, 0, sizeof(DRMS_Value_t));
          drms_sscanf2(bzero, NULL, 0, DRMS_TYPE_DOUBLE, &vholder);
          sckey->value = vholder.value;
          snprintf(sckey->info->format, DRMS_MAXFORMATLEN, "%s", "%f");
          snprintf(sckey->info->unit, DRMS_MAXUNITLEN, "%s", "none");
          sckey->info->recscope = kRecScopeType_Variable;
          snprintf(sckey->info->description, DRMS_MAXCOMMENTLEN, "%s", "");
          drms_keyword_unsetintprime(sckey);
          drms_keyword_unsetextprime(sckey);
          drms_keyword_setimplicit(sckey);

          /* Although this container was originally used for holding 
           * the comp params for FITS, it is now being used for
           * holding TAS bzero/bscale keywords too.
           */
          if (cparmkeys)
          {
             hcon_insert(cparmkeys, buf, &sckey);
          }

          snprintf(buf, sizeof(buf), "%s_bscale", name);

          sckey = calloc(1, sizeof(DRMS_Keyword_t));
          XASSERT(sckey);

          XASSERT(sckey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
          memset(sckey->info, 0, sizeof(DRMS_KeywordInfo_t));
          snprintf(sckey->info->name, DRMS_MAXKEYNAMELEN, "%s", buf);
          sckey->record = template;
          drms_keyword_unsetperseg(sckey);;
          sckey->info->islink = 0;
          sckey->info->linkname[0] = 0;
          sckey->info->target_key[0] = 0;
          sckey->info->type = DRMS_TYPE_DOUBLE;
          memset(&vholder, 0, sizeof(DRMS_Value_t));
          drms_sscanf2(bscale, NULL, 0, DRMS_TYPE_DOUBLE, &vholder);
          sckey->value = vholder.value;
          snprintf(sckey->info->format, DRMS_MAXFORMATLEN, "%s", "%f");
          snprintf(sckey->info->unit, DRMS_MAXUNITLEN, "%s", "none");
          sckey->info->recscope = kRecScopeType_Variable;
          snprintf(sckey->info->description, DRMS_MAXCOMMENTLEN, "%s", "");
          drms_keyword_unsetintprime(sckey);
          drms_keyword_unsetextprime(sckey);
          drms_keyword_setimplicit(sckey);

          /* Although this container was originally used for holding 
           * the comp params for FITS, it is now being used for
           * holding TAS bzero/bscale keywords too.
           */
          if (cparmkeys)
          {
             hcon_insert(cparmkeys, buf, &sckey);
          }
       } /* TAS, FITS, or FITSZ */
    } /* 2.1 */

    if (getstring(&q,seg->info->description,sizeof(seg->info->description))<0) goto failure;
  }
  p = ++q;
  *in = q; 
 
  return 0;
 failure:
  fprintf(stderr,"%s, line %d: Invalid segment descriptor on line %d.\n",
	  __FILE__, __LINE__,lineno);
  return 1;
}



static int parse_links(char *desc, DRMS_Record_t *template)
{
  int len;
  char *start, *p, *q;

  /* Parse the description line by line, filling 
     out the template struct. */
  start = desc;
  len = getnextline(&start);
  while(*start)
  {
    p = start;
    SKIPWS(p);
    q = p;
    if (getkeyword(&q))
      return 1;
    
    if (prefixmatch(p,"Link:"))
    {
      if (parse_link(&q,template))
	return 1;
    }
    start += len+1;
    len = getnextline(&start);
  }
  return 0;
}



static int parse_link(char **in, DRMS_Record_t *template)
{
  char *p,*q;
  char name[DRMS_MAXLINKNAMELEN]={0}, target[DRMS_MAXSERIESNAMELEN]={0}, type[DRMS_MAXNAMELEN]={0},
       description[DRMS_MAXCOMMENTLEN]={0};
  DRMS_Link_t *link;
  

  p = q = *in;
  SKIPWS(p);
  q = p;

  /* Collect tokens */
  if (gettoken(&q,name,sizeof(name)) <= 0) goto failure;
  if (gettoken(&q,target,sizeof(target)) <= 0) goto failure;
  if (gettoken(&q,type,sizeof(type)) <= 0) goto failure;
  if (getstring(&q,description,sizeof(description))<0) goto failure;
  
  XASSERT((link = hcon_allocslot_lower(&template->links, name)));
  memset(link,0,sizeof(DRMS_Link_t));
  XASSERT(link->info = malloc(sizeof(DRMS_LinkInfo_t)));
  memset(link->info,0,sizeof(DRMS_LinkInfo_t)); /* ISS */
  link->record = template;
  strcpy(link->info->name, name);
  strcpy(link->info->target_series,target);
  if (!strcasecmp(type,"static"))
    link->info->type = STATIC_LINK;
  else if (!strcasecmp(type,"dynamic")) {
    link->info->type = DYNAMIC_LINK;
    link->info->pidx_num = -1;
  }
  else
    goto failure;
  strncpy(link->info->description, description, DRMS_MAXCOMMENTLEN);
  link->isset = 0;
  link->recnum = -1; 

  p = ++q;
  *in = q; 
 
  return 0;
 failure:
  fprintf(stderr,"%s, line %d: Invalid Link descriptor on line %d.\n",
	  __FILE__, __LINE__,lineno);
  return 1;
}



int parse_keywords(char *desc, DRMS_Record_t *template, HContainer_t *cparmkeys)
{
  int len;
  char *start, *p, *q;

  HContainer_t *slotted = hcon_create(sizeof(DRMS_Keyword_t *),
				      DRMS_MAXKEYNAMELEN,
				      NULL,
				      NULL,
				      NULL,
				      NULL,
				      0);

  HContainer_t *indexkws = hcon_create(sizeof(DRMS_Keyword_t *),
				      DRMS_MAXKEYNAMELEN,
				      NULL,
				      NULL,
				      NULL,
				      NULL,
				      0);


  if (!slotted || !indexkws)
  {
     fprintf(stderr, "Couldn't create container in drms_parse_description().\n");
     return 1;
  }
  else
  {
     /* Parse the description line by line, filling 
	out the template struct. */
     start = desc;
     len = getnextline(&start);
     while(*start)
     {
	p = start;
	SKIPWS(p);
	q = p;
	if (getkeyword(&q))
	  return 1;
    
	if (prefixmatch(p,"Keyword:"))
	{
	   if (parse_keyword(&q,template, slotted, indexkws))
	     return 1;
	}
	start += len+1;
	len = getnextline(&start);
     }

     if (indexkws->num_total > 0)
     {
	HIterator_t *hit_index = hiter_create(indexkws);

	if (hit_index)
	{
	   DRMS_Keyword_t **pIndexKey = NULL;
	   const char *indexKeyname = NULL;
	   char *kname = NULL;
	   char *underscore = NULL;
	   DRMS_Keyword_t *existslotted = NULL;

	   /* Ensure that if there is an index keyword, there is a corresponding
	    * slotted keyword. */
	   while ((pIndexKey = 
		   (DRMS_Keyword_t **)hiter_extgetnext(hit_index, &indexKeyname)) != NULL)
	   {
	      if (drms_keyword_isindex(*pIndexKey))
	      {
		 kname = strdup(indexKeyname);
		 underscore = strstr(kname, kSlotAncKey_Index);

		 if (underscore)
		 {
		    *underscore = '\0';
		    if ((existslotted = 
			 (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), kname)) 
			== NULL)
		    {
		       fprintf(stderr, 
			       "Can't specify an index keyword (%s) without specifying "
			       "the corresponding slotted keyword.\n",
			       indexKeyname);
		       hiter_destroy(&hit_index);
		       hcon_destroy(&slotted);
		       hcon_destroy(&indexkws);
		       return 1;
		    }

		    if (!drms_keyword_isslotted(existslotted))
		    {
		       fprintf(stderr, 
			       "Index keyword '%s' must be associated with a "
			       "slotted keyword, but it is associated with a "
			       "%s type of keyword.\n",
			       kname,
			       drms_keyword_getrecscopestr(existslotted, NULL));
		       hiter_destroy(&hit_index);
		       hcon_destroy(&slotted);
		       hcon_destroy(&indexkws);
		       return 1;
		    }
		 }
		 else
		 {
		    fprintf(stderr, "Invalid index keyword name %s.\n", kname);
		    hiter_destroy(&hit_index);
		    hcon_destroy(&slotted);
		    hcon_destroy(&indexkws);
		    return 1;
		 }

		 if (kname)
		 {
		    free(kname);
		 }
	      }
	   }

	   hiter_destroy(&hit_index);
	}
	else
	{
	   fprintf(stderr, "Couldn't create iterator.\n");
	   hcon_destroy(&slotted);
	   hcon_destroy(&indexkws);
	   return 1;
	}
     }

     if (slotted->num_total > 0)
     {
	/* All keywords have been parsed.  Need to iterate through all slotted keywords.
	 * For each slotted keyword, check if the corresponding index keyword exists.
	 * If not, create it and make it prime.  Insert this keyword into
	 * template->keywords.  If it does exist, ensure that it 
	 * is prime and that it is of recscope 'index'.  If not, fail.  If no
	 * failure at this point, 
	 * find all keywords implied by the slotted keyword type (eg., TS_EQ requires
	 * _epoch, _step).  They must be recscope constant, if not, fail.  If no
	 * failure at this point, remove the 'primeness' of the slotted keyword.
	 */
	HIterator_t *hit = hiter_create(slotted);
	
	if (hit)
	{
	   DRMS_Keyword_t **pSlotKey = NULL;
	   const char *slotKeyname = NULL;
	   char keyname[DRMS_MAXKEYNAMELEN];
	   DRMS_Keyword_t *newkey = NULL;
           DRMS_Keyword_t *existkey = NULL;

	   while ((pSlotKey = 
		   (DRMS_Keyword_t **)hiter_extgetnext(hit, &slotKeyname)) != NULL)
	   {
	      snprintf(keyname, sizeof(keyname), "%s%s", slotKeyname, kSlotAncKey_Index);
	      if ((existkey = 
                   (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), keyname)) 
		  == NULL)
	      {
		 /* The corresponding index keyword does not exist - create. */

		 /* If this is a per-segment keyword, then there will be 
		  * multiple keywords already (eg., KEY_001, KEY_002, KEY_003).
		  * So, don't do anything special since this look will be executed
		  * multiple times, once for each of the per-segment keywords in
		  * the group. */

		 /* hcon_allocslot puts the new key in template->keywords */
		 XASSERT(newkey = hcon_allocslot_lower(&(template->keywords), keyname));
		 memset(newkey,0,sizeof(DRMS_Keyword_t));
		 XASSERT(newkey->info = malloc(sizeof(DRMS_KeywordInfo_t)));    
		 memset(newkey->info,0,sizeof(DRMS_KeywordInfo_t));
		 strcpy(newkey->info->name, keyname);
		 newkey->record = template;
                 drms_keyword_unsetperseg(newkey); /* Must be per record*/
                 drms_keyword_setimplicit(newkey);
		 newkey->info->islink = 0;
		 newkey->info->linkname[0] = 0;
		 newkey->info->target_key[0] = 0;
		 newkey->info->type = kIndexKWType;
		 strcpy(newkey->info->format, kIndexKWFormat);
		 strcpy(newkey->info->unit, "none");
		 newkey->info->recscope = kRecScopeType_Index;
		 strcpy(newkey->info->description, (*pSlotKey)->info->description);

		 /* Make new key DRMS-prime */
		 template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
		   newkey; 
                 drms_keyword_setintprime(newkey);
                 drms_keyword_unsetextprime(newkey);

                 /* Index keywords must have a db index */
                 template->seriesinfo->dbidx_keywords[(template->seriesinfo->dbidx_num)++]
                   = newkey;
	      }
	      else
	      {
		 /* The corresponding index keyword DOES exist - don't allow this.
                  * Allowing the user to create it is not good, since they could
                  * create it improperly.
                  */
                 fprintf(stderr, "Keywords with the suffix '_index' are reserved; cannot specify '%s' in a jsd file\n", existkey->info->name);
                 return 1;
	      }

	      /* Find all other required constant keywords for this type of 
	       * slotted keyword */
	      int failure = 0;
	      switch ((int)drms_keyword_getrecscope(*pSlotKey))
	      {
                 case kRecScopeType_TS_SLOT:
                   {
                      DRMS_Keyword_t *anckey = NULL;
                      snprintf(keyname, sizeof(keyname), "%s%s", slotKeyname, kSlotAncKey_Round);
                      anckey = (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), keyname);
                      if (anckey)
                      {
                         /* if Round present, check for constant round */
                         if (!drms_keyword_isconstant(anckey))
                         {
                            fprintf(stderr, "Ancillary keyword '%s' must be constant.\n",
                                    keyname);
                            failure = 1;
                         }
                      }
                   }

                   if (failure)
                   {
                      break;
                   }
                   /* Intentional fall-through */
		 case kRecScopeType_TS_EQ:
		   {
                      DRMS_Keyword_t *anckey = NULL;
		      snprintf(keyname, 
			       sizeof(keyname), "%s%s", 
			       slotKeyname, 
			       kSlotAncKey_Epoch);
		      anckey = (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
                                                                   keyname);
		      if (anckey)
		      {
			 /* Epoch must be constant and a time or string */
			 if (drms_keyword_isconstant(anckey) &&
			     (drms_keyword_gettype(anckey) == DRMS_TYPE_TIME ||
			      drms_keyword_gettype(anckey) == DRMS_TYPE_STRING))
			 {
			    snprintf(keyname, 
				     sizeof(keyname), "%s%s", 
				     slotKeyname, 
				     kSlotAncKey_Step);
			    anckey = (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
                                                                         keyname);
			 }
			 else
			 {
			    fprintf(stderr, "Ancillary keyword '%s' must be constant"
				    " and of data type 'time' or 'string'.\n", keyname);
			    failure = 1;
			 }
		      }

		      if (!anckey)
		      {
			 fprintf(stderr, 
				 "Missing required ancillary keyword '%s'.\n",
				 keyname);
			 failure = 1;
		      }
		      
		      if (!failure && anckey)
		      {
			 /* Step must be constant */
			 if (drms_keyword_isconstant(anckey))
			 {
			    /* _unit is optional */
			    snprintf(keyname, 
				     sizeof(keyname), "%s%s", 
				     slotKeyname, 
				     kSlotAncKey_Unit);
			    anckey = (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
                                                                         keyname);
			 }
			 else
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant.\n",
				    keyname);
			    failure = 1;
			 }
		      }

		      if (!failure && anckey)
		      {
			 /* if Unit present, check for valid unit */
			 if (drms_keyword_gettype(anckey) == DRMS_TYPE_STRING &&
			     drms_keyword_isconstant(anckey))
			 {
			    DRMS_SlotKeyUnit_t utype = drms_keyword_getunit(anckey, NULL);

			    if (utype == kSlotKeyUnit_Invalid)
			    {
			       fprintf(stderr, 
				       "Slot keyword unit '%s' is not valid.\n",
				       drms_keyword_getvalue(anckey)->string_val);
			       failure = 1;
			    }
			 }
			 else
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant"
				    " and of data type 'string'.\n",
				    keyname);
			    failure = 1;
			 }
		      }
		   }
		   break;
		 case kRecScopeType_SLOT:
		   {
		      snprintf(keyname, 
			       sizeof(keyname), "%s%s", 
			       slotKeyname, 
			       kSlotAncKey_Base);
		      DRMS_Keyword_t *anckey = 
			(DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
							    keyname);

		      if (anckey)
		      {
			 /* Base must be constant and a double, float, or time */
			 if (drms_keyword_isconstant(anckey) &&
			     (drms_keyword_gettype(anckey) == DRMS_TYPE_DOUBLE ||
			      drms_keyword_gettype(anckey) == DRMS_TYPE_FLOAT ||
			      drms_keyword_gettype(anckey) == DRMS_TYPE_TIME))

			 {
			    snprintf(keyname, 
				     sizeof(keyname), "%s%s", 
				     slotKeyname, 
				     kSlotAncKey_Step);
			    anckey = 
			      (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
								  keyname);
			 }
			 else
			 {
			    fprintf(stderr, "Ancillary keyword '%s' must be constant"
				    " and of data type 'double', 'float', or 'time'.\n", 
				    keyname);
			    failure = 1;
			 }
		      }

		      if (!anckey)
		      {
			 fprintf(stderr, 
				 "Missing required ancillary keyword '%s'.\n",
				 keyname);
			 failure = 1;
		      }

		      if (!failure && anckey)
		      {
			 /* Step must be constant */
			 if (drms_keyword_isconstant(anckey))
			 {
			    /* _unit is optional */
			    snprintf(keyname, 
				     sizeof(keyname), "%s%s", 
				     slotKeyname, 
				     kSlotAncKey_Unit);
			    anckey = 
			      (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
								  keyname);
			 }
			 else
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant.\n",
				    keyname);
			    failure = 1;
			 }
		      }

		      if (!failure && anckey)
		      {
			 /* if Unit present, check for valid unit, which can be any string for a SLOT
			  * type of slotted key  */
			 if (drms_keyword_gettype(anckey) != DRMS_TYPE_STRING ||
			     !drms_keyword_isconstant(anckey))
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant"
				    " and of data type 'string'.\n",
				    keyname);
			    failure = 1;
			 }
		      }
		   }
		   break;
		 case kRecScopeType_ENUM:
		   break;
		 case kRecScopeType_CARR:
		   {
		      snprintf(keyname, 
			       sizeof(keyname), "%s%s", 
			       slotKeyname, 
			       kSlotAncKey_Step);
		      DRMS_Keyword_t *anckey = 
			(DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
							    keyname);

		      if (!anckey)
		      {
			 fprintf(stderr, 
				 "Missing required ancillary keyword '%s'.\n",
				 keyname);
			 failure = 1;
		      }

		      if (!failure && anckey)
		      {
			 /* Must be constant */
			 if (drms_keyword_isconstant(anckey))
			 {
			    /* _unit is optional */
			    snprintf(keyname, 
				     sizeof(keyname), "%s%s", 
				     slotKeyname, 
				     kSlotAncKey_Unit);
			    anckey = 
			      (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), 
								  keyname);
			 }
			 else
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant.\n",
				    keyname);
			    failure = 1;
			 }
		      }

		      if (!failure && anckey)
		      {
			 /* if present, check for valid unit */
			 if (drms_keyword_gettype(anckey) == DRMS_TYPE_STRING &&
			     drms_keyword_isconstant(anckey))
			 {
			    DRMS_SlotKeyUnit_t utype = 
			      drms_keyword_getunit(anckey, NULL);

			    if (utype == kSlotKeyUnit_Invalid)
			    {
			       fprintf(stderr, 
				       "Slot keyword unit '%s' is not valid.\n",
				       drms_keyword_getvalue(anckey)->string_val);
			       failure = 1;
			    }
			 }
			 else
			 {
			    fprintf(stderr, 
				    "Ancillary keyword '%s' must be constant"
				    " and of data type 'string'.\n",
				    keyname);
			    failure = 1;
			 }
		      }
		   }
		   break;
		 default:
		   fprintf(stderr, 
			   "Invalid recscope type '%d'.\n", 
			   (int)drms_keyword_getrecscope(*pSlotKey));
		   failure = 1;
	      }

	      if (failure == 1)
	      {
		 hiter_destroy(&hit);
		 hcon_destroy(&slotted);
		 hcon_destroy(&indexkws);
		 return 1;
	      }
	   } /* while */

	   hiter_destroy(&hit);
	}
	else
	{
	   fprintf(stderr, "Couldn't create iterator.\n");
	   hcon_destroy(&slotted);
	   hcon_destroy(&indexkws);
	   return 1;
	}
     }

     hcon_destroy(&slotted);
     hcon_destroy(&indexkws);
  } /* successful creation of slotted container */

  if (cparmkeys)
  {
     /* Done with all other keywords - now move cparms_sgXXX keywords to template record. */
     HIterator_t *hiter = hiter_create(cparmkeys);
     DRMS_Keyword_t *cpkeyin = NULL;
     DRMS_Keyword_t **pcpkeyin = NULL;
     DRMS_Keyword_t *cpkey = NULL;
     if (hiter)
     {
	while ((pcpkeyin = (DRMS_Keyword_t **)hiter_getnext(hiter)) != NULL)
	{
	   cpkeyin = *pcpkeyin;
	   XASSERT(cpkey = hcon_allocslot_lower(&template->keywords, cpkeyin->info->name));
	   memset(cpkey, 0, sizeof(DRMS_Keyword_t));
	   drms_copy_keyword_struct(cpkey, cpkeyin);
	}

	hiter_destroy(&hiter);
     }
  }

  return 0;
}

enum PFormatLMod_enum
{
   kPFormatLMod_None = 0,
   kPFormatLMod_Char,
   kPFormatLMod_Short,
   kPFormatLMod_LongLong,
   kPFormatLMod_LongDouble
};
typedef enum PFormatLMod_enum PFormatLMod_t;

static int FormatChk(const char *format, DRMS_Type_t dtype)
{
   int ok = 1;
   int gotformat = 0;

   if (dtype == DRMS_TYPE_TIME)
   {
      signed char val; /* must specify 'signed' for some reason. */

      if (sscanf(format, "%hhd", &val) != 1 || val <= -10 || val >= 10)
      {
         ok = 0;
      }
   }
   else
   {
      /* All format strings must have ONE format specifier (a single '%' followed by a specifiers) */
      /* But "%%" is allowed, which means 'print a percent sign' */
      char *fcopy = strdup(format);
      char *ppcnt = strchr(fcopy, '%');

      while (ok && ppcnt)
      {
         while (ppcnt && ppcnt - fcopy < strlen(fcopy) - 1 && *(ppcnt + 1) == '%')
         {
            ppcnt = strchr(ppcnt + 2, '%');
         }

         if (ppcnt && gotformat)
         {
            ok = 0;
            fprintf(stderr, "Too many format specifiers in '%s'.  There should be one only.\n", format); 
            break;
         }

         if (ppcnt)
         {
            char *lasts;
            char *fcopy2 = strdup(ppcnt);
            char *md = NULL; /* modifier */
            PFormatLMod_t lmod;
            int fmtstrlen = 0;

            /* Since all DRMS values are signed values:
             *   ignore unsigned arguments - %u,%c
             *   ignore pointer arguments - %p,%n 
             *   ignore Unicode arguments - %C,%S  */
            md = strtok_r(fcopy2, "dioxXfFeEgGaAs", &lasts);
            if (md)
            {
               fmtstrlen = strlen(md);
               char sp = ppcnt[fmtstrlen]; /* specifier */
               gotformat = 1;
               md++; /* flags/length modifier starts after '%' */

               /* figure out which modifier is present */
               if (strstr(md, "hh"))
               {
                  /* The argument is converted to an int and passed to printf, 
                     then cast to char before printing */
                  lmod = kPFormatLMod_Char;
               }
               else if (strchr(md, 'h'))
               {
                  /* The argument is converted to an int and passed to printf,
                     then cast to short before printing */
                  lmod = kPFormatLMod_Short;
               }
               else if (strstr(md, "ll"))
               {
                  /* The argument is converted to a long long and passed to printf, 
                     and printed as a long long */
                  lmod = kPFormatLMod_LongLong;
               }
               else if (strchr(md, 'l'))
               {
                  /* Don't use this modifier - on 32-bit machines this is 32 bits, but on 
                   * 64-bit machines, this is 64 bits. So if your data type is long long, 
                   * this will be converted to long, which could be 32-bits, which is too
                   * small to hold a 64-bit value, which would result in demotion. */
                  if (dtype == DRMS_TYPE_STRING)
                  {
                     fprintf(stderr, "Format 'ls' implies the string argument is Unicode - DRMS does not support Unicode strings.\n");
                  }
                  else
                  {
                     fprintf(stderr, "Using format length modifier 'l' is dangerous; on 32-bit machines, a 32-bit value is expected, but on 64-bit machines, a 64-bit value is expected.\n");
                  }
                  ok = 0;
               }
               else if (strchr(md, 'j'))
               {
                  /* same as long long - 64 bits */
                  lmod = kPFormatLMod_LongLong;
               }
               else if (strchr(md, 'L'))
               {
                  lmod = kPFormatLMod_LongDouble;
               }
               else if (strchr(md, 'z') || strchr(md, 't'))
               {
                  /* z - unsigned, but not sure if 32 or 64 bits */
                  /* Just to be safe, warn about these weird length modifiers */
                  ok = 0;
               }
               else
               {
                  lmod = kPFormatLMod_None;
               }

               if (ok)
               {
                  /* d , i , o , u , x , and X cause conversion of argument to int (but length modifiers
                   * modify this).  The rest cause conversion to double.
                   */
                  switch (dtype)
                  {
                     case DRMS_TYPE_CHAR:
                       ok &= (lmod == kPFormatLMod_None || lmod == kPFormatLMod_Char || 
                              lmod == kPFormatLMod_Short || lmod == kPFormatLMod_LongLong);
                       ok &= (sp == 'd' || sp == 'i' || sp == 'o' || sp == 'x' || sp == 'X' || 
                              sp == 'f' || sp == 'F' || sp == 'e' || sp == 'E' || sp == 'g' || sp == 'G' || 
                              sp == 'a' || sp == 'A');
                       break;
                     case DRMS_TYPE_SHORT:
                       ok &= (lmod == kPFormatLMod_None || lmod == kPFormatLMod_Short || 
                              lmod == kPFormatLMod_LongLong);
                       ok &= (sp == 'd' || sp == 'i' || sp == 'o' || sp == 'x' || sp == 'X' || 
                              sp == 'f' || sp == 'F' || sp == 'e' || sp == 'E' || sp == 'g' || sp == 'G' || 
                              sp == 'a' || sp == 'A');
                       break; 
                     case DRMS_TYPE_INT:
                       ok &= (lmod == kPFormatLMod_None || lmod == kPFormatLMod_LongLong);
                       ok &= (sp == 'd' || sp == 'i' || sp == 'o' || sp == 'x' || sp == 'X' || 
                              sp == 'f' || sp == 'F' || sp == 'e' || sp == 'E' || sp == 'g' || sp == 'G' || 
                              sp == 'a' || sp == 'A');
                       break;
                     case DRMS_TYPE_LONGLONG:
                       ok &= ((lmod == kPFormatLMod_None && (sp != 'd' && sp!= 'i' 
                                                             && sp != 'o' && sp != 'x' && sp != 'X')) || 
                              lmod == kPFormatLMod_LongLong);
                       ok &= (sp == 'd' || sp == 'i' || sp == 'o' || sp == 'x' || sp == 'X' || 
                              sp == 'f' || sp == 'F' || sp == 'e' || sp == 'E' || sp == 'g' || sp == 'G' || 
                              sp == 'a' || sp == 'A');
                       break;
                     case DRMS_TYPE_FLOAT:
                       /* intentional fall-through */
                     case DRMS_TYPE_DOUBLE:
                       /* float vals shouldn't be converted to integer vals */
                       /* length modifiers, other than 'L', have no impact on these and 'L' is fine here. */
                       ok &= (sp == 'f' || sp == 'F' || sp == 'e' || sp == 'E' || sp == 'g' || sp == 'G' || 
                              sp == 'a' || sp == 'A');
                       break;
                     case DRMS_TYPE_STRING:     
                       /* The only bad modifier is 'l', which was blocked above */
                       ok &= (sp == 's');
                       break;
                     default:
                       /* Unsupported data type */
                       ok = 0;
                       fprintf(stderr, "Unsupported data type '%s'.\n", drms_type2str(dtype));
                  }
               }
            }
            else
            {
               /* Invalid format specifier */
               ok = 0;
               fprintf(stderr, "Invalid format specifier.\n"); 
            }

            if (fcopy2)
            {
               free(fcopy2);
            }

            if (ok)
            {
               /* Start searching with the character immediately following the specifier, which could
                * be a null termintor. */
               ppcnt = strchr(ppcnt + fmtstrlen + 1, '%');
            }
         }
      } /* while*/

      if (!gotformat)
      {
         ok = 0;
         fprintf(stderr, "No format specifier found.\n"); 
      }

      if (fcopy)
      {
         free(fcopy);
      }
   }

   return ok;
}

static int parse_keyword(char **in, 
			 DRMS_Record_t *template, 
			 HContainer_t *slotted,
			 HContainer_t *indexkws)
{
  char *p,*q;
  char name[DRMS_MAXKEYNAMELEN]={0}, type[DRMS_MAXNAMELEN]={0}, linkname[DRMS_MAXLINKNAMELEN]={0}, defval[DRMS_DEFVAL_MAXLEN]={0};
  char unit[DRMS_MAXUNITLEN]={0}, description[DRMS_MAXCOMMENTLEN]={0}, format[DRMS_MAXFORMATLEN]={0};
  char target_key[DRMS_MAXKEYNAMELEN]={0}, constant[DRMS_MAXNAMELEN]={0},  scope[DRMS_MAXNAMELEN]={0}, name1[DRMS_MAXKEYNAMELEN+10]={0};
  int num_segments, per_segment, seg;
  DRMS_Keyword_t *key;
  int chused = 0;

  p = q = *in;
  SKIPWS(p);
  q = p;


  /* Collect tokens */
  if (gettoken(&q,name,sizeof(name)) <= 0) goto failure;
  if (gettoken(&q,type,sizeof(type)) <= 0) goto failure;
  if ( !strcasecmp(type,"link") )
  {
    /* Link keyword. */
    if(gettoken(&q,linkname,sizeof(linkname)) <= 0)     goto failure;
    if(gettoken(&q,target_key,sizeof(target_key)) <= 0) goto failure;
    if(gettoken(&q,description,sizeof(description)) < 0)       goto failure;
  }
  else if ( !strcasecmp(type,"index") )
  {
     /* Slotted-key index keyword */
     if(gettoken(&q,format,sizeof(format)) <= 0)     goto failure;
     if(gettoken(&q,description,sizeof(description)) < 0)   goto failure;
  }
  else
  {
    /* Simple keyword. */
    if(gettoken(&q,constant,sizeof(constant)) <= 0) goto failure;
    if(gettoken(&q,scope,sizeof(scope)) <= 0)       goto failure;
    if(getvaltoken(&q,drms_str2type(type), defval,sizeof(defval)) < 0)  goto failure;
    if(gettoken(&q,format,sizeof(format)) <= 0)     goto failure;
    if(gettoken(&q,unit,sizeof(unit)) < 0)         goto failure;
    if(gettoken(&q,description,sizeof(description)) < 0)   goto failure;
  }


#ifdef DEBUG
  printf("name       = '%s'\n",name);
  printf("linkname   = '%s'\n",linkname);
  printf("target_key = '%s'\n",target_key);
  printf("name       = '%s'\n",name);
  printf("type       = '%s'\n",type);
  printf("constant   = '%s'\n",constant);
  printf("scope      = '%s'\n",scope);
  printf("defval     = '%s'\n",defval);
  printf("format     = '%s'\n",format);
  printf("unit       = '%s'\n",unit);
  printf("description    = '%s'\n",description);
#endif

  /* Populate structure */
  if ( !strcasecmp(type,"link") )
  {
    XASSERT(key = hcon_allocslot_lower(&template->keywords,name));
    memset(key,0,sizeof(DRMS_Keyword_t));
    XASSERT(key->info = malloc(sizeof(DRMS_KeywordInfo_t)));    
    memset(key->info,0,sizeof(DRMS_KeywordInfo_t));
    strcpy(key->info->name,name);
    key->record = template;
    key->info->islink = 1;   
    strcpy(key->info->linkname,linkname);
    strcpy(key->info->target_key,target_key);
    key->info->type = DRMS_TYPE_INT;
    key->value.int_val = 0;
    key->info->format[0] = 0;
    key->info->unit[0] = 0;
    key->info->recscope = kRecScopeType_Variable;
    drms_keyword_unsetperseg(key);
    drms_keyword_unsetintprime(key);
    drms_keyword_unsetextprime(key);

    strcpy(key->info->description,description);
  }  
  else if (!strcasecmp(constant,"index"))
  {
     /* Index keywords are prime, so they must not be per-segment */
     strcpy(name1,name);
     if ((key = hcon_lookup_lower(&template->keywords,name1))) {
	// this is an earlier definition
	free(key->info);
     }
     XASSERT(key = hcon_allocslot_lower(&template->keywords,name1));
     memset(key,0,sizeof(DRMS_Keyword_t));
     XASSERT(key->info = malloc(sizeof(DRMS_KeywordInfo_t)));
     memset(key->info,0,sizeof(DRMS_KeywordInfo_t));
     strncpy(key->info->name,name1,sizeof(key->info->name));
     if (strlen(name1) >= sizeof(key->info->name))
       fprintf(stderr,
	       "WARNING keyword name %s truncated to %lld characters.\n", 
	       name1, 
	       (long long)sizeof(key->info->name)-1);
     key->record = template;
     drms_keyword_unsetperseg(key);
     key->info->islink = 0;
     key->info->linkname[0] = 0;
     key->info->target_key[0] = 0;
     key->info->type = kIndexKWType;
     if (!FormatChk(format, key->info->type))
     {
        fprintf(stderr, 
                "WARNING: The format specified '%s' is incompatible with the data type '%s' of keyword '%s'.\n",
                format, 
                drms_type2str(key->info->type),
                key->info->name);
     }
     strcpy(key->info->format, format);
     strcpy(key->info->unit, "none");
     key->info->recscope = kRecScopeType_Index;
     strcpy(key->info->description,description);

     /* Index keywords must be DRMS-internal prime (but DRMS-external not prime) */
     template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
       key;

     drms_keyword_setintprime(key);
     drms_keyword_unsetextprime(key);

     /* Index keywords must have a db index */
     template->seriesinfo->dbidx_keywords[(template->seriesinfo->dbidx_num)++] =
       key; 

     hcon_insert(indexkws, key->info->name, &key);
  } /* index keyword */
  else
  {
    num_segments = hcon_size(&template->segments);

    if (!strcasecmp(scope,"segment"))
      per_segment = 1;
    else if (!strcasecmp(scope,"record"))
      per_segment = 0;
    else
      goto failure;

    if (per_segment == 1 && num_segments < 1)
    {
       fprintf(stderr, 
	       "'%s' declared per_segment, but no segments declared.\n",
	       name);
       goto failure;
    }

    for (seg=0; seg<(per_segment==1?num_segments:1); seg++)
    {    
      /* If this is a per-segment keyword create one copy for each segment
	 with the segment number formatted as "_%03d" appended to the name */

      if (per_segment)
        sprintf(name1,"%s_%03d",name,seg);
      else
        strcpy(name1,name);

      if ((key = hcon_lookup_lower(&template->keywords,name1))) {
	// this is an earlier definition
	free(key->info);
      }
      XASSERT(key = hcon_allocslot_lower(&template->keywords,name1));
      memset(key,0,sizeof(DRMS_Keyword_t));
      XASSERT(key->info = malloc(sizeof(DRMS_KeywordInfo_t)));
      memset(key->info, 0, sizeof(DRMS_KeywordInfo_t));
      strncpy(key->info->name,name1,sizeof(key->info->name));
      if (strlen(name1) >= sizeof(key->info->name))
        fprintf(stderr,
		"WARNING keyword name %s truncated to %lld characters.\n", 
		name1, 
		(long long)sizeof(key->info->name)-1);
      key->record = template;

      if (per_segment)
      {
         drms_keyword_setperseg(key);
      }
      else
      {
         drms_keyword_unsetperseg(key);
      }

      key->info->islink = 0;
      key->info->linkname[0] = 0;
      key->info->target_key[0] = 0;
      key->info->type = drms_str2type(type);
      
      /* Since we are parsing a value, this could be a string with escape characters or
       * quotes. We want to preserve the quotes surrounding the string and pass those
       * through to drms_sscanf2(). */
      DRMS_Value_t vholder;
      chused = drms_sscanf2(defval, NULL, 0, key->info->type, &vholder);
      key->value = vholder.value;
      memset(&(vholder.value), 0, sizeof(DRMS_Type_Value_t));

      if (chused < 0 || 
          (chused == 0 && key->info->type != DRMS_TYPE_STRING && key->info->type != DRMS_TYPE_TIME))
	goto failure;
#ifdef DEBUG      
      printf("Default value = '%s' = ",defval);
      drms_printfval(key->info->type, &key->value);
      printf("\n");
#endif
      if (!FormatChk(format, key->info->type))
      {
         fprintf(stderr, 
                 "WARNING: The format specified '%s' is incompatible with the data type '%s' of keyword '%s'.\n",
                 format, 
                 drms_type2str(key->info->type),
                 key->info->name);
      }
      strcpy(key->info->format, format);
      strcpy(key->info->unit, unit);
      key->info->recscope = kRecScopeType_Variable;
      int stat;
      DRMS_RecScopeType_t rscope = drms_keyword_str2recscope(constant, &stat);
      if (stat == DRMS_SUCCESS)
	key->info->recscope = rscope;
      else
	goto failure;
      strcpy(key->info->description,description);

      drms_keyword_unsetintprime(key);
      drms_keyword_unsetextprime(key);

      if (drms_keyword_isslotted(key))
      {
	 hcon_insert(slotted, key->info->name, &key);
      }
    }
  } /* not a link or an index keyword */
  p = ++q;
  *in = q; 
 
  return 0;
 failure:
  fprintf(stderr,"%s, line %d: Invalid keyword descriptor on line %d.\n",
	  __FILE__, __LINE__,lineno);
  return 1;
}


static int parse_primaryindex(char *desc, DRMS_Record_t *template)
{
  int len;
  char *start, *p, *q;
  DRMS_Keyword_t *key;
  char name[DRMS_MAXKEYNAMELEN];

  /* Parse the description line by line, filling 
     out the template struct. */
  start = desc;
  len = getnextline(&start);
  while(*start)
  {
    p = start;
    SKIPWS(p);
    q = p;
    if (getkeyword(&q))
      return 1;
    
    if (prefixmatch(p,"Index:") || prefixmatch(p,"PrimeKeys:"))
    {
      p = q;
      SKIPWS(p);

      while(p<=(start+len) && *p)
      {	
	if (template->seriesinfo->pidx_num >= DRMS_MAXPRIMIDX)
	{
	  printf("Too many keywords in primary index.\n");
	  return 1;
	}
	  

	while(p<=(start+len)  && isspace(*p))
	  ++p;
	q = name;
	while(p<=(start+len) && q<name+sizeof(name) && !isspace(*p) && *p!=',')
	  *q++ = *p++;	       
	*q++ = 0;
	p++;
	
	key = hcon_lookup_lower(&template->keywords,name);
	if (key==NULL)
	{
	   printf("Invalid keyword '%s' in primary index.\n",name);
	   return 1;
	}

	if (drms_keyword_getsegscope(key) == 1)
	{
	   /* The purpose of per-segment keywords is to select 
	    * among segments within a record, not to select among records
	    * within a series (which is the purpose of a prime keyword). */
#ifdef DEBUG
	   printf("NOT adding primary key '%s' because it is a per-segment keyword.\n",
		  name);
#endif
	}
	else if (drms_keyword_isslotted(key))
	{
#ifdef DEBUG
	   printf("NOT adding primary key '%s' because it is slotted."
		  "The corresopnding index key is prime instead.\n",
		  name);

	   /* But use the kw flags to track that this keyword was specified as prime in the jsd's 'index' */
#endif
           drms_keyword_setextprime(key);
	}
	else
	{
#ifdef DEBUG
	   printf("adding primary key '%s'\n",name);
#endif
	   template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
	     key; 

           /* This keyword is not slotted, and it is not an index keyword, so 
            * it is prime in both the DRMS-internal and DRMS-external senses. */
           drms_keyword_setintprime(key);
           drms_keyword_setextprime(key);
	}
      }

#ifdef DEBUG
      { int i;
      printf("Primary indices: ");
      for (i=0; i<template->seriesinfo->pidx_num; i++)
	printf("'%s' ",(template->seriesinfo->pidx_keywords[i])->info->name); }
      printf("\n");
#endif     

      break;
    }

    start += len+1;
    len = getnextline(&start);
  }

  /* Ensure all index keywords have slotted keys that have been declared prime. */
  DRMS_Keyword_t *pKey = NULL;
  const char *keyname = NULL;
  HIterator_t *hit = hiter_create(&(template->keywords));
      
  if (hit)
  {
     while ((pKey = 
	     (DRMS_Keyword_t *)hiter_extgetnext(hit, &keyname)) != NULL)
     {
	if (drms_keyword_isslotted(pKey))
	{
           if (!drms_keyword_getextprime(pKey))
	   {
	      fprintf(stderr, 
		      "Slotted key '%s' was not declared drms prime.\n",
		      keyname);
	      return 1;
	   }
	}
     }

     hiter_destroy(&hit);
  }

  return 0; 
}

static int parse_dbindex(char *desc, DRMS_Record_t *template)
{
  int len;
  char *start, *p, *q;
  DRMS_Keyword_t *key;
  char name[DRMS_MAXNAMELEN];
  int exist = 0;

  /* Parse the description line by line, filling 
     out the template struct. */
  start = desc;
  len = getnextline(&start);
  while(*start)
  {
    p = start;
    SKIPWS(p);
    q = p;
    if (getkeyword(&q))
      return 1;
    
    if (prefixmatch(p,"DBIndex:"))
    {
      exist = 1;
      p = q;
      SKIPWS(p);

      while(p<=(start+len) && *p)
      {	
	if (template->seriesinfo->dbidx_num >= DRMS_MAXDBIDX)
	{
	  printf("Too many keywords in primary index.\n");
	  return 1;
	}
	  

	while(p<=(start+len)  && isspace(*p))
	  ++p;
	q = name;
	while(p<=(start+len) && q<name+sizeof(name) && !isspace(*p) && *p!=',')
	  *q++ = *p++;	       
	*q++ = 0;
	p++;
	
	key = hcon_lookup_lower(&template->keywords,name);
	if (key==NULL)
	{
	   printf("Invalid keyword '%s' in db index.\n",name);
	   return 1;
	}

	if (drms_keyword_getsegscope(key) == 1)
	{
	   /* The purpose of per-segment keywords is to select 
	    * among segments within a record, not to select among records
	    * within a series (which is the purpose of a prime keyword). */
#ifdef DEBUG
	   printf("NOT adding index key '%s' because it is a per-segment keyword.\n",
		  name);
#endif
	}
        else if (drms_keyword_isslotted(key))
	{
#ifdef DEBUG
	   printf("NOT adding index key '%s' because it is slotted."
		  "The corresopnding index key will have an index instead.\n",
		  name);
#endif
	}
	else
	{
#ifdef DEBUG
	   printf("adding db idx '%s'\n",name);
#endif
	   template->seriesinfo->dbidx_keywords[(template->seriesinfo->dbidx_num)++] =
	     key; 
	}
      }

#ifdef DEBUG
      { int i;
      printf("DB indices: ");
      for (i=0; i<template->seriesinfo->dbidx_num; i++)
	printf("'%s' ",(template->seriesinfo->dbidx_keywords[i])->info->name); }
      printf("\n");
#endif     

      break;
    }

    start += len+1;
    len = getnextline(&start);
  }

  if (!exist) {
      template->seriesinfo->dbidx_num = -1;
  }

  return 0; 
}


/* Skip empty lines and lines where the first non-whitespace character 
   is '#' */
static int getnextline(char **start)
{
  char *p;
  int length;

  length = 0;
  p = *start;  
  SKIPWS(p);
  while(*p && *p=='#')
  {
    while(*p && *p++ != '\n');
    lineno++;
    SKIPWS(p);
  }
  *start = p;

  /* find the length of the line. */
  while(*p && *p != '\n')
  {       
    length++; 
    ++p; 
  } 
  lineno++;
  return length;
}


/* getstring: Copy a (possibly empty) string starting at address 
   *in to *out. Copy a maximum of maxlen-1 characters. 
   A string is either any sequence of characters quoted by " or ', 
   or an unquoted sequence of non-whitespace characters.
   The quotes are not copied to *out. On exit *in points
   to the character immediately following the last character 
   copied from the input string. Return value is the number
   of characters copied or -1 if an error occured. */

static int getstring(char **inn, char *out, int maxlen) {
  char escape;
  int len;
  char *in;
  
  in = *inn;
  /* Skip leading whitespace. */
  SKIPWS(in);

  len = 0;
  if ( *in=='"' || *in=='\'' )
  {
    /* an escaped string */
    escape = *in;
    //    printf("escape = '%c'\n",escape);
    in++;
    while(*in && *in != escape && len<maxlen-1)
    {
      *out++ = *in++;
      len++;
    }
    if ( *in==escape )
      ++in; /* advance past the closing quote. */
    //    printf("len = %d\n",len);
  }
  else
  {
    /* an un-escaped string (cannot contain whitespace or comma) */    
    while(*in && !ISBLANK(*in) && *in!=',' && len<maxlen-1)
    {
      *out++ = *in++;
      len++;
    }
  }
  /* Terminate output string. */
  *out = 0;

  /* Forward input pointer past the string and WS. */
  SKIPWS(in);
  *inn = in;
  return len;
}

/* same as above, but don't strip off quotes - you need these because there might be escape chars 
 * in the string (like tabs)
 */
static int getvalstring(char **inn, char *out, int maxlen)
{
   char escape;
   int len;
   char *in;
  
   in = *inn;
   /* Skip leading whitespace. */
   SKIPWS(in);

   len = 0;
   if ( *in=='"' || *in=='\'' )
   {
      /* an escaped string - copy quotes to out */
      escape = *in;
      *out++ = *in++;
      len++;

      while(*in && *in != escape && len<maxlen-1)
      {
         *out++ = *in++;
         len++;
      }
      if ( *in==escape )
      {
         *out++ = *in++;
         len++;
      }
   }
   else
   {
      /* an un-escaped string (cannot contain whitespace or comma) */    
      return getstring(inn, out, maxlen);
   }
   /* Terminate output string. */
   *out = 0;

   /* Forward input pointer past the string and WS. */
   SKIPWS(in);
   *inn = in;
   return len;
}

static int getint(char **in, int *val)
{
  char *endptr;
  
  *val = (int)strtol(*in,&endptr,0);
  if (*val==0 && endptr==*in )
  {
    fprintf(stderr,"%s, line %d: The string '%s' on line %d of JSOC series descriptor is not an integer.\n",__FILE__, __LINE__,  *in, lineno);
    return 1;
  }
  else
  {
    *in = endptr;
    return 0;
  }
}


/*
static int getfloat(char **in, float *val)
{
  char *endptr;

  *val = strtof(*in,&endptr);
  if (*val==0 && endptr==*in )
  {
      fprintf(stderr,"%s, line %d: The string '%s' on line %d of JSOC series descriptor is not a float.\n",__FILE__, __LINE__, *in, lineno);
      return 1;
  }
  else
  {
    *in = endptr;
    return 0;
  }
}

static int getdouble(char **in, double *val)
{
  char *endptr;

  *val = strtod(*in, &endptr);
  if (*val==0 && endptr==*in )
  {
      fprintf(stderr, "%s, line %d: The string '%s' on line %d of JSOC series descriptor is not a double.\n",__FILE__, __LINE__, *in, lineno);
      return 1;
  }
  else
  {
    *in = endptr;
    return 0;
  }
}
*/

/* Get a string followed by comma (if the comma exists). */
static int gettoken(char **in, char *copy, int maxlen) 
{
  int len;
  int ln = lineno;

  if((len = getstring(in,copy,maxlen))<0)
    return -1;

  if (**in != '\0' && ln == lineno)
  {
     if (**in != ',')
     {
	fprintf(stderr,"%s, line %d: Expected comma (',') on line %d of JSOC series descriptor.\n", __FILE__, __LINE__, lineno);
	return 0;
     }

     ++(*in);
  }
 
  return len;
}

static int getvaltoken(char **in, DRMS_Type_t type, char *copy, int maxlen)
{
   if (type == DRMS_TYPE_STRING)
   {
      int len;
      int ln = lineno;

      if((len = getvalstring(in,copy,maxlen))<0)
        return -1;

      if (**in != '\0' && ln == lineno)
      {
         if (**in != ',')
         {
            fprintf(stderr,"%s, line %d: Expected comma (',') on line %d of JSOC series descriptor.\n", __FILE__, __LINE__, lineno);
            return 0;
         }

         ++(*in);
      }

      return len;
   }
   else
   {
      return gettoken(in, copy, maxlen);
   }
}

static inline int prefixmatch(char *token, const char *pattern)
{
  return !strncasecmp(token,pattern,strlen(pattern));
}

/* Check that the line has the form 
  <WS> <keyword> <WS>':' 
   i.e. begins with a keyword followed by ':'. Advance
   the line pointer to the next character after ':' 
*/
int getkeyword(char **line)
{
  char *p;

  p = *line;
  SKIPWS(p);               /* <WS> */
  while (isalpha(*p)) ++p; /* KEYWORD */
  SKIPWS(p);               /* <WS> */
  if (*p != ':')
  {
    fprintf(stderr,"%s, line %d: Syntax error in JSOC series descriptor. "
	    "Expected ':' at line %d, column %d.\n",__FILE__, 
	    __LINE__, (int)(p - *line),lineno); 
    return 1;
  }
  *line = p+1;
  return 0;
}

void drms_keyword_print_jsd(DRMS_Keyword_t *key) {
    printf("Keyword:%s",key->info->name);
    if (key->info->islink) {
      printf(", link, %s, %s, %s\n", key->info->linkname, 
	     key->info->target_key,
	     key->info->description);
    } else {
      printf(", %s", drms_type2str(key->info->type));
      int stat;
      const char *rscope = drms_keyword_getrecscopestr(key, &stat);
      fprintf(stdout, ", %s", stat == DRMS_SUCCESS ? rscope : NULL);

    /* key->info->per_segment overload: <= drms_parser.c:1.30 will fail here if the the value in 
     * the persegment column of the drms_keyword table has been overloaded to contain 
     * all the keyword bit-flags. */
      if (drms_keyword_getperseg(key))
	printf(", segment");
      else 
	printf(", record");
      printf(", ");
      if (key->info->type == DRMS_TYPE_STRING) {
	char qf[DRMS_MAXFORMATLEN+2];
	sprintf(qf, "\"%s\"", key->info->format);
	printf(qf, key->value.string_val);
      }
      else 
	drms_keyword_printval(key);      
      if (key->info->unit[0] && key->info->unit[0] != ' ') {
	printf(", \"%s\", \"%s\", \"%s\"", key->info->format,
	       key->info->unit,
	       key->info->description);
      } else {
	printf(", %s, none, \"%s\"", key->info->format,
	       key->info->description);
      }
    }
    printf("\n");
}

void drms_segment_print_jsd(DRMS_Segment_t *seg) {
  int i;
  printf("Data: %s, ", seg->info->name);
  if (seg->info->islink) {
    printf("link, %s, %s", seg->info->linkname, seg->info->target_seg);
    if (seg->info->naxis) {
      printf(", %d", seg->info->naxis);
      printf(", %d", seg->axis[0]);
      for (i=1; i<seg->info->naxis; i++) {
	printf(", %d", seg->axis[i]);
      }
    }
  } else {
    switch(seg->info->scope)
      {
      case DRMS_CONSTANT:
	printf("constant");
	break;
      case DRMS_VARIABLE:
	printf("variable");
	break;
      case DRMS_VARDIM:
	printf("vardim");
	break;
      default:
	printf("Illegal value: %d", (int)seg->info->scope);
      }
    printf(", %s, %d", drms_type2str(seg->info->type), seg->info->naxis);
    if (seg->info->naxis) {
      printf(", %d", seg->axis[0]);
      for (i=1; i<seg->info->naxis; i++) {
	printf(", %d", seg->axis[i]);
      }
    }
    printf(", \"%s\", ", seg->info->unit);

    const char *protstr = drms_prot2str(seg->info->protocol);
    if (protstr)
    {
       printf(protstr);
    }
    else
    {
       printf("Illegal protocol: %d", (int)seg->info->protocol);
    }

    if (seg->info->protocol == DRMS_FITS || 
        seg->info->protocol == DRMS_TAS)
    {
       /* compression string */
       printf(", \"%s\"", seg->cparms);
    }

    if (seg->info->protocol == DRMS_TAS || 
        seg->info->protocol == DRMS_FITS ||
        seg->info->protocol == DRMS_FITZ ||
        seg->info->protocol == DRMS_BINARY ||
        seg->info->protocol == DRMS_BINZIP)
    {
       printf(", %f", seg->bzero);
       printf(", %f", seg->bscale);
    }
  }
  printf(", \"%s\"\n", seg->info->description);
}

void drms_link_print_jsd(DRMS_Link_t *link) {
  printf("Link: %s, %s, ", link->info->name, link->info->target_series);
  if (link->info->type == STATIC_LINK)
    printf("static");
  else
    printf("dynamic");
  printf(", \"%s\"\n", link->info->description);
}

void drms_jsd_printfromrec(DRMS_Record_t *rec) {
   const int fwidth=17;
   HIterator_t hit;
   DRMS_Link_t *link;
   DRMS_Keyword_t *key;
   DRMS_Segment_t *seg;
   int npkeys = 0;
   char **extpkeys; 
   DRMS_Keyword_t *dbidxkw = NULL;
   const char *dbidxkwname = NULL;

   printf("#=====General Series Information=====\n");
   printf("%-*s\t%s\n",fwidth,"Seriesname:",rec->seriesinfo->seriesname);
   printf("%-*s\t\"%s\"\n",fwidth,"Author:",rec->seriesinfo->author);
   printf("%-*s\t%s\n",fwidth,"Owner:",rec->seriesinfo->owner);
   printf("%-*s\t%d\n",fwidth,"Unitsize:",rec->seriesinfo->unitsize);
   printf("%-*s\t%d\n",fwidth,"Archive:",rec->seriesinfo->archive);
   printf("%-*s\t%d\n",fwidth,"Retention:",rec->seriesinfo->retention);
   printf("%-*s\t%d\n",fwidth,"Tapegroup:",rec->seriesinfo->tapegroup);

   extpkeys = drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &npkeys, NULL);
   if (extpkeys)
   {
      if ( npkeys > 0)
      { int i;
      printf("%-*s\t%s",fwidth,"PrimeKeys:",extpkeys[0]);
      for (i=1; i<npkeys; i++)
        printf(", %s", extpkeys[i]);
      printf("\n");
      }
      drms_series_destroypkeyarray(&extpkeys, npkeys);
   }

   if (rec->seriesinfo->dbidx_num) {
     for (int i = 0; i < rec->seriesinfo->dbidx_num; i++) {
        dbidxkw = rec->seriesinfo->dbidx_keywords[i];

        /* An db index may be the index keyword for a slotted keyword. In that case, 
         * the jsd should contain the slotted keyword as a db index, not the 
         * index keyword. */
        if (drms_keyword_isindex(dbidxkw))
        {
           dbidxkwname = dbidxkw->info->name;
           dbidxkw = drms_keyword_slotfromindex(dbidxkw);
           if (!dbidxkw)
           {
              fprintf(stderr, "Invalid index keyword '%s'\n.", dbidxkwname);
           }
        }

        if (i == 0)
        {
           printf("%-*s\t%s",fwidth,"DBIndex:", dbidxkw->info->name);
        }
        else
        {
           printf(", %s", dbidxkw->info->name);
        }
     }
     printf("\n");
   }

   printf("%-*s\t\"%s\"\n",fwidth,"Description:",rec->seriesinfo->description);
   printf("\n#=====Links=====\n");
   hiter_new(&hit, &rec->links); 
   while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
     drms_link_print_jsd(link);

   printf("\n#=====Keywords=====\n");
   hiter_new(&hit, &rec->keywords);
   while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
   {
      if (!drms_keyword_getimplicit(key))
      {
         drms_keyword_print_jsd(key);
      }
   }

   printf("\n#=====Segments=====\n");
   hiter_new(&hit, &rec->segments);
   while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
     drms_segment_print_jsd(seg);
}

void drms_jsd_print(DRMS_Env_t *drms_env, const char *seriesname) {
   int status = DRMS_SUCCESS;

   /* Don't use drms_template_record() as it expands per-segment keywords 
    * into multiple record-specific keywords. */
   /* DRMS_Record_t *rec = drms_template_record(drms_env, seriesname, &status); */

   DRMS_Record_t *rec = drms_create_jsdtemplate_record(drms_env, seriesname, &status);

   if (rec==NULL)
   {
      printf("Series '%s' does not exist. drms_template_record returned "
	     "status=%d\n",seriesname,status);
   }
   else
   {
      drms_jsd_printfromrec(rec);
      drms_destroy_jsdtemplate_record(&rec);
   }
}

/* print a query that will return the given record */

/* string version */
void drms_sprint_rec_query(char *querystring, DRMS_Record_t *rec)
  {
  int iprime, nprime=0;
  char **external_pkeys, *pkey;
  DRMS_Keyword_t *rec_key;
  if (!querystring)
    return;
  if (!rec)
    {
    sprintf(querystring, "** No Record **");
    return;
    }
  sprintf(querystring, "%s",rec->seriesinfo->seriesname);
  external_pkeys =
        drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &nprime, NULL);
  if (external_pkeys && nprime > 0)
    {
    for (iprime = 0; iprime < nprime; iprime++)
      {
      pkey = external_pkeys[iprime];
      rec_key = drms_keyword_lookup (rec, pkey, 1);
      strcat(querystring, "[");
      drms_keyword_snprintfval (rec_key, querystring+strlen(querystring), DRMS_MAXQUERYLEN);
      strcat(querystring, "]");
      }
    }
  else
    sprintf(querystring, "[:#%lld]",rec->recnum);
  }

/* FILE* version */
void drms_fprint_rec_query(FILE *fp, DRMS_Record_t *rec)
  {
  char querystring[DRMS_MAXQUERYLEN];
  drms_sprint_rec_query(querystring, rec);
  fprintf(fp, "%s", querystring);
  }

/* stdout version */
void drms_print_rec_query(DRMS_Record_t *rec)
  {
  drms_fprint_rec_query(stdout, rec);
  }
