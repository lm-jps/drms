// #define DEBUG

#include "drms.h"
#include "ctype.h"
#include "xmem.h"


/* Utility functions. */
static int getstring(char **inn, char *out, int maxlen);
/*
static int getdouble(char **in, double *val);
static int getfloat(char **in, float *val); 
*/
static int getint(char **in, int *val);
static int stripcomma(char **in, char *copy, int len, int maxlen);
static inline int prefixmatch(char *token, const char *pattern);
static int gettoken(char **in, char *copy, int maxlen);

/* Main parsing functions. */
int getkeyword(char **line);
static int getnextline(char **start);
static int parse_seriesinfo(char *desc, DRMS_Record_t *template);
static int parse_segments(char *desc, DRMS_Record_t *template);
static int parse_segment(char **in, DRMS_Record_t *template, int segnum);
static int parse_keyword(char **in, DRMS_Record_t *ds);
static int parse_links(char *desc, DRMS_Record_t *template);
static int parse_link(char **in, DRMS_Record_t *template);
static int parse_primaryindex(char *desc, DRMS_Record_t *template);


/* This macro advances the character pointer argument past all 
   whitespace or until it points to end-of-string (0). */
#define SKIPWS(p) {while(*p && ISBLANK(*p)) { if(*p=='\n') {lineno++;} ++p;}}
#define ISBLANK(c) (c==' ' || c=='\t' || c=='\n' || c=='\r')
#define TRY(__code__) {if ((__code__)) return 1;}

static int lineno;

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
  if (parse_segments(desc, template))
  {
    fprintf(stderr,"Failed to parse segment info.\n");
    goto bailout;
  }

  lineno = 0;
  if (parse_links(desc, template))
  {
    fprintf(stderr,"Failed to parse links info.\n");
    goto bailout;
  }
  lineno = 0;
  if (parse_keywords(desc, template))
  {
    fprintf(stderr,"Failed to parse keywords info.\n");
    goto bailout; 
  }
  lineno = 0;
  if (parse_primaryindex(desc, template))
  {
    fprintf(stderr,"Failed to parse series info.\n");
    goto bailout;
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
      TRY(getstring (&q, template->seriesinfo->seriesname, DRMS_MAXNAMELEN) <= 0)
    else if (prefixmatch (p, "Description"))
      TRY(getstring (&q, template->seriesinfo->description, DRMS_MAXCOMMENTLEN) <= 0)
    else if (prefixmatch (p, "Owner"))
      TRY(getstring (&q, template->seriesinfo->owner, DRMS_MAXNAMELEN) <= 0)
    else if (prefixmatch (p, "Author"))
      TRY(getstring (&q, template->seriesinfo->author, DRMS_MAXCOMMENTLEN) <= 0)
    else if (prefixmatch (p, "Archive"))
      TRY(getint (&q, &(template->seriesinfo->archive)))
    else if (prefixmatch (p, "Unitsize"))
      TRY(getint (&q, &(template->seriesinfo->unitsize)))
    else if (prefixmatch (p, "Tapegroup"))
      TRY(getint (&q, &(template->seriesinfo->tapegroup)))
    else if (prefixmatch (p, "Retention"))
      TRY(getint (&q, &(template->seriesinfo->retention)))

    start += len + 1;
    len = getnextline (&start);
  }

  //  /* Force series name to be all lower case. */
  //  strtolower(template->seriesinfo->seriesname);

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


static int parse_segments (char *desc, DRMS_Record_t *template) {
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
      if (parse_segment(&q, template, segnum))
	return 1;
      ++segnum;
    }
    start += len+1;
    len = getnextline(&start);
  }
  return 0;
}



static int parse_segment(char **in, DRMS_Record_t *template, int segnum)
{
  int i;
  char *p,*q;
  char name[DRMS_MAXNAMELEN]={0}, scope[DRMS_MAXNAMELEN]={0};
  char type[DRMS_MAXNAMELEN]={0}, naxis[24]={0}, axis[24]={0}, protocol[DRMS_MAXNAMELEN]={0};
  char unit[DRMS_MAXUNITLEN]={0};
  DRMS_Segment_t *seg;
  

  p = q = *in;
  SKIPWS(p);
  q = p;


  /* Collect tokens */
  if (!gettoken(&q,name,sizeof(name))) goto failure;
  XASSERT((seg = hcon_allocslot(&template->segments, name)));
  memset(seg,0,sizeof(DRMS_Segment_t));
  XASSERT(seg->info = malloc(sizeof(DRMS_SegmentInfo_t)));
  memset(seg->info,0,sizeof(DRMS_SegmentInfo_t));
  seg->record = template;
  /* Name */
  strcpy(seg->info->name, name);
  /* Number */
  seg->info->segnum = segnum;

  if (!gettoken(&q,scope,sizeof(scope))) goto failure;
  if ( !strcasecmp(scope,"link") ) {
    /* Link segment */
    seg->info->islink = 1;
    seg->info->scope= DRMS_VARIABLE;
    seg->info->type = DRMS_TYPE_INT;
    seg->info->protocol = DRMS_GENERIC;
    if(!gettoken(&q,seg->info->linkname,sizeof(seg->info->linkname)))     goto failure;
    if(!gettoken(&q,seg->info->target_seg,sizeof(seg->info->target_seg))) goto failure;
    /* Naxis */      
    if (!gettoken(&q,naxis,sizeof(naxis))) goto failure;
    seg->info->naxis = atoi(naxis);
    /* Axis */
    for (i=0; i<seg->info->naxis; i++)
      {
	if (!gettoken(&q,axis,sizeof(axis))) goto failure;
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
    if (!gettoken(&q,type,sizeof(type))) goto failure;
    seg->info->type  = drms_str2type(type);
    /* Naxis */      
    if (!gettoken(&q,naxis,sizeof(naxis))) goto failure;
    seg->info->naxis = atoi(naxis);
    /* Axis */
    for (i=0; i<seg->info->naxis; i++)
      {
	if (!gettoken(&q,axis,sizeof(axis))) goto failure;
	seg->axis[i] = atoi(axis);
      }
    if (!gettoken(&q,unit,sizeof(unit))) goto failure;
    strcpy(seg->info->unit, unit);
    if (!gettoken(&q,protocol,sizeof(protocol))) goto failure;
    seg->info->protocol = drms_str2prot(protocol);
    if (seg->info->protocol == DRMS_TAS)
      {
	/* Tile sizes */
	for (i=0; i<seg->info->naxis; i++)
	  {
	    if (!gettoken(&q,axis,sizeof(axis))) goto failure;
	    seg->blocksize[i] = atoi(axis);
	  }
      }
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
  char name[DRMS_MAXNAMELEN]={0}, target[DRMS_MAXNAMELEN]={0}, type[DRMS_MAXNAMELEN]={0},
       description[DRMS_MAXCOMMENTLEN]={0};
  DRMS_Link_t *link;
  

  p = q = *in;
  SKIPWS(p);
  q = p;

  /* Collect tokens */
  if (!gettoken(&q,name,sizeof(name))) goto failure;
  if (!gettoken(&q,target,sizeof(target))) goto failure;
  if (!gettoken(&q,type,sizeof(type))) goto failure;
  if (getstring(&q,description,sizeof(description))<0) goto failure;
  
  XASSERT((link = hcon_allocslot(&template->links, name)));
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



int parse_keywords(char *desc, DRMS_Record_t *template)
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
    
    if (prefixmatch(p,"Keyword:"))
    {
      if (parse_keyword(&q,template))
	return 1;
    }
    start += len+1;
    len = getnextline(&start);
  }
  return 0;
}


static int parse_keyword(char **in, DRMS_Record_t *template)
{
  char *p,*q;
  char name[DRMS_MAXNAMELEN]={0}, type[DRMS_MAXNAMELEN]={0}, linkname[DRMS_MAXNAMELEN]={0}, defval[DRMS_DEFVAL_MAXLEN]={0};
  char unit[DRMS_MAXUNITLEN]={0}, description[DRMS_MAXCOMMENTLEN]={0}, format[DRMS_MAXFORMATLEN]={0};
  char target_key[DRMS_MAXNAMELEN]={0}, constant[DRMS_MAXNAMELEN]={0},  scope[DRMS_MAXNAMELEN]={0}, name1[DRMS_MAXNAMELEN+10]={0};
  int num_segments, per_segment, seg;
  DRMS_Keyword_t *key;
  

  p = q = *in;
  SKIPWS(p);
  q = p;


  /* Collect tokens */
  if (!gettoken(&q,name,sizeof(name))) goto failure;
  if (!gettoken(&q,type,sizeof(type))) goto failure;
  if ( !strcasecmp(type,"link") )
  {
    /* Link keyword. */
    if(!gettoken(&q,linkname,sizeof(linkname)))     goto failure;
    if(!gettoken(&q,target_key,sizeof(target_key))) goto failure;
    if(getstring(&q,description,sizeof(description))<0)       goto failure;
  }
  else
  {
    /* Simple keyword. */
    if(!gettoken(&q,constant,sizeof(constant))) goto failure;
    if(!gettoken(&q,scope,sizeof(scope)))       goto failure;
    if(!gettoken(&q,defval,sizeof(defval)))     goto failure;
    if(!gettoken(&q,format,sizeof(format)))     goto failure;
    if(!gettoken(&q,unit,sizeof(unit)))         goto failure;
    if(getstring(&q,description,sizeof(description))<0)   goto failure;
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
    XASSERT(key = hcon_allocslot(&template->keywords,name));
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
    key->info->isconstant = 0;
    key->info->per_segment = 0;    
    strcpy(key->info->description,description);
  }  
  else
  {
    num_segments = hcon_size(&template->segments);
    if (!strcasecmp(scope,"segment"))
      per_segment = 1;
    else if (!strcasecmp(scope,"record"))
      per_segment = 0;
    else
      goto failure;    
    for (seg=0; seg<(per_segment==1?num_segments:1); seg++)
    {    
      /* If this is a per-segment keyword create one copy for each segment
	 with the segment number formatted as "_%03d" appended to the name */
      if (per_segment)
	sprintf(name1,"%s_%03d",name,seg);
      else
	strcpy(name1,name);

      if ((key = hcon_lookup(&template->keywords,name1))) {
	// this is an earlier definition
	free(key->info);
      }
      XASSERT(key = hcon_allocslot(&template->keywords,name1));
      memset(key,0,sizeof(DRMS_Keyword_t));
      XASSERT(key->info = malloc(sizeof(DRMS_KeywordInfo_t)));

      strncpy(key->info->name,name1,DRMS_MAXNAMELEN);
      if (strlen(name1) >= DRMS_MAXNAMELEN)
        fprintf(stderr,"WARNING keyword name %s truncated to %d characters.\n", name1, DRMS_MAXNAMELEN-1);
      key->record = template;
      key->info->per_segment = per_segment;
      key->info->islink = 0;
      key->info->linkname[0] = 0;
      key->info->target_key[0] = 0;
      key->info->type = drms_str2type(type);  
      if (drms_sscanf(defval, key->info->type, &key->value) <= 0)
	goto failure;
#ifdef DEBUG      
      printf("Default value = '%s' = ",defval);
      drms_printfval(key->info->type, &key->value);
      printf("\n");
#endif
      strcpy(key->info->format, format);
      strcpy(key->info->unit, unit);
      if (!strcasecmp(constant,"constant"))
	key->info->isconstant = 1;
      else if (!strcasecmp(constant,"variable"))
	key->info->isconstant = 0;
      else
	goto failure;
      strcpy(key->info->description,description);
    }
  }
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
  char name[DRMS_MAXNAMELEN];

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
    
    if (prefixmatch(p,"Index:"))
    {
      p = q;
      SKIPWS(p);
      template->seriesinfo->pidx_num = 0;
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
	
#ifdef DEBUG
	printf("adding primary key '%s'\n",name);
#endif
	key = hcon_lookup(&template->keywords,name);
	if (key==NULL)
	{
	  printf("Invalid keyword '%s' in primary index.\n",name);
	  return 1;
	}
	template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
	key; 
      }
#ifdef DEBUG
      { int i;
      printf("Primary indices: ");
      for (i=0; i<template->seriesinfo->pidx_num; i++)
	printf("'%s' ",(template->seriesinfo->pidx_keywords[i])->info->name); }
      printf("\n");
#endif     
      return 0;
    }
    start += len+1;
    len = getnextline(&start);
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


/* getstring: Copy a non-empty string starting at address 
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

  /* Check for empty string. */
  if (*in == 0)
  {
    fprintf(stderr,"%s, line %d: Expected non-empty string on line %d of JSOC series descriptor.\n",
	    __FILE__, __LINE__,lineno);
    return -1;
  }

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

  /* Forward input pointer past the string. */
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

/* Get a string followed by comma. */
static int gettoken(char **in, char *copy, int maxlen) 
{
  int len;
  if( (len = getstring(in,copy,maxlen))<=0)
    return 0;
  if (stripcomma(in, copy, len, maxlen))
    return 0;
  else
    return len;
}
  
    
/* Really ugly hack: */
int stripcomma(char **in, char *copy, int len, int maxlen) 
{ 
  if ((len)<(maxlen-1) && copy[(len)-1]==',') 
  {
    copy[(len)-1] = 0;
  }
  else
  {
    if (**in != ',')
    {
      fprintf(stderr,"%s, line %d: Expected comma (',') on line %d of JSOC series descriptor.\n", __FILE__, __LINE__, lineno);
      return 1;
    }
    ++(*in);
  }    
  return 0;
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

