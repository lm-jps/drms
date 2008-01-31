// #define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "drms_names.h"



/******** Front-end: Parse input string and generate AST **********/

static int parse_name(char **in, char *out, int maxlen);
static RecordSet_Filter_t *parse_record_set_filter(DRMS_Record_t *template, 
						    char **in);
static RecordQuery_t *parse_record_query(char **in);
static RecordList_t *parse_record_list(DRMS_Record_t *template, char **in);
static PrimekeyRangeSet_t *parse_primekey_set(DRMS_Keyword_t *keyword, 
					      char **in);
static PrimekeyRangeSet_t *parse_slottedkey_set(DRMS_Keyword_t *slotkey,
						char **in);
static IndexRangeSet_t *parse_index_set(char **in);
static ValueRangeSet_t *parse_value_set(DRMS_Keyword_t *keyword, char **in);
static int parse_duration(char **in, double *duration);

static int syntax_error;
static int prime_keynum=0;
static int recnum_filter; /* Did the query refer to absolute record numbers? */ 


#define SKIPWS(p) while(*p && isspace(*p)) { ++p;}

RecordSet_t *parse_record_set(DRMS_Env_t *env, char **in)
{
  int status;
  char *p = *in;
  RecordSet_t *rs;

#ifdef DEBUG
  printf("enter parse_record_set\n");
#endif

  prime_keynum = 0;  
  syntax_error = 0;  /* So far so good... */
  recnum_filter = 0;

  XASSERT(rs = malloc(sizeof(RecordSet_t)));
  memset(rs,0,sizeof(RecordSet_t));
  /* Remove leading whitespace. */
  SKIPWS(p);
  if (!parse_name(&p, rs->seriesname, DRMS_MAXNAMELEN))
  {
    /* Force series name to be all lower case. */
    //    strtolower(rs->seriesname);

#ifdef DEBUG
    printf("got seriesname='%s'\n",rs->seriesname);
#endif
    SKIPWS(p);
    if (*p==0)
    {
      rs->template = NULL;
      rs->recordset_spec = NULL;
      return rs;
    }
    else if (*p != '[')
    {
      fprintf(stderr,"Syntax error in record_set: Series name must be "
	      "followed by '[', found '%c'\n",*p);
      ++syntax_error;
      goto empty;
    }

    /* Get series template. It is needed to look up the data type 
       and other information about primary indices. */
    
    if ((rs->template = drms_template_record(env,rs->seriesname,&status)) == NULL)
    {
      fprintf(stderr,"Couldn't get template record for series '%s'."
	      "drms_template_record return status=%d\n",
	      rs->seriesname, status);
      goto empty;
    }
    rs->recordset_spec = parse_record_set_filter(rs->template, &p);

    if (syntax_error)
      goto empty;
    
    *in = p;
#ifdef DEBUG
  printf("exit parse_record_set\n");
#endif
    return rs;
  }
 empty:
  free(rs);
  return NULL;  
}

/* <name> = <alpha> (<alphanum> | '_')*  */
static int parse_name(char **in, char *out, int maxlen)
{
  int len;
  char *p=*in;
#ifdef DEBUG
  printf("enter parse_name\n");
#endif
  len = 0;
  *out = 0; /* maxlen must be greater than 0 */
  /* Get series name. */
  if (*p==0 || !isalpha(*p))
  {
    return 1;
#ifdef DEBUG
    printf("exit parse_name\n");
#endif
  }
  *out++ = *p++;
  ++len;
  while (len < maxlen && *p && (isalnum(*p) || *p == '_' || *p == '.')) {
    *out++ = *p++;
    ++len;
  }
  if (len == maxlen) {
    fprintf (stderr,"name '%.*s' is too long.\n",len,*in);
    syntax_error++;
#ifdef DEBUG
    printf ("exit parse_name\n");
#endif
    return 1;
  }
  else if (len) *out = 0;
  *in = p;
#ifdef DEBUG
  printf("got name='%s'\nexit parse_name\n",out);
#endif

  return 0;
}



static RecordSet_Filter_t *parse_record_set_filter(DRMS_Record_t *template, 
						   char **in)
{
  RecordSet_Filter_t *head=NULL, *rsp=NULL;
  char *p = *in;

#ifdef DEBUG
  printf("enter parse_record_set_filter\n");
#endif
  if (*p!='[')
  {
    fprintf(stderr,"Syntax error: Record_set_filter should "
	    "start with '[', found '%s'\n", p);
    syntax_error++;
    goto error1;
  }
  while (*p && *p++=='[')
  {
    if (*p==']') /* empty set, increment prime key counter */
    {
      prime_keynum++;
      p++;
      continue;
    }
    if (rsp)
    {
      XASSERT(rsp->next = malloc(sizeof(RecordSet_Filter_t)));
      rsp = rsp->next;
      memset(rsp,0,sizeof(RecordSet_Filter_t));
    }
    else
    {
      XASSERT(rsp = malloc(sizeof(RecordSet_Filter_t)));
      head = rsp;
      memset(rsp,0,sizeof(RecordSet_Filter_t));
    }

    /* dont do this twice?
    memset(rsp,0,sizeof(RecordSet_Filter_t));
    */
    if (*p=='?')
    {
      rsp->type = RECORDQUERY;
      if ((rsp->record_query = parse_record_query(&p))==NULL && 
	  syntax_error)
	goto error;
    }
    else
    {
      rsp->type = RECORDLIST;
      if ((rsp->record_list = parse_record_list(template,&p))==NULL && 
	  syntax_error)
	goto error;
    }
    while (*p == ' ')
      p++;

    if (*p++ != ']')
    {
      fprintf(stderr,"Syntax error: Record_set_filter should "
	      "end in ']', found '%c' then '%s'\n", *(p-1), p);
      syntax_error++;
      goto error;
    }
  }
  *in = p;
#ifdef DEBUG
  printf("exit parse_record_set\n");
#endif
  return head;

 error:
  while(head)
  { rsp = head->next; free(head); head = rsp; }
 error1:
  return NULL;
}

  

static RecordQuery_t *parse_record_query(char **in)
{
  int len;
  RecordQuery_t *query;
  char *p = *in, *out;

#ifdef DEBUG
  printf("enter parse_record_query\n");
#endif
  if (*p++ =='?')
  {
    XASSERT(query = malloc(sizeof(RecordQuery_t)));
    memset(query,0,sizeof(RecordQuery_t));
    out = query->where;
    /* Remove leading whitespace. */
    // SKIPWS(p);
    len = 0;
    /* Get SQL-where-clause-like string.  */
    while(len<DRMS_MAXQUERYLEN && *p && *p != '?')
    {
      *out++ = *p++;
      ++len;
    }    
    if (*p++ =='?')
    {
      *out-- = '\0';
      /* Remove trailing whitespace. */
      while(out>=query->where && isspace(*out)) { *out-- = '\0'; };
      *in = p;
    }
    else
    {
      fprintf(stderr,"Embedded SQL query should end with '?', query is:%s.\n",*in);
      free(query);
      goto error;
    }
  }
  else
  {
    fprintf(stderr,"Embedded SQL query should start with '?', found '%c', then '%s'.\n", *(p-1), p);
    goto error;
  }
#ifdef DEBUG
  printf("got query='%s'\nexit parse_record_query\n",query->where);
#endif
  return query;

 error: 
  ++syntax_error;
  return NULL;
}

static RecordList_t *parse_record_list(DRMS_Record_t *template, char **in) {
  int err,i,keynum;
  RecordList_t *rl;
  char *p = *in;
  char pk[DRMS_MAXNAMELEN];
  DRMS_SeriesInfo_t *si;
  DRMS_Keyword_t *nprimekey = NULL;

#ifdef DEBUG
  printf ("enter parse_record_list\n");
#endif

  XASSERT(rl = malloc(sizeof(RecordList_t)));
  if (*p==':') {
    recnum_filter = 1; 
    ++p;
    rl->type = RECNUMSET;
    if ((rl->recnum_rangeset = parse_index_set(&p)) == NULL && syntax_error)
      goto error;   
  } else {			/* record set list based on prime key(s) */
    if (template->seriesinfo->pidx_num <= 0) {
      fprintf (stderr, "Error: Primary key query issued for series with no "
	      "primary keys, query is '%s'\n", p);
      return NULL;
    }

    rl->type = PRIMEKEYSET;
    si = template->seriesinfo;

    /* Try to match an optional '<prime_key>=' string. */
    err = parse_name (&p, pk, DRMS_MAXNAMELEN);
    
    if (*p == '=' && !err) {
					/* A keyword was given explicitly. */
      ++p;
      keynum = -1;
		/* Search the primary index list of the series template and
		match it to the primary key argument given in the descriptor. */
      for (i=0; i<si->pidx_num; i++) {
	if (!strcasecmp (pk, si->pidx_keywords[i]->info->name)) {
	  keynum = i;
	  break;
	}
      }
      if (keynum == -1) {
	 /* The user specified [<key>=value], but <key> was not a prime key.
	  * This COULD be due to the user querying a slotted key (which is
	  * NOT drms prime.  It that is the case, then must pass <slotted key>
	  * to parse_slottedkey_set() (NOTE: parse_slottedkey_set() is the
	  * same function as parse_primekey_set().  Parsing non-primekeys 
	  * other than slotted keys is not allowed, so there is no
	  * parse_key_set() function).
	  *
	  * If a user specifies a query of the form [<slotted key> = <valueA>],
	  * then this is ALWAYS changed into [<index key> = <valueB>],
	  * where <index key> is the index keyword that is associated with
	  * the slotted keyword <slotted key).  And <valueB> is what the 
	  * <valueA> is mapped to (the slot number) when the slotted keyword
	  * value is mapped onto a slot
	  */

	 nprimekey = hcon_lookup_lower(&(template->keywords), pk);

	 if (nprimekey == NULL)
	 {
	    fprintf(stderr, 
		    "Error: '%s' is not a keyword of series '%s'.\n",
		    pk,
		    si->seriesname);
	    ++syntax_error;
	    goto error;
	 }

	 if (!drms_keyword_isslotted(nprimekey))
	 {
	    /* No match was found - report an error. */
	    fprintf(stderr,"Error: '%s' is not a primary index for series '%s'.\n",
		    pk,si->seriesname);
	    ++syntax_error;
	    goto error;
	 }
      }
    } else {
      /* No keyword was given - prime key number is implied by order of
	 filter [...] terms. */
      keynum = prime_keynum++; 
      if (keynum >= si->pidx_num) {
	fprintf(stderr, "Error: More primary keys are implied than exist (%d) "
		"for series %s\n", si->pidx_num,	si->seriesname);
	goto error;
      }
      p = *in;


    }
			/* check if the type of the key is time, and if so
					skip to the first time type prime key */
    if (nprimekey != NULL)
    {
       rl->primekey_rangeset = parse_slottedkey_set (nprimekey, &p);       
    }
    else if (drms_keyword_isindex(si->pidx_keywords[keynum]))
    {
       DRMS_Keyword_t *slottedkey = 
	 drms_keyword_slotfromindex(si->pidx_keywords[keynum]);
       rl->primekey_rangeset = parse_slottedkey_set (slottedkey, &p);
    }
    else
    {
       rl->primekey_rangeset = parse_primekey_set (si->pidx_keywords[keynum], &p);
    }

    if (rl->primekey_rangeset == NULL) {
      fprintf (stderr, "Syntax error: Expected index list at '%s'\n", *in);
      ++syntax_error;
      goto error;
    }
  }
  *in = p;
#ifdef DEBUG
  printf("got type = %d\nexit parse_record_set\n", rl->type);
#endif
  return rl;

error:
  free (rl);
  return NULL;
}

static PrimekeyRangeSet_t *parse_primekey_set(DRMS_Keyword_t *keyword,
					      char **in)
{
  PrimekeyRangeSet_t *pks;
  char *p=*in;
 
#ifdef DEBUG
  printf("enter parse_primekey_set\n");
#endif

  XASSERT(pks = malloc(sizeof( PrimekeyRangeSet_t)));
  pks->keyword = keyword;
  if (*p=='#')
  {    
    pks->type = INDEX_RANGE;
    if ((pks->index_rangeset = parse_index_set(&p))==NULL && 
	syntax_error)
      goto error;
  }
  else
  {
    pks->type = VALUE_RANGE;
    if ((pks->value_rangeset = parse_value_set(keyword,&p))==NULL &&
	syntax_error)
      goto error;
  }
  *in=p;

#ifdef DEBUG
  printf("exit parse_primekey_set\n");
#endif
  return pks;

 error:
  free(pks);
  return NULL;
}

/* From the user's point of view, a slotted keyword IS a drms prime keyword. 
 * the user can substitute, in the grammar, a slotted keyword wherever 
 * a drms prime keyword is expected. */
PrimekeyRangeSet_t *parse_slottedkey_set(DRMS_Keyword_t *slotkey,
					 char **in)
{
   PrimekeyRangeSet_t *ret = NULL;
   ValueRangeSet_t *onerange = NULL;

   if (drms_keyword_isslotted(slotkey))
   {
      ret = parse_primekey_set(slotkey, in);

      for (onerange = ret->value_rangeset; onerange != NULL; onerange = onerange->next)
      {
	 DRMS_Value_t valin = {slotkey->info->type, onerange->start};
	 DRMS_Value_t valout;
	 DRMS_Value_t rangestart = valin;
	 
	 drms_keyword_slotval2indexval(slotkey, &valin, &valout, NULL);
	 onerange->start = valout.value;

	 /* x could be an end time, or a duration (in seconds) */
	 if (onerange->type == START_END)
	 {
	    valin.type = slotkey->info->type;
	    valin.value = onerange->x;
	    drms_keyword_slotval2indexval(slotkey, &valin, &valout, NULL);
	    onerange->x = valout.value;
	 }
	 else if (onerange->type == START_DURATION)
	 {
	    /* A duration is in seconds (a double), so it might not be 
	     * a multiple of the slot size. If this is the case, then
	     * round up to the next largest multiple.  Then make
	     * the duration an integer. */	    
	    valin.type = slotkey->info->type;
	    valin.value = onerange->x;
	    drms_keyword_slotval2indexval(slotkey, 
					  &valin, 
					  &valout, 
					  &rangestart);
	    onerange->x = valout.value;
	 }
	 else if (onerange->type != SINGLE_VALUE)
	 {
	    fprintf(stderr, 
		    "Invalid range set type '%d'.\n", 
		    onerange->type);
	 }
      }

      /* Must associate the parsed range with the slotted keyword's index
       * keyword */
      ret->keyword = drms_keyword_indexfromslot(slotkey);
   }
   
   return ret;
}

static IndexRangeSet_t *parse_index_set(char **in)
{ 
  char *end,*p=*in;
  IndexRangeSet_t *head=NULL,*ir=NULL;
#ifdef DEBUG
  printf("enter parse_index_set\n");
#endif

  do {
    if (ir)
    {
      XASSERT(ir->next = malloc(sizeof( IndexRangeSet_t)));
      ir = ir->next;
      memset(ir,0,sizeof( IndexRangeSet_t));
    }
    else {
      XASSERT(ir = malloc(sizeof( IndexRangeSet_t)));
      head = ir;
      memset(ir,0,sizeof( IndexRangeSet_t));
    }
    if (*p++ != '#') 
    {
      fprintf(stderr,"Syntax Error: Index set must start with '#', found '%c', then '%s'.\n", *(p-1), p);
      ++syntax_error;
      goto error;
    }

    if (*p=='^') {
      ir->type = FIRST_VALUE;
      p++;
    }
    else if (*p == '$') {
      ir->type = LAST_VALUE;
      p++;
    }

    if (ir->type != FIRST_VALUE &&
	ir->type != LAST_VALUE) {

      if (*p == '-') {
	ir->type = RANGE_END;
      } else {

	ir->start = strtoll(p,&end,10);
	if (end==p)
	  {
	    fprintf(stderr,"Syntax Error: Expected integer start in index range, found '%s'.\n", p);
	    ++syntax_error;      
	    goto error;
	  }
	else
	  p = end;

	if (*p=='-')
	  ir->type = START_END;
	else if (*p=='/')
	  ir->type = START_DURATION;
	else 
	  ir->type = SINGLE_VALUE;
      }

      if (ir->type != SINGLE_VALUE)
	{
	  ++p;
      
	  if (ir->type != START_DURATION)
	    {
	      if (*p++!='#') 
		{
		  fprintf(stderr,"Syntax Error: Index set must start with '#', found '%c', then '%s'.\n", *(p-1), p);
		  ++syntax_error;
		  goto error;
		}
	    }

	  /* must be a duration */
	  ir->x = strtoll(p,&end,10);
	  if (end==p)
	    {
	      if (ir->type == START_DURATION) {
		fprintf(stderr,"Syntax Error: Expected integer for end or" 
			" duration in index range., found '%s'\n", p);
		++syntax_error;      
		goto error;
	      } else if (ir->type == RANGE_END) {
		ir->type = RANGE_ALL;
	      } else {
		ir->type = RANGE_START;
	      }
	    }
	  else
	    p = end;    

	  if (*p=='@')
	    {
	      ++p;
	      ir->skip = strtoll(p,&end,10);
	      if (end==p)
		{
		  fprintf(stderr,"Syntax Error: Expected integer skip in index range, found '%s'.\n", p);
		  ++syntax_error;      
		  goto error;
		}
	      else
		p = end;    
	    }
	  else
	    ir->skip = 1;
	}
#ifdef DEBUG
      printf("got type=%d, start=%lld, x=%lld, skip=%lld, p=%s\nexit parse_index_set\n",
	     ir->type, ir->start, ir->x, ir->skip,p);
#endif
    }
  } while(*p++ == ',');
  *in=p-1;

#ifdef DEBUG
  printf("exit parse_index_set\n");
#endif
  return head;

 error:
  while(head)
  { ir = head->next; free(head); head = ir; }
  return NULL;
}



static ValueRangeSet_t *parse_value_set(DRMS_Keyword_t *keyword,
					char **in)
{
  int n;
  char *p=*in;
  ValueRangeSet_t *vr=NULL,*head=NULL;
  DRMS_Type_t datatype = drms_keyword_gettype(keyword);
  int slotdur;

#ifdef DEBUG
  printf("enter parse_value_set\n");
#endif
  do {
    slotdur = 0;
    if (vr)
    {
      XASSERT(vr->next = malloc(sizeof( ValueRangeSet_t)));
      vr = vr->next;
      memset(vr,0,sizeof( ValueRangeSet_t));
    }
    else {
      XASSERT(vr = malloc(sizeof( ValueRangeSet_t)));
      head = vr;
      memset(vr,0,sizeof( ValueRangeSet_t));
    }

    if (*p=='^') {
      vr->type = FIRST_VALUE;
      p++;
    }
    else if (*p == '$') {
      vr->type = LAST_VALUE;
      p++;
    }

    if (vr->type != FIRST_VALUE &&
	vr->type != LAST_VALUE) {

      /* Get start */
      if ((n = drms_sscanf_int(p, datatype, &vr->start, 1)) == 0 ||
	  n == -1)    
	{
	   /* Could be a duration, relative to epoch, for slotted key. */
	   if (datatype == DRMS_TYPE_TIME &&
	       drms_keyword_isslotted(keyword))
	   {
	      if (parse_duration(&p, &vr->x.time_val))
	      {
		 fprintf(stderr,"Syntax Error: Expected time duration "
			 " in value range, found '%s'.\n", p);
		 goto error;
	      }
	      else
	      {
		 DRMS_Keyword_t *epochKey = drms_keyword_epochfromslot(keyword);	     
		 vr->start.time_val = drms_keyword_gettime(epochKey, NULL);
		 vr->type = START_DURATION;
		 slotdur = 1;
	      }
	   }
	   else
	   {
	      fprintf(stderr,"Syntax Error: Expected start value of type %s in"
		      " value range, found '%s'.\n",drms_type2str(datatype), p);
	      goto error;
	   }
	}
      else
	p += n;     

      if (*p=='-')
      {
	vr->type = START_END;
	++p;
      }
      else if (*p=='/')
      {
	vr->type = START_DURATION;
	++p;
      }
      else if (vr->type != START_DURATION)
	vr->type = SINGLE_VALUE;

      /* Get end or duration "x" */
      if (vr->type != SINGLE_VALUE)
	{
	  /* Special handling of time intervals and durations. */
	  if (datatype==DRMS_TYPE_TIME )
	    {
	      if (vr->type == START_END)
		{
		  if ((n = drms_sscanf(p, datatype, &vr->x)) == 0)    
		    {
		      fprintf(stderr,"Syntax Error: Expected end value of"
			      " type %s in value range, found '%s'.\n",drms_type2str(datatype), p);
		      goto error;
		    }
		  else
		    p+=n;
		}
	      else if (slotdur == 0)
		{
		  if (parse_duration(&p,&vr->x.time_val))
		    {
		      fprintf(stderr,"Syntax Error: Expected time duration "
			      " in value range, found '%s'.\n", p);
		      goto error;
		    }	  
		}
	      /* Get skip */
	      if (*p=='@')
		{
		  ++p;
		  vr->has_skip = 1;
		  if (parse_duration(&p,&vr->skip.time_val))    
		    {
		      fprintf(stderr,"Syntax Error: Expected skip (time duration)"
			      " in value range, found '%s'.\n", p);
		      goto error;
		    }
		}
	      else
		vr->has_skip = 0;
	    }
	  else
	    { /* Non-time types. */
	      if ((n = drms_sscanf(p, datatype, &vr->x)) == 0)    
		{
		  fprintf(stderr,"Syntax Error: Expected end or duration value of"
			  " type %s in value range, found '%s'.\n",drms_type2str(datatype), p);
		  goto error;
		}
	      else
		p+=n;
	      /* Get skip */
	      if (*p=='@')
		{
		  ++p;
		  vr->has_skip = 1;
		  if ((n = drms_sscanf(p, datatype, &vr->skip)) == 0)    
		    {
		      fprintf(stderr,"Syntax Error: Expected skip value of type %s in"
			      " value range, found '%s'.\n",drms_type2str(datatype), p);
		      goto error;
		    }
		  else
		    p+=n;
		}
	      else
		vr->has_skip = 0;
	    }
#ifdef DEBUG
	  printf("got type=%d ",vr->type);
	  printf("got datatype=");
	  printf("%s",drms_type2str(datatype));
	  printf(", start="); 
	  drms_printfval(datatype, &vr->start);
	  printf(" ,x=");
	  drms_printfval(datatype, &vr->x);
	  if (vr->has_skip)
	    {
	      printf(", skip=");
	      drms_printfval(datatype, &vr->ski);
	    }
	  printf("\n");
#endif
	}
    }
  } while(*p++ == ',');
  *in=p-1;  
#ifdef DEBUG
  printf("exit parse_value_set\n");
#endif
  return head;

 error:
  printf("ERROR\n");
  while(head)
  { vr = head->next; free(head); head = vr; }
  return NULL;
}




/* Parse time duration constant */


static int parse_duration(char **in, double *duration)
{
  char *end, *p = *in;
  double dval;

  dval = (int)strtod(p,&end);
  if ( (dval == 0 && end==p)  || 
       ((dval == HUGE_VALF || dval == -HUGE_VALF) && errno==ERANGE))
  {
    fprintf(stderr,"Syntax Error: Expected finite floating point value at "
	    "beginning of time duration, found '%s'.\n", p);
    ++syntax_error;      
    goto error;
  }
  else
    p = end;
  switch(*p++)
  {
  case 's':
    *duration = 1.0*dval;
    break;
  case 'm':
    *duration = 60.0*dval;
    break;
  case 'h':
    *duration = 3600.0*dval;
    break;
  case 'd':
    *duration = 86400.0*dval;
    break;
  default:
    fprintf(stderr,"Syntax Error: Time duration unit must be one of 's', "
	    "'m', 'h', 'd', found '%c', '%s'.\n", *(p-1), p);
    ++syntax_error;      
    goto error;
  }
  *in = p;
  return 0;
 error:
  return 1;
}




/***************** Middle-end: Generate SQL from AST ********************/

static int sql_record_set_filter(RecordSet_Filter_t *rs, char *seriesname, char **query);
static int sql_record_query(RecordQuery_t *rs, char **query);
static int sql_record_list(RecordList_t *rs, char *seriesname,  char **query);
static int sql_recnum_set(IndexRangeSet_t  *rs, char *seriesname, char **query);
static int sql_primekey_set(PrimekeyRangeSet_t *rs, char *seriesname,char **query);
static int sql_primekey_index_set(IndexRangeSet_t *rs, DRMS_Keyword_t *keyword,
				  char *seriesname, char **query);
static int sql_primekey_value_set(ValueRangeSet_t *rs, DRMS_Keyword_t *keyword,
				  char *seriesname, char **query);



int sql_record_set(RecordSet_t *rs, char *seriesname, char *query)
{
  char *p=query;
  /*  char *field_list; */
#ifdef DEBUG
  printf("Enter sql_record_set\n");
#endif
  /*  field_list = drms_field_list(rs->template, NULL);
  p += sprintf(p,"SELECT %s FROM %s",field_list,rs->seriesname);
  free(field_list);
  */
#ifdef DEBUG
  printf("Exit sql_record_set\n");
#endif
  if (rs->recordset_spec)
  {
    /*    p += sprintf(p," WHERE "); */
    return sql_record_set_filter(rs->recordset_spec, seriesname, &p);  
  }
  else
  {
    *p = 0;
    return 0;
  }
}

static int sql_record_set_filter(RecordSet_Filter_t *rs, char *seriesname, char **query)
{
  char *p=*query;

#ifdef DEBUG
  printf("Enter sql_record_set_filter\n");
#endif
  do {
    p += sprintf(p,"( ");
    switch(rs->type)
    {
    case RECORDQUERY:
      sql_record_query(rs->record_query, &p);
      break;
    case RECORDLIST:
      sql_record_list(rs->record_list, seriesname, &p);
      break;
    default:    
      fprintf(stderr,"Wrong type (%d) in sql_record_set_filter.\n",
	      rs->type);
      return 1;
    }
    p += sprintf(p," )");
    if (rs->next)
      p += sprintf(p," AND ");
    rs = rs->next;
  } while (rs);
#ifdef DEBUG
  printf("Added '%s'\nExit sql_record_set_filter\n",*query);
#endif
  *query=p;
  return 0;
}


static int sql_record_query(RecordQuery_t *rs, char **query)
{
  char *p=*query;
#ifdef DEBUG
  printf("Enter sql_record_query\n");
#endif
  p += sprintf(p,"%s ",rs->where);
#ifdef DEBUG
  printf("Added '%s '\nExit sql_record_query\n",*query);
#endif
  *query=p;
  return 0;
}

static int sql_record_list(RecordList_t *rs, char *seriesname, char **query)
{
  char *p=*query;  
#ifdef DEBUG
  printf("Enter sql_record_list\n");
#endif

  switch(rs->type)
  {
  case RECNUMSET:
    sql_recnum_set(rs->recnum_rangeset, seriesname, &p);
    break;
  case PRIMEKEYSET:
    sql_primekey_set(rs->primekey_rangeset, seriesname, &p);
    break;
  default:    
    fprintf(stderr,"Wrong type (%d) in sql_record_list.\n",
	    rs->type);
    return 1;
  }

#ifdef DEBUG
  printf("Added '%s'\nExit sql_record_list\n",*query);
#endif
  *query=p;
  return 0;
}

static int sql_recnum_set(IndexRangeSet_t  *rs, char *seriesname, char **query)
{
  char *p=*query;
#ifdef DEBUG
  printf("Enter sql_recnum_set\n");
#endif
  do {
    p += sprintf(p,"( ");
    if (rs->type == FIRST_VALUE) {
      p += sprintf(p,"recnum=(select min(recnum) from %s)", seriesname);
    } else if (rs->type == LAST_VALUE) {
      p += sprintf(p,"recnum=(select max(recnum) from %s)", seriesname);
    } else if (rs->type == SINGLE_VALUE)
      p += sprintf(p,"recnum=%lld ",rs->start);
    else
    {
      if (rs->type == RANGE_ALL) 
	p += sprintf(p,"1 = 1 ");
      else if (rs->type == RANGE_START) 
	p += sprintf(p,"%lld<=recnum ",rs->start);
      else if (rs->type == RANGE_END) 
	p += sprintf(p,"recnum<=%lld ",rs->x);
      else if (rs->type == START_END)
	p += sprintf(p,"%lld<=recnum AND recnum<=%lld ",rs->start,rs->x);
      else if (rs->type == START_DURATION)
	p += sprintf(p,"%lld<=recnum AND recnum<%lld ",rs->start,rs->start+rs->x);
      if (rs->skip!=1) {
	if (rs->type == RANGE_END || rs->type == RANGE_ALL) 
	  p += sprintf(p,"AND (recnum-(select min(recnum) from %s))%%%lld=0 ",seriesname,rs->skip);	  
	else
	  p += sprintf(p,"AND (recnum-%lld)%%%lld=0 ",rs->start,rs->skip);
      }
    }
    p += sprintf(p," )");
    if (rs->next)
      p += sprintf(p," OR ");
    rs = rs->next;
  }
  while (rs);
#ifdef DEBUG
  printf("Added '%s'\nExit sql_recnum_set\n",*query);
#endif
  *query=p;
  return 0;
}


static int sql_primekey_set(PrimekeyRangeSet_t *rs, char *seriesname, char **query)
{
  if (rs->type == INDEX_RANGE) 
    return sql_primekey_index_set(rs->index_rangeset, rs->keyword, seriesname, query);
  else if(rs->type == VALUE_RANGE) 
    return sql_primekey_value_set(rs->value_rangeset, rs->keyword, seriesname, query);
  else 
    return 1;
}


static int sql_primekey_index_set(IndexRangeSet_t *rs, DRMS_Keyword_t *keyword,
				  char *seriesname, char **query)
{
  char *p=*query;
  DRMS_Type_t datatype;
  DRMS_Type_Value_t base, step, one;
  DRMS_Keyword_t *step_key;
  char tmp[1024];

#ifdef DEBUG
  printf("Enter sql_primekey_index_set\n");
#endif
  
  datatype = keyword->info->type;
  base = keyword->value;
  sprintf(tmp,"%s_step",keyword->info->name);
  if ((step_key = drms_keyword_lookup(keyword->record, tmp, 1)) == NULL)
  {
    /* No step given in series definition. Defaults to 1. */
    one.double_val = 1.0;
    drms_convert(datatype, &step, DRMS_TYPE_DOUBLE, &one);
  }
  else
    step = step_key->value;
  do {
    p += sprintf(p,"( ");
    if (rs->type == FIRST_VALUE) {
      p += sprintf(p,"%s=(select min(%s) from %s)", keyword->info->name, keyword->info->name, seriesname);
    } else if (rs->type == LAST_VALUE) {
      p += sprintf(p,"%s=(select max(%s) from %s)", keyword->info->name, keyword->info->name, seriesname);
    } else if (rs->type == SINGLE_VALUE)
    {
      p += sprintf(p,"%s=((%lld*",keyword->info->name,rs->start);
      p += drms_sprintfval(p, datatype, &step, 1);
      p += sprintf(p,")+");
      p += drms_sprintfval(p, datatype, &base, 1);
      p += sprintf(p,")");
    }
    else
    {
      if (rs->type == RANGE_ALL) {
	p += sprintf(p,"1 = 1 ");
      } else if (rs->type == RANGE_START) {
	p += sprintf(p,"%s>=((%lld*",keyword->info->name,rs->start);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,") ");
      } else if (rs->type == RANGE_END) {
	p += sprintf(p,"%s<=((%lld*",keyword->info->name,rs->x);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,") ");
      } else if (rs->type == START_END) {
	p += sprintf(p,"%s>=((%lld*",keyword->info->name,rs->start);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,") AND %s<=((%lld*",keyword->info->name,rs->x);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,")");
      }
      else if (rs->type == START_DURATION)
      {
	p += sprintf(p,"%s>=((%lld*",keyword->info->name,rs->start);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,") AND %s<((%lld*",keyword->info->name,rs->start+rs->x);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,")+");
	p += drms_sprintfval(p, datatype, &base, 1);
	p += sprintf(p,")");
      }
      if (rs->skip!=1)
      {
	if (rs->type == RANGE_END || rs->type == RANGE_ALL) 
	  p += sprintf(p," AND (cast (round((%s-(select min(%s) from %s))/",keyword->info->name, keyword->info->name, seriesname);
	else 
	  p += sprintf(p," AND (cast (round((%s-%lld)/",keyword->info->name, rs->start);
	p += drms_sprintfval(p, datatype, &step, 1);
	p += sprintf(p,") as integer) %%%lld)=0",rs->skip);
      }
    }
    p += sprintf(p," )");
    if (rs->next)
      p += sprintf(p," OR ");
    rs = rs->next;
  }
  while (rs);     
  
#ifdef DEBUG
  printf("Added '%s'\nExit sql_primekey_index_set\n",*query);
#endif
  *query=p;
  return 0;
}


/* If has_skip==0 then select the set of records with prime index value
   in the specified range (either [start:end] or [start:start+duration) ).

   If has_skip==1, type = START_END then for each gridpoint 
         x = start + i*skip, i=0,1,...floor((end-start)/skip)
   return the record(s) with prime index values of minimal distance from x 
   and contained in the closed interval [start-skip/2:end+skip/2]

   If has_skip==1, type = START_DURATION then for each gridpoint 
         x = start + i*skip, i=0,1,...floor(duration/skip)
   return the record(s) with prime index values of minimal distance from x 
   and contained in the open interval [start-skip/2:start+duration+skip/2).
*/

static int sql_primekey_value_set(ValueRangeSet_t *rs, DRMS_Keyword_t *keyword,
				  char *seriesname, char **query)
{
  char *p=*query;
#ifdef DEBUG
  printf("Enter sql_primekey_value_set\n");
#endif
  do {
    p += sprintf(p,"( ");
    if (rs->type == FIRST_VALUE) {
      p += sprintf(p,"%s=(select min(%s) from %s)", keyword->info->name, keyword->info->name, seriesname);
    } else if (rs->type == LAST_VALUE) {
      p += sprintf(p,"%s=(select max(%s) from %s)", keyword->info->name, keyword->info->name, seriesname);
    } else if (rs->type == SINGLE_VALUE)
    {
      p += sprintf(p,"%s=",keyword->info->name);
      p += drms_sprintfval(p, keyword->info->type,  &rs->start, 1);
    }
    else
    {
      p += drms_sprintfval(p, keyword->info->type,  &rs->start, 1);
      if (rs->type == START_END)
      {
	p += sprintf(p,"<=%s AND %s<=",keyword->info->name,keyword->info->name);
	p += drms_sprintfval(p, keyword->info->type,  &rs->x, 1);
      }
      else if (rs->type == START_DURATION)
      {
	p += sprintf(p,"<=%s AND %s<( ",keyword->info->name,keyword->info->name);
	p += drms_sprintfval(p, keyword->info->type,  &rs->start, 1);
	p += sprintf(p," + ");
	p += drms_sprintfval(p, keyword->info->type,  &rs->x, 1);
	p += sprintf(p," )");
      }
      if (rs->has_skip)
	fprintf(stderr,"Skip!=1 not yet supported in value queries.\n");
      //	p += sprintf(p,"AND (recnum-%d)%%%d=0 ",rs->start,rs->skip);
    }
    p += sprintf(p," )");
    if (rs->next)
      p += sprintf(p," OR ");
    rs = rs->next;
  }
  while (rs);     
#ifdef DEBUG
  printf("Added '%s'\nExit sql_primekey_value_set\n",*query);
#endif
  *query=p;
  return 0;
}



/***************** free structures ******************/
static void free_record_set_filter(RecordSet_Filter_t *rs);
static void free_record_query(RecordQuery_t *rs);
static void free_record_list(RecordList_t *rs);
static void free_primekey_set(PrimekeyRangeSet_t *rs);
static void free_index_set(IndexRangeSet_t *rs);
static void free_value_set(ValueRangeSet_t *rs);

void free_record_set(RecordSet_t *rs)
{
  if (rs->recordset_spec)
    free_record_set_filter(rs->recordset_spec);    
  free(rs);
}

static void free_record_set_filter(RecordSet_Filter_t *rs)
{
  RecordSet_Filter_t *old;
  do {
    switch(rs->type)
    {
    case RECORDQUERY:
      free_record_query(rs->record_query);
      break;
    case RECORDLIST:
      free_record_list(rs->record_list);
      break;
    }
    old = rs;
    rs = rs->next;
    free(old);
  } while (rs);
}

static void free_record_query(RecordQuery_t *rs)
{
  free(rs);
}

static void free_record_list(RecordList_t *rs)
{
  switch(rs->type)
  {
  case RECNUMSET:
    free_index_set(rs->recnum_rangeset);
    break;
  case PRIMEKEYSET:
    free_primekey_set(rs->primekey_rangeset);
    break;
  }
  free(rs);
}

static void free_primekey_set(PrimekeyRangeSet_t *rs)
{
  if (rs->type == INDEX_RANGE) 
    free_index_set(rs->index_rangeset);
  else if(rs->type == VALUE_RANGE) 
    free_value_set(rs->value_rangeset);
  free(rs);
}

static void free_index_set(IndexRangeSet_t *rs)
{
  IndexRangeSet_t *old;
  while(rs) 
  {
    old = rs;
    rs = rs->next;
    free(old);
  }
}

static void free_value_set(ValueRangeSet_t *rs)
{
  ValueRangeSet_t *old;
  while (rs)
  {
    old = rs;
    rs = rs->next;
    free(old);
  }
}


/* prime index value queries with skips: 

A query of the form 

  "series[key=start-end@skip,start2-end2,start3-end3]"

should give rise to a query of the form

SELECT <field list> FROM series,
  (
  SELECT grididx, min(abs((key-start)-skip*grididx)) AS modulodist
  FROM (
       SELECT key,round((key-start)/skip) AS grididx FROM test
       WHERE  key>=start AND key<=end
       ) AS temp1
  GROUP BY grididx 
  ) AS temp2
WHERE 
  (key>=start-skip/2 AND
   key<=end+skip/2 AND  
   abs((key-start)-skip*round((key-start)/skip))=modulodist AND
   round(b-start)/skip=grididx)
  OR
  (key>=start2 AND key<=end2)
  OR
  (key>=start3 AND key<=end3)






Multiple sub-sampled intervals (same key):

  "series[key=start1-end1@skip1,start2-end2@skip2]"

should give rise to the query:


SELECT <field list> FROM series,
  (
  SELECT grididx1, min(abs((key-start1)-skip1*grididx)) AS modulodist1
  FROM (
       SELECT key,round((key-start1)/skip1) AS grididx1 FROM test
       WHERE  key>=start1 AND key<=end1
       ) AS temp11
  GROUP BY grididx 
  ) AS temp12,
 (
  SELECT grididx2, min(abs((key-start2)-skip2*grididx)) AS modulodist2
  FROM (
       SELECT key,round((key-start2)/skip2) AS grididx2 FROM test
       WHERE  key>=start2 AND key<=end2
       ) AS temp21
  GROUP BY grididx 
  ) AS temp22
WHERE 
  (key>=start1-skip1/2 AND
   key<=end1+skip1/2 AND  
   abs((key-start1)-skip1*round((key-start1)/skip1))=modulodist1 AND
   round(b-start1)/skip1=grididx1)
  OR
  (key>=start2-skip2/2 AND
   key<=end2+skip2/2 AND  
   abs((key-start2)-skip2*round((key-start2)/skip2))=modulodist2 AND
   round(b-start2)/skip2=grididx2)




Multiple sub-sampled intervals (different key):

  "series[key1=start1-end1@skip1][key2=start2-end2@skip2]"

should give rise to the query:


SELECT <field list> FROM series,
  (
  SELECT grididx1, min(abs((key1-start1)-skip1*grididx)) AS modulodist1
  FROM (
       SELECT key1,round((key1-start1)/skip1) AS grididx1 FROM test
       WHERE (key1>=start1-skip1/2 AND key1<=end1+skip2/2)
       ) AS temp11
  GROUP BY grididx 
  ) AS temp12,
  (
  SELECT grididx2, min(abs((key2-start2)-skip2*grididx)) AS modulodist2
  FROM (
       SELECT key2,round((key2-start2)/skip2) AS grididx2 FROM test
       WHERE (key2>=start2-skip2/2 AND key2<=end2+skip2/2)
       ) AS temp21
  GROUP BY grididx 
  ) AS temp22
WHERE 
  (key1>=start1-skip1/2 AND
   key1<=end1+skip1/2 AND  
   abs((key1-start1)-skip1*round((key1-start1)/skip1))=modulodist1 AND
   round(b-start1)/skip1=grididx1)
  AND **** <=== notice! *****
  (key2>=start2-skip2/2 AND
   key2<=end2+skip2/2 AND  
   abs((key2-start2)-skip2*round((key2-start2)/skip2))=modulodist2 AND
   round(b-start2)/skip2=grididx2)

*/


// The mixed flag is meant to differentiate queries with prime index
// only and those on both prime and non-prime index. As it would
// involve the query statement, an approximation of the latter case is
// the where clause in between ?'s.
int drms_recordset_query(DRMS_Env_t *env, char *recordsetname, 
			 char **query, char **seriesname, int *filter, int *mixed)
{
  RecordSet_t *rs;
  char *p = recordsetname;
  *mixed = 0;

  if ((rs = parse_record_set(env,&p)))
  {
    if (rs->recordset_spec &&
	rs->recordset_spec->record_query) {
      *mixed = 1;
    }
    XASSERT(*query = malloc(DRMS_MAXQUERYLEN));
    *seriesname = strdup(rs->seriesname);
    *filter = !recnum_filter;
    sql_record_set(rs,*seriesname, *query);
    free_record_set(rs);
    return 0;
  }
  else
    return 1;
}
