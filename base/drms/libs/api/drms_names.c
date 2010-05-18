// #define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "drms_names.h"



/******** Front-end: Parse input string and generate AST **********/

static int parse_name(char **in, char *out, int maxlen);
static RecordSet_Filter_t *parse_record_set_filter(DRMS_Record_t *template, 
                                                   char **in,
                                                   int *allvers);
static RecordQuery_t *parse_record_query(char **in);
static RecordList_t *parse_record_list(DRMS_Record_t *template, char **in);
static PrimekeyRangeSet_t *parse_primekey_set(DRMS_Keyword_t *keyword, 
					      char **in);
static PrimekeyRangeSet_t *parse_slottedkey_set(DRMS_Keyword_t *slotkey,
						char **in);
static IndexRangeSet_t *parse_index_set(char **in);
static ValueRangeSet_t *parse_value_set(DRMS_Keyword_t *keyword, char **in);
static int is_duration(const char *in);
static int parse_duration(char **in, double *duration, double width);

static int syntax_error;
static int prime_keynum=0;
static int recnum_filter; /* Did the query refer to absolute record numbers? */ 


#define SKIPWS(p) while(*p && isspace(*p)) { ++p;}

RecordSet_t *parse_record_set(DRMS_Env_t *env, char **in)
{
  int status;
  char *p = *in;
  RecordSet_t *rs;
  int allvers = 0;

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
  if (!parse_name(&p, rs->seriesname, DRMS_MAXSERIESNAMELEN))
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
    rs->recordset_spec = parse_record_set_filter(rs->template, &p, &allvers);

    if (syntax_error)
      goto empty;
    
    rs->allvers = allvers;

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
						   char **in,
                                                   int *allvers)
{
  RecordSet_Filter_t *head=NULL, *rsp=NULL;
  char *p = *in;

  if (allvers)
  {
     *allvers = 0;
  }

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
    if (*p=='?' || *p=='!')
    {
      if (*p == '!' && allvers)
      {
         *allvers = 1;
      }
      
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
  char endchar = *p;

  if (*p =='?' || *p == '!')
  {
    p++;
    XASSERT(query = malloc(sizeof(RecordQuery_t)));
    memset(query,0,sizeof(RecordQuery_t));
    out = query->where;
    len = 0;

    /* Get SQL-where-clause-like string.  */
    while(len<DRMS_MAXQUERYLEN && *p)
    {
       if (*p == '"' || *p == '\'')
       {
          char endq = *p;
          /* skip quoted strings */
          DRMS_Type_Value_t val;
          memset(&val, 0, sizeof(DRMS_Type_Value_t));
          /* drms_sscanf_str will end up pointing to one char after the quote, because
           * the first character was a quote. If there is no end matching quote, 
           * it will return -1. */
          int rlen = drms_sscanf_str(p, NULL, &val);
          int ilen = 0;

          if (rlen == -1)
          {
             /* no end quote */
             fprintf(stderr, "End quote '%c' missing./n", endq);
             goto error;
          }

          while (ilen < rlen)
          {
             *out++ = *p++;
             ilen++;
          }
       }
       // put catching of time_convert flag X here too, see ParseRecSetDesc in drms_record.c
       else if (*p == '$' && *(p+1) == '(')
       {
          /* A form of '$(xxxx)' is taken to be a DRMS preprocessing function
           * which is evaluated prior to submitting the query to psql.  The
           * result of the function must be a valid SQL operand or expression.
           * the only DRMS preprocessing function at the moment is to
           * convert an explicit time constant into an internal DRMS TIME
           * expressed as a double constant. */
           char *rparen = strchr(p+2, ')');
           if (!rparen || rparen - p > 40) // leave room for microsecs
           {
              fprintf(stderr,"Time conversion error starting at %s\n",p+2);
              goto error;
           }
           else
           {
              char temptime[100];
              /* pick function here, if ever more than time conversion */
              TIME t; 
              int consumed;
              strncpy(temptime,p+2,rparen-p-2);
              consumed = sscan_time_ext(temptime, &t);
              if (time_is_invalid(t))
                  fprintf(stderr,"Warning in parse_record_query: invalid time from %s\n",temptime);
#ifdef DEBUG
fprintf(stderr,"XXXXX in parse_record_query, convert time %s uses %d chars, gives %f\n",temptime, consumed,t);
#endif
              p = rparen + 1;
              out += sprintf(out, "%16.6f", t);
           }
       }
       else if (*p == endchar)
       {
          /* if char after '?'/'!' is r bracket, done */
          if (*(p + 1) == ']')
          {
             break;
          }
          else
          {
             /* This ?/! is not inside a quoted string, so it MUST be followed by a ], 
              * otherwise this is a syntax error. */
             fprintf(stderr, "Expecting filter to end in '%c]', but '%c]' found;\n", endchar, *(p + 1));
             goto error;
          }
       }
       else
       {
          /* whitespace okay in sql query */
          *out++ = *p++;
       }
    }

    if (*p++ == endchar)
    {
      *out-- = '\0';
      /* Remove trailing whitespace. */
      while(out>=query->where && isspace(*out)) { *out-- = '\0'; };
      *in = p;
    }
    else
    {
      fprintf(stderr,"Embedded SQL query should end with '%c', query is:%s.\n", endchar, *in);
      free(query);
      goto error;
    }
  }
  else
  {
    fprintf(stderr,"Embedded SQL query should start with '%c', found '%c', then '%s'.\n", endchar, *p, ++p);
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
  char pk[DRMS_MAXKEYNAMELEN];
  char strbuf[DRMS_MAXQUERYLEN];
  DRMS_SeriesInfo_t *si;
  DRMS_Keyword_t *nprimekey = NULL;
  int explKW = 0;

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
      goto error;
    }

    rl->type = PRIMEKEYSET;
    si = template->seriesinfo;

    SKIPWS(p);

    /* Try to match an optional '<prime_key>=' string. */
    err = parse_name (&p, strbuf, sizeof(strbuf));

    if (!err)
    {
       SKIPWS(p);
    }
    
    if (*p == '=' && !err) {
					/* A keyword was given explicitly. */
      if (strlen(strbuf) > DRMS_MAXKEYNAMELEN)
      {
	 fprintf(stderr, 
		 "Error: Keyword name expected but '%s' exceeds maximum name length.\n", 
		 strbuf);
	 goto error;
      }

      snprintf(pk, sizeof(pk), "%s", strbuf);
      explKW = 1;
      ++p;
      SKIPWS(p);
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
       /* User specified [SLOTKEY=<value>] */
       rl->primekey_rangeset = parse_slottedkey_set (nprimekey, &p);       
    }
    else if (!explKW && drms_keyword_isindex(si->pidx_keywords[keynum]))
    {
       /* User specified [<value>] */
       DRMS_Keyword_t *slottedkey = 
	 drms_keyword_slotfromindex(si->pidx_keywords[keynum]);
       rl->primekey_rangeset = parse_slottedkey_set (slottedkey, &p);
    }
    else
    {
       /* User specified either [INDEXKEY=<indexvalue>] or 
	* [SOMEOTHERPRIMEKEY=<value>] */
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

      /* If the range set is a value range set, then convert the values to 
       * index keyword values. */
      if (ret->type == VALUE_RANGE && 
          ret->value_rangeset->type != FIRST_VALUE &&
          ret->value_rangeset->type != LAST_VALUE)
      {
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

#ifdef OLDWAY_NEVER_USE

/* Ideally, we woudln't have to parse the time string twice (here and in the 
 * subsetquent call to drms_sscanf(). But the way we represent times with
 * strings isn't suitable to parsing time zones without having to parse the
 * whole time string.  A better time string would not overload '_' as a separator
 * for time zones and as a separator between other time components.  timeio kind
 * of has to parse the whole string to get to the point where it can determine if
 * there is a time zone in the string.
 * 
 * Also, you can't simply look for _XXX in a time string and if that exists then
 * deduce that the time string has a valid time zone.  _XXX may not be a valid time zone
 * for one thing, but drms_sscanf() will still parse it as UT (it probably shouldn't).
 *
 * timeio is buggy.  It will think that the timezone of 1996.05.01_00:00 is UTC.
 * But it will think that the timezone of 2006.05.01 is not known.  But downstread,
 * timeio will somehow think that 2006.05.01 is UTC.
 */

// This is nonsense.  We have since decided that ALL time strings default to UTC if the zone is not
// specified. In any case, parse_date_time_inner which is called by parsetimestr always fills
// in the dattim.zone, with "UTC" if not from the string itself.  Further it does not
// do anything with the zone to adjust the values.  So all of this code can
// be replaced - if it is still necessary - with a simple call to parse_date_time_inner
// via a call to sscan_time_ext, then twiddle the value in zone

// furthermore, the strings passed back 

static char *AdjTimeZone(const char *timestr, DRMS_Keyword_t *keyword, int *len)
{
  /* If we are parsing a time string, and the time-string has NO time-zone
    * specified, use the keyword's unit field as the time-zone*/
   char *ret = NULL; 
   int *year = NULL;
   int *month = NULL;
   int *dofm = NULL;
   int *hour = NULL;
   int *minute = NULL;
   double *second = NULL;
   double *juliday = NULL;
   char *zone = NULL;

   char *lasts;
   char *tokenstr = strdup(timestr);
   *len = 0;

   if (tokenstr)
   {
      if (parsetimestr(tokenstr, &year, &month, &dofm, NULL, &hour, &minute, 
                       &second, &zone, &juliday, len))
      {
         if (!zone)
         {
            /* Valid time, but no zone - append keyword's unit field. */
// But this would be wrong.  default zone must be UTC in all cases, not just sometimes.
            ret = malloc(256);

            /* Either juliday must be not NULL or year/month/dofm must be present */
            /* timeio automatically provides month = 1 and dofm = 1 if they 
             * are not in the time string.  Ideally it wouldn't do that so you could 
             * tell what fields were provided in the time string, but just work
             * around that and assume those two fields exist. Same thing with 
             * the hour/day/seconds fields - even if some are not in the time
             * string, timeio adds them. But you can tell if NO clock field is
             * in the time string, in which case all three are missing. Otherwise, 
             * all three are present. */
            if (juliday)
            {
               snprintf(ret, 256, "JD_%f_%s", *juliday, keyword->info->unit);
            }
            else
            {
               int hh = 0;
               int mm = 0;
               double sec = 0.0;

               XASSERT(year != NULL && month != NULL && dofm != NULL);

               if (hour)
               {
                  hh = *hour;
               }

               if (minute)
               {
                  mm = *minute;
               }

               if (second)
               {
                  sec = *second;
               }

               snprintf(ret, 256, "%d.%d.%d_%d:%d:%f_%s", 
                        *year, *month, *dofm, hh, mm, sec, keyword->info->unit);
            }

         }
      }
      else
      {
         *len = 0;
      }

      free(tokenstr);
   }

   if (year)
   {
      free(year);
   }

   if (month)
   {
      free(month);
   }

   if (dofm)
   {
      free(dofm);
   }

   if (hour)
   {
      free(hour);
   }

   if (minute)
   {
      free(minute);
   }

   if (second)
   {
      free(second);
   }

   if (zone)
   {
      free(zone);
   }

   if (juliday)
   {
      free(juliday);
   }

   return ret;
}
#endif

static ValueRangeSet_t *parse_value_set(DRMS_Keyword_t *keyword,
					char **in)
{
  int n;
  char *p=*in;
  ValueRangeSet_t *vr=NULL,*head=NULL;
  DRMS_Type_t datatype = drms_keyword_gettype(keyword);
  int gotstart;
  int stat;
  DRMS_Value_t vholder;

  double step;
  int isslotted = 0;

  isslotted = drms_keyword_isslotted(keyword);

  if (isslotted)
  {
     step = drms_keyword_getslotstep(keyword, NULL, &stat);

     if (stat != DRMS_SUCCESS)
     {
        goto error;
     }
  }
  else
  {
     step = 1.0;
  }

#ifdef DEBUG
  printf("enter parse_value_set\n");
#endif
  do {
    gotstart = 0;

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

       /* If this is a TS_EQ- or SLOT-slotted key, this could be a duration 
	* instead of a drms type value, check for that. 
	* If that fails, try to parse as a drms value. */
       if ((datatype == DRMS_TYPE_TIME ||
	    datatype == DRMS_TYPE_FLOAT ||
	    datatype == DRMS_TYPE_DOUBLE) &&
	   drms_keyword_isslotted(keyword) &&
	   is_duration(p))
       {
	  /* Could be an offset relative to epoch, eg. 3000d 
	   * (for time slotted key only). */
	  double offset;
	  double base;

          if (!parse_duration(&p, &offset, step))
	  {
	     /* The start time is really relative to the epoch, 
	      * so need to convert to DRMS time. */
	     base = drms_keyword_getslotbase(keyword, &stat);
	     if (stat == DRMS_SUCCESS)
	     {
		DRMS_Type_Value_t sum;
		sum.double_val = base + offset;
		drms_convert(datatype, &(vr->start), DRMS_TYPE_DOUBLE, &sum);
		gotstart = 1;
	     }
	  }
       }

       if (!gotstart)
       {
          int adv = -1;
          if (datatype == DRMS_TYPE_TIME)
          {
             /* This will consume as much time string as possible. So, in this time string:
              * 
              * 2009.01.03_12:45:00-2009.01.04_00:55:00 
              *
              * -2009 will be considered the time zone string of the time string
              * 2009.01.03_12:45:00-2009. This is wrong, so if we fail, then we need to 
              * 'break' the string at the '-', and try again.
              * 
              */

             adv = sscan_time_ext(p, &(vholder.value.time_val));

             /* If this is a single-value time string, then p + adv should contain 
              * spaces or ']'. If this is a start-end value, then p + adv
              * should point to spaces or '-'. If this is a start-duration value, 
              * then p + adv should point to spaces or '/'
              */
             while (*(p + adv) == ' ')
             {
                adv++;
             }

             if (*(p + adv) != ']' && *(p + adv) != '-' && *(p + adv) != '/')
             {
                /* possibly associated '-2009' with time string, as described above */
                char *tmp = strdup(p);
                char *dash = NULL;

                if (tmp)
                {
                   dash = strchr(tmp, '-');
                   if (dash)
                   {
                      *dash = '\0';
                      adv = sscan_time_ext(tmp, &(vholder.value.time_val));
                   }

                   free(tmp);
                }
             }

             if (adv > 0)
             {
                p += adv;
             }
             else
             {
                fprintf(stderr,"Syntax Error: Expected either time duraton " 
                        "or start value of type %s in "
                        "value range, found '%s'.\n", drms_type2str(datatype), p);
                goto error;
             }
          }
          else
          {
             if ((n = drms_sscanf2(p, ",]", 1, datatype, &vholder)) == 0 || n == -1)
             {
                fprintf(stderr,"Syntax Error: Expected either time duraton " 
                        "or start value of type %s in "
                        "value range, found '%s'.\n", drms_type2str(datatype), p);
                goto error;
             }

             p += n;
          }

          vr->start = vholder.value; /* if string, vr->start owns */
          memset(&(vholder.value), 0, sizeof(DRMS_Type_Value_t));
       }

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
      else
	vr->type = SINGLE_VALUE;

      /* Get end or duration "x" */
      if (vr->type != SINGLE_VALUE)
	{
           /* ignore spaces */
           while (*p == ' ')
           {
              ++p;
           };

	  /* Special handling of time intervals and durations. */
           if (datatype==DRMS_TYPE_TIME)
           {
	      double dval = 0.0;
	      DRMS_Type_Value_t dvalval;
             

	      if (vr->type == START_END)
		{
                   int adv = -1;
                   adv = sscan_time_ext(p, NULL);

		  if ((n = drms_sscanf2(p, ",]", 0, datatype, &vholder)) == 0)    
		    {
		      fprintf(stderr,"Syntax Error: Expected end value of"
			      " type %s in value range, found '%s'.\n",drms_type2str(datatype), p);
		      goto error;
		    }
		  else if (adv > 0)
                     {
                     p += adv;
                     }
                  else
                     {
                     p += n;
                     }
                  vr->x = vholder.value;
                  memset(&(vholder.value), 0, sizeof(DRMS_Type_Value_t));
		}
	      else
              {
                 /* in other words, this is a duration */
                 if (parse_duration(&p,&dval,step))
		   {
		      fprintf(stderr,"Syntax Error: Expected time or float duration "
			      " in value range, found '%s'.\n", p);
		      goto error;
		   }
 
		   dvalval.double_val = dval;
		   drms_convert(datatype, &(vr->x), DRMS_TYPE_DOUBLE, &dvalval);

		}
	      /* Get skip */
	      if (*p=='@')
		{
		  ++p;
		  vr->has_skip = 1;

		  if (parse_duration(&p,&dval,step))    
		    {
		      fprintf(stderr,"Syntax Error: Expected skip (time duration)"
			      " in value range, found '%s'.\n", p);
		      goto error;
		    }

                  /* If this is a slotted keyword, then we need to convert into  
                   * "index space" - the skip value needs to be expressed in terms
                   * of slots, not seconds. */
                  if (isslotted)
                  {
                     double exact = dval / step;
                     double rounded = round(dval / step);

                     if (fabs(exact - rounded) > 1.0e-11 * (fabs(exact) + fabs(rounded)))
                     {
                        fprintf(stdout, "NOTE: the skip value '%f' is not a multiple of step size; rounding to nearest step-size multiple '%f'\n", dval, rounded * step);
                     }

                     dvalval.double_val = rounded;
                     drms_convert(DRMS_TYPE_LONGLONG, &(vr->skip), DRMS_TYPE_DOUBLE, &dvalval);
                  }
                  else
                  {
                     /* Not slotted - just use original skip value, converted into the keyword's data type */
                     dvalval.double_val = dval;
                     drms_convert(datatype, &(vr->skip), DRMS_TYPE_DOUBLE, &dvalval);
                  }
		}
	      else
		vr->has_skip = 0;
	    }
	  else
          { /* Non-time types. */
               double dval = 0.0;
               DRMS_Type_Value_t dvalval;

               if (vr->type == START_DURATION && 
                   keyword->info->recscope == kRecScopeType_SLOT &&
                   is_duration(p))
               {
                  /* It might be that all you need is the is_duration() check, but for now
                   * the only known case of a non-time keyword having a duration
                   * is for a SLOT slotted keyword. */
                  if (parse_duration(&p, &dval, step))    
                  {
                     fprintf(stderr,"Syntax Error: Expected skip (time duration)"
                             " in value range, found '%s'.\n", p);
                     goto error;
                  }

		  dvalval.double_val = dval;
		  drms_convert(datatype, &(vr->x), DRMS_TYPE_DOUBLE, &dvalval);             
               }
               else
               {
                  if ((n = drms_sscanf2(p, ",]", 0, datatype, &vholder)) == 0)    
                  {
                     fprintf(stderr,"Syntax Error: Expected end or duration value of"
                             " type %s in value range, found '%s'.\n",drms_type2str(datatype), p);
                     goto error;
                  }
                  else
                  {
                     vr->x = vholder.value;
                     memset(&(vholder.value), 0, sizeof(DRMS_Type_Value_t));
                     p+=n;
                  }
               }
	      /* Get skip */
	      if (*p=='@')
              {
                 ++p;
                 vr->has_skip = 1;

                 /* Again, this could be a 'u' duration for SLOT slotted keywords. */
                 if (keyword->info->recscope == kRecScopeType_SLOT && is_duration(p))
                 {
                    if (parse_duration(&p, &dval, step))    
                    {
                       fprintf(stderr,"Syntax Error: Expected skip (time duration)"
                               " in value range, found '%s'.\n", p);
                       goto error;
                    }

                    /* If this is a slotted keyword, then we need to convert into  
                     * "index space" - the skip value needs to be expressed in terms
                     * of slots, not seconds. */
                    if (isslotted)
                    {
                       double exact = dval / step;
                       double rounded = round(dval / step);

                       if (fabs(exact - rounded) > 1.0e-11 * (fabs(exact) + fabs(rounded)))
                       {
                          fprintf(stdout, "NOTE: the skip value '%f' is not a multiple of step size; rounding to nearest step-size multiple '%f'\n", dval, rounded * step);
                       }

                       dvalval.double_val = rounded;
                       drms_convert(DRMS_TYPE_LONGLONG, &(vr->skip), DRMS_TYPE_DOUBLE, &dvalval);
                    }
                    else
                    {
                       /* Not slotted - just use original skip value, converted into the keyword's data type */
                       dvalval.double_val = dval;
                       drms_convert(datatype, &(vr->skip), DRMS_TYPE_DOUBLE, &dvalval);
                    }
                 }
                 else 
                 {
                    if ((n = drms_sscanf2(p, ",]", 0, datatype, &vholder)) == 0)    
                    {
                       fprintf(stderr,"Syntax Error: Expected skip value of type %s in"
                               " value range, found '%s'.\n",drms_type2str(datatype), p);
                       goto error;
                    }

                    vr->skip = vholder.value;
                    memset(&(vholder.value), 0, sizeof(DRMS_Type_Value_t));
                    p+=n;
                 }
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

/* Detect either a time duration or a generic duration (denoted by 'u' - used by SLOT slotted key) */
static int is_duration(const char *in)
{
   char *end = NULL;
   const char *p = in;
   double dval;
   int ret = 0;

   dval = (int)strtod(p,&end);
   if ( (IsZero(dval) && end==p)  || 
	((IsPosHugeVal(dval) || IsNegHugeVal(dval)) && errno==ERANGE))
   {
      ret = 0;
   }
   else
   {
      p = end;

      switch(*p++)
      {
	 case 's':
	 case 'm':
	 case 'h':
	 case 'd':
	 case 'u':
	   ret = 1;
	   break;
	 default:
	   break;
      }
   }

   return ret;
}

/* Parse time duration constant */
static int parse_duration(char **in, double *duration, double width)
{
  char *end, *p = *in;
  double dval;

  dval = strtod(p,&end);
  if ( (IsZero(dval) && end==p)  || 
       ((IsPosHugeVal(dval) || IsNegHugeVal(dval)) && errno==ERANGE))
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
  case 'u':
    /* Means the unit is a slot - for SLOT type slotted keys, can have a query of the form
     * [392.3/100u]. If the slot width is 3.0, this just means [392.3 - 692.3). */
    *duration = width*dval;
    break;
  default:
    fprintf(stderr,"Syntax Error: Time duration unit must be one of 's', "
	    "'m', 'h', 'd', 'u', found '%c', '%s'.\n", *(p-1), p);
    ++syntax_error;      
    goto error;
  }
  *in = p;
  return 0;
 error:
  return 1;
}

int drms_names_parseduration(char **in, double *duration, double width)
{
   char *tmp = strdup(*in);
   int ret = 1;

   if (tmp)
   {
      char *tmp2 = tmp;
      ret = parse_duration(&tmp2, duration, width);
      free(tmp);
   }

   return ret;
}

int drms_names_parsedegreedelta(char **deltastr, DRMS_SlotKeyUnit_t *unit, double *delta)
{
   char *end, *p = *deltastr;
   double dval;
   char *unitstr = NULL;

   dval = (int)strtod(p,&end);
   if ( (IsZero(dval) && end==p)  || 
	((IsPosHugeVal(dval) || IsNegHugeVal(dval)) && errno==ERANGE))
   {
      fprintf(stderr,"Syntax Error: Expected finite floating point value at "
	      "beginning of time duration, found '%s'.\n", p);
      ++syntax_error;      
      goto error;
   }
   else
     p = end;

   unitstr = p;

   if (strncasecmp(unitstr, "d", 1) == 0)
   {
      *delta = dval;
      if (unit)
      {
         *unit = kSlotKeyUnit_Degrees;
      }
      p++;
   }
   else if (strncasecmp(unitstr, "m", 1) == 0)
   {
      *delta = 60.0 * dval;
      if (unit)
      {
         *unit = kSlotKeyUnit_Arcminutes;
      }
      p++;
   }
   else if (strncasecmp(unitstr, "s", 1) == 0)
   {
      *delta = 3600.0 * dval;
      *unit = kSlotKeyUnit_Arcseconds;
      p++;
   }
   else if (strncasecmp(unitstr, "ms", 2) == 0)
   {
      *delta = 3600000.0 * dval;
      *unit = kSlotKeyUnit_MAS;
      p++;
      p++;
   }
   else if (strncasecmp(unitstr, "r", 1) == 0)
   {
      *delta =  ((M_PI) / 648000) * dval;
      if (unit)
      {
         *unit = kSlotKeyUnit_Radians;
      }
      p++;
   }
   else if (strncasecmp(unitstr, "ur", 2) == 0)
   {
      *delta =  ((M_PI) / 648000) * 1000000.0 * dval;
      if (unit)
      {
         *unit = kSlotKeyUnit_MicroRadians;
      }
      p++;
      p++;
   }
   else
   {
      fprintf(stderr,"Syntax Error: Degree delta unit must be one of 'd', "
	      "'m', 's', 'ms', 'r', 'ur', found '%s'.\n", unitstr);
      ++syntax_error;      
      goto error;
   }

   *deltastr = p;
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
#ifdef DEBUG
  printf("Enter sql_record_set_filter\n");
#endif

  /* If there is a [#^] or [#$] in the filter, AND it is not the first 
   * record list, then a different query needs to be done.  If the 
   * filter is [165]['twiggy'], then a where clause of "id = 165 AND 
   * model = 'twiggy' suffices.  But if the filter is [165][#^], then
   * the where clause should be 
   * "id = 165 AND model = (select min(model) from <seriesname> where id = 165)".
   *
   * So, instead of sequentially writing the query to *query, it is
   * probably better to figure out what the entire where clause is, 
   * and then concatenate it to query.
   */

  char whereclz[DRMS_MAXQUERYLEN] = {0};
  char wherebuf[DRMS_MAXQUERYLEN];
  char *bogus = NULL;

  do {
     memset(wherebuf, sizeof(wherebuf), 0);
     bogus = wherebuf;

     /* opening parenthesis around single var=xxx */
     snprintf(wherebuf, sizeof(wherebuf), "( ");
     bogus +=2;

    switch(rs->type)
    {
    case RECORDQUERY:
      sql_record_query(rs->record_query, &bogus);
      break;
    case RECORDLIST:
      sql_record_list(rs->record_list, seriesname, &bogus);
      break;
    default:    
      fprintf(stderr,"Wrong type (%d) in sql_record_set_filter.\n",
	      rs->type);
      return 1;
    }

    /* If rs->type == FIRST_VALUE or rs->type == LAST_VALUE, then 
     * wherebuf is of the format (xxx=(select max(xxx) from series)), 
     * and the existing whereclz must be embedded like this:
     * yyy=max(yyy) AND (xxx=(select max(xxx) from series WHERE (whereclz)))
     */
    if (*whereclz && 
        strlen(wherebuf) && 
        rs->type == RECORDLIST && 
        rs->record_list->type == PRIMEKEYSET && 
        ((rs->record_list->primekey_rangeset->type == INDEX_RANGE &&        
          (rs->record_list->primekey_rangeset->index_rangeset->type == FIRST_VALUE || 
           rs->record_list->primekey_rangeset->index_rangeset->type == LAST_VALUE)) ||
         (rs->record_list->primekey_rangeset->type == VALUE_RANGE &&
          (rs->record_list->primekey_rangeset->value_rangeset->type == FIRST_VALUE || 
           rs->record_list->primekey_rangeset->value_rangeset->type == LAST_VALUE)
          )))
    {
       /* working backward, find first non-')', non-space */
       char *pin = &(wherebuf[strlen(wherebuf) - 1]);
       char savebuf[DRMS_MAXQUERYLEN] = {0};

       while (*pin == ')' || *pin == ' ')
       {
          pin--;
       }

       pin++;
       snprintf(savebuf, sizeof(savebuf), "%s", pin);
       *pin = '\0';

       base_strlcat(pin, " WHERE ( ", sizeof(wherebuf) - (pin - wherebuf));
       
       /* embed existing whereclz */
       base_strlcat(pin, whereclz, sizeof(wherebuf) - (pin - wherebuf));
       
       base_strlcat(pin, " )", sizeof(wherebuf) - (pin - wherebuf));

       /* now concatenate savebuf back onto wherebuf */
       base_strlcat(wherebuf, savebuf, sizeof(wherebuf));

       /* closing parenthesis */
       base_strlcat(wherebuf, " )", sizeof(wherebuf));

       /* put back into whereclz */
       base_strlcat(whereclz, " AND ", sizeof(whereclz));
       base_strlcat(whereclz, wherebuf, sizeof(whereclz));
    }
    else
    {
       /* closing parenthesis around single var=xxx */
       base_strlcat(wherebuf, " )", sizeof(wherebuf));

       if (*whereclz)
       {
          /* simple AND of record lists */
          base_strlcat(whereclz, " AND ", sizeof(whereclz));
       }

       base_strlcat(whereclz, wherebuf, sizeof(whereclz));
    }

    rs = rs->next;
  } while (rs);
#ifdef DEBUG
  printf("Added '%s'\nExit sql_record_set_filter\n",*query);
#endif

  /* whereclz contains final where clause - concatenate onto query */
  /* DON'T know the size of query - ack - just strcat (very dangerous!!) */
  sprintf(*query, whereclz);
  *query += strlen(whereclz);

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
  DRMS_Type_Value_t base, step, tmpval;
  double dbase;
  double dstep;
  int drmsstat = DRMS_SUCCESS;

#ifdef DEBUG
  printf("Enter sql_primekey_index_set\n");
#endif

  /* If the original query involved a keyword with an associated index keyword
   * (like a slotted keyword), then 'keyword' is the index keyword.*/
  datatype = keyword->info->type;

  /* Get optional base and step keyword values. If keyword is a slotted key, then 
   * 'keyword' IS the associated index keyword (*_index), not the slotted keyword.
   */
  if (drms_keyword_isindex(keyword))
  {
     /* If 'keyword' is associated with a slotted keyword, then base and step
      * have already been applied when deriving the values for 'keyword' - 
      * a function has applied both values to the original, slotted keyword
      * values.
      */
     dbase = 0;
     dstep = 1;
  }
  else
  {
     /* Must get yyy_base and yyy_step from yyy - 'keyword' is the  */
     dbase = drms_keyword_getvalkeybase(keyword, &drmsstat);
     dstep = drms_keyword_getvalkeystep(keyword, &drmsstat);
  }

  if (drms_ismissing_double(dbase))
  {
     dbase = 0.0;
  }

  base.string_val = NULL;
  tmpval.double_val = dbase;
  drms_convert(datatype, &base, DRMS_TYPE_DOUBLE, &tmpval);

  if (drms_ismissing_double(dstep))
  {
     dstep = 1.0;
  }

  step.string_val = NULL;
  tmpval.double_val = dstep;
  drms_convert(datatype, &step, DRMS_TYPE_DOUBLE, &tmpval);

  do {
    p += sprintf(p,"( ");
    /* FIRST_VALUE/LAST_VALUE in index sets mean to find the record with the 
     * keyword with smallest/largest value. */
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
  DRMS_Type_t datatype;

#ifdef DEBUG
  printf("Enter sql_primekey_value_set\n");
#endif

  datatype = keyword->info->type;

  /* If the original query involved a keyword with an associated index keyword
   * (like a slotted keyword), then keyword is the index keyword.*/
  do {
    p += sprintf(p,"( ");

    /* FIRST_VALUE/LAST_VALUE in value sets mean to find the record with the 
     * associated INDEX keyword with smallest/largest value. But since 'keyword'
     * is already the associated index keyword (if one exists), you cannot
     * search for the record with smallest/largest value of the value keyword
     * (the keyword associated with the index keyword). You are forced to search
     * on the index keyword. */
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
      {
         double dskip = drms2double(datatype, &rs->skip, NULL);

         /* rs->type == START_END || rs->type == START_DURATION */
         if (datatype == DRMS_TYPE_CHAR || datatype == DRMS_TYPE_SHORT ||
             datatype == DRMS_TYPE_INT || datatype == DRMS_TYPE_LONGLONG)
         {
            long long llstart = drms2longlong(datatype, &rs->start, NULL);;
            p += sprintf(p, " AND (cast((%s - %lld)", keyword->info->name, llstart);
            p += sprintf(p, " as integer) %% ");
            p += drms_sprintfval(p, datatype, &rs->skip, 1);
            p += sprintf(p, ") = 0");
         }
         else
         {
            /* select records where <value> - rs->start is a multiple of rs->skip */
            double dstart = drms2double(datatype, &rs->start, NULL);
            double tolerance = 1.0e-11 * 2 * dstart;

            p += sprintf(p, 
                         " AND (abs(((%s - %g) / %g) - (round((%s - %g) / %g))) < %g)",
                         keyword->info->name, 
                         dstart,
                         dskip,
                         keyword->info->name, 
                         dstart,
                         dskip,
                         tolerance);
         }
      }
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
int drms_recordset_query(DRMS_Env_t *env, const char *recordsetname, 
			 char **query, char **seriesname, int *filter, int *mixed,
                         int *allvers)
{
  RecordSet_t *rs;
  char *rsn = strdup(recordsetname);
  char *p = rsn;
  int ret = 0;
  RecordSet_Filter_t *filt = NULL;
  int pkeyqfound = 0;
  int npkeyqfound = 0;

  *mixed = 0;

  if ((rs = parse_record_set(env,&p)))
  {
     /* Aha! This isn't the correct logic to detect a mixed case.
      * You need to traverse all nodes in the list rs->recordset_spec,
      * and if you see both a non-NULL record_list and a non-NULL record_query,
      * then you have a mixed query.
      * if (rs->recordset_spec && rs->recordset_spec->record_query) {
      *   *mixed = 1;
      * }
      */

     filt = rs->recordset_spec;

     /* Traverse linked-list, looking for both record_list and record_query. */
     while (filt != NULL)
     {
        if (filt->record_list)
        {
           pkeyqfound = 1;
        }
        else if (filt->record_query)
        {
           npkeyqfound = 1;
        }

        if (pkeyqfound && npkeyqfound)
        {
           *mixed = 1;
           break;
        }

        filt = filt->next;
     }

    XASSERT(*query = malloc(DRMS_MAXQUERYLEN));
    *seriesname = strdup(rs->seriesname);
    *filter = !recnum_filter;

    if (allvers)
    {
       *allvers = rs->allvers;
    }

    sql_record_set(rs,*seriesname, *query);
    free_record_set(rs);
    ret = 0;
  }
  else
    ret =  1;

  if (rsn)
  {
     free(rsn);
  }

  return ret;
}
