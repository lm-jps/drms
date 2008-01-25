//#define DEBUG 
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"

/* Slotted keywords - index keyword type (some kind of integer) */
const DRMS_Type_t kIndexKWType = DRMS_TYPE_INT;
const char *kIndexKWFormat = "%d";

struct RecScopeStrings_struct
{
  DRMS_RecScopeType_t type;
  const char *str;
};

typedef struct RecScopeStrings_struct RecScopeStrings_t;

struct SlotKeyUnitStrings_struct
{
  DRMS_SlotKeyUnit_t type;
  const char *str;
};

typedef struct SlotKeyUnitStrings_struct SlotKeyUnitStrings_t;

static RecScopeStrings_t gSS[] =
{
   {kRecScopeType_Variable, "variable"},
   {kRecScopeType_Constant, "constant"},
   {kRecScopeType_Index, "index"},
   {kRecScopeType_TS_EQ, "ts_eq"},
   {kRecScopeType_SLOT, "slot"},
   {kRecScopeType_ENUM, "enum"},
   {kRecScopeType_CARR, "carr"},
   {(DRMS_RecScopeType_t)-99, ""}
};

static SlotKeyUnitStrings_t gSUS[] =
{
   {kSlotKeyUnit_TSeconds, "tenthsecs"},
   {kSlotKeyUnit_Seconds, "secs"},
   {kSlotKeyUnit_Minutes, "mins"},
   {kSlotKeyUnit_Days, "days"},
   {(DRMS_SlotKeyUnit_t)-99, ""}
};

static HContainer_t *gRecScopeStrHC = NULL;
static HContainer_t *gRecScopeHC = NULL;
static HContainer_t *gSlotUnitHC = NULL;

const int kMaxRecScopeTypeKey = 4096;
const int kMaxSlotUnitKey = 128;

void drms_keyword_term()
{
   if (gRecScopeStrHC)
   {
      hcon_destroy(&gRecScopeStrHC);
   }
   if (gRecScopeHC)
   {
      hcon_destroy(&gRecScopeHC);
   }
   if (gSlotUnitHC)
   {
      hcon_destroy(&gSlotUnitHC);
   }
}

/* Prototypes for static functions. */
static DRMS_Keyword_t * __drms_keyword_lookup(DRMS_Record_t *rec, 
					      const char *key, int depth);


void drms_free_template_keyword_struct(DRMS_Keyword_t *key)
{
  if (key->info->type==DRMS_TYPE_STRING)
    free(key->value.string_val);

  free(key->info);
}

void drms_free_keyword_struct(DRMS_Keyword_t *key)
{
  if (key->info->type==DRMS_TYPE_STRING)
    free(key->value.string_val);

  return;
}


void drms_copy_keyword_struct(DRMS_Keyword_t *dst, DRMS_Keyword_t *src)
{

  /* If the new value is a variable length string, allocate space 
     for and copy it. */
  if (dst->info==NULL)
    dst->info = src->info;
  if (src->info->type == DRMS_TYPE_STRING)
  {
    if (dst->info->type == DRMS_TYPE_STRING)
      free(dst->value.string_val);
    /* Copy main struct. */
    memcpy(dst, src, sizeof(DRMS_Keyword_t));
    dst->value.string_val = strdup(src->value.string_val);
  }
  else
  {
    /* Copy main struct. */
    memcpy(dst, src, sizeof(DRMS_Keyword_t));
  }
}

/* target must have no existing links, since this function fills in the link container */
HContainer_t *drms_create_keyword_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status)
{
   HContainer_t *ret = NULL;
   DRMS_Keyword_t *tKey = NULL;
   DRMS_Keyword_t *sKey = NULL;

   XASSERT(target != NULL && target->keywords.num_total == 0 && source != NULL);

   if (target != NULL && target->keywords.num_total == 0 && source != NULL)
   {
      *status = DRMS_SUCCESS;
      HIterator_t *hit = hiter_create(&(source->keywords));
      XASSERT(hit);
      
      while (hit && ((sKey = hiter_getnext(hit)) != NULL))
      {
	 if (sKey->info && strlen(sKey->info->name) > 0)
	 {
	    XASSERT(tKey = hcon_allocslot_lower(&(target->keywords), sKey->info->name));
	    memset(tKey, 0, sizeof(DRMS_Keyword_t));
	    XASSERT(tKey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
	    memset(tKey->info, 0, sizeof(DRMS_KeywordInfo_t));
	    
	    if (tKey && tKey->info)
	    {
	       /* record */
	       tKey->record = target;

	       /* keyword info */
	       memcpy(tKey->info, sKey->info, sizeof(DRMS_KeywordInfo_t));

	       if (tKey->info->type == DRMS_TYPE_STRING &&
		   sKey->value.string_val != NULL)
	       {
		  copy_string(&(tKey->value.string_val), sKey->value.string_val);
	       }
	       else
	       {
		  tKey->value = sKey->value;
	       }	       
	    }
	    else
	    {
	       *status = DRMS_ERROR_OUTOFMEMORY;
	    }
	 }
	 else
	 {
	    *status = DRMS_ERROR_INVALIDKEYWORD;
	 }
      }
      
      if (hit)
      {
	 hiter_destroy(&hit);
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 ret = &(target->keywords);
      }
   }
   else
   {
      *status = DRMS_ERROR_INVALIDRECORD;
   }

   return ret;
}

/* Print the fields of a keyword struct to stdout. */
void drms_keyword_print(DRMS_Keyword_t *key)
{
  const int fieldwidth=13;

  printf("\t%-*s:\t'%s'\n", fieldwidth, "name", key->info->name);
  printf("\t%-*s:\t%d\n", fieldwidth, "islink", key->info->islink);
  if (key->info->islink)
  {
    printf("\t%-*s:\t%s\n", fieldwidth, "linkname", key->info->linkname);
    printf("\t%-*s:\t%s\n", fieldwidth, "target keyword", key->info->target_key);
    printf("\t%-*s:\t'%s'\n", fieldwidth, "description", key->info->description);
  }
  else
  {
    printf("\t%-*s:\t'%s'\n", fieldwidth, "type", drms_type2str(key->info->type));
    printf("\t%-*s:\t'%s'\n", fieldwidth, "format", key->info->format);
    printf("\t%-*s:\t'%s'\n", fieldwidth, "unit", key->info->unit);
    printf("\t%-*s:\t'%s'\n", fieldwidth, "description", key->info->description);
    printf("\t%-*s:\t%d\n", fieldwidth, "record_scope", (int)key->info->recscope);
    printf("\t%-*s:\t%d\n", fieldwidth, "per_segment", key->info->per_segment);
    printf("\t%-*s:\t%d\n", fieldwidth, "isprime", (int)key->info->isdrmsprime);
    printf("\t%-*s:\t",fieldwidth,"value");
  
    drms_keyword_printval(key);
  }
  
  printf("\n");
}


/* Print the fields of a keyword struct to stdout. */
void drms_keyword_printval(DRMS_Keyword_t *key)
{
  drms_keyword_fprintval(stdout, key);
}


/* Print the fields of a keyword struct to a file. */
void drms_keyword_fprintval(FILE *keyfile, DRMS_Keyword_t *key)
{
  switch(key->info->type)
  {
  case DRMS_TYPE_CHAR:
    fprintf(keyfile, key->info->format, key->value.char_val);
    break;
  case DRMS_TYPE_SHORT:
    fprintf(keyfile, key->info->format, key->value.short_val);
    break;
  case DRMS_TYPE_INT:
    fprintf(keyfile, key->info->format, key->value.int_val);
    break;
  case DRMS_TYPE_LONGLONG:
    fprintf(keyfile, key->info->format, key->value.longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    fprintf(keyfile, key->info->format, key->value.float_val);
    break;
  case DRMS_TYPE_DOUBLE:
    fprintf(keyfile, key->info->format, key->value.double_val);
    break;
  case DRMS_TYPE_TIME:
    {
      char buf[1024];
      sprint_time(buf, key->value.time_val, key->info->format, 0);
      // sprint_time(buf, key->value.time_val, "UT", 0);
      fprintf(keyfile,"%s",buf);
    }
    break;
  case DRMS_TYPE_STRING:
    fprintf(keyfile, key->info->format, key->value.string_val);
    break;
  default:
    break;
  }
}

  
/* 
   Build the keyword part of a dataset template by
   using the query result holding a list of 
     (keywordname, linkname, targetkeyw, type, 
      defaultval, format, unit, 
      isconstant, persegment, description)
   tuples to initialize the array of keyword descriptors.
*/
int  drms_template_keywords(DRMS_Record_t *template)
{
  DRMS_Env_t *env;
  int i,num_segments, per_segment, seg, status;
  char name0[DRMS_MAXNAMELEN], name[DRMS_MAXNAMELEN];
  char defval[DRMS_DEFVAL_MAXLEN], query[DRMS_MAXQUERYLEN];
  DRMS_Keyword_t *key;
  DB_Binary_Result_t *qres;

  env = template->env;
  /* Initialize container structure. */
  hcon_init(&template->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_keyword_struct, 
	    (void (*)(const void *, const void *)) drms_copy_keyword_struct);

  /* Get keyword definitions and add to template. */
  char *namespace = ns(template->seriesinfo->seriesname);
  sprintf(query, "select keywordname, islink, linkname, targetkeyw, type, "
	  "defaultval, format, unit, isconstant, persegment, "
	  "description from %s.%s where seriesname ~~* '%s' order by keywordname",
	  namespace, DRMS_MASTER_KEYWORD_TABLE, template->seriesinfo->seriesname);
  free(namespace);
  if ((qres = drms_query_bin(env->session, query)) == NULL)
    return 1;  /* non-existent ID or SQL error. */

  if (qres->num_rows>0 && qres->num_cols != 11 )
  {
    status = DRMS_ERROR_BADFIELDCOUNT;
    goto bailout;
  }
  
  num_segments = hcon_size(&template->segments);
  for (i = 0; i<(int)qres->num_rows; i++)
  {
    per_segment  = db_binary_field_getint(qres, i, 9);
    for (seg=0; seg<(per_segment==1?num_segments:1); seg++)
    {
      /* Construct name, possibly by appending segment selector. */
      db_binary_field_getstr(qres, i, 0, sizeof(name0), name0);
      if (per_segment)
	sprintf(name,"%s_%03d",name0,seg);
      else
	strcpy(name,name0);

      
      /* Allocate space for new structure in hashed container. */
      key = hcon_allocslot_lower(&template->keywords, name);
      memset(key,0,sizeof(DRMS_Keyword_t));      
      /* Set parent pointer. */
      key->record = template;
      XASSERT(key->info = malloc(sizeof(DRMS_KeywordInfo_t)));
      memset(key->info,0,sizeof(DRMS_KeywordInfo_t));
      /* Copy field values from query result. */
      strcpy(key->info->name, name);

      
      key->info->islink = db_binary_field_getint(qres, i, 1);
      db_binary_field_getstr(qres, i, 10, sizeof(key->info->description),key->info->description);
      if (!key->info->islink)
      {
	key->info->linkname[0] = 0;
	key->info->target_key[0] = 0;
	db_binary_field_getstr(qres, i, 4, sizeof(name),name);
	key->info->type         = drms_str2type(name);
	db_binary_field_getstr(qres, i, 5, sizeof(defval), defval);
	drms_strval(key->info->type, &key->value, defval);
	db_binary_field_getstr(qres, i, 6, sizeof(key->info->format),  key->info->format);
	db_binary_field_getstr(qres, i, 7, sizeof(key->info->unit), key->info->unit);
	key->info->recscope = (DRMS_RecScopeType_t)db_binary_field_getint(qres, i, 8);
	key->info->per_segment  = db_binary_field_getint(qres, i, 9);
	key->info->isdrmsprime = 0;
      }
      else
      {
	db_binary_field_getstr(qres, i, 2, sizeof(key->info->linkname),key->info->linkname);
	db_binary_field_getstr(qres, i, 3, sizeof(key->info->target_key),key->info->target_key);
	key->info->type = DRMS_TYPE_INT;
	key->value.int_val = 0;
	key->info->format[0] = 0;
	key->info->unit[0] = 0;
	key->info->recscope = kRecScopeType_Variable;
	key->info->per_segment = 0;
	key->info->isdrmsprime = 0;
      }
	
#ifdef DEBUG
      printf("Keyword[%4d] name    = '%s'\n",i,key->info->name);
      printf("Keyword[%4d] type    = '%d'\n",i,key->info->type);
      printf("Keyword[%4d] format  = '%s'\n",i,key->info->format);
      printf("Keyword[%4d] unit    = '%s'\n",i,key->info->unit);
      printf("Keyword[%4d] description = '%s'\n",i,key->info->description);
#endif
    }
  }
  db_free_binary_result(qres);
  return DRMS_SUCCESS;

 bailout:
  db_free_binary_result(qres);
  return status;
}


/* Wrapper for __drms_keyword_lookup without the recursion depth counter. */
DRMS_Keyword_t *drms_keyword_lookup(DRMS_Record_t *rec, const char *key, int followlink)
{
  char *lb,*rb;
  char tmp[DRMS_MAXNAMELEN+5]={0};
  int segnum;
  /* Handle array syntax for per-segment keywords. */

  strcpy(tmp,key);
  if ((lb = index(tmp,'[')))
  {
    rb = lb+1;
    for (rb=lb+1; *rb && *rb!=']'; rb++)
    {
      if (!isdigit(*rb))
	break;
    }
    if (*rb!=']' || (*rb==']' && *(rb+1)!=0))
      return NULL;
    *rb = 0;
    segnum = atoi(lb+1);
    sprintf(lb,"_%03d",segnum);
#ifdef DEBUG
    printf("Mangled keyword name = '%s'.\n",tmp);
#endif
    if (strlen(tmp) >= DRMS_MAXNAMELEN)
      fprintf(stderr,"WARNING keyword name too long, %s\n",tmp);
  }    
  if (!followlink) 
    return hcon_lookup_lower(&rec->keywords, tmp);
  return __drms_keyword_lookup(rec, tmp, 0);
}  

/* 
   Recursive keyword lookup that follows linked keywords to 
   their destination until a non-link keyword is reached. If the 
   recursion depth exceeds DRMS_MAXLINKDEPTH it is assumed that there 
   is an erroneous link cycle, an error message is written to stderr,
   and a NULL pointer is returned.
*/
static DRMS_Keyword_t * __drms_keyword_lookup(DRMS_Record_t *rec, 
					      const char *key, int depth)
{
  int stat;
  DRMS_Keyword_t *keyword;

  keyword = hcon_lookup_lower(&rec->keywords, key);
  if (keyword!=NULL && depth<DRMS_MAXLINKDEPTH )
  {
    if (keyword->info->islink)
    {
      rec = drms_link_follow(rec, keyword->info->linkname, &stat);
      if (stat)
	return NULL;
      else
	return __drms_keyword_lookup(rec, keyword->info->target_key, depth+1);
    }
    else
    {
      return keyword;
    }
  }
  if (depth>=DRMS_MAXLINKDEPTH)
    fprintf(stderr, "WARNING: Max link depth exceeded for keyword '%s' in "
	    "record %lld from series '%s'\n",keyword->info->name, rec->recnum, 
	    rec->seriesinfo->seriesname);
  
  return NULL;
}

DRMS_Type_t drms_keyword_type(DRMS_Keyword_t *key)
{
     return key->info->type;
}

HContainer_t *drms_keyword_createinfocon(DRMS_Env_t *drmsEnv, 
					  const char *seriesName, 
					  int *status)
{
     HContainer_t *ret = NULL;

     DRMS_Record_t *template = drms_template_record(drmsEnv, seriesName, status);
     
     if (*status == DRMS_SUCCESS)
     {
	  int size = hcon_size(&(template->keywords));
	  if (size > 0)
	  {
	       char **nameArr = (char **)malloc(sizeof(char *) * size);
	       DRMS_KeywordInfo_t **valArr = 
		 (DRMS_KeywordInfo_t **)malloc(sizeof(DRMS_KeywordInfo_t *) * size);

	       if (nameArr != NULL && valArr != NULL)
	       {
		    HIterator_t hit;
		    hiter_new(&hit, &(template->keywords));
		    DRMS_Keyword_t *kw = NULL;

		    int iKW = 0;
		    while ((kw = hiter_getnext(&hit)) != NULL)
		    {
			 nameArr[iKW] = kw->info->name;
			 valArr[iKW] = kw->info;

			 iKW++;
		    }

		    ret = hcon_create(sizeof(DRMS_KeywordInfo_t), 
				      DRMS_MAXNAMELEN,
				      NULL,
				      NULL,
				      (void **)valArr,
				      nameArr,
				      size);
	       }
	       else
	       {
		    *status = DRMS_ERROR_OUTOFMEMORY;
	       }

	       if (nameArr != NULL)
	       {
		    free(nameArr);
	       }

	       if (valArr != NULL)
	       {
		    free(valArr);
	       }
	  }
     }

     return ret;
}

void drms_keyword_destroyinfocon(HContainer_t **info)
{
   hcon_destroy(info);
}

int drms_keyword_keysmatch(DRMS_Keyword_t *k1, DRMS_Keyword_t *k2)
{
   int ret = 0;

   DRMS_KeywordInfo_t *key1 = k1->info;
   DRMS_KeywordInfo_t *key2 = k2->info;

   char exp1[DRMS_MAXNAMELEN];
   char exp2[DRMS_MAXNAMELEN];
   int exp1Valid = drms_keyword_getextname(k1, exp1, sizeof(exp1));
   int exp2Valid = drms_keyword_getextname(k2, exp2, sizeof(exp2));

   if (exp1Valid && exp2Valid)
   {
      /* Default keyword value lives in DRMS_Keyword_t.value. */
      ret = (strcmp(key1->name, key2->name) == 0 &&
	     key1->islink == key2->islink &&
	     strcmp(key1->linkname, key2->linkname) == 0 &&
	     strcmp(key1->target_key, key2->target_key) == 0 &&
	     key1->type == key2->type &&
	     drms_equal(key1->type, &(k1->value), &(k2->value)) &&
	     strcmp(key1->format, key2->format) == 0 &&
	     strcmp(key1->unit, key2->unit) == 0 &&
	     strcmp(key1->description, key2->description) == 0 &&
	     key1->recscope == key2->recscope &&
	     key1->per_segment == key2->per_segment &&
	     key1->isdrmsprime == key2->isdrmsprime);

  
      ret = (ret && (strcmp(exp1, exp2) == 0));
   }

   return ret;
}

/***************** getkey_<type> family of functions **************/


/* Slightly less ugly pieces of crap that should be used instead: */
char drms_getkey_char(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  char result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword != NULL )
  {  
    result = drms2char(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    result = DRMS_MISSING_CHAR;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}


short drms_getkey_short(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  short result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword!=NULL )
  {
    result = drms2short(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    result =  DRMS_MISSING_SHORT;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}


int drms_getkey_int(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  int result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword!=NULL )
  {  
    result = drms2int(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    result =  DRMS_MISSING_INT;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}


long long drms_getkey_longlong(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  long long result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword!=NULL )
  {  
    result = drms2longlong(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    result = DRMS_MISSING_LONGLONG;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}


float drms_getkey_float(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  float result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword != NULL )
  {  
    result = drms2float(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    result = DRMS_MISSING_FLOAT;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}



double drms_getkey_double(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  double result;

  keyword = drms_keyword_lookup(rec, key, 1);
  if ( keyword!=NULL )
  {  
    result = drms2double(keyword->info->type, &keyword->value, &stat);
  }
  else 
  {
    result = DRMS_MISSING_DOUBLE;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;
  return result;  
}

char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  char *result=NULL;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword!=NULL )
  {   
    result = drms2string(keyword->info->type, &keyword->value, &stat);
  }
  else
  {
    stat = DRMS_ERROR_UNKNOWNKEYWORD;
    copy_string(&result, DRMS_MISSING_STRING);
  }
  if (status)
    *status = stat;
  return result;  
}

DRMS_Type_Value_t drms_getkey(DRMS_Record_t *rec, const char *key, 
			      DRMS_Type_t *type, int *status)
{
  DRMS_Type_Value_t value;
  DRMS_Keyword_t *keyword;
  int stat;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword != NULL )
  {
    *type = keyword->info->type;
    drms_copy_drms2drms(keyword->info->type, &value, &keyword->value);
    stat = DRMS_SUCCESS;
  }
  else
  {
    *type = DRMS_TYPE_DOUBLE; /* return a double NAN - kind of arbitrary,
				 but we have to return something... */
    value.double_val = DRMS_MISSING_DOUBLE;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;

  return value;
}  

/* returned DRMS_Value_t owns any string it may contain */
DRMS_Value_t drms_getkey_p(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Type_Value_t value;
  DRMS_Value_t retval;
  DRMS_Keyword_t *keyword;
  int stat;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword != NULL )
  {
    retval.type = keyword->info->type;
    drms_copy_drms2drms(keyword->info->type, &value, &keyword->value);
    stat = DRMS_SUCCESS;
  }
  else
  {
    retval.type = DRMS_TYPE_DOUBLE; /* return a double NAN - kind of arbitrary,
				       but we have to return something... */
    value.double_val = DRMS_MISSING_DOUBLE;
    stat = DRMS_ERROR_UNKNOWNKEYWORD;  
  }
  if (status)
    *status = stat;

  retval.value = value;

  return retval;
}  

/***************** setkey_<type> family of functions **************/
int drms_setkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t type, 
		DRMS_Type_Value_t *value)
{
  DRMS_Keyword_t *keyword;

  if (rec ->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else
    {
      //      rec->key_dirty = 1;
      return drms_convert(keyword->info->type, &keyword->value, type, value);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}  

/* This is a more generic version of drms_setkey() - it assumes that the source type
 * is equal to the target type. */
int drms_setkey_p(DRMS_Record_t *rec, const char *key, DRMS_Value_t *value)
{
  DRMS_Keyword_t *keyword;

  if (rec ->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else
    {
      //      rec->key_dirty = 1;
      return drms_convert(keyword->info->type, &keyword->value, value->type, &(value->value));
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}  


/* Slightly less ugly pieces of crap that should be used instead: */
int drms_setkey_char(DRMS_Record_t *rec, const char *key, char value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      //    rec->key_dirty = 1;
      val.char_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_CHAR, &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
  
  
}

int drms_setkey_short(DRMS_Record_t *rec, const char *key, short value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      val.short_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_SHORT, &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}

int drms_setkey_int(DRMS_Record_t *rec, const char *key, int value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      //    rec->key_dirty = 1;
      val.int_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_INT, &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}

int drms_setkey_longlong(DRMS_Record_t *rec, const char *key, long long value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      val.longlong_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_LONGLONG, 
			  &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}

int drms_setkey_float(DRMS_Record_t *rec, const char *key, float value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      //    rec->key_dirty = 1;
      val.float_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_FLOAT, &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}

int drms_setkey_double(DRMS_Record_t *rec, const char *key, double value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;

  if (rec->readonly)
    return DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      return DRMS_ERROR_KEYWORDREADONLY;
    else {
      //    rec->key_dirty = 1;
      val.double_val = value;
      return drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_DOUBLE, &val);
    }
  }
  else
    return DRMS_ERROR_UNKNOWNKEYWORD;  
}

int drms_setkey_string(DRMS_Record_t *rec, const char *key, const char *value)
{
  DRMS_Type_Value_t val;
  DRMS_Keyword_t *keyword;
  int ret;

  if (rec->readonly)
    ret = DRMS_ERROR_RECORDREADONLY;

  keyword = drms_keyword_lookup(rec, key, 0);
  if (keyword != NULL )
  {
    if (keyword->info->islink)
      ret = DRMS_ERROR_KEYWORDREADONLY;
    else {
      //    rec->key_dirty = 1;
      val.string_val = strdup(value);
      ret = drms_convert(keyword->info->type, &keyword->value, DRMS_TYPE_STRING, &val);
      free(val.string_val);
      val.string_val = NULL;
    }
  }
  else
    ret = DRMS_ERROR_UNKNOWNKEYWORD;  

  return ret;
}

const char *drms_keyword_getname(DRMS_Keyword_t *key)
{
   return key->info->name;
}

int drms_keyword_getintname(const char *keyname, char *nameOut, int size)
{
   int success = 0;
   char *potential = NULL;

   *nameOut = '\0';

   /* 1 - Try FITS name. */
   if (IsValidDRMSKeyName(keyname))
   {
      strcpy(nameOut, keyname);
      success = 1;
   }

   /* 2 - Use default rule. */
   if (!success)
   {
      potential = (char *)malloc(sizeof(char) * size);
      if (potential)
      {
	 if (GenerateDRMSKeyName(keyname, potential, size))
	 {
	    strcpy(nameOut, potential);
	    success = 1;
	 }

	 free(potential);
      }
   }

   return success;
}

int drms_keyword_getintname_ext(const char *keyname, 
				DRMS_KeyMapClass_t *classid, 
				DRMS_KeyMap_t *map,
				char *nameOut, 
				int size)
{
   int success = 0;
   const char *potential = NULL;
   *nameOut = '\0';

   /* 1 - Try KeyMap */
   if (map != NULL)
   {
      potential = drms_keymap_intname(map, keyname);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   /* 2 - Try KeyMapClass */
   if (!success && classid != NULL)
   {
      potential = drms_keymap_classidintname(*classid, keyname);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   if (!success)
   {
      /* Now try the map- and class-independent schemes. */
      char buf[DRMS_MAXNAMELEN];
      success = drms_keyword_getintname(keyname, buf, sizeof(buf));
      if (success)
      {
	 strncpy(nameOut, buf, size);
      }
   }
   
   return success;
}

int drms_keyword_getextname(DRMS_Keyword_t *key, char *nameOut,	int size)
{
   int success = 0;
   char *desc = strdup(key->info->description);
   char *pFitsName = NULL;
   char *potential = NULL;
   *nameOut = '\0';

   /* 1 - Try keyword name in description field. */
   if (desc)
   {
      pFitsName = strtok(desc, " ");
      if (pFitsName)
      {
	 int len = strlen(pFitsName);

	 if (len > 2 &&
	     pFitsName[0] == '[' &&
	     pFitsName[len - 1] == ']')
	 {
	    if (len - 2 < size)
	    {
	       potential = (char *)malloc(sizeof(char) * size);
	       if (potential)
	       {
		  memcpy(potential, pFitsName + 1, len - 2);
		  potential[len - 2] = '\0';

		  if (IsValidFitsKeyName(potential))
		  {
		     strcpy(nameOut, potential);
		     success = 1;
		  }

		  free(potential);
	       }
	    }
	 }
      }

      free(desc);
   }
   
   /* 2 - Try DRMS name. */
   if (!success)
   {
      if (IsValidFitsKeyName(key->info->name))
      {
	 strcpy(nameOut, key->info->name);
	 success = 1;
      }
   }

   /* 3 - Use default rule. */
   if (!success)
   {
      potential = (char *)malloc(sizeof(char) * size);
      if (potential)
      {
	 if (GenerateFitsKeyName(key->info->name, potential, size))
	 {
	    strcpy(nameOut, potential);
	    success = 1;
	 }

	 free(potential);
      }
   }

   return success;
}

/* Same as above, but try a KeyMap first, then a KeyMapClass */
int drms_keyword_getextname_ext(DRMS_Keyword_t *key, 
				DRMS_KeyMapClass_t *classid, 
				DRMS_KeyMap_t *map,
				char *nameOut,
				int size)
{
   int success = 0;
   const char *potential = NULL;

   /* 1 - Try KeyMap. */
   if (map != NULL)
   {
      potential = drms_keymap_extname(map, key->info->name);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }

   }

   /* 2 - Try KeyMapClass. */
   if (!success && classid != NULL)
   {
      potential = drms_keymap_classidextname(*classid, key->info->name);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   if (!success)
   {
      /* Now try the map- and class-independent schemes. */
      char buf[DRMS_MAXNAMELEN];
      success = drms_keyword_getextname(key, buf, sizeof(buf));
      if (success)
      {
	 strncpy(nameOut, buf, size);
      }
   }

   return success;
}

int drms_copykey(DRMS_Record_t *target, DRMS_Record_t *source, const char *key)
{
   int status = DRMS_SUCCESS;
   DRMS_Value_t srcval; /* owns contained string */

   srcval = drms_getkey_p(source, key, &status);
   
   if (status == DRMS_SUCCESS)
   {
      status = drms_setkey_p(target, key, &srcval);
   }

   drms_value_free(&srcval);

   return status;
}

int drms_keyword_getsegscope(DRMS_Keyword_t *key)
{
   return key->info->per_segment;
}

DRMS_RecScopeType_t drms_keyword_getrecscope(DRMS_Keyword_t *key)
{
   return key->info->recscope;
}

const char *drms_keyword_getrecscopestr(DRMS_Keyword_t *key, int *status)
{
   int stat = DRMS_SUCCESS;
   const char *ret = NULL;
   const char **pRet = NULL;
   char buf[kMaxRecScopeTypeKey];

   if (!gRecScopeStrHC)
   {
      gRecScopeStrHC = hcon_create(sizeof(const char *), 
			       kMaxRecScopeTypeKey, 
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       0);

      if (gRecScopeStrHC)
      {
	 int i = 0;

	 while (gSS[i].type != -99)
	 {
	    snprintf(buf, sizeof(buf), "%d", (int)gSS[i].type);
	    hcon_insert_lower(gRecScopeStrHC, buf, &(gSS[i].str));
	    i++;
	 }
      }
      else
      {
	 fprintf(stderr, "Error creating record scope type string container.\n");
	 stat = DRMS_ERROR_CANTCREATEHCON;
      }
   }

   if (gRecScopeStrHC)
   {
      snprintf(buf, sizeof(buf), "%d", (int)key->info->recscope);
      pRet = (const char **)hcon_lookup(gRecScopeStrHC, buf);

      if (pRet == NULL)
      {
	 fprintf(stderr, "Invalid record scope type %d.\n", (int)key->info->recscope);
	 stat = DRMS_ERROR_INVALIDRECSCOPETYPE;
      }
      else
      {
	 ret = *pRet;
      }
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

DRMS_RecScopeType_t drms_keyword_str2recscope(const char *str, int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_RecScopeType_t ret = kRecScopeType_Variable;

   if (!gRecScopeHC)
   {
      gRecScopeHC = hcon_create(sizeof(int), 
				kMaxRecScopeTypeKey, 
				NULL,
				NULL,
				NULL,
				NULL,
				0);

      if (gRecScopeHC)
      {
	 int i = 0;

	 while (gSS[i].type != -99)
	 {
	    hcon_insert_lower(gRecScopeHC, gSS[i].str, (int *)(&(gSS[i].type)));
	    i++;
	 }
      }
      else
      {
	 fprintf(stderr, "Error creating record scope type container.\n");
	 stat = DRMS_ERROR_CANTCREATEHCON;
      }
   }

   if (gRecScopeHC)
   {
      int *pVal = (int *)hcon_lookup_lower(gRecScopeHC, str);

      if (pVal == NULL)
      {
	 fprintf(stderr, "Invalid record scope type string %s.\n", str);
	 stat = DRMS_ERROR_INVALIDRECSCOPETYPE;
      }
      else
      {
	 ret = (DRMS_RecScopeType_t)(*pVal);
      }
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

int drms_keyword_isvariable(DRMS_Keyword_t *key)
{
   return (key->info->recscope != kRecScopeType_Constant);
}

int drms_keyword_isconstant(DRMS_Keyword_t *key)
{
   return (key->info->recscope == kRecScopeType_Constant);
}

int drms_keyword_isindex(DRMS_Keyword_t *key)
{
   return (key->info->recscope >= kRecScopeIndex_B &&
	   key->info->recscope < kRecScopeSlotted_B);
}

int drms_keyword_isslotted(DRMS_Keyword_t *key)
{
   return (key->info->recscope >= kRecScopeSlotted_B);
}

int drms_keyword_isprime(DRMS_Keyword_t *key)
{
   return (key->info->isdrmsprime == 1);
}

DRMS_Type_t drms_keyword_gettype(DRMS_Keyword_t *key)
{
   return key->info->type;
}

const DRMS_Type_Value_t *drms_keyword_getvalue(DRMS_Keyword_t *key)
{
   return &(key->value);
}

DRMS_SlotKeyUnit_t drms_keyword_getslotunit(DRMS_Keyword_t *key, int *status)
{
   DRMS_SlotKeyUnit_t ret = kSlotKeyUnit_Invalid;
   char buf[kMaxSlotUnitKey];
   int stat = DRMS_SUCCESS;

   if (key->info->type == DRMS_TYPE_STRING && strstr(key->info->name, kSlotAncKey_Unit))
   {
      if (!gSlotUnitHC)
      {
	 gSlotUnitHC = hcon_create(sizeof(int), 
				   kMaxSlotUnitKey, 
				   NULL,
				   NULL,
				   NULL,
				   NULL,
				   0);

	 if (gSlotUnitHC)
	 {
	    int i = 0;

	    while (gSUS[i].type != -99)
	    {
	       snprintf(buf, sizeof(buf), "%s", (int)gSUS[i].str);
	       hcon_insert_lower(gSlotUnitHC, buf, &(gSUS[i].type));
	       i++;
	    }
	 }
	 else
	 {
	    fprintf(stderr, "Error creating slot unit string container.\n");
	    stat = DRMS_ERROR_CANTCREATEHCON;
	 }
      }

      DRMS_SlotKeyUnit_t *pSU = hcon_lookup_lower(gSlotUnitHC, (key->value).string_val);
      if (pSU)
      {
	 ret = *pSU;
      }
      else
      {
	 fprintf(stderr, "Invalid slot unit string '%s'.\n", (key->value).string_val);
      }
   }
   else
   {
      fprintf(stderr, 
	      "Keyword '%s' does not contain slotted keyword unit information.\n",
	      key->info->name);
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}
