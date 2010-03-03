//#define DEBUG 
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "cfitsio.h"
#include "drms_fitsrw.h"
#include "atoinc.h"

/* Slotted keywords - index keyword type (some kind of integer) */
const DRMS_Type_t kIndexKWType = DRMS_TYPE_LONGLONG;
const char *kIndexKWFormat = "%lld";

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
   {kRecScopeType_TS_SLOT, "ts_slot"},
   {(DRMS_RecScopeType_t)-99, ""}
};

static SlotKeyUnitStrings_t gSUS[] =
{
   {kSlotKeyUnit_TSeconds, "tenthsecs"},
   {kSlotKeyUnit_Seconds, "secs"},
   {kSlotKeyUnit_Minutes, "mins"},
   {kSlotKeyUnit_Hours, "hours"},
   {kSlotKeyUnit_Days, "days"},
   {kSlotKeyUnit_Degrees, "degrees"},
   {kSlotKeyUnit_Arcminutes, "arcmins"},
   {kSlotKeyUnit_Arcseconds, "arcsecs"},
   {kSlotKeyUnit_MAS, "milliarcsecs"},
   {kSlotKeyUnit_Radians, "rads"},
   {kSlotKeyUnit_MicroRadians, "microrads"},
   {(DRMS_SlotKeyUnit_t)-99, ""}
};

char *DRMS_Keyword_ExtType_Strings[] =
{
   "NONE",
   "INTEGER",
   "FLOAT",
   "STRING",
   "LOGICAL"
};

const double kSlotKeyBase_Carr = 0.0;

static HContainer_t *gRecScopeStrHC = NULL;
static HContainer_t *gRecScopeHC = NULL;
static HContainer_t *gSlotUnitHC = NULL;

const int kMaxRecScopeTypeKey = 4096;
const int kMaxSlotUnitKey = 128;

/* Per Tim, FITS doesn't support char, short, long long, or float keyword types. */
static int DRMSKeyValToFITSKeyVal(DRMS_Keyword_t *key, 
                                  char *fitstype, 
                                  void **fitsval)
{
   int err = 0;
   DRMS_Type_Value_t *valin = &key->value;
   void *res = NULL;
   int status = DRMS_SUCCESS;
   DRMS_Keyword_ExtType_t casttype = drms_keyword_getcast(key);

   if (valin && fitstype)
   {
      if (casttype != kDRMS_Keyword_ExtType_None)
      {
         /* cast specified in key's description field */
         if (key->info->type != DRMS_TYPE_RAW)
         {
            switch (casttype)
            {
               case kDRMS_Keyword_ExtType_Integer:
                 res = malloc(sizeof(long long));
                 *(long long *)res = drms2int(key->info->type, valin, &status);
                 *fitstype = kFITSRW_Type_Integer;
                 break;
               case kDRMS_Keyword_ExtType_Float:
                 res = malloc(sizeof(long long));
                 *(double *)res = drms2double(key->info->type, valin, &status);
                 *fitstype = kFITSRW_Type_Float;
                 break;
               case kDRMS_Keyword_ExtType_String:
                 {
                    char tbuf[1024];
                    drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));
                    res = (void *)strdup(tbuf);
                    *fitstype = kFITSRW_Type_String;
                 }
                 break;
               case kDRMS_Keyword_ExtType_Logical:
                 res = malloc(sizeof(long long));

                 if (drms2longlong(key->info->type, valin, &status))
                 {
                    *(long long *)res = 0;
                 }
                 else
                 {
                    *(long long *)res = 1;
                 }
                 *fitstype = kFITSRW_Type_Logical;
                 break;
               default:
                 fprintf(stderr, "Unsupported FITS type '%d'.\n", (int)casttype);
                 err = 1;
                 break;
            }
         }
         else
         {
            /* This shouldn't happen, unless somebody mucked with key->info->description. */
            fprintf(stderr, "DRMS_TYPE_RAW is not supported.\n");
            err = 1;
         }
      }
      else
      {
         /* default conversion */
         switch (key->info->type)
         {
            case DRMS_TYPE_CHAR:
              res = malloc(sizeof(long long)); 
              *(long long *)res = (long long)(valin->char_val);
              *fitstype = 'I';
              break;
            case DRMS_TYPE_SHORT:
              res = malloc(sizeof(long long)); 
              *(long long *)res = (long long)(valin->short_val);
              *fitstype = 'I';
              break;
            case DRMS_TYPE_INT:
              res = malloc(sizeof(long long)); 
              *(long long *)res = (long long)(valin->int_val);
              *fitstype = 'I';
              break;
            case DRMS_TYPE_LONGLONG:
              res = malloc(sizeof(long long)); 
              *(long long *)res = valin->longlong_val;
              *fitstype = 'I';
              break;
            case DRMS_TYPE_FLOAT:
              res = malloc(sizeof(double));
              *(double *)res = (double)(valin->float_val);
              *fitstype = 'F';
              break;
            case DRMS_TYPE_DOUBLE:
              res = malloc(sizeof(double));
              *(double *)res = valin->double_val;
              *fitstype = 'F';
              break;
            case DRMS_TYPE_TIME:
              {
                 char tbuf[1024];
                 drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));
                 res = (void *)strdup(tbuf);
                 *fitstype = 'C';
              }
              break;
            case DRMS_TYPE_STRING:
              res = (void *)strdup(valin->string_val);
              *fitstype = 'C';
              break;
            default:
              fprintf(stderr, "Unsupported DRMS type '%d'.\n", (int)key->info->type);
              err = 1;
              break;
         }
      }
   }
   else
   {
      fprintf(stderr, "DRMSKeyValToFITSKeyVal() - Invalid argument.\n");
      err = 1;
   }

   if (!err)
   {
      *fitsval = res;
   }

   return err;
}

static int FITSKeyValToDRMSKeyVal(CFITSIO_KEYWORD *fitskey, 
                                  DRMS_Type_t *type, 
                                  DRMS_Type_Value_t *value,
                                  DRMS_Keyword_ExtType_t *casttype)
{
   int err = 0;

   if (fitskey && type &&value &&casttype)
   {
      switch (fitskey->key_type)
      {
         case kFITSRW_Type_String:
           {
              /* FITS-file string values will have single quotes and 
               * may have leading or trailing spaces; strip those. */

              char *strval = strdup((fitskey->key_value).vs);
              char *pb = strval;
              char *pe = pb + strlen(pb) - 1;
              
              if (*pb == '\'' && *pe == '\'')
              {
                 *pe = '\0';
                 pe--;
                 pb++;
              }

              while (*pb == ' ')
              {
                 pb++;
              }

              while (*pe == ' ')
              {
                 *pe = '\0';
                 pe--;
              }

              value->string_val = strdup(pb);

              if (strval)
              {
                 free(strval);
              }

              *type = DRMS_TYPE_STRING;
              *casttype = kDRMS_Keyword_ExtType_String;
           }
           break;
         case kFITSRW_Type_Logical:
           /* Arbitrarily choose DRMS_TYPE_CHAR to store FITS logical type */
           if ((fitskey->key_value).vl == 1)
           {
              value->char_val = 1;
           }
           else
           {
              value->char_val = 0;
           }
           *type = DRMS_TYPE_CHAR;
           *casttype = kDRMS_Keyword_ExtType_Logical;
           break;
         case kFITSRW_Type_Integer:
           {
              long long intval = (fitskey->key_value).vi;

              if (intval <= (long long)SCHAR_MAX && 
                  intval >= (long long)SCHAR_MIN)
              {
                 value->char_val = (char)intval;
                 *type = DRMS_TYPE_CHAR;
              }
              else if (intval <= (long long)SHRT_MAX && 
                       intval >= (long long)SHRT_MIN)
              {
                 value->short_val = (short)intval;
                 *type = DRMS_TYPE_SHORT;
              }
              else if (intval <= (long long)INT_MAX && 
                       intval >= (long long)INT_MIN)
              {
                 value->int_val = (int)intval;
                 *type = DRMS_TYPE_INT;
              }
              else
              {
                 value->longlong_val = intval;
                 *type = DRMS_TYPE_LONGLONG;
              }

              *casttype = kDRMS_Keyword_ExtType_Integer;
           }
           break;
         case kFITSRW_Type_Float:
           {
              double floatval = (fitskey->key_value).vf;

              if (floatval <= (double)FLT_MAX &&
                  floatval >= (double)-FLT_MAX)
              {
                 value->float_val = (float)floatval;
                 *type = DRMS_TYPE_FLOAT;
              }
              else
              {
                 value->double_val = floatval;
                 *type = DRMS_TYPE_DOUBLE;
              }

              *casttype = kDRMS_Keyword_ExtType_Float;
           }
           break;
         default:
           fprintf(stderr, "Unsupported FITS type '%c'.\n", fitskey->key_type);
           break;
      }
   }
   else
   {
      fprintf(stderr, "FITSKeyValToDRMSKeyVal() - Invalid argument.\n");
      err = 1;
   }

   return err;
}

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
     /* Make sure you don't free something still being used!! */
     if (dst->info->type == DRMS_TYPE_STRING && dst->value.string_val != src->value.string_val)
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
/*Print the fields of a keyword struct to stdout.*/
void drms_keyword_print(DRMS_Keyword_t *key)
{ 
	drms_keyword_fprint(stdout, key);
}

/* Print the fields of a keyword struct to file "keyfile". */
void drms_keyword_fprint(FILE *keyfile, DRMS_Keyword_t *key)
{
  const int fieldwidth=13;

  fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "name", key->info->name);
  fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "islink", key->info->islink);
  if (key->info->islink)
  {
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "linkname", key->info->linkname);
    fprintf(keyfile, "\t%-*s:\t%s\n", fieldwidth, "target keyword", key->info->target_key);
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "description", key->info->description);
  }
  else
  {
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "type", drms_type2str(key->info->type));
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "format", key->info->format);
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "unit", key->info->unit);
    fprintf(keyfile, "\t%-*s:\t'%s'\n", fieldwidth, "description", key->info->description);
    fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "recordscope", (int)key->info->recscope);
    fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "per_segment", drms_keyword_getperseg(key));
    fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "intprime", drms_keyword_getintprime(key));
    fprintf(keyfile, "\t%-*s:\t%d\n", fieldwidth, "extprime", drms_keyword_getextprime(key));
    fprintf(keyfile, "\t%-*s:\t",fieldwidth,"value");
  
    drms_keyword_fprintval(keyfile, key);
  }
  
  fprintf(keyfile, "\n");
}


/* Print the fields of a keyword struct to stdout. */
void drms_keyword_printval(DRMS_Keyword_t *key)
{
  drms_keyword_fprintval(stdout, key);
}

static void PrintTimeVal(DRMS_Keyword_t *key, char *outbuf, int size)
{
   char buf[1024];
   char *endptr = NULL;
   TIME interval = 0;

   interval = atoinc(key->info->unit);
   if (interval > 0)
   {
      /* This is a time interval. Must convert the value to the unit of the time interval. 
       * format is a printf format, not the precision. */
      snprintf(buf, sizeof(buf), key->info->format, key->value.time_val / interval);
   }
   else
   {
      /* key->info->format can be converted to an integer safely - it has been checked already */
      int format = (int)strtod(key->info->format, &endptr);
      sprint_time(buf, key->value.time_val, key->info->unit, format);
   }

   if (outbuf)
   {
      snprintf(outbuf, size, "%s", buf);
   }
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
       PrintTimeVal(key, buf, sizeof(buf));
       fprintf(keyfile, "%s", buf);
    }
    break;
  case DRMS_TYPE_STRING:
    fprintf(keyfile, key->info->format, key->value.string_val);
    break;
  default:
    break;
  }
}

void drms_keyword_snprintfval(DRMS_Keyword_t *key, char *buf, int size)
{
   switch(key->info->type)
   {
      case DRMS_TYPE_CHAR:
	snprintf(buf, size, key->info->format, key->value.char_val);
	break;
      case DRMS_TYPE_SHORT:
	snprintf(buf, size, key->info->format, key->value.short_val);
	break;
      case DRMS_TYPE_INT:
	snprintf(buf, size, key->info->format, key->value.int_val);
	break;
      case DRMS_TYPE_LONGLONG:
	snprintf(buf, size, key->info->format, key->value.longlong_val);
	break;
      case DRMS_TYPE_FLOAT:
	snprintf(buf, size, key->info->format, key->value.float_val);
	break;
      case DRMS_TYPE_DOUBLE:
	snprintf(buf, size, key->info->format, key->value.double_val);
	break;
      case DRMS_TYPE_TIME:
	{
	   char intbuf[1024];
           PrintTimeVal(key, intbuf, sizeof(intbuf));
	   snprintf(buf, size, "%s", intbuf);
	}
	break;
      case DRMS_TYPE_STRING:
	snprintf(buf, size, key->info->format, key->value.string_val);
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
int drms_template_keywords_int(DRMS_Record_t *template, int expandperseg, const char *cols)
{
  DRMS_Env_t *env;
  int num_segments, per_segment, seg, status;
  char name0[DRMS_MAXKEYNAMELEN], name[DRMS_MAXKEYNAMELEN];
  char defval[DRMS_DEFVAL_MAXLEN], query[DRMS_MAXQUERYLEN];
  DRMS_Keyword_t *key;
  int irow;
  int rankindeterminate;
  int constrank;
  DB_Binary_Result_t *qres;

  DRMS_SeriesVersion_t vers2_1 = {"2.1", ""};

  env = template->env;
  /* Initialize container structure. */
  hcon_init(&template->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_keyword_struct, 
	    (void (*)(const void *, const void *)) drms_copy_keyword_struct);

  num_segments = hcon_size(&template->segments);

  /* Get the list of keywords from the 'record-series' (eg, su_arta.fd_M_96m_lev18). Then
   * iterate through each of those keywords and find the corresponding record in the 
   * <ns>.drms_keyword table.  */

  int rank;
  char *colnames = strdup(cols);
  char *pch = NULL;
  char *pdel = NULL;

   /* loop through keywords in colnames */
   pch = colnames;
   char *namespace = ns(template->seriesinfo->seriesname);
   rank = 0;

   /* Fetch all keyword rows relevant to this series - must create a keyword struct for each */
   sprintf(query, "select keywordname, islink, linkname, targetkeyw, type, "
           "defaultval, format, unit, isconstant, persegment, "
           "description from %s.%s where seriesname ~~* '%s'",
           namespace, DRMS_MASTER_KEYWORD_TABLE, template->seriesinfo->seriesname);

   if ((qres = drms_query_bin(env->session, query)) == NULL)
   {
      if (colnames)
      {
         free(colnames);
      }

      if (namespace)
      {
         free(namespace);
      }
      return DRMS_ERROR_QUERYFAILED;  /* non-existent ID or SQL error. */
   }

   if (qres->num_rows>0 && qres->num_cols != 11 )
   {
      if (colnames)
      {
         free(colnames);
      }

      if (namespace)
      {
         free(namespace);
      }
      status = DRMS_ERROR_BADFIELDCOUNT;
      goto bailout;
   }

   rankindeterminate = 0;
   constrank = 0;

   for (irow = 0; irow < (int)qres->num_rows; irow++)
   {
    if (expandperseg)
    {
       per_segment = ((db_binary_field_getint(qres, irow, 9) & kKeywordFlag_PerSegment) != 0);
    }
    else
    {
       per_segment = 0;
    }

    for (seg=0; seg<(per_segment==1?num_segments:1); seg++)
    {
      /* Construct name, possibly by appending segment selector. */
      db_binary_field_getstr(qres, irow, 0, sizeof(name0), name0);

      /* key->info->per_segment overload: <= drms_keyword.c:1.44 will fail here if the the value in 
       * the persegment column of the drms_keyword table has been overloaded to contain 
       * all the keyword bit-flags. */
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

      key->info->islink = db_binary_field_getint(qres, irow, 1);
      db_binary_field_getstr(qres, irow, 10, sizeof(key->info->description),key->info->description);
      if (!key->info->islink)
      {
	key->info->linkname[0] = 0;
	key->info->target_key[0] = 0;
	db_binary_field_getstr(qres, irow, 4, sizeof(name),name);
	key->info->type         = drms_str2type(name);
	db_binary_field_getstr(qres, irow, 5, sizeof(defval), defval);
	drms_strval(key->info->type, &key->value, defval);
	db_binary_field_getstr(qres, irow, 6, sizeof(key->info->format),  key->info->format);
	db_binary_field_getstr(qres, irow, 7, sizeof(key->info->unit), key->info->unit);
	key->info->recscope = (DRMS_RecScopeType_t)db_binary_field_getint(qres, irow, 8);

       
        if (key->info->type == DRMS_TYPE_TIME)
        {
           /* If unit is an interval unit (eg, day), then key->value is incorrect 
            * (the assumption was that defval was a time string). This is only 
            * true for time keywords that are being used as time intervals. */
           TIME interval = atoinc(key->info->unit);
           if (interval > 0)
           {
              key->value.double_val = atof(defval);
           }
        }

        /* The persegment column used to be either 0 or 1 and it said whether the keyword
         * was a segment-specific column or not.  But starting with series version 2.1, 
         * the persegment column was overloaded to hold all the keyword flags (including
         * per_seg).  So, the following code works on both < 2.1 series and >= 2.1 series.
         */

        key->info->kwflags = db_binary_field_getint(qres, irow, 9);

        /* Examine top 16 bits of kwflags.  If a keyword has 0x0000, then this is an old
         * series in which the keyword rankings are not completely determined; set a 
         * flag and handle below. */
        rank = (key->info->kwflags & 0xFFFF0000) >> 16; /* 1-based rank */

        if (rank == 0)
        {
           rankindeterminate = 1;
           /* This is (or will be) a constant keyword. If this is actually a 
            * variable keyword, then this value will be replaced below. We
            * might end up with some holes in the rank, because
            * there might be a variable keyword here that gets replaced
            * with an appropriate rank below, but this doesn't matter
            * as rank is relative, and this code is to support
            * old series. */
           key->info->rank = constrank++;
        }
        else
        {
           key->info->rank = rank - 1; /* 0-based rank in DRMS */
        }

        /* For vers 2.1 and greater, the persegment column contains the intprime and extprime 
         * information.  But for older versions, you can figure this out by looking 
         * at the series' primary index keywords (in drms_template_record()) and knowing
         * whether a primary index keyword is an index keyword (in the time-slotting sense). */
        if (!drms_series_isvers(template->seriesinfo, &vers2_1))
        {
           drms_keyword_unsetintprime(key);
           drms_keyword_unsetextprime(key);
        }
      }
      else
      {
	db_binary_field_getstr(qres, irow, 2, sizeof(key->info->linkname),key->info->linkname);
	db_binary_field_getstr(qres, irow, 3, sizeof(key->info->target_key),key->info->target_key);
	key->info->type = DRMS_TYPE_INT;
	key->value.int_val = 0;
	key->info->format[0] = 0;
	key->info->unit[0] = 0;
	key->info->recscope = kRecScopeType_Variable;
        key->info->kwflags = db_binary_field_getint(qres, irow, 9);
        rank = (key->info->kwflags & 0xFFFF0000) >> 16; /* 1-based rank */

        if (rank == 0)
        {
           rankindeterminate = 1;
           key->info->rank = constrank++;
        }
        else
        {
           key->info->rank = rank - 1; /* 0-based rank in DRMS */
        }

        drms_keyword_unsetperseg(key);
        drms_keyword_unsetintprime(key);
        drms_keyword_unsetextprime(key);
      }

      /* perform an amazing switcheroo - per Phil, existing series 
       * have not been using the format and unit fields for TIME 
       * keywords consistently.
       * -format is supposed to specify precision when printing out
       * a time string.  0 means whole seconds, 1 means nearest tenth
       * of a second, etc.
       * -unit is supposed to specify time zone.
       *
       * Apply a bandaid:
       *   if format is a number, keep it (to be used as precision)
       *   else if format is a recognized zone, assign the zone to unit
       *   else set format to 0 (means round to nearest whole second).
       *   if unit is not set, or if unit is none or time or "" or whitespace,
       *     set unit to UTC. */
      if (key->info->type == DRMS_TYPE_TIME)
      {
	 char formatn = 0;
	 char *format = key->info->format;
	 char *unit = key->info->unit;
	 char *unittmp = strdup(unit);
	 char *endptr = NULL;
	 int64_t val = strtod(format, &endptr);
         char tmpout[64];

         TIME interval = 0;
         interval = atoinc(unittmp);

         if (interval <= 0)
         {
            /* If unit is a recognizable time-interval unit, like "sec", then this time 
             * keyword is actually a time-interval keyword. In this case, preserve the 
             * format (to be used for formatting a double for output), and preserve 
             * the unit (keep "sec"). Parsing code will need to interpret the numerical
             * value according to this unit so that the actual value saved is in seconds
             * (so if the unit is "day", and the numerical value is "1", then the value 
             * saved should be 86400. Code that prints the jsd will have to do the reverse
             * conversion. The stored value of 86400 will need to be converted to 
             * 1. For now, getkey and setkey should just use seconds.
             */
            
            if (val != 0 || endptr != format)
            {
               /* a recognizable number */
               if (val >= INT8_MIN && val <= INT8_MAX)
               {
                  formatn = val;
               }
            }
            else if (!parse_zone(format, tmpout, sizeof(tmpout)))
            {
               /* format is a zone */
               snprintf(unit, DRMS_MAXUNITLEN, "%s", format);
            }

            snprintf(format, DRMS_MAXFORMATLEN, "%d", formatn);

            if (*unit == '\0' || 
                !strcasecmp(unit, "none") || 
                !strcasecmp(unit, "time") ||
                !strtok(unittmp, " \t\b"))
            {
               snprintf(unit, DRMS_MAXUNITLEN, "%s", "UTC");
            }
         }

	 if (unittmp)
	 {
	    free(unittmp);
	 }
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

   /* If the ranking wasn't completely specified in the "persegment" column, 
    * then this is an old series for which libdrms did not save the 
    * order in the "persegment" column. Enact Plan B: use the order of the
    * columns in the "record" table (eg., <ns>.series) to determine 
    * the order of the variable keywords, and use the order of rows of the 
    * constant keywords in the <ns>.drms_keywords as the order of the
    * constant keywords. */
   if (rankindeterminate)
   {
      rank = 0;
      while (pch)
      {
         if ((pdel = strchr(pch, ',')) != NULL)
         {
            *pdel = '\0';
         }

         if (!strcmp(pch, "recnum") ||
             !strcmp(pch, "sunum") ||
             !strcmp(pch, "slotnum") ||
             !strcmp(pch, "sessionid") ||
             !strcmp(pch, "sessionns"))
         {
            pch = pdel ? pdel + 1 : NULL;
            continue;
            /* There may be some other columns, like sg_000_file, that aren't 
             * in the drms_keywords table, but that is okay. If a column isn't
             * found in the drms_keywords table, */
         }

         /* Use the order of the record table to determine the order of the variable keywords. */
         if ((key = (DRMS_Keyword_t *)hcon_lookup_lower(&template->keywords, pch)) != NULL)
         {
            key->info->rank = rank + constrank; /* Put variable keywords at end. */
            rank++;
         }

         pch = pdel ? pdel + 1 : NULL;
      }      
   }

  if (colnames)
  {
     free(colnames);
  }

  if (namespace)
  {
     free(namespace);
  }

  db_free_binary_result(qres);
  return DRMS_SUCCESS;

 bailout:
  if (colnames)
  {
     free(colnames);
  }

  if (namespace)
  {
     free(namespace);
  }

  db_free_binary_result(qres);
  return status;
}

int drms_template_keywords(DRMS_Record_t *template)
{
   int err = 0;
   char *ns = NULL;
   char *table = NULL;
   char *oid = NULL;
   char *serieslwr = strdup(template->seriesinfo->seriesname);
   char *colnames = NULL;

   strtolower(serieslwr);
   get_namespace(serieslwr, &ns, &table);

   if (serieslwr)
   {
      free(serieslwr);
   }

   err = GetTableOID(template->env, ns, table, &oid);

   if (ns)
   {
      free(ns);
   }

   if (table)
   {
      free(table);
   }

   if (!err)
   {
      err = GetColumnNames(template->env, oid, &colnames);
   }

   if (oid)
   {
      free(oid);
   }

   err = drms_template_keywords_int(template, 1, colnames);

   if (colnames)
   {
      free(colnames);
   }

   return err;
}

/* Wrapper for __drms_keyword_lookup without the recursion depth counter. */
DRMS_Keyword_t *drms_keyword_lookup(DRMS_Record_t *rec, const char *key, int followlink)
{
  char *lb,*rb;
  char tmp[DRMS_MAXKEYNAMELEN+5]={0};
  int segnum;

  /* Handle explicit link syntax, <linkname>:<keyname> */
  char tmplink[DRMS_MAXLINKNAMELEN]={0};
  char *colonchar;
  int status;
  colonchar = strchr(key, ':');
  if (colonchar)
  {
    strncpy(tmplink, key, colonchar-key);
    rec = drms_link_follow(rec, tmplink, &status);
    key = colonchar+1;
    if (!rec || !(*key) || status)
      return(NULL);
  }

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
    if (strlen(tmp) >= DRMS_MAXKEYNAMELEN)
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
		    hiter_new_sort(&hit, &(template->keywords), drms_keyword_ranksort);
		    DRMS_Keyword_t *kw = NULL;

		    int iKW = 0;
		    while ((kw = hiter_getnext(&hit)) != NULL)
		    {
			 nameArr[iKW] = kw->info->name;
			 valArr[iKW] = kw->info;

			 iKW++;
		    }

		    ret = hcon_create(sizeof(DRMS_KeywordInfo_t), 
				      DRMS_MAXKEYNAMELEN,
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

   char exp1[DRMS_MAXKEYNAMELEN];
   char exp2[DRMS_MAXKEYNAMELEN];
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
             drms_keyword_getperseg(k1) == drms_keyword_getperseg(k2) &&
             drms_keyword_getintprime(k1) == drms_keyword_getintprime(k2) &&
             drms_keyword_getextprime(k1) == drms_keyword_getextprime(k2));
  
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

  if (keyword != NULL)
  {  
     result = drms_keyword_getdouble(keyword, &stat);
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

double drms_keyword_getdouble(DRMS_Keyword_t *keyword, int *status)
{
   double result;
   int stat = DRMS_SUCCESS;

   result = drms2double(keyword->info->type, &keyword->value, &stat);  

   if (status)
     *status = stat;
   return result;
}

char *drms_keyword_getstring(DRMS_Keyword_t *keyword, int *status)
{
  char *result = NULL;
  int stat = DRMS_SUCCESS;

  if (keyword->info->type == DRMS_TYPE_TIME)
  {
     XASSERT(result = malloc(32));
     memset(result, 0, 32);
     drms_keyword_snprintfval(keyword, result, 32);
  }
  else
  {
     result = drms2string(keyword->info->type, &keyword->value, &stat);
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
     result = drms_keyword_getstring(keyword, &stat);
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

TIME drms_getkey_time(DRMS_Record_t *rec, const char *key, int *status)
{
  DRMS_Keyword_t *keyword;
  int stat;
  TIME result=DRMS_MISSING_TIME;

  keyword = drms_keyword_lookup(rec, key, 1);
  if (keyword!=NULL )
  {   
     result = drms_keyword_gettime(keyword, &stat);
  }
  else
  {
    stat = DRMS_ERROR_UNKNOWNKEYWORD;
  }
  if (status)
    *status = stat;
  return result;  
}

TIME drms_keyword_gettime(DRMS_Keyword_t *keyword, int *status)
{
   double result;
   int stat = DRMS_SUCCESS;

   result = drms2time(keyword->info->type, &keyword->value, &stat);  

   if (status)
     *status = stat;
   return (TIME)result;
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
    value.string_val = NULL;
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
    value.string_val = NULL;
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
static int SetKeyInternal(DRMS_Record_t *rec, const char *key, DRMS_Value_t *value)
{
   DRMS_Keyword_t *keyword = NULL;;
   DRMS_Keyword_t *indexkw = NULL;

   if (rec ->readonly)
     return DRMS_ERROR_RECORDREADONLY;

   keyword = drms_keyword_lookup(rec, key, 0);
   if (keyword != NULL )
   {
      if (keyword->info->islink)
	return DRMS_ERROR_KEYWORDREADONLY;
      else
      {
	 int retstat = drms_convert(keyword->info->type, 
				    &keyword->value, 
				    value->type, 
				    &(value->value));

	 if (!retstat && drms_keyword_isslotted(keyword))
	 {
	    /* Must also set index keyword */
	    DRMS_Value_t indexval;
	    DRMS_Value_t inval;

	    inval.value = keyword->value;
	    inval.type = keyword->info->type;

	    retstat = drms_keyword_slotval2indexval(keyword, 
						    &inval,
						    &indexval,
						    NULL);

	    if (!retstat)
	    {
	       indexkw = drms_keyword_indexfromslot(keyword);
	       retstat = drms_convert(indexkw->info->type,
				      &(indexkw->value),
				      indexval.type,
				      &(indexval.value));
	    }
	 }

	 return retstat;
      }
   }
   else
     return DRMS_ERROR_UNKNOWNKEYWORD; 
}

int drms_setkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t type, 
		DRMS_Type_Value_t *value)
{
  DRMS_Value_t val = {type, *value};
  return SetKeyInternal(rec, key, &val);
}  

/* This is a more generic version of drms_setkey() - it assumes that the source type
 * is equal to the target type. */
int drms_setkey_p(DRMS_Record_t *rec, const char *key, DRMS_Value_t *value)
{
  return SetKeyInternal(rec, key, value);
}  

/* Slightly less ugly pieces of crap that should be used instead: */
int drms_setkey_char(DRMS_Record_t *rec, const char *key, char value)
{
   DRMS_Type_Value_t v;
   v.char_val = value;
   DRMS_Value_t val = {DRMS_TYPE_CHAR, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_short(DRMS_Record_t *rec, const char *key, short value)
{
   DRMS_Type_Value_t v;
   v.short_val = value;
   DRMS_Value_t val = {DRMS_TYPE_SHORT, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_int(DRMS_Record_t *rec, const char *key, int value)
{
   DRMS_Type_Value_t v;
   v.int_val = value;
   DRMS_Value_t val = {DRMS_TYPE_INT, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_longlong(DRMS_Record_t *rec, const char *key, long long value)
{
   DRMS_Type_Value_t v;
   v.longlong_val = value;
   DRMS_Value_t val = {DRMS_TYPE_LONGLONG, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_float(DRMS_Record_t *rec, const char *key, float value)
{
   DRMS_Type_Value_t v;
   v.float_val = value;
   DRMS_Value_t val = {DRMS_TYPE_FLOAT, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_double(DRMS_Record_t *rec, const char *key, double value)
{
   DRMS_Type_Value_t v;
   v.double_val = value;
   DRMS_Value_t val = {DRMS_TYPE_DOUBLE, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_time(DRMS_Record_t *rec, const char *key, TIME value)
{
   DRMS_Type_Value_t v;
   v.time_val = value;
   DRMS_Value_t val = {DRMS_TYPE_TIME, v};
   return SetKeyInternal(rec, key, &val);
}

int drms_setkey_string(DRMS_Record_t *rec, const char *key, const char *value)
{
   int ret;

   DRMS_Type_Value_t v;
   v.string_val = strdup(value);
   DRMS_Value_t val = {DRMS_TYPE_STRING, v};
   ret = SetKeyInternal(rec, key, &val);
   free(v.string_val);
   v.string_val = NULL;
   return ret;
}

int drms_keyword_getintname(const char *keyname, char *nameOut, int size)
{
   int success = 0;
   char *potential = NULL;

   *nameOut = '\0';

   /* 1 - Try FITS name. */
   if (base_drmskeycheck(keyname) == 0)
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

int drms_keyword_getmappedintname(const char *keyname, 
                                  const char *class, 
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
   if (!success && class != NULL)
   {
      potential = drms_keymap_classintname(class, keyname);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   if (!success)
   {
      /* Now try the map- and class-independent schemes. */
      char buf[DRMS_MAXKEYNAMELEN];
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
   return drms_keyword_getmappedextname(key, NULL, NULL, nameOut, size);
}

/* Same as above, but try a KeyMap first, then a KeyMapClass */
int drms_keyword_getmappedextname(DRMS_Keyword_t *key, 
				  const char *class, 
				  DRMS_KeyMap_t *map,
				  char *nameOut,
				  int size)
{
   int success = 0;
   const char *potential = NULL;
   int vstat = 0;

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
   if (!success && class != NULL)
   {
      potential = drms_keymap_classextname(class, key->info->name);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   if (!success)
   {
      /* Now try the map- and class-independent schemes. */
      char *pot = NULL;
      char *psep = NULL;
      char *desc = strdup(key->info->description);
      char *pFitsName = NULL;
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
		  pot = (char *)malloc(sizeof(char) * size);
		  if (pot)
		  {
		     memcpy(pot, pFitsName + 1, len - 2);
		     pot[len - 2] = '\0';

                     /* The description might contain [X:Y], where
                      * X is the external keyword name, and Y is the 
                      * external keyword cast. There could be a ':' */
                     if (drms_keyword_getcast(key) != kDRMS_Keyword_ExtType_None)
                     {
                        psep = strchr(pot, ':');
                        *psep = '\0';
                     }

                     vstat = base_fitskeycheck(pot);

		     if (vstat == 0)
		     {
			strcpy(nameOut, pot);
			success = 1;
		     }

		     free(pot);
		  }
	       }
	    }
	 }

	 free(desc);
      }
   
      /* 2 - Try DRMS name (must be upper case). */
      if (!success)
      {
	 char nbuf[DRMS_MAXKEYNAMELEN];
         
	 snprintf(nbuf, sizeof(nbuf), "%s", key->info->name);
	 strtoupper(nbuf);

	 vstat = base_fitskeycheck(nbuf);
	 if (vstat == 0)
	 {
	    strcpy(nameOut, nbuf);
	    success = 1;
	 }
         else if (vstat == 2)
         {
            fprintf(stderr, "FITS keyword name '%s' is reserved.\n", key->info->name);
         }
      }

      /* 3 - Use default rule. */
      if (!success)
      {
	 pot = (char *)malloc(sizeof(char) * size);
	 if (pot)
	 {
	    if (GenerateFitsKeyName(key->info->name, pot, size))
	    {
	       strcpy(nameOut, pot);
	       success = 1;
	    }

	    free(pot);
	 }
      }
   }

   return success;
}

int drms_keyword_inclass(DRMS_Keyword_t *key, DRMS_KeywordClass_t class)
{
   int inclass = 0;

   switch(class)
   {
      case kDRMS_KeyClass_All:
        inclass = 1;
        break;
      case kDRMS_KeyClass_Explicit:
        inclass = !(drms_keyword_getimplicit(key));
        break;
      case kDRMS_KeyClass_DRMSPrime:
        inclass = drms_keyword_getextprime(key);
        break;
      case kDRMS_KeyClass_Persegment:
        inclass = drms_keyword_getperseg(key);
        break;
   }

   return inclass;
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

int drms_copykeyB(DRMS_Keyword_t *tgtkey, DRMS_Keyword_t *srckey)
{
   int status = DRMS_SUCCESS;
   DRMS_Value_t srcval; /* owns contained string */

   srcval.type = srckey->info->type;
   srcval.value = srckey->value;
   
   /* Must go through this function - may be setting a keyword that
    * has an associated index keyword and this function will set
    * the associated keyword appropriately. */
   status = drms_setkey_p(tgtkey->record, srckey->info->name, &srcval);

   return status;
}

int drms_copykeys(DRMS_Record_t *target, 
                  DRMS_Record_t *source, 
                  int usesrcset,
                  DRMS_KeywordClass_t class)
{
   HIterator_t sethit;
   DRMS_Keyword_t *srckey = NULL;
   DRMS_Keyword_t *tgtkey = NULL;
   DRMS_Keyword_t *setkey = NULL;
   DRMS_Keyword_t *lookupkey = NULL;
   DRMS_Record_t *lookuprec = NULL;
   int status = DRMS_SUCCESS;

   if (usesrcset)
   {
      hiter_new_sort(&sethit, &source->keywords, drms_keyword_ranksort);
      lookuprec = target;
   }
   else
   {
      hiter_new_sort(&sethit, &target->keywords, drms_keyword_ranksort);
      lookuprec = source;
   }

   while ((setkey = (DRMS_Keyword_t *)hiter_getnext(&sethit)) != NULL)
   {
      if (drms_keyword_inclass(setkey, class))
      {
         lookupkey = drms_keyword_lookup(lookuprec, setkey->info->name, 1);

         if (usesrcset)
         {
            srckey = setkey;
            tgtkey = lookupkey;
         }
         else
         {
            srckey = lookupkey;
            tgtkey = setkey;
         }

         if (tgtkey && srckey)
         {
            if (!drms_keyword_islinked(tgtkey))
            {
               /* You don't want to copy to a keyword if it is a linked keyword - the
                * link itself will point to read-only record containing the desired value. */
               status = drms_copykeyB(tgtkey, srckey);
            }
         }
      }

      if (status)
      {
         if (usesrcset)
         {
            fprintf(stderr, "drms_copykeys() failed on keyword '%s'.\n", srckey->info->name);
         }
         else
         {
            fprintf(stderr, "drms_copykeys() failed on keyword '%s'.\n", tgtkey->info->name);
         }
         break;
      }
   }

   return status;
}

int drms_keyword_getsegscope(DRMS_Keyword_t *key)
{
   return drms_keyword_getperseg(key);
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

int drms_keyword_islinked(DRMS_Keyword_t *key)
{
   return (key->info->islink != 0);
}

int drms_keyword_isprime(DRMS_Keyword_t *key)
{
   return drms_keyword_getintprime(key);
}

DRMS_Type_t drms_keyword_gettype(DRMS_Keyword_t *key)
{
   return key->info->type;
}

const DRMS_Type_Value_t *drms_keyword_getvalue(DRMS_Keyword_t *key)
{
   return &(key->value);
}

/* Gets the unit value for a slotted keyword. */
DRMS_SlotKeyUnit_t drms_keyword_getslotunit(DRMS_Keyword_t *slotkey, int *status)
{
   DRMS_SlotKeyUnit_t ret = kSlotKeyUnit_Invalid;
   int stat = DRMS_SUCCESS;
   DRMS_Keyword_t *unitKey = NULL;

   if (slotkey)
   {
      unitKey = drms_keyword_unitfromslot(slotkey);
   }
   else
   {
      fprintf(stderr, 
	      "Keyword '%s' is not associated with a unit ancillary keyword.\n",
	      slotkey->info->name);
   }

   if (unitKey)
   {
      ret = drms_keyword_getunit(unitKey, &stat);
   }   

   if (ret == kSlotKeyUnit_Invalid)
   {
      stat = DRMS_ERROR_INVALIDDATA;
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

/* Operates on a XXX_unit key. */
DRMS_SlotKeyUnit_t drms_keyword_getunit(DRMS_Keyword_t *key, int *status)
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
	       snprintf(buf, sizeof(buf), "%s", gSUS[i].str);
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

/* Gets the epoch value for a slotted keyword. */
TIME drms_keyword_getslotepoch(DRMS_Keyword_t *slotkey, int *status)
{
   TIME ret = DRMS_MISSING_TIME;
   int stat = DRMS_SUCCESS;
   DRMS_Keyword_t *epochKey = NULL;

   if (slotkey)
   {
      /* Shouldn't be an epoch key, unless keyword is of recscope TS_EQ or TS_SLOT. */
      epochKey = drms_keyword_epochfromslot(slotkey);

      if (epochKey)
      {
	 ret = drms_keyword_getepoch(epochKey, &stat);
      }   
      else
      {
	 fprintf(stderr, 
		 "Keyword '%s' is not associated with an epoch ancillary keyword.\n",
		 slotkey->info->name);
      }
   }

   if (drms_ismissing_time(ret))
   {
      stat = DRMS_ERROR_INVALIDDATA;
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}
   
/* Operates on a XXX_epoch key. */
TIME drms_keyword_getepoch(DRMS_Keyword_t *key, int *status)
{
   TIME ret = DRMS_MISSING_TIME;
   int stat = DRMS_SUCCESS;

   if (key->info->type == DRMS_TYPE_STRING && strstr(key->info->name, kSlotAncKey_Epoch))
   {
      const TIME *timeval = drms_time_getepoch((key->value).string_val, NULL, &stat);

      if (timeval)
      {
	 ret = *timeval;
      }
      else
      {
	 fprintf(stderr, "Invalid slot unit string value '%s'.\n", (key->value).string_val);
      }
   }
   else if (key->info->type == DRMS_TYPE_TIME)
   {
      ret = drms_keyword_gettime(key, NULL);
   }
   else
   {
      fprintf(stderr, 
	      "Keyword '%s' does not contain slotted keyword epoch information.\n",
	      key->info->name);
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

double drms_keyword_getslotcarr0(void)
{
   return kSlotKeyBase_Carr;
}

double drms_keyword_getslotbase(DRMS_Keyword_t *slotkey, int *status)
{
   double ret = DRMS_MISSING_DOUBLE;
   int stat = DRMS_SUCCESS;

   if (slotkey)
   {
      switch (slotkey->info->recscope)
      {
	 case kRecScopeType_TS_EQ:
         case kRecScopeType_TS_SLOT:
	   ret = drms_keyword_getslotepoch(slotkey, status);
	   break;
	 case kRecScopeType_SLOT:
	   {
	      DRMS_Keyword_t *baseKey = drms_keyword_basefromslot(slotkey);

	      if (baseKey)
	      {
		 ret = drms2double(baseKey->info->type, &baseKey->value, &stat);
	      }   
	      else
	      {
		 fprintf(stderr, 
			 "Keyword '%s' is not associated with a base ancillary keyword.\n",
			 slotkey->info->name);
	      }

	      if (drms_ismissing_double(ret))
	      {
		 stat = DRMS_ERROR_INVALIDDATA;
	      }
	   }
	   break;
	 case kRecScopeType_ENUM:
	   break;
	 case kRecScopeType_CARR:
	   ret = drms_keyword_getslotcarr0();
	   break;
	 default:
	   fprintf(stderr, 
		   "Invalid recscope type '%d'.\n", 
		   (int)slotkey->info->recscope);
      }
   }
   else
   {
      stat = DRMS_ERROR_INVALIDDATA;
   }
   
   if (status)
   {
      *status = stat;
   }
   
   return ret;
}

double drms_keyword_getvalkeybase(DRMS_Keyword_t *valkey, int *status)
{
   double ret = DRMS_MISSING_DOUBLE;
   int statint = DRMS_SUCCESS;

   if (drms_keyword_isslotted(valkey))
   {
      return drms_keyword_getslotbase(valkey, status);
   }
   else
   {
      /* Any integer-type value keyword could have auxilliary keywords, like 
       * _base, _step, etc. */
      DRMS_Keyword_t *baseKey = drms_keyword_basefromvalkey(valkey);

      if (baseKey)
      {
         ret = drms2double(baseKey->info->type, &baseKey->value, &statint);
      }   
      else
      {
         statint = DRMS_ERROR_UNKNOWNKEYWORD;
      }

      if (status)
      {
         *status = statint;
      }
      
      return ret;
   }
}

/* Gets the step value for a slotted keyword. */
double drms_keyword_getslotstep(DRMS_Keyword_t *slotkey, DRMS_SlotKeyUnit_t *unit, int *status)
{
   double ret = DRMS_MISSING_DOUBLE;
   int stat = DRMS_SUCCESS;
   DRMS_Keyword_t *stepKey = NULL;

   if (slotkey)
   {
      stepKey = drms_keyword_stepfromslot(slotkey);
   }
   else
   {
      fprintf(stderr, 
	      "Keyword '%s' is not associated with a step ancillary keyword.\n",
	      slotkey->info->name);
   }

   if (stepKey)
   {
      ret = drms_keyword_getstep(stepKey, slotkey->info->recscope, unit, &stat);
   }   

   if (stat == DRMS_SUCCESS)
   {
      if (drms_ismissing_double(ret))
      {
	 stat = DRMS_ERROR_INVALIDDATA;
      }
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

/*  Operates on a XXX_step key. */
double drms_keyword_getstep(DRMS_Keyword_t *key, 
			    DRMS_RecScopeType_t recscope, 
			    DRMS_SlotKeyUnit_t *unit, 
			    int *status)
{
   double step = DRMS_MISSING_DOUBLE;
   int stat = DRMS_SUCCESS;
   *unit = kSlotKeyUnit_Invalid;

   if (drms_keyword_gettype(key) == DRMS_TYPE_STRING)
   {
      switch (recscope)
      {
	 case kRecScopeType_TS_EQ:
           /* intentional fall-through */
         case kRecScopeType_TS_SLOT:
	   {
	      /* This is a duration - parse it. */
	      char *durstr = drms_keyword_getstring(key, NULL);
	      if (durstr)
	      {
		 /* Always returns in units of seconds */
		 if (drms_names_parseduration(&durstr, &step))
		 {
		    fprintf(stderr,
			    "Invalid step keyword value for '%s'.\n",
			    key->info->name);
		    stat = DRMS_ERROR_INVALIDDATA;
		 }

		 *unit = kSlotKeyUnit_Seconds;
		 free(durstr);
	      }
	   }
	   break;
	 case kRecScopeType_CARR:
	   {
	      /* This is a degree delta - parse it. */
	      char *deltastr = drms_keyword_getstring(key, NULL);
	      if (deltastr)
	      {
		 /* Always returns in units of degrees */
		 if (drms_names_parsedegreedelta(&deltastr, unit, &step))
		 {
		     fprintf(stderr,
			    "Invalid step keyword value for '%s'.\n",
			    key->info->name);
		    stat = DRMS_ERROR_INVALIDDATA;
		 }

		 free(deltastr);
	      }
	   }
	   break;
	 default:
	   /* kRecScopeType_SLOT isn't necessarily a time or degrees, so 
	    * it can't really be a string because we wouldn't know how to 
	    * parse and interpret that string.  The string could be anything. */
	   fprintf(stderr, 
		   "Invalid recscope type '%d'.\n", 
		   (int)recscope);

      }
   }
   else
   {
      step = drms_keyword_getdouble(key, NULL);
   }

   if (*status)
     *status = stat;

   return step;
}

double drms_keyword_getvalkeystep(DRMS_Keyword_t *valkey, int *status)
{
   double ret = DRMS_MISSING_DOUBLE;
   int statint = DRMS_SUCCESS;

   if (drms_keyword_isslotted(valkey))
   {
      DRMS_SlotKeyUnit_t unit;
      return drms_keyword_getslotstep(valkey, &unit, status);
   }
   else
   {
      /* Any integer-type value keyword could have auxilliary keywords, like 
       * _base, _step, etc. */
      DRMS_Keyword_t *stepKey = drms_keyword_stepfromvalkey(valkey);

      if (stepKey)
      {
         ret = drms2double(stepKey->info->type, &stepKey->value, &statint);
      }   
      else
      {
         statint = DRMS_ERROR_UNKNOWNKEYWORD;
      }

      if (status)
      {
         *status = statint;
      }
      
      return ret;
   }
}

static DRMS_Keyword_t *GetAncillaryKey(DRMS_Keyword_t *valkey, const char *suffix)
{
   DRMS_Keyword_t *ret = NULL;
   char buf[DRMS_MAXKEYNAMELEN];

   snprintf(buf, sizeof(buf), "%s%s", valkey->info->name, suffix);
   ret = (DRMS_Keyword_t *)hcon_lookup_lower(&(valkey->record->keywords), buf);

   return ret;
}

DRMS_Keyword_t *drms_keyword_indexfromslot(DRMS_Keyword_t *slot)
{
   return GetAncillaryKey(slot, kSlotAncKey_Index);
}

DRMS_Keyword_t *drms_keyword_indexfromvalkey(DRMS_Keyword_t *valkey)
{
   DRMS_Keyword_t *ret = NULL;

   if (drms_keyword_isslotted(valkey))
   {
      ret = drms_keyword_indexfromslot(valkey);
   }
   else
   {
      /* Just look for an associated _index keyword */
      ret = GetAncillaryKey(valkey, kSlotAncKey_Index);
   }

   return ret;
}

DRMS_Keyword_t *drms_keyword_epochfromslot(DRMS_Keyword_t *slot)
{
   return GetAncillaryKey(slot, kSlotAncKey_Epoch);
}

/* If this is a slotted keyword, then the "base" keyword might be
 * "_base" or it might be "_epoch". */
DRMS_Keyword_t *drms_keyword_basefromslot(DRMS_Keyword_t *slotkey)
{
   if (slotkey->info->recscope == kRecScopeType_TS_EQ || 
       slotkey->info->recscope == kRecScopeType_TS_SLOT)
   {
      return GetAncillaryKey(slotkey, kSlotAncKey_Epoch);
   }
   else
   {
      return GetAncillaryKey(slotkey, kSlotAncKey_Base);
   }
}

DRMS_Keyword_t *drms_keyword_basefromvalkey(DRMS_Keyword_t *valkey)
{
   if (drms_keyword_isslotted(valkey))
   {
      return drms_keyword_basefromslot(valkey);
   }
   else
   {
      return GetAncillaryKey(valkey, kSlotAncKey_Base);
   }
}

DRMS_Keyword_t *drms_keyword_stepfromslot(DRMS_Keyword_t *slot)
{
   return GetAncillaryKey(slot, kSlotAncKey_Step);
}

DRMS_Keyword_t *drms_keyword_stepfromvalkey(DRMS_Keyword_t *valkey)
{
   if (drms_keyword_isslotted(valkey))
   {
      return drms_keyword_stepfromslot(valkey);
   }
   else
   {
      return GetAncillaryKey(valkey, kSlotAncKey_Step);
   }
}

DRMS_Keyword_t *drms_keyword_unitfromslot(DRMS_Keyword_t *slot)
{
   return GetAncillaryKey(slot, kSlotAncKey_Unit);
}

DRMS_Keyword_t *drms_keyword_roundfromslot(DRMS_Keyword_t *slot)
{
   return GetAncillaryKey(slot, kSlotAncKey_Round);
}

DRMS_Keyword_t *drms_keyword_slotfromindex(DRMS_Keyword_t *indx)
{
   DRMS_Keyword_t *ret = NULL;
   char *kname = strdup(indx->info->name);

   if (kname)
   {
      char *underscore = strstr(kname, kSlotAncKey_Index);

      if (underscore && drms_keyword_isindex(indx))
      {
	 DRMS_Record_t *template = 
	   drms_template_record(indx->record->env, 
				indx->record->seriesinfo->seriesname,
				NULL);
	 *underscore = '\0';
	 ret = (DRMS_Keyword_t *)hcon_lookup_lower(&(template->keywords), kname);
      }

      free(kname);
   }

   return ret;
}

/* Maps the floating-point value of the slotted keyword, into 
 * the index value of the corresponding index keyword.
 *
 * A floating-point value exactly (within precision) on the boundary
 * between slots maps to the slot number with a smaller value.  So, if the
 * slot boundaries are 122305305.0 for slot 0, 122305315.0 for slot 1, 
 * 122305325.0 for slot 2, ..., then a value of 122305315.0 falls into 
 * slot 0.
 */
int drms_keyword_slotval2indexval(DRMS_Keyword_t *slotkey, 
				  DRMS_Value_t *valin,
				  DRMS_Value_t *valout,
				  DRMS_Value_t *startdur)
{
   int stat = DRMS_SUCCESS;

   if (valin && valout && drms_keyword_isslotted(slotkey))
   {
      valout->type = kIndexKWType;
      drms_missing(valout->type, &(valout->value));

      /* Must convert slotted-key value into associated index-key value */
      double step;
      double stepsecs;
      double roundstep = 0.0;
      DRMS_SlotKeyUnit_t unit;
      double unitVal;
      int usedefunit = 0;
      int round_down = 0;

      DRMS_Keyword_t *stepKey = drms_keyword_stepfromslot(slotkey);
      DRMS_Keyword_t *unitKey = drms_keyword_unitfromslot(slotkey);
      DRMS_Keyword_t *roundKey = drms_keyword_roundfromslot(slotkey);

      double base;
      double valind;

      if (roundKey)
      {
         roundstep = drms_keyword_getdouble(roundKey, NULL);
      }

      step = drms_keyword_getslotstep(slotkey, &unit, &stat);

      /* unit will be valid if user provided a string step (eg, 60s).
       * if kRecScopeType_SLOT, unit will never be valid. */
      if (unit != kSlotKeyUnit_Invalid)
      {
	 if (unitKey)
	 {
	    /* This will be ignored because step unit specified in step. */
	    fprintf(stderr, 
		    "Warning: '%s' specifies step unit, so '%s' will"
		    "be ignored.\n", 
		    stepKey->info->name,
		    unitKey->info->name);
	 }
      }
      else
      {
	 if (unitKey)
	 {
	    unit = drms_keyword_getunit(unitKey, NULL);
	 }
	 else
	 {
	    usedefunit = 1;
	 }
      }

      DRMS_RecScopeType_t recscope = drms_keyword_getrecscope(slotkey);
      switch (recscope)
      {
         case kRecScopeType_TS_SLOT:
           round_down = 1;
           /* intential fall-through */
	 case kRecScopeType_TS_EQ:
	   {
	      TIME epoch;

	      epoch = drms_keyword_getslotepoch(slotkey, &stat);
	      base = epoch;

	      if (usedefunit)
	      {
		 unit = kSlotKeyUnit_Seconds;
	      }

	      switch(unit)
	      {
		 case kSlotKeyUnit_TSeconds:
		   unitVal = 0.1;
		   break;
		 case kSlotKeyUnit_Seconds:
		   unitVal = 1.0;
		   break;
		 case kSlotKeyUnit_Minutes:
		   unitVal = 60.0;
		   break;
		 case kSlotKeyUnit_Days:
		   unitVal = 86400.0;
		   break;
		 default:
		   fprintf(stderr, "Invalid slotted key unit '%d'.\n", (int)unit);
		   break;
	      }

	      valind = valin->value.time_val;
	   }
	   break;
	 case kRecScopeType_CARR:
	   {
	      base = drms_keyword_getslotcarr0();

	      if (usedefunit)
	      {
		 unit = kSlotKeyUnit_Degrees;
	      }

	      switch(unit)
	      {
		 case kSlotKeyUnit_Degrees:
		   unitVal = 1.0;
		   break;
		 case kSlotKeyUnit_Arcminutes:
		   unitVal = 1/ 60.0;
		   break;
		 case kSlotKeyUnit_Arcseconds:
		   unitVal = 1/ 3600.0;
		   break;
		 case kSlotKeyUnit_MAS:
		   unitVal = 1 / 3600000.0;
		   break;
		 case kSlotKeyUnit_Radians:
		   unitVal = 180.0 / (M_PI);
		   break;
		 case kSlotKeyUnit_MicroRadians:
		   unitVal = (180.0 / (M_PI)) / 1000.0;
		   break;
		 default:
		   fprintf(stderr, "Invalid slotted key unit '%d'.\n", (int)unit);
		   break;
	      }

	      valind = drms2double(valin->type, &(valin->value), NULL);
	   }
	   break;
	 case kRecScopeType_SLOT:
	   {
	      base = drms_keyword_getslotbase(slotkey, &stat);
	      unitVal = 1.0;

	      valind = drms2double(valin->type, &valin->value, &stat);
	   }
	   break;
	 default:
	   fprintf(stderr, "Invalid rec scope '%d'.\n", (int)recscope);
	   stat = DRMS_ERROR_INVALIDDATA;
      } /* case */

      stepsecs = unitVal * step;
      double exact = 0;
      int inexact = 0;
      int toosmall = 0;

      if (round_down)
      {
         if (startdur)
         {
            /* valind is actually a duration, in seconds. */
            exact = valind / roundstep;
            inexact = (int)exact;

            if (valind < roundstep)
            {
               toosmall = 1;
               fprintf(stderr, "Invalid slotted-keyword duration '%f seconds' specified.  Should be at least the step size of '%f seconds'.  Duration was rounded up to step size.\n", valind, roundstep);
               /* ensures that at least one slot is returned */
               valout->value.longlong_val = CalcSlot(roundstep, 0.0, stepsecs, roundstep);
            }
            else
            {
               valout->value.longlong_val = CalcSlot(inexact * roundstep, 0.0, stepsecs, roundstep);
            }
         }
         else
           valout->value.longlong_val = CalcSlot(valind, base, stepsecs, roundstep);
      }
      else
      {
         if (startdur)
         {
            /* valind is actually a duration, in seconds. */  
            exact = valind / stepsecs;
            inexact = (int)exact;

            if (valind < stepsecs)
            {
               toosmall = 1;
               fprintf(stderr, "Invalid slotted-keyword duration '%f seconds' specified.  Should be at least the step size of '%f seconds'.  Duration was rounded up to step size.\n", valind, stepsecs);
               /* ensures that at least one slot is returned */ 
               valout->value.longlong_val = CalcSlot(stepsecs, 0.0, stepsecs, stepsecs);
            }
            else
            {
               valout->value.longlong_val = CalcSlot(inexact * stepsecs, 0.0, stepsecs, stepsecs);               
            }
         }
         else
           valout->value.longlong_val = CalcSlot(valind, base, stepsecs, stepsecs);
      }

      if (startdur)
      {
         if (!toosmall && (fabs(exact - inexact) > 1.0e-11 * (fabs(exact) + fabs(inexact))))
         {
            fprintf(stderr, "Invalid slotted-keyword duration '%f seconds' specified.  Should be a multiple of step size '%f seconds'.  Duration was rounded to nearest multiple.\n", valind, stepsecs);
         }
      }
   }
   else
   {
      stat = DRMS_ERROR_INVALIDDATA;
   }

   return stat;
}

int drms_keyword_export(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys)
{
   return drms_keyword_mapexport(key, NULL, NULL, fitskeys);
}


int drms_keyword_mapexport(DRMS_Keyword_t *key,
			   const char *clname, 
			   const char *mapfile,
			   CFITSIO_KEYWORD **fitskeys)
{
   int stat = DRMS_SUCCESS;

   if (key && fitskeys)
   {
      char nameout[16];

      DRMS_KeyMap_t *map = NULL;
      DRMS_KeyMap_t intmap;
      FILE *fptr = NULL;
      
      if (mapfile)
      {
	 fptr = fopen(mapfile, "r");
         if (fptr)
         {
            if (drms_keymap_parsefile(&intmap, fptr))
            {
               map = &intmap;
            }

            fclose(fptr);
         }
      }
      
      if (drms_keyword_getmappedextname(key, clname, map, nameout, sizeof(nameout)))
      {
	 int fitsrwRet = 0;
         char fitskwtype = '\0';
         void *fitskwval = NULL;

	 if (!DRMSKeyValToFITSKeyVal(key, &fitskwtype, &fitskwval))
	 {
	    if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key(fitskeys, 
								   nameout, 
								   fitskwtype, 
								   NULL,
								   fitskwval)))
	    {
	       fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
	       stat = DRMS_ERROR_FITSRW;
	    }
	 }
	 else
	 {
	    fprintf(stderr, 
		    "Could not convert DRMS keyword '%s' to FITS keyword.\n", 
		    key->info->name);
	    stat = DRMS_ERROR_INVALIDDATA;
	 }

         if (fitskwval)
         {
            free(fitskwval);
         }
      }
      else
      {
	 fprintf(stderr, 
		 "Could not determine external FITS keyword name for DRMS name '%s'.\n", 
		 key->info->name);
	 stat = DRMS_ERROR_INVALIDDATA;
      }
   }

   return stat;  
}

int drms_keyword_import(CFITSIO_KEYWORD *fitskey, HContainer_t *keys)
{
   return drms_keyword_mapimport(fitskey, NULL, NULL, keys);
}

int drms_keyword_mapimport(CFITSIO_KEYWORD *fitskey,
			   const char *clname, 
			   const char *mapfile,
			   HContainer_t *keys)
{
   int stat = DRMS_SUCCESS;

   if (fitskey && keys)
   {
      char nameout[DRMS_MAXKEYNAMELEN];
      char *namelower = NULL;
      DRMS_KeyMap_t *map = NULL;
      DRMS_KeyMap_t intmap;
      FILE *fptr = NULL;

      if (mapfile)
      {
	 fptr = fopen(mapfile, "r");
         if (fptr)
         {
            if (drms_keymap_parsefile(&intmap, fptr))
            {
               map = &intmap;
            }

            fclose(fptr);
         }
      }

      if (drms_keyword_getmappedintname(fitskey->key_name, clname, map, nameout, sizeof(nameout)))
      {
         DRMS_Type_t drmskwtype;
         DRMS_Type_Value_t drmskwval;
         DRMS_Keyword_ExtType_t cast;
         DRMS_Keyword_t *newkey = NULL;

         /* If drmskwtype is string, then it is an alloc'd string, and ownership
          * gets passed all the way to the DRMS_Keyword_t in the keys. Caller of
          * drms_keyword_mapimport() must free. */
	 if (!FITSKeyValToDRMSKeyVal(fitskey, &drmskwtype, &drmskwval, &cast))
	 {
            namelower = strdup(nameout);
            strtolower(namelower);

            if ((newkey = hcon_lookup(keys, namelower)) != NULL)
            {
               if (strcmp(namelower, "comment") == 0 || strcmp(namelower, "history") == 0 )
               {
                  /* If this is a FITS comment or history keyword, then 
                   * append to the existing string;separate from previous 
                   * values with a newline character. */
                  size_t size = strlen(newkey->value.string_val) + 1;
                  newkey->value.string_val = base_strcatalloc(newkey->value.string_val, "\n", &size);
                  newkey->value.string_val = base_strcatalloc(newkey->value.string_val, 
                                                              drmskwval.string_val, 
                                                              &size);
               }
               else if (newkey->record &&
                        newkey->info &&
                        newkey->info->type == drmskwtype)
               {
                  /* key already exists in container - assume the user is trying to 
                   * copy the key value into an existing DRMS_Record_t */
                  memcpy(&(newkey->value), &drmskwval, sizeof(DRMS_Type_Value_t));
               }
               else
               {
                  fprintf(stderr, "Keyword '%s' already exists; the DRMS value is the value of the first instance .\n", nameout);
               }
            }
            else if ((newkey = (DRMS_Keyword_t *)hcon_allocslot(keys, namelower)) != NULL)
            {
               memset(newkey, 0, sizeof(DRMS_Keyword_t));
               newkey->info = calloc(1, sizeof(DRMS_KeywordInfo_t));
               memcpy(&(newkey->value), &drmskwval, sizeof(DRMS_Type_Value_t));

               snprintf(newkey->info->name, DRMS_MAXKEYNAMELEN, "%s", nameout);
               newkey->info->islink = 0;
               newkey->info->type = drmskwtype;

               /* Only write out the [fitsname:cast] if export will be confused otherwise - 
                * write it out if the fits keyword was of logical type (which is stored
                * as a DRMS type of CHAR), or if the FITS name was not a legal DRMS name
                */
               if (cast == kDRMS_Keyword_ExtType_Logical || strcmp(nameout, fitskey->key_name) != 0)
               {
                  snprintf(newkey->info->description, 
                           DRMS_MAXCOMMENTLEN, 
                           "[%s:%s]", 
                           fitskey->key_name,
                           DRMS_Keyword_ExtType_Strings[cast]);
               }

               /* guess a format, so the keywords will print with 
                * functions like drms_keyword_printval() */
               switch (drmskwtype)
               {
                  case DRMS_TYPE_CHAR:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%hhd");
                    break;
                  case DRMS_TYPE_SHORT:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%hd");
                    break;
                  case DRMS_TYPE_INT:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%d");
                    break;
                  case DRMS_TYPE_LONGLONG:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%lld");
                    break;
                  case DRMS_TYPE_FLOAT:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%f");
                    break;
                  case DRMS_TYPE_DOUBLE:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%lf");
                    break;
                  case DRMS_TYPE_STRING:
                    snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%s");
                    break;
                  default:
                    fprintf(stderr, "Unsupported keyword data type '%d'", (int)drmskwtype);
                    stat = DRMS_ERROR_INVALIDDATA;
                    break;
               }
            }
            else
            {
               fprintf(stderr, "Failed to alloc a new slot in keys container.\n");
	       stat = DRMS_ERROR_OUTOFMEMORY;
            }

            if (namelower)
            {
               free(namelower);
            }
	 }
	 else
	 {
	    fprintf(stderr, 
		    "Could not convert FITS keyword '%s' to DRMS keyword.\n", 
		    fitskey->key_name);
	    stat = DRMS_ERROR_INVALIDDATA;
	 }
      }
      else
      {
         fprintf(stderr, 
		 "Could not determine internal DRMS keyword name for FITS name '%s'.\n", 
		 fitskey->key_name);
	 stat = DRMS_ERROR_INVALIDDATA;
      }
   }

   return stat;
}

DRMS_Keyword_ExtType_t drms_keyword_getcast(DRMS_Keyword_t *key)
{
   DRMS_Keyword_ExtType_t type = kDRMS_Keyword_ExtType_None;
   char *pcast = NULL;
   int icast;

   if (key && key->info->description)
   {
      pcast = strchr(key->info->description, ':');
      if (pcast)
      {
         pcast++;

         for (icast = 0; icast < (int)kDRMS_Keyword_ExtType_End; icast++)
         {
            if (strcasecmp(pcast, DRMS_Keyword_ExtType_Strings[icast]) == 0)
            {
               break;
            }
         }

         if (icast != kDRMS_Keyword_ExtType_End)
         {
            type = (DRMS_Keyword_ExtType_t)icast;
         }
      }
   }

   return type;
}

void drms_keyword_setdate(DRMS_Record_t *rec)
{
  /* Get current date */
  TIME today = CURRENT_SYSTEM_TIME;

  /* Set the DATE_imp keyword to this value */
  if (drms_setkey_time(rec, kDATEKEYNAME, today))
  {
    fprintf(stderr, "Couldn't set DATE keyword.\n");
  }
}

TIME drms_keyword_getdate(DRMS_Record_t *rec)
{
  int status = DRMS_SUCCESS;
  TIME thedate = DRMS_MISSING_TIME;

  thedate = drms_getkey_time(rec, kDATEKEYNAME, &status);

  if (status)
  {
    fprintf(stderr, "Couldn't get DATE keyword.\n");
  }

  return thedate;
}
