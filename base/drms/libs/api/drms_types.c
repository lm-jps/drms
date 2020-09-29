//#define DEBUG

#include <string.h>
#define DRMS_TYPES_C
#include "drms.h"
#include "drms_priv.h"
#undef DRMS_TYPES_C
#include "xmem.h"
#include "timeio.h"
#include "atoinc.h"

const char kDRMS_MISSING_VALUE[] = "DRMS_MISSING_VALUE";

static HContainer_t *gSlotEpochHC = NULL;
struct TimeEpochStrings_struct
{
  DRMS_TimeEpoch_t type;
  const char *str;
  const char *timestr;
  TIME internalTime;
};

typedef struct TimeEpochStrings_struct TimeEpochStrings_t;
static TimeEpochStrings_t gSEpochS[] =
{
   {kTimeEpoch_DRMS, "DRMS_EPOCH", DRMS_EPOCH_S, DRMS_EPOCH},
   {kTimeEpoch_MDI, "MDI_EPOCH", MDI_EPOCH_S, MDI_EPOCH},
   {kTimeEpoch_WSO, "WSO_EPOCH", WSO_EPOCH_S, WSO_EPOCH},
   {kTimeEpoch_TAI, "TAI_EPOCH", TAI_EPOCH_S, TAI_EPOCH},
   {kTimeEpoch_MJD, "MJD_EPOCH", MJD_EPOCH_S, MJD_EPOCH},
   {kTimeEpoch_TSEQ, "TSEQ_EPOCH", TSEQ_EPOCH_S, TSEQ_EPOCH},
   {(DRMS_TimeEpoch_t)-99, ""}
};

/* This is the way that strtoll checks for a hex string. */
static int IsHexString(const char *str)
{
   int ishex = 0;
   char *s = strdup(str);

   /* First, skip whitespace. */
   while (isspace(*s))
   {
      ++s;
   }

   /* Then look for a '0' followed by a 'X' or 'x'. */
   if (s[0] == '0' && toupper(s[1]) == 'X')
   {
      ishex = 1;
   }

   free(s);

   return ishex;
}

DB_Type_t drms2dbtype(DRMS_Type_t type)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return DB_CHAR;
    break;
  case DRMS_TYPE_SHORT:
    return DB_INT2;
    break;
  case DRMS_TYPE_INT:
    return DB_INT4;
    break;
  case DRMS_TYPE_LONGLONG:
    return DB_INT8;
    break;
  case DRMS_TYPE_FLOAT:
    return DB_FLOAT;
    break;
  case DRMS_TYPE_DOUBLE:
  case DRMS_TYPE_TIME:
    return DB_DOUBLE;
    break;
  case DRMS_TYPE_STRING:
    return DB_STRING;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
    return DB_STRING;
    break;
  }
}

/* Copy a field in a DB_Binary_Result_t table to a field
   in  an DRMS_Type_Value_t union. */
int drms_copy_db2drms(DRMS_Type_t drms_type, DRMS_Type_Value_t *drms_dst,
		      DB_Type_t db_type, char *db_src)
{
  int len;


  switch(drms_type)
  {
  case DRMS_TYPE_CHAR:
    drms_dst->char_val = dbtype2char(db_type,db_src);
    break;
  case DRMS_TYPE_SHORT:
    drms_dst->short_val = dbtype2short(db_type,db_src);
    break;
  case DRMS_TYPE_INT:
    drms_dst->int_val = dbtype2longlong(db_type,db_src);
    break;
  case DRMS_TYPE_LONGLONG:
    drms_dst->longlong_val = dbtype2longlong(db_type,db_src);
    break;
  case DRMS_TYPE_FLOAT:
    drms_dst->float_val = dbtype2float(db_type,db_src);
    break;
  case DRMS_TYPE_DOUBLE:
    drms_dst->double_val = dbtype2double(db_type,db_src);
    break;
  case DRMS_TYPE_TIME:
    drms_dst->time_val = dbtype2double(db_type,db_src);
    break;
  case DRMS_TYPE_STRING:
    if (drms_dst->string_val)
      free(drms_dst->string_val);
    if (db_type ==  DB_STRING || db_type ==  DB_VARCHAR)
      drms_dst->string_val = strdup((char *)db_src);
    else
    {
      len = db_binary_default_width(db_type);
      drms_dst->string_val = malloc(len);
      XASSERT(drms_dst->string_val);
      dbtype2str(db_type,db_src,len,drms_dst->string_val);
    }
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)drms_type);
    XASSERT(0);
    return -1;
    break;
  }
  return 0;
}



/* Copy a field in a DB_Binary_Result_t table to a field
   in  an DRMS_Type_Value_t union. */
void drms_copy_drms2drms(DRMS_Type_t type, DRMS_Type_Value_t *dst,
			 DRMS_Type_Value_t *src)
{
  if (type==DRMS_TYPE_STRING)
    copy_string(&dst->string_val, src->string_val);
  else
    memcpy(dst, src, sizeof(DRMS_Type_Value_t));
}


DRMS_Type_t drms_str2type(const char *str)
{
  if (!strncasecmp(str,drms_type_names[DRMS_TYPE_CHAR],DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_CHAR;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_SHORT],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_SHORT;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_INT],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_INT;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_LONGLONG],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_LONGLONG;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_FLOAT],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_FLOAT;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_DOUBLE],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_DOUBLE;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_TIME],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_TIME;
  else if (!strncasecmp(str,drms_type_names[DRMS_TYPE_STRING],
			DRMS_MAXTYPENAMELEN))
    return DRMS_TYPE_STRING;
  else
  {
    fprintf(stderr, "ERROR: Unhandled DRMS type '%s'\n",str);
    XASSERT(0);
    return DRMS_TYPE_RAW;
  }
}


const char  *drms_type2str(DRMS_Type_t type)
{
  return drms_type_names[(int) type];
}


/* Return size of simple types in bytes. */
int drms_sizeof(DRMS_Type_t type)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
  case DRMS_TYPE_SHORT:
  case DRMS_TYPE_INT:
  case DRMS_TYPE_LONGLONG:
  case DRMS_TYPE_FLOAT:
  case DRMS_TYPE_DOUBLE:
  case DRMS_TYPE_TIME:
    return db_sizeof(drms2dbtype(type));
    break;
  case DRMS_TYPE_STRING:
    return sizeof(char *);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
    return 0;
    break;
  }
}

int drms_equal(DRMS_Type_t type, DRMS_Type_Value_t *x, DRMS_Type_Value_t *y)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return x->char_val == y->char_val;
    break;
  case DRMS_TYPE_SHORT:
    return x->short_val == y->short_val;
    break;
  case DRMS_TYPE_INT:
    return x->int_val == y->int_val;
    break;
  case DRMS_TYPE_LONGLONG:
    return x->longlong_val == y->longlong_val;
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(x->float_val) && isnan(y->float_val))
    {
       return 1;
    }
    else if (isnan(x->float_val) || isnan(y->float_val))
    {
       return 0;
    }
    return x->float_val == y->float_val;
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(x->double_val) && isnan(y->double_val))
    {
       return 1;
    }
    else if (isnan(x->double_val) || isnan(y->double_val))
    {
       return 0;
    }
    return x->double_val == y->double_val;
    break;
  case DRMS_TYPE_TIME:
    if (isnan(x->time_val) && isnan(y->time_val))
    {
       return 1;
    }
    else if (isnan(x->time_val) || isnan(y->time_val))
    {
       return 0;
    }
    return x->time_val == y->time_val;
    break;
  case DRMS_TYPE_STRING:
    if (x->string_val == NULL && y->string_val == NULL)
    {
       return 1;
    }
    else if (x->string_val == NULL || y->string_val == NULL)
    {
       return 0;
    }
    return !strcmp(x->string_val, y->string_val);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    return 0;
  }
}

void drms_missing(DRMS_Type_t type, DRMS_Type_Value_t *val)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    val->char_val = DRMS_MISSING_CHAR;
    break;
  case DRMS_TYPE_SHORT:
    val->short_val = DRMS_MISSING_SHORT;
    break;
  case DRMS_TYPE_INT:
    val->int_val = DRMS_MISSING_INT;
    break;
  case DRMS_TYPE_LONGLONG:
    val->longlong_val = DRMS_MISSING_LONGLONG;
    break;
  case DRMS_TYPE_FLOAT:
    val->float_val = DRMS_MISSING_FLOAT;
    break;
  case DRMS_TYPE_DOUBLE:
    val->double_val = DRMS_MISSING_DOUBLE;
    break;
  case DRMS_TYPE_TIME:
    val->time_val = DRMS_MISSING_TIME;
    break;
  case DRMS_TYPE_STRING:
    copy_string(&(val->string_val), DRMS_MISSING_STRING);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
}

void drms_missing_vp(DRMS_Type_t type, void *val)
{
 switch(type)
  {
  case DRMS_TYPE_CHAR:
    *((char *)val) = DRMS_MISSING_CHAR;
    break;
  case DRMS_TYPE_SHORT:
    *((short *)val) = DRMS_MISSING_SHORT;
    break;
  case DRMS_TYPE_INT:
    *((int *)val) = DRMS_MISSING_INT;
    break;
  case DRMS_TYPE_LONGLONG:
    *((long long *)val) = DRMS_MISSING_LONGLONG;
    break;
  case DRMS_TYPE_FLOAT:
    *((float *)val) = DRMS_MISSING_FLOAT;
    break;
  case DRMS_TYPE_DOUBLE:
    *((double *)val) = DRMS_MISSING_DOUBLE;
    break;
  case DRMS_TYPE_TIME:
    *((double *)val) = DRMS_MISSING_TIME;
    break;
  case DRMS_TYPE_STRING:
    copy_string((char **)val, DRMS_MISSING_STRING);
    break;
  default:
    fprintf(stderr, "ERROR: Unsupported DRMS type %d\n",(int)type);
    XASSERT(0);
  }
}

int drms_strval(DRMS_Type_t type, DRMS_Type_Value_t *val, char *str)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    val->char_val = str[0];
    break;
  case DRMS_TYPE_SHORT:
    val->short_val = (short)strtol(str, NULL, 0);
    break;
  case DRMS_TYPE_INT:
    val->int_val = (int)strtol(str, NULL, 0);
    break;
  case DRMS_TYPE_LONGLONG:
    val->longlong_val = strtoll(str, NULL, 0);
    break;
  case DRMS_TYPE_FLOAT:
    val->float_val = (float) atof(str);
    break;
  case DRMS_TYPE_DOUBLE:
    val->double_val = (double) atof(str);
    break;
  case DRMS_TYPE_TIME:
    val->time_val = sscan_time (str);
    break;
  case DRMS_TYPE_STRING:
    copy_string(&val->string_val, str);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
  return 0;
}





int drms_sprintfval_format(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val,
			   char *format, int internal)
{
  if (format==NULL)
    return drms_sprintfval(dst, type, val, internal);
  else
  {
    switch(type)
    {
    case DRMS_TYPE_CHAR:
      return sprintf(dst, format, val->char_val);
      break;
    case DRMS_TYPE_SHORT:
      return sprintf(dst, format, val->short_val );
      break;
    case DRMS_TYPE_INT:
      return sprintf(dst, format, val->int_val );
      break;
    case DRMS_TYPE_LONGLONG:
      return sprintf(dst, format, val->longlong_val);
      break;
    case DRMS_TYPE_FLOAT:
      return sprintf(dst, format, val->float_val);
      break;
    case DRMS_TYPE_DOUBLE:
      return sprintf(dst, format, val->double_val);
      break;
    case DRMS_TYPE_TIME:
      if (internal) 	/* Print in internal representation i.e. as double. */
	return sprintf(dst, "%lf", val->time_val);
      else
      {
	/* From timerep.c: format should be a time zone or a standard time
	   like TAI/TDT/TT/UTC. */
	sprint_time(dst, val->time_val, format, 3);
	return strlen(dst);
      }
      break;
    case DRMS_TYPE_STRING:
      return sprintf(dst, format, val->string_val);
      break;
    default:
      fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
      XASSERT(0);
    }
    return 0;
  }
}



int drms_sprintfval(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val,
		    int internal)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return sprintf(dst, "%hhd", val->char_val);
    break;
  case DRMS_TYPE_SHORT:
    return sprintf(dst, "%hd", val->short_val );
    break;
  case DRMS_TYPE_INT:
    return sprintf(dst, "%d", val->int_val );
    break;
  case DRMS_TYPE_LONGLONG:
    return sprintf(dst, "%lld", val->longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    return sprintf(dst, "%.9g", val->float_val);
    break;
  case DRMS_TYPE_DOUBLE:
    return sprintf(dst, "%.17lg", val->double_val);
    break;
  case DRMS_TYPE_TIME:
    if (!internal)
    {
      sprint_time(dst, val->time_val, "UTC", 0); /* From timerep.c */
      return strlen(dst);
    }
    else
      return sprintf(dst, "%lf", val->time_val);
    break;
  case DRMS_TYPE_STRING:
    return sprintf(dst, "'%s'", val->string_val);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
  return 0;
}



/* Print value according to type. */
int drms_printfval(DRMS_Type_t type, DRMS_Type_Value_t *val) {
   return drms_fprintfval(stdout, type, val);
}

int drms_fprintfval(FILE *keyfile, DRMS_Type_t type, DRMS_Type_Value_t *val) {
  switch(type) {
  case DRMS_TYPE_CHAR:
    return fprintf(keyfile, "%hhd", val->char_val);
    break;
  case DRMS_TYPE_SHORT:
    return fprintf(keyfile, "%hd", val->short_val );
    break;
  case DRMS_TYPE_INT:
    return fprintf(keyfile, "%d", val->int_val );
    break;
  case DRMS_TYPE_LONGLONG:
    return fprintf(keyfile, "%lld", val->longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    return fprintf(keyfile, "%15.9g", val->float_val);
    break;
  case DRMS_TYPE_TIME:
    {
      char buf[128];
      sprint_time(buf, val->time_val, "UTC", 0);
      fprintf(keyfile, "%s",buf);
    }
    break;
  case DRMS_TYPE_DOUBLE:
    return fprintf(keyfile, "%24.17lg", val->double_val);
    break;
  case DRMS_TYPE_STRING:
    return fprintf(keyfile, "'%s'", val->string_val);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
  return 0;
}


/* Print value according to type. */
int drms_printfval_raw(DRMS_Type_t type, void *val)
{
   return drms_fprintfval_raw(stdout, type, val);
}

int drms_fprintfval_raw(FILE *keyfile, DRMS_Type_t type, void *val)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return fprintf(keyfile, "%hhd", *((char *)val));
    break;
  case DRMS_TYPE_SHORT:
    return fprintf(keyfile, "%hd", *((short *)val));
    break;
  case DRMS_TYPE_INT:
    return fprintf(keyfile, "%d", *((int *)val));
    break;
  case DRMS_TYPE_LONGLONG:
    return fprintf(keyfile, "%lld", *((long long *)val));
    break;
  case DRMS_TYPE_FLOAT:
    return fprintf(keyfile, "%15.9g", *((float *)val));
    break;
  case DRMS_TYPE_TIME:
    {
      char buf[128];
      sprint_time(buf, *((double *)val), "UTC", 0);
      fprintf(keyfile, "%s",buf);
    }
    break;
  case DRMS_TYPE_DOUBLE:
    return fprintf(keyfile, "%24.17lg", *((double *)val));
    break;
  case DRMS_TYPE_STRING:
    return fprintf(keyfile, "'%s'", (char *)val);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
  return 0;
}

/* converts string representations of integers to integer values - supports hexidecimal
 * and decimal strings */
long long drms_types_strtoll(const char *str, DRMS_Type_t inttype, int *consumed, int *status)
{
   int ishex = 0;
   int istat = 0;
   long long ival = 0;
   char *endptr = 0;

   if (str)
   {
      if (inttype == DRMS_TYPE_LONGLONG)
      {
         ival = strtoll(str, &endptr, 0);

         if (ival==0 && endptr==str)
         {
            istat = 1;
         }
         else
         {
            *consumed = endptr - str;
         }
      }
      else
      {
         const char *pc = str;

         /* ignore ws */
         while (isspace(*pc))
         {
            ++pc;
         }

         /* ignore +/- for now */
         if (*pc == '+' || *pc == '-')
         {
            ++pc;
         }

         if (strstr(pc, "0X") == pc || (strstr(pc, "0x") == pc))
         {
            ishex = 1;
         }

         if (ishex)
         {
            /* For the char, short, and int ranges, ival can never be negative */
            ival = strtoll(str, &endptr, 16);
            switch (inttype)
            {
               case DRMS_TYPE_CHAR:
                 if ((ival==0 && endptr==str) || ival > UCHAR_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    ival = (char)ival;
                    *consumed = endptr - str;
                 }
                 break;
               case DRMS_TYPE_SHORT:
                 if ((ival==0 && endptr==str) || ival > USHRT_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    ival = (short)ival;
                    *consumed = endptr - str;
                 }
                 break;
               case DRMS_TYPE_INT:
                 if ((ival==0 && endptr==str) || ival > UINT_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    ival = (int)ival;
                    *consumed = endptr - str;
                 }
                 break;
               default:
                 fprintf(stderr, "ERROR: Unsupported DRMS integer type %d\n", (int)inttype);
                 istat = 1;
            }
         }
         else
         {
            /* not hex string */
            ival = strtoll(str, &endptr, 10);
            switch (inttype)
            {
               case DRMS_TYPE_CHAR:
                 if ((ival==0 && endptr==str) || ival < SCHAR_MIN || ival > SCHAR_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    *consumed = endptr - str;
                 }
                 break;
               case DRMS_TYPE_SHORT:
                 if ((ival==0 && endptr==str) || ival < SHRT_MIN || ival > SHRT_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    *consumed = endptr - str;
                 }
                 break;
               case DRMS_TYPE_INT:
                 if ((ival==0 && endptr==str) || ival < INT_MIN || ival > INT_MAX)
                 {
                    istat = 1;
                 }
                 else
                 {
                    *consumed = endptr - str;
                 }
                 break;
               default:
                 fprintf(stderr, "ERROR: Unsupported DRMS integer type %d\n", (int)inttype);
                 istat = 1;
            }
         }
      }
   }

   if (status)
   {
      *status = istat;
   }

   return ival;
}

/* scan for one instance of dsttype.  If dsttype is DRMS_STRING it is terminated by
 * the end of input string, a comma, or a right square bracket.
 */
static int drms_sscanf_int(const char *str, DRMS_Type_t dsttype, DRMS_Type_Value_t *dst, int silent, int binary)
{
    int status = DRMS_SUCCESS;
    const TIME *te = NULL;
    char *endptr = 0;
    int iival;
    long long ival;
    float fval;
    double dval;
    char cval[2];
    char *wrk = NULL;
    size_t sz_wrk = 64;
    char *bracket = NULL;
    char *comma = NULL;
    int usemissing = 0;
    int usemissinglen = strlen(kDRMS_MISSING_VALUE);
    int consumed = 0;

    if (!strncasecmp(kDRMS_MISSING_VALUE, str, usemissinglen))
    {
        usemissing = 1;
    }

    switch (dsttype)
    {
        case DRMS_TYPE_CHAR:
            if (usemissing)
            {
                dst->char_val = DRMS_MISSING_CHAR;
                return usemissinglen;
            }
            else
            {
                if (binary)
                {
                    if (sscanf(str, "%02X", (unsigned int *)&iival) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    ival = iival;
                    consumed = strlen(str);
                }
                else
                {
                    ival = drms_types_strtoll(str, DRMS_TYPE_CHAR, &consumed, &status);

                    if (status)
                    {
                        if (!silent)
                        {
                            fprintf (stderr, "Integer constant '%s' is not a signed char.\n", str);
                        }

                        return -1;
                    }
                }

                dst->char_val = (char)ival;

                return consumed;
            }
        case DRMS_TYPE_SHORT:
            if (usemissing)
            {
                dst->short_val = DRMS_MISSING_SHORT;
                return usemissinglen;
            }
            else
            {
                if (binary)
                {
                    if (sscanf(str, "%04X", (unsigned int *)&iival)== 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    ival = iival;
                    consumed = strlen(str);
                }
                else
                {
                    ival = drms_types_strtoll(str, DRMS_TYPE_SHORT, &consumed, &status);

                    if (status)
                    {
                        if (!silent)
                        {
                            fprintf (stderr, "Integer constant '%s' is not a signed short.\n", str);
                        }

                        return -1;
                    }
                }

                dst->short_val = (short)ival;

                return consumed;
            }
        case DRMS_TYPE_INT:
            if (usemissing)
            {
                dst->int_val = DRMS_MISSING_INT;
                return usemissinglen;
            }
            else
            {
                if (binary)
                {
                    if (sscanf(str, "%08X", (unsigned int *)&iival) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    ival = iival;
                    consumed = strlen(str);
                }
                else
                {
                    ival = drms_types_strtoll(str, DRMS_TYPE_INT, &consumed, &status);

                    if (status)
                    {
                        if (!silent)
                        {
                            fprintf (stderr, "Integer constant '%s' is not a signed integer.\n", str);
                        }

                        return -1;
                    }
                }

                dst->int_val = (int)ival;

                return consumed;
            }
        case DRMS_TYPE_LONGLONG:
            if (usemissing)
            {
                dst->longlong_val = DRMS_MISSING_LONGLONG;
                return usemissinglen;
            }
            else
            {
                if (binary)
                {
                    if (sscanf(str, "%016llX", (unsigned long long *)&ival) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    consumed = strlen(str);
                }
                else
                {
                    ival = drms_types_strtoll(str, DRMS_TYPE_LONGLONG, &consumed, &status);

                    if (status)
                    {
                        if (!silent)
                        {
                            fprintf (stderr, "Integer constant '%s' is not a signed long long.\n", str);
                        }

                        return -1;
                    }
                }

                dst->longlong_val = ival;

                return consumed;
          }
        case DRMS_TYPE_FLOAT:
            if (usemissing)
            {
                dst->float_val = DRMS_MISSING_FLOAT;
                return usemissinglen;
            }
            else
            {
                errno = 0;

                if (binary)
                {
                    // sscanf(str, "%A", &dval);
                    // fval = dval
                    if (sscanf(str, "%08X", (unsigned int *)&fval) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    consumed = strlen(str);
                }
                else
                {
                    fval = strtof(str, &endptr);
                    consumed = (int)(endptr-str);
                }

                if ((IsZeroF(fval) && consumed == 0) || ((IsPosHugeValF(fval) || IsNegHugeValF(fval)) && errno == ERANGE))
                {
                    if (!silent)
                    {
                        fprintf (stderr, "Real constant '%s' is not a float.\n", str);
                    }

                    return -1;
                }
                else
                {
                    dst->float_val = fval;
                }

                return consumed;
            }
        case DRMS_TYPE_DOUBLE:
            if (usemissing)
            {
                dst->double_val = DRMS_MISSING_DOUBLE;
                return usemissinglen;
            }
            else
            {
                errno = 0;

                if (binary)
                {
                    // sscanf(str, "%A", &dval);
                    if (sscanf(str, "%016llX", (unsigned long long int *)&dval) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    consumed = strlen(str);
                }
                else
                {
                    dval = strtod(str, &endptr);
                    consumed = (int)(endptr-str);
                }

                if ((IsZero(dval) && consumed == 0) || ((IsPosHugeVal(dval) || IsNegHugeVal(dval)) && errno==ERANGE))
                {
                    if (!silent)
                    {
                        fprintf (stderr, "Real constant '%s' is not a double.\n", str);
                    }

                    return -1;
                }
                else
                {
                    dst->double_val = dval;
                }

                return consumed;
            }
        case DRMS_TYPE_TIME:
            if (usemissing)
            {
                dst->time_val = DRMS_MISSING_TIME;
                return usemissinglen;
            }
            else if ((te = drms_time_getepoch(str, NULL, &status)) != NULL)
            {
                /* If this is a string identifying an epoch, use the double this refers to. */
                dst->time_val = *te;
                return strlen(str);
            }
            else
            {
                if (binary)
                {
                    // sscanf(str, "%A", &dval);
                    if (sscanf(str, "%016llX", (unsigned long long int *)&dval) == 0)
                    {
                        if (!silent)
                        {
                            fprintf(stderr, "invalid hexadecimal string\n");
                        }

                        return -1;
                    }

                    consumed = strlen(str);
                    dst->time_val= dval;
                }
                else
                {
                    // enumerate a few ways a time string terminates. this is to
                    // handle command line parameter parsing.

                    // assume the time string does not have any leading white spaces
                    /* Ack, can't do this! What if you have
                    * 1966.12.25_00:00:00_UTC,1966.12.25_00:02:00_UTC-1966.12.25_00:07:08_UTC
                    endptr = strchr(str, ' ');
                    if (!endptr) {
                    endptr = strrchr(str, '-');
                    }
                    if (!endptr) {
                    endptr = strchr(str, '/');
                    }
                    if (!endptr) {
                    endptr = strchr(str, ',');
                    }
                    if (!endptr) {
                    endptr = strchr(str, ']');
                    }
                    */
                    char *tokenstr = strdup(str);

                    if (tokenstr)
                    {
                        /* "ext"  returns the number of parsed chars */
                        consumed = sscan_time_ext(tokenstr, &dst->time_val);
                        free(tokenstr);

                        /* time_is_invalid doesn't really check for invalid time - it checks for the time
                        * being equivalent to JD_0, which can happen if the time string is really equivalent
                        * to JD_0, but it can happen also if the time string is gibberish. If a time string
                        * has a legitimate time followed by gibberish, the parser will accept the time, but
                        * it won't have consumed all the chars (but the parser will think the time is good
                        * and not set the time value to JD_0). */

                        /* So if time_is_invalid == true and NONE of the chars in the time string were consumed,
                        * you definitely have a bad time string. But time_is_invalid could be true and
                        * some of the chars in the time string may not have been consumed if JD_0 is followed by
                        * other non-time chars (eg, JD_0gibberish). Distinguishing between the latter is
                        * is up to the caller to do. */
                    }
                }

                if (time_is_invalid(dst->time_val) && consumed == 0)
                {
                    if (!silent)
                    {
                        fprintf (stderr, "Invalid time string '%s'.\n", str);
                    }

                    return -1;
                }

                return consumed;
            }
        case DRMS_TYPE_STRING:
            if (*str == '\0')
            {
                /* Empty string is acceptable. */
                dst->string_val = strdup("");
                return 0;
            }
          else if (usemissing)
          {
              dst->string_val = strdup(DRMS_MISSING_STRING);
              return usemissinglen;
          }
          else
          {
              consumed = 0;

              if (binary)
              {
                  /* this is a byte string, so a fixed number of hexadecimal digits does not exist */
                  wrk = calloc(sz_wrk, sizeof(char));

                  while (sscanf(str + consumed, "%02X", (unsigned int *)&iival) > 0)
                  {
                      snprintf(cval, sizeof(cval), "%c", iival);
                      wrk = base_strcatalloc(wrk, cval, &sz_wrk);
                      consumed += 2;
                  }

                  if (consumed == 0)
                  {
                      if (!silent)
                      {
                          fprintf(stderr, "invalid hexadecimal string\n");
                      }

                      free(wrk);
                      return -1;
                  }
              }
              else
              {
                  wrk = strdup(str);
                  bracket = strchr(wrk, '[');
                  comma = strchr(wrk, ',');

                  if (bracket)
                  {
                      if (comma)
                      {
                          if (bracket < comma)
                          {
                              *bracket = '\0';
                          }
                          else
                          {
                              *comma = '\0';
                          }
                      }
                      else
                      {
                          *bracket = '\0';
                      }
                  }
                  else if (comma)
                  {
                      *comma = '\0';
                  }

                  consumed = strlen(wrk);

                  if (consumed == 0)
                  {
                      if (!silent)
                      {
                          fprintf (stderr, "String value not found at %s.\n", str);
                      }

                      free(wrk);
                      return -1;
                  }
              }

              if (dst->string_val)
              {
                  free(dst->string_val);
                  dst->string_val = NULL;
              }

              dst->string_val = wrk;
              return consumed;
          }
      default:
      if (!silent)
        fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)dsttype);
      XASSERT(0);
    }
    return -1;
}

/* drms_sscanf has a bug - the string cannot contain commas.  But
 * there are many places in the code that rely upon this bad behavior.
 * So, don't use sscanf here - do something special for strings. */
int drms_sscanf_str3(const char *str, const char *delim, int binary, DRMS_Type_Value_t *dst)
{
    int usemissing = 0;
    int usemissinglen = strlen(kDRMS_MISSING_VALUE);
    int consumed = 0;
    char *wrk = NULL;
    size_t sz_wrk = 64;
    int ival;
    char cval[2];


    if (!strncasecmp(kDRMS_MISSING_VALUE, str, usemissinglen))
    {
        usemissing = 1;
    }

    if (*str == '\0')
    {
        /* Empty string is acceptable. */
        dst->string_val = strdup("");
        return 0;
    }
    else if (usemissing)
    {
        dst->string_val = strdup(DRMS_MISSING_STRING);
        return usemissinglen;
    }
    else
    {
        if (binary)
        {
            /* this is a byte string, so a fixed number of hexadecimal digits does not exist */
            consumed = 0;
            wrk = calloc(sz_wrk, sizeof(char));

            while (sscanf(str + consumed, "%02X", (unsigned int *)&ival) > 0)
            {
                snprintf(cval, sizeof(cval), "%c", ival);
                wrk = base_strcatalloc(wrk, cval, &sz_wrk);
                consumed += 2;
            }

            if (consumed == 0)
            {
                free(wrk);
                return -1;
            }
        }
        else
        {
            /* need a state machine to handle quoted strings that may contain
             * quotes within them */
            int state = 0; /* 0 - start
                            * 1 - not within quotes, not escaped
                            * 2 - within quotes (single or double)
                            * 3 - escaped (char follows a '\')
                            * 4 - done
                            */
            int len = strlen(str);
            char *actstr = calloc(len + 1, sizeof(char));
            char *pactstr = actstr;
            const char *pstr = str;
            int quotetype = 0; /* 1 - single
                                * 2 - double
                                */

            while (state != 4)
            {
               if (!*pstr)
               {
                  if (state == 1 || state == 0)
                  {
                     /* always end with pstr pointing to one beyond last char used */
                     state = 4;
                  }

                  /* If not state 1 or 0, then function returns -1 below */
                  break;
               }
               if (state == 0)
               {
                  while (pstr && (*pstr == ' ' || *pstr == '\t'))
                  {
                     /* strip leading whitespace */
                     pstr++;
                  }

                  if (*pstr == '\'' || *pstr == '"')
                  {
                     if (*pstr == '\'')
                     {
                        quotetype = 1;
                     }
                     else
                     {
                        quotetype = 2;
                     }

                     state = 2;
                     pstr++; /* skip special char */
                  }
                  else
                  {
                     state = 1;
                  }
               }
               else if (state == 1)
               {
                  int isdelim = 0;
                  if (delim)
                  {
                     const char *pdelim = delim;
                     while (*pdelim)
                     {
                        if (*pstr == *pdelim)
                        {
                           isdelim = 1;
                           break;
                        }

                        pdelim++;
                     }
                  }

                  if (isdelim || *pstr == ' ') /* no spaces allowed in unquoted string */
                  {
                     /* always end with pstr pointing to one beyond last char used */
                     state = 4;
                  }
                  else
                  {
                     *pactstr = *pstr;
                     pactstr++;
                     pstr++;
                  }
               }
               else if (state == 2)
               {
                  /* Can's use any stdlib function, like sscanf(), to interpret escaped chars. Escape
                   * sequences are turned into string/char values at compile time.  Must instead
                   * manually do it. For now, just support a couple. */

                  /* If inside quotes, can use escape char ('\') */
                  if (*pstr == '\\')
                  {
                     state = 3;
                     pstr++;
                  }
                  else if ((quotetype == 1 && *pstr == '\'') || (quotetype == 2 && *pstr == '"'))
                  {
                     /* end quote */
                     pstr++; /* always end with pstr pointing to one beyond last char used */
                     state = 4;
                  }
                  else
                  {
                     *pactstr = *pstr;
                     pactstr++;
                     pstr++;
                  }
               }
               else if (state == 3)
               {
                  if (*pstr == 't')
                  {
                     *pactstr = '\t';
                  }
                  else if (*pstr == 'n')
                  {
                     *pactstr = '\n';
                  }
                  else if (*pstr == '\\')
                  {
                     *pactstr = '\\';
                  }
                  else if (*pstr == '"')
                  {
                     *pactstr = '"';
                  }
                  else if (*pstr == '\'')
                  {
                     *pactstr = '\'';
                  }
                  else
                  {
                     /* escaped quotes would be here */
                     *pactstr = *pstr;
                  }

                  state = 2;
                  pactstr++;
                  pstr++;
               }
            }

            *pactstr = '\0';

            if (state != 4)
            {
                if (actstr)
                {
                    free(actstr);
                }

                return -1;
            }
            else
            {
                wrk = actstr;
            }

            consumed = pstr - str;
        }

        if (dst->string_val)
        {
            free(dst->string_val);
            dst->string_val = NULL;
        }

        if (wrk)
        {
            /* transfer ownership */
            dst->string_val = wrk;
        }

        return consumed;
    }
}

int drms_sscanf_str(const char *str, const char *delim, DRMS_Type_Value_t *dst)
{
    return drms_sscanf_str3(str, delim, 0, dst);
}

/* doesn't suffer from the limitation that strings can't contain commas and other chars */
/* ret == -1 ==> error, else ret == num chars processed; caller owns any string in *dst */
/* support binary strings */
int drms_sscanf3(const char *str, const char *delim, int silent, DRMS_Type_t dsttype, int binary, DRMS_Value_t *dst)
{
   DRMS_Type_Value_t idst;
   int err = 0;
   int ret;

   /* so drms_sscanf_str() doesn't delete something that is being used! */
   memset(&(idst), 0, sizeof(DRMS_Type_Value_t));

   if (dsttype == DRMS_TYPE_STRING)
   {
      if ((ret = drms_sscanf_str3(str, delim, binary, &idst)) < 0)
      {
         err = 1;
      }
   }
   else
   {
      ret = drms_sscanf_int(str, dsttype, &idst, silent, binary);

      if (dsttype == DRMS_TYPE_TIME)
      {
         /* Okay if nothing was consumed - sscan_time() considers any invalid string
          * as JD_0.  No other data type works like this. */
         if (ret < 0)
         {
            err = 1;
         }
      }
      else if (ret <= 0)
      {
         err = 1;
      }
   }

   if (!err)
   {
      if (dst)
      {
         (*dst).value = idst;
         (*dst).type = dsttype;
      }
   }
   else
   {
      ret = -1;
   }

   return ret;
}

/* doesn't suffer from the limitation that strings can't contain commas and other chars */
/* does not handle binary strings */
int drms_sscanf2(const char *str, const char *delim, int silent, DRMS_Type_t dsttype, DRMS_Value_t *dst)
{
    return drms_sscanf3(str, delim, silent, dsttype, 0, dst);
}

void drms_memset(DRMS_Type_t type, int n, void *array,
		 DRMS_Type_Value_t val)
{
  int i;

  switch(type)
  {
  case DRMS_TYPE_CHAR:
    { char *p;
      p  = (char *) array;
      for (i=0; i<n; i++)
	*p++ = val.char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    { short *p;
      p  = (short *) array;
      for (i=0; i<n; i++)
	*p++ = val.short_val;
    }
    break;
  case DRMS_TYPE_INT:
    { int *p;
      p  = (int *) array;
      for (i=0; i<n; i++)
	*p++ = val.int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    { long long *p;
      p  = (long long *) array;
      for (i=0; i<n; i++)
	*p++ = val.longlong_val;
    }
    break;
  case DRMS_TYPE_FLOAT:
    { float *p;
      p  = (float *) array;
      for (i=0; i<n; i++)
	*p++ = val.float_val;
    }
    break;
  case DRMS_TYPE_TIME:
  case DRMS_TYPE_DOUBLE:
    { double *p;
      p  = (double *) array;
      for (i=0; i<n; i++)
	*p++ = val.double_val;
    }
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
}


/* Return address of data value. */
void *drms_addr(DRMS_Type_t type, DRMS_Type_Value_t *val)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return (void *) &(val->char_val);
    break;
  case DRMS_TYPE_SHORT:
    return (void *) &(val->short_val);
    break;
  case DRMS_TYPE_INT:
    return (void *) &(val->int_val);
    break;
  case DRMS_TYPE_LONGLONG:
    return (void *) &(val->longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    return (void *) &(val->float_val);
    break;
  case DRMS_TYPE_TIME:
  case DRMS_TYPE_DOUBLE:
    return (void *) &(val->double_val);
    break;
  case DRMS_TYPE_STRING:
    return (void *) val->string_val;
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
  }
  return NULL;
}


/* Convert value from one DRMS type to another. */
int drms_convert(DRMS_Type_t dsttype, DRMS_Type_Value_t *dst,
		 DRMS_Type_t srctype, DRMS_Type_Value_t *src)
{
  int status;

  switch(dsttype)
  {
  case DRMS_TYPE_CHAR:
    dst->char_val = drms2char(srctype, src, &status);
    break;
  case DRMS_TYPE_SHORT:
    dst->short_val = drms2short(srctype, src, &status);
    break;
  case DRMS_TYPE_INT:
    dst->int_val = drms2int(srctype, src, &status);
    break;
  case DRMS_TYPE_LONGLONG:
    dst->longlong_val = drms2longlong(srctype, src, &status);
    break;
  case DRMS_TYPE_FLOAT:
    dst->float_val = drms2float(srctype, src, &status);
    break;
  case DRMS_TYPE_TIME:
    dst->time_val = drms2time(srctype, src, &status);
    break;
  case DRMS_TYPE_DOUBLE:
    dst->double_val = drms2double(srctype, src, &status);
    break;
  case DRMS_TYPE_STRING:
    if (dst->string_val)
      free(dst->string_val);
    dst->string_val = drms2string(srctype, src, &status);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)dsttype);
    XASSERT(0);
  }
  return status;
}



/*************** Careful conversion routines. ********************/
char drms2char(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  int ustat;
  volatile char result;
  long long val;
  char *endptr;

  result =  DRMS_MISSING_CHAR;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    stat = DRMS_SUCCESS;
    result = value->char_val;
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->short_val < SCHAR_MIN ||
	       value->short_val > SCHAR_MAX))
    {
      stat = DRMS_SUCCESS;
      result = (char) value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->int_val < SCHAR_MIN ||
	       value->int_val > SCHAR_MAX))
    {
      stat = DRMS_SUCCESS;
      result = (char) value->int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->longlong_val < SCHAR_MIN ||
	       value->longlong_val > SCHAR_MAX))
    {
      stat = DRMS_SUCCESS;
      result = (char) value->longlong_val;
    }
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->float_val < SCHAR_MIN ||
	       value->float_val > SCHAR_MAX))
    {
       result = (char)FloatToLongLong(value->float_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(value->time_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->double_val < SCHAR_MIN ||
	       value->double_val > SCHAR_MAX))
    {
      result = (char)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_CHAR;
    }
    else if (!(value->double_val < SCHAR_MIN ||
	       value->double_val > SCHAR_MAX))
    {
      result = (char)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = (long long)strtoll(value->string_val, &endptr, 0);
    if (val==0 && endptr==value->string_val)
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_CHAR;
    }
    else if ((IsHexString(value->string_val) &&
              ((val & 0xFFFFFFFFFFFFFF00) != 0) &&
              ((val & 0xFFFFFFFFFFFFFF00) != 0xFFFFFFFFFFFFFF00)) ||
             (!IsHexString(value->string_val) && (val < SCHAR_MIN || val > SCHAR_MAX)))
    {
       stat = DRMS_RANGE;
       result = DRMS_MISSING_CHAR;
    }
    else {
      stat = DRMS_SUCCESS;
      result = (char) val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_CHAR;
  }
  if (status)
    *status = stat;
  return result;
}


short drms2short(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  int ustat;
  volatile short result;
  long long val;
  char *endptr;

  result =  DRMS_MISSING_SHORT;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (short) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    stat = DRMS_SUCCESS;
    result = value->short_val;
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else if (!(value->int_val < SHRT_MIN ||
	       value->int_val > SHRT_MAX))
    {
      stat = DRMS_SUCCESS;
      result = (short) value->int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else if (!(value->longlong_val < SHRT_MIN ||
	       value->longlong_val > SHRT_MAX))
    {
      stat = DRMS_SUCCESS;
      result = (short) value->longlong_val;
    }
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else if (!(value->float_val < SHRT_MIN ||
	       value->float_val > SHRT_MAX))
    {
      result = (short)FloatToLongLong(value->float_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(value->time_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else if (!(value->double_val < SHRT_MIN ||
	       value->double_val > SHRT_MAX))
    {
      result = (short)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_SHORT;
    }
    else if (!(value->double_val < SHRT_MIN ||
	       value->double_val > SHRT_MAX))
    {
      result = (short)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = strtoll(value->string_val, &endptr, 0);
    if (val==0 && endptr==value->string_val)
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_SHORT;
    }
    else if ((IsHexString(value->string_val) &&
              ((val & 0xFFFFFFFFFFFF0000) != 0) &&
              ((val & 0xFFFFFFFFFFFF0000) != 0xFFFFFFFFFFFF0000)) ||
             (!IsHexString(value->string_val) && (val < SHRT_MIN || val > SHRT_MAX)))
    {
       stat = DRMS_RANGE;
      result = DRMS_MISSING_SHORT;
    }
    else {
      stat = DRMS_SUCCESS;
      result = (short) val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_SHORT;
  }
  if (status)
    *status = stat;
  return result;
}


int drms2int(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  int ustat;
  volatile int result;
  long long val;
  char *endptr;

  result =  DRMS_MISSING_INT;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else
    {    stat = DRMS_SUCCESS;
      result = (int) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    stat = DRMS_SUCCESS;
    result = (int) value->int_val;
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else
      if (!(value->longlong_val < INT_MIN ||
	    value->longlong_val > INT_MAX))
      {
	stat = DRMS_SUCCESS;
	result = (int) value->longlong_val;
      }

    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else  if (!(value->float_val < INT_MIN ||
		value->float_val > INT_MAX))
    {
      result = (int)FloatToLongLong(value->float_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(value->time_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else   if (!(value->double_val < INT_MIN ||
		 value->double_val > INT_MAX))
    {
      result = (int)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_INT;
    }
    else   if (!(value->double_val < INT_MIN ||
		 value->double_val > INT_MAX))
    {
      result = (int)DoubleToLongLong(value->double_val, &ustat);
      stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = strtoll(value->string_val, &endptr, 0);
    if (val==0 && endptr==value->string_val )
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_INT;
    }
    else if ((IsHexString(value->string_val) &&
              ((val & 0xFFFFFFFF00000000) != 0) &&
              ((val & 0xFFFFFFFF00000000) != 0xFFFFFFFF00000000)) ||
             (!IsHexString(value->string_val) && (val < INT_MIN || val > INT_MAX)))
    {
       stat = DRMS_RANGE;
       result = DRMS_MISSING_INT;
    }
    else {
      stat = DRMS_SUCCESS;
      result = (int) val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_INT;
  }
  if (status)
    *status = stat;
  return result;
}

long long drms2longlong(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  int ustat;
  volatile long long result;
  long long val;
  char *endptr;

  result =  DRMS_MISSING_LONGLONG;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (long long) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (long long) value->int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = value->longlong_val;
    }
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else if (!(value->float_val < LLONG_MIN ||
	       value->float_val > LLONG_MAX))
    {
       result = FloatToLongLong(value->float_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(value->time_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else if (!(value->double_val < LLONG_MIN ||
	       value->double_val > LLONG_MAX))
    {
       result = DoubleToLongLong(value->double_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else if (!(value->double_val < LLONG_MIN ||
	       value->double_val > LLONG_MAX))
    {
       result = DoubleToLongLong(value->double_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = strtoll(value->string_val, &endptr, 0);
    if (val==0 && endptr==value->string_val)
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_LONGLONG;
    }
    else {
      stat = DRMS_SUCCESS;
      result = val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_LONGLONG;
  }
  if (status)
    *status = stat;
  return result;
}

/* Don't treat missing as special. */
long long conv2longlong(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  int ustat;
  volatile long long result;
  long long val;
  char *endptr;

  result =  DRMS_MISSING_LONGLONG;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    stat = DRMS_SUCCESS;
    result = (long long) value->char_val;

    break;
  case DRMS_TYPE_SHORT:
      stat = DRMS_SUCCESS;
      result = (long long)value->short_val;
    break;
  case DRMS_TYPE_INT:
    stat = DRMS_SUCCESS;
    result = (long long) value->int_val;

    break;
  case DRMS_TYPE_LONGLONG:
    stat = DRMS_SUCCESS;
    result = value->longlong_val;

    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else if (!(value->float_val < LLONG_MIN ||
	       value->float_val > LLONG_MAX))
    {
       result = FloatToLongLong(value->float_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_TIME:
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_LONGLONG;
    }
    else if (!(value->double_val < LLONG_MIN ||
	       value->double_val > LLONG_MAX))
    {
       result = DoubleToLongLong(value->double_val, &ustat);
       stat = (ustat == kExact) ? DRMS_SUCCESS : DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = strtod(value->string_val,&endptr);
    if (val==0 && endptr==value->string_val )
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_LONGLONG;
    }
    else {
      stat = DRMS_SUCCESS;
      result = val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_LONGLONG;
  }
  if (status)
    *status = stat;
  return result;
}

float drms2float(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  volatile float result;
  float val;
  char *endptr;

  result =  DRMS_MISSING_FLOAT;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (float) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (float)value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else
    {
      result = (float) value->int_val;
      if ((int) result == value->int_val)
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else
    {
      result = (float) value->longlong_val;
      if ((long long) result == value->longlong_val)
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
      break;
    }
  case DRMS_TYPE_FLOAT:
    stat = DRMS_SUCCESS;
    result = value->float_val;
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(value->time_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else if (!(value->double_val < -FLT_MAX ||
	       value->double_val > FLT_MAX))
    {
      result = (float) value->double_val;
      if ( (double)result == value->double_val )
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_FLOAT;
    }
    else if (!(value->double_val < -FLT_MAX ||
	       value->double_val > FLT_MAX))
    {
      result = (float) value->double_val;
      if ( (double)result == value->double_val )
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_STRING:
    val = strtof(value->string_val,&endptr);
    if (IsZeroF(val) && endptr==value->string_val )
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_FLOAT;
    }
    else if ((IsPosHugeValF(val) || IsNegHugeValF(val)) && errno==ERANGE)
    {
      stat = DRMS_RANGE;
      result = DRMS_MISSING_FLOAT;
    }
    else {
      stat = DRMS_SUCCESS;
      result = (float) val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_FLOAT;
  }
  if (status)
    *status = stat;
  return result;
}



double drms2double(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  volatile double result;
  double val;
  char *endptr;

  result =  DRMS_MISSING_DOUBLE;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_DOUBLE;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_DOUBLE;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double)value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_DOUBLE;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_DOUBLE;
    }
    else
    {
      result = (double) value->longlong_val;
      if ((long long) result == value->longlong_val)
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_DOUBLE;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->float_val;
    }
    break;
  case DRMS_TYPE_TIME:
    {
       stat = DRMS_SUCCESS;
       if (drms_ismissing_time(value->time_val))
       {
	  result = DRMS_MISSING_DOUBLE;
       }
       else
       {
	  result = (double) value->time_val;
       }
    }
    break;
  case DRMS_TYPE_DOUBLE:
    stat = DRMS_SUCCESS;
    result = (double) value->double_val;
    break;
  case DRMS_TYPE_STRING:
    val = strtod(value->string_val,&endptr);
    if (IsZero(val) && endptr==value->string_val )
    {
      stat = DRMS_BADSTRING;
      result = DRMS_MISSING_DOUBLE;
    }
    else if ((IsPosHugeVal(val) || IsNegHugeVal(val)) && errno==ERANGE)
    {
      stat = DRMS_RANGE;
      result = DRMS_MISSING_DOUBLE;
    }
    else {
      stat = DRMS_SUCCESS;
      result = (double) val;
    }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_DOUBLE;
  }
  if (status)
    *status = stat;
  return result;
}

/* Unit is used only if the input is a string. That string might be a time interval, not a time string, but
 * this function should still convert the time-interval string into a double */
double drms2time(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  volatile double result;

  result =  DRMS_MISSING_TIME;
  stat = DRMS_RANGE;
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (value->char_val == DRMS_MISSING_CHAR)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->char_val;
    }
    break;
  case DRMS_TYPE_SHORT:
    if (value->short_val == DRMS_MISSING_SHORT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double)value->short_val;
    }
    break;
  case DRMS_TYPE_INT:
    if (value->int_val == DRMS_MISSING_INT)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->int_val;
    }
    break;
  case DRMS_TYPE_LONGLONG:
    if (value->longlong_val == DRMS_MISSING_LONGLONG)
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      result = (double) value->longlong_val;
      if ((long long) result == value->longlong_val)
	stat = DRMS_SUCCESS;
      else
	stat = DRMS_INEXACT;
    }
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(value->float_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->float_val;
    }
    break;
  case DRMS_TYPE_TIME:
    stat = DRMS_SUCCESS;
    result =  value->time_val;
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(value->double_val))
    {
      stat = DRMS_SUCCESS;
      result = DRMS_MISSING_TIME;
    }
    else
    {
      stat = DRMS_SUCCESS;
      result = (double) value->double_val;
    }
    break;
  case DRMS_TYPE_STRING:
      {
          TIME interval = 0;

          /* atoinc will return a number that is to be interpreted as a number of seconds. */
          interval = atoinc2(value->string_val);
          if (interval >= 0)
          {
              /* The string input is an interval, a number of seconds. */
              result = interval;
          }
          else
          {
              /* The string input is a time string. */
              result = sscan_time(value->string_val);
              /*    printf("Howdy! string='%s', result=%f, stat = %d\n",value->string_val,        result, stat); */
          }

          if (result < 0)
          {
              stat = DRMS_BADSTRING;
              result = DRMS_MISSING_TIME;
          }
          else
              stat = DRMS_SUCCESS;
      }
    break;
  default:
    stat = DRMS_RANGE;
    result = DRMS_MISSING_DOUBLE;
  }
  if (status)
    *status = stat;
  return result;
}


char *drms2string(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status)
{
  int stat;
  char *result;

  result = NULL;
  stat = DRMS_SUCCESS;
  if (type == DRMS_TYPE_STRING)
  {
    if (value->string_val)
    {
       copy_string(&result, value->string_val);
    }
  }
  else
  {
    result = malloc(30);
    XASSERT(result);
    memset(result,0,30);
    switch(type)
    {
    case DRMS_TYPE_CHAR:
      CHECKSNPRINTF(snprintf(result, 30, "%hhd", value->char_val), 30);
      break;
    case DRMS_TYPE_SHORT:
      CHECKSNPRINTF(snprintf(result, 30, "%hd", value->short_val), 30);
      break;
    case DRMS_TYPE_INT:
      CHECKSNPRINTF(snprintf(result, 30, "%d", value->int_val), 30);
      break;
    case DRMS_TYPE_LONGLONG:
      CHECKSNPRINTF(snprintf(result, 30, "%lld", value->longlong_val), 30);
      break;
    case DRMS_TYPE_FLOAT:
      CHECKSNPRINTF(snprintf(result, 30, "%15.9g", value->float_val), 30);
      break;
    case DRMS_TYPE_DOUBLE:
      CHECKSNPRINTF(snprintf(result, 30, "%24.17lg", value->double_val), 30);
      break;
    case DRMS_TYPE_TIME:
      sprint_time(result, value->double_val, "TAI", 0);
      break;
    default:
      stat = DRMS_RANGE;
      result[0] = 0;
    }
  }
  if (status)
    *status = stat;
  return result;
}


void drms_byteswap(DRMS_Type_t type, int n, char *val)
{
  if (type == DRMS_TYPE_STRING)
    return;

  byteswap(drms_sizeof(type), n, val);
}





/* Arithmetic operation y = y + alpha*x for DRMS data types. */
int drms_daxpy(DRMS_Type_t type, const double alpha, DRMS_Type_Value_t *x,
	       DRMS_Type_Value_t *y )
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    if (x->char_val == DRMS_MISSING_CHAR)
      y->char_val =  DRMS_MISSING_CHAR;
    else
      y->char_val = (char)(alpha*x->char_val + y->char_val);
    break;
  case DRMS_TYPE_SHORT:
    if (x->short_val == DRMS_MISSING_SHORT)
      y->short_val =  DRMS_MISSING_SHORT;
    else
      y->short_val = (short)(alpha*x->short_val + y->short_val);
    break;
  case DRMS_TYPE_INT:
    if (x->int_val == DRMS_MISSING_INT)
      y->int_val =  DRMS_MISSING_INT;
    else
      y->int_val = (int)(alpha*x->int_val + y->int_val);
    break;
  case DRMS_TYPE_LONGLONG:
    if (x->longlong_val == DRMS_MISSING_LONGLONG)
      y->longlong_val =  DRMS_MISSING_LONGLONG;
    else
      y->longlong_val = (long long)(alpha*x->longlong_val + y->longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    if (isnan(x->float_val))
      y->float_val =  DRMS_MISSING_FLOAT;
    else
      y->float_val = (float)(alpha*x->float_val + y->float_val);
    break;
  case DRMS_TYPE_DOUBLE:
    if (isnan(x->double_val))
      y->double_val =  DRMS_MISSING_DOUBLE;
    else
      y->double_val = (double)(alpha*x->double_val + y->double_val);
    break;
  case DRMS_TYPE_TIME:
    if (drms_ismissing_time(x->time_val))
      y->time_val =  DRMS_MISSING_TIME;
    else
      y->time_val = (double)(alpha*x->time_val + y->time_val);
    break;
  case DRMS_TYPE_STRING:
    fprintf(stderr, "DRMS_DAXPY: Operation undefined for strings.\n");
    XASSERT(0);
    return 1;
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
    return 1;
  }
  return 0;
}

const TIME *drms_time_getepoch(const char *str, DRMS_TimeEpoch_t *epochenum, int *status)
{
   int stat = DRMS_SUCCESS;
   char buf[kTIMEIO_MaxTimeEpochStr];
   TIME *ret = NULL;

   /* If the enum value is provided, it overrides the string. */
   if (epochenum && *epochenum > kTimeEpoch_Invalid && *epochenum < kTimeEpoch_END)
   {
      ret = &(gSEpochS[*epochenum].internalTime);
   }
   else
   {
      if (!gSlotEpochHC)
      {
	 gSlotEpochHC = hcon_create(sizeof(TIME),
				    kTIMEIO_MaxTimeEpochStr,
				    NULL,
				    NULL,
				    NULL,
				    NULL,
				    0);

	 if (gSlotEpochHC)
	 {
	    int i = 0;

	    while (gSEpochS[i].type != -99)
	    {
	       snprintf(buf, sizeof(buf), "%s", gSEpochS[i].str);
	       hcon_insert_lower(gSlotEpochHC, buf, &(gSEpochS[i].internalTime));
	       i++;
	    }
	 }
	 else
	 {
	    fprintf(stderr, "Error creating slot epoch string container.\n");
	    stat = DRMS_ERROR_CANTCREATEHCON;
	 }
      }

      ret = (TIME *)hcon_lookup_lower(gSlotEpochHC, str);
   }

   if (status)
   {
      *status = stat;
   }

   return ret;
}

void drms_time_term()
{
   if (gSlotEpochHC)
   {
      hcon_destroy(&gSlotEpochHC);
   }
}
