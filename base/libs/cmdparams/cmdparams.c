/*
 *  cmdparams.c
 *
 *  Parse arguments/parameters appearing in the default/required list of
 *    the module, and any others appearing on the command line or in included
 *    parameter files; provide a common interface for access to parameter
 *    key/value pairs
 *
 *  Functions defined:
 *	int cmdparams_exists (CmdParams_t *parms, char *name)
 *	void cmdparams_freeall (CmdParams_t *parms)
 *	char *cmdparams_getarg (CmdParams_t *parms, int num)
 *	double cmdparams_get_double (CmdParams_t *parms, const char *name, int *status)
 *	float cmdparams_get_float (CmdParams_t *parms, char *name, int *status)
 *	int cmdparams_get_int (CmdParams_t *parms, char *name, int *status)
 *	int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status)
 *	int16_t cmdparams_get_int16 (CmdParams_t *parms, char *name, int *status)
 *	int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status)
 *	int64_t cmdparams_get_int64 (CmdParams_t *parms, char *name, int *status)
 *	char *cmdparams_get_str (CmdParams_t *parms, const char *name, int *status)
 *	TIME cmdparams_get_time (CmdParams_t *parms, char *name, int *status)
 *	int cmdparams_numargs (CmdParams_t *parms)
 *	int cmdparams_parse (CmdParams_t *parms, int argc, char **argv)
 *	int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth)
 *	void cmdparams_printall (CmdParams_t *parms)
 *	void cmdparams_remove (CmdParams_t *parms, char *name)
 *	void cmdparams_set (CmdParams_t *parms, char *name, char *value)
 *	double params_get_double (CmdParams_t *parms, char *name)
 *	float params_get_float (CmdParams_t *parms, char *name)
 *	int params_get_int (CmdParams_t *parms, char *name)
 *	int8_t params_get_int8 (CmdParams_t *parms, char *name)
 *	int16_t params_get_int16 (CmdParams_t *parms, char *name)
 *	int32_t params_get_int32 (CmdParams_t *parms, char *name)
 *	int64_t params_get_int64 (CmdParams_t *parms, char *name)
 *	char *params_get_str (CmdParams_t *parms, char *name)
 *	TIME params_get_time (CmdParams_t *parms, char *name)
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include "foundation.h"
#include "hash_table.h"
#include "cmdparams.h"
#include "cmdparams_priv.h"
#include "xassert.h"
#include "xmem.h"
#include "timeio.h"
#include "util.h"
					       /* declared module parameters */
extern ModuleArgs_t *gModArgs;

#define CP_MAXNAMELEN        (32)
#define CP_MISSING_CHAR     (SCHAR_MIN)
#define CP_MISSING_SHORT    (SHRT_MIN)
#define CP_MISSING_INT      (INT_MIN)
#define CP_MISSING_LONGLONG (LLONG_MIN)
#define CP_MISSING_FLOAT    (F_NAN)  
#define CP_MISSING_DOUBLE   (D_NAN)
#define CP_MISSING_STRING   ("")
#define CP_MISSING_TIME     (-211087684832.184)

/*
 *  Parse a list of tokens extracted from a command line or a line in an
 *    options file
 */

const double gHugeValF = HUGE_VALF;
const double gNHugeValF = -HUGE_VALF;
const double gHugeVal = HUGE_VAL;
const double gNHugeVal = -HUGE_VAL;

#define kARGSIZE     (128)
#define kKEYSIZE     (128)

/* Name can be the actual name for a named arg, or for an unnamed arg, it can be the
 * pseudo name under which the value is hashed (ie., CpUnNamEDaRg_XXX). Or, for unnamed
 * args, the name can be $XXX, where XXX is the number of the arg. Or, unammed
 * args can be identified by setting the num parameter to the the number of the arg.
 * If both name and num are set, then name takes precedence.
 */
static CmdParams_Arg_t *GetCPArg(CmdParams_t *parms, const char *name, int *num)
{
   CmdParams_Arg_t *arg = NULL;
   int arg_num = -1;

   if (name[0] == '$' || (!name && num))
   {
      char namebuf[512];

      if (name)
      {
         if (sscanf(name + 1, "%d", &arg_num) != 1)
         {
            arg_num = -1;
         }
      }
      else
      {
         arg_num = *num;
      }

      if (arg_num >=0 && arg_num < parms->numunnamed)
      {
         snprintf(namebuf, sizeof(namebuf), "%s_%03d", CMDPARAMS_MAGICSTR, arg_num);
         arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, namebuf);
      }
   }
   else 
   {
      arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, name);
   }
  
   return arg;
}

/* DEEP free arg, but don't actually free the arg structure. */
static void FreeArg(const void *data)
{
   if (data)
   {
      CmdParams_Arg_t *arg = (CmdParams_Arg_t *)data;

      /* name */
      if (arg->name)
      {
         free(arg->name);
      }

      /* cmdlinestr */
      if (arg->cmdlinestr)
      {
         free(arg->cmdlinestr);
      }

      /* strval */
      if (arg->strval)
      {
         free(arg->strval);
      }

      /* actvals */
      if (arg->actvals)
      {
         free(arg->actvals);
      }
   }
}

/* Handle a few for now - if anybody wants more, add later */
static int ResolveEscSequence(const char *str, char *resolved, int size)
{
   int err = 0;
   char *pout = resolved;
   const char *pin = str;
   int len = 0;

   if (str && resolved)
   {
      while (*pin && len < size)
      {
         if (*pin == '\\' && *(pin + 1) && len + 1 < size)
         {
            if (*(pin + 1) == 't')
            {
               *pout = '\t';
            }
            else if (*(pin + 1) == 'n')
            {
               *pout = '\n';
            }
            else if (*(pin + 1) == '\\')
            {
               *pout = '\\';
            }
            else if (*(pin + 1) == '"')
            {
               *pout = '"';
            }
            else if (*(pin + 1) == '\'')
            {
               *pout = '\'';
            }
            else
            {
               /* error - bad escape sequence*/
               err = 1;
               fprintf(stderr, "Bad escape sequence in '%s'.\n", str);
               break;
            }

            pin++;
            pin++;
            pout++;
            len++;
         }
         else
         {
            *pout = *pin;
            pin++;
            pout++;
            len++;
         }         
      }

      *pout = '\0';
   }

   return err;
}

static char *RemoveTrailSp (const char *str, int *status) {
  char *ret = NULL;
  char *cstr = strdup(str);
  int len = strlen(cstr);
  int idx = 0;

  if (!cstr) *status = CMDPARAMS_OUTOFMEMORY;
  else {
    for (idx = len - 1; idx >= 0; idx--) {
      if (cstr[idx] == ' ' || cstr[idx] == '\t')
	cstr[idx] = '\0';
       else break;
    }
    ret = strdup (cstr);
    free (cstr);
  }

  return ret;
}

static int cmdparams_parsetokens (CmdParams_t *parms, int argc, char *argv[], 
    int depth) {
  int len, arg = 0;
  char *p, flagbuf[2]={0};
  char *escbuf = NULL;
  CmdParams_Arg_t *thisarg = NULL;
  char *argsaved = NULL;

  /* parse options given on the command line */
  arg = 0; 
  while (arg < argc) {
     /* save argument since code below modified argv (which is not a good practice) */
     argsaved = strdup(argv[arg]);

    if (argv[arg][0] == '-' && !isdigit (argv[arg][1])) {
      if (argv[arg][1] == '-') {
        if (strcasecmp (argv[arg], "--help") == 0) return CMDPARAMS_QUERYMODE;
			/*  This and the next argument  "--name value"  */

        /* There may or may not be a value associated with a --<name> flag.
         * If not, then the argument is a flag with a name that has one or more
         * chars in it. */
        int valueexists = 0;

        if (arg + 1 < argc)
        {
           valueexists = 1;

           /* This argument might be an escaped string - make sure \t turns into a tab, for example */
           escbuf = malloc(strlen(argv[arg + 1]) + 128);
           if (ResolveEscSequence(argv[arg+1], escbuf, strlen(argv[arg + 1]) + 128))
           {
              if (escbuf)
              {
                 free(escbuf);
              }

              return CMDPARAMS_FAILURE;
           }

           int quoteseen = 0;
           char *pc = escbuf;

           if (pc[0] == '-' || pc[0] == '@')
           {
              valueexists = 0;
           }
           else
           {
              while (*pc)
              {
                 if (*pc == '\'' || *pc == '"')
                 {
                    quoteseen = 1;
                 }

                 if (*pc == '=' && !quoteseen)
                 {
                    valueexists = 0;
                    break;
                 }

                 pc++;
              }
           }
        }

	if (valueexists)
        {
           thisarg = cmdparams_set (parms, argv[arg]+2, escbuf);
           thisarg->cmdlinestr = strdup(argsaved);
           arg += 2;
        }
	else 
        {
           /* --X flags are case insensitive */
           char *fbuf = strdup(argv[arg] + 2);
           strtolower(fbuf);
           thisarg = cmdparams_set(parms, fbuf, "1"); 
           thisarg->cmdlinestr = strdup(argsaved);
           free(fbuf);
           arg++;
	}

        if (escbuf)
        {
           free(escbuf);
           escbuf = NULL;
        }
      } else {	
				/* argument is a list of one letter flags. */
	p = argv[arg]+1; 
	while (*p) {
	  flagbuf[0] = *p++;
	  thisarg = cmdparams_set (parms, flagbuf, "1");
          thisarg->cmdlinestr = strdup(argsaved);
	}
	++arg;
      }
    } else {
	/* Could be an "argument= value" or an "optionsfile" argument 
			or an unnamed argument to be passed to the program. */
      len = strlen (argv[arg]);
      p = argv[arg];
      while (*p && (isalnum(*p) || *p=='_' || *p=='-')) ++p;
      if (*p == '=') {
	*p = 0;
	if (p == argv[arg]+len-1 ) {		/* "argument= value" */
	  if (arg < argc-1) {
             /* This argument might be an escaped string - make sure \t turns into a tab, for example */
             escbuf = malloc(strlen(argv[arg + 1]) + 128);
             if (ResolveEscSequence(argv[arg+1], escbuf, strlen(argv[arg + 1]) + 128))
             {
                if (escbuf)
                {
                   free(escbuf);
                   escbuf = NULL;
                }

                return CMDPARAMS_FAILURE;
             }

             thisarg = cmdparams_set (parms, argv[arg], escbuf);
             thisarg->cmdlinestr = strdup(argsaved);
             arg += 2;

             if (escbuf)
             {
                free(escbuf);
                escbuf = NULL;
             }
	  } else {
	    fprintf (stderr, "The value of option %s is missing.\n", argv[arg]);
	    return -1;
	  }
	} else {				/* "argument=value" */
           /* This argument might be an escaped string - make sure \t turns into a tab, for example */
           escbuf = malloc(strlen(p + 1) + 128);
           if (ResolveEscSequence(p+1, escbuf, strlen(p + 1) + 128))
           {
              if (escbuf)
              {
                 free(escbuf);
                 escbuf = NULL;
              }

              return CMDPARAMS_FAILURE;
           }

           thisarg = cmdparams_set (parms, argv[arg], escbuf);
           thisarg->cmdlinestr = strdup(argsaved);
           ++arg;

           if (escbuf)
           {
              free(escbuf);
              escbuf = NULL;
           }
	}
      } else {
	if (argv[arg][0] == '@') {	/* This is of the form "@filename" */
	  if (cmdparams_parsefile (parms, argv[arg]+1, depth) < 0) return -1;
	} 
        else 
        {
           /*  An unnamed argument */
           thisarg = cmdparams_set(parms, NULL, argv[arg]);
           thisarg->cmdlinestr = strdup(argsaved);
	}
	++arg;
      }
    }
    
    if (argsaved)
    {
       free(argsaved);
       argsaved = NULL;
    }
  }
  return CMDPARAMS_SUCCESS;
}

static void str_compress (char *s) {
   char *source, *dest;
   source = dest = s;
   while (*source) {
      if (!isspace (*source)) {
         *dest = *source;
         dest++;
      }
      source++;
   }
   *dest = '\0';
}

static char *strip_delimiters (char *s) {
  char *match;
  switch (*s) {
    case '{': {
      if ((match = strchr (s, '}')) != NULL) *match = 0;
      else return NULL;
      return s+1;
    }
    case '[': {
      if ((match = strchr (s, ']')) != NULL) *match = 0;
      else return NULL;
      return s+1;
    }
    case '(': {
      if ((match = strchr (s, ')')) != NULL)  *match = 0;
      else return NULL;
      return s+1;
    }
    default:
      return s;
  }
}

/* Warning: converts ints to 64-bit numbers only. Cast to 32-bit, 16-bit, or 8-bit if needed. */
static int cmdparams_conv2type(const char *sdata, 
                               ModuleArgs_Type_t dtype, 
                               char *bufout, 
                               int size)
{
   int status = CMDPARAMS_SUCCESS;
   char *endptr = NULL;
   int64_t intval;
   float fval;
   double dval;

   switch (dtype)
   {
      case ARG_INT:
        /* 64-bit ints */
        intval = (int64_t)strtoll(sdata, &endptr, 0);
        if ((intval == 0 && endptr == sdata)) 
        {
           intval = CP_MISSING_INT;
           status = CMDPARAMS_INVALID_CONVERSION;
        } 
        else
        {
           XASSERT(sizeof(intval) <= size);
           memcpy(bufout, &intval, sizeof(intval));
        }

        break;
      case ARG_FLOAT:
        fval = strtof(sdata, &endptr);
        if ((*endptr != '\0' || endptr == sdata) || 
            ((IsPosHugeValF(fval) || IsNegHugeValF(fval)) && errno == ERANGE))
        {
           status = CMDPARAMS_INVALID_CONVERSION;
           fval = CP_MISSING_FLOAT;
        } 
        else
        {
           XASSERT(sizeof(fval) <= size);
           memcpy(bufout, &fval, sizeof(fval));
        }

        break;
      case ARG_DOUBLE:
        dval = strtod(sdata, &endptr);
        if ((*endptr != '\0' || endptr == sdata) || 
            ((IsPosHugeVal(dval) || IsNegHugeVal(dval)) && errno == ERANGE))
        {
           status = CMDPARAMS_INVALID_CONVERSION;
           dval = CP_MISSING_DOUBLE;
        } 
        else
        {
           XASSERT(sizeof(dval) <= size);
           memcpy(bufout, &dval, sizeof(dval));
        }

        break;
      default:
        fprintf(stderr, "Incomplete implementation - type '%d' not supported.\n", (int)dtype);
   }

   return status;
}

/* Prior to this function call, each array will have been saved as a comma-delimited string of 
 * values, in an argument with the name of "root". This function then parses that comma-delimited 
 * string into an array of actual, typed values, and places that resulting array into the arg's
 * actvals array.  So far so good.
 *
 * But then, it does this thing I don't like where it creates one brand new argument for each
 * item in the array. That seems wasteful - you can access the values via the array (described
 * above).
 */
static int parse_array (CmdParams_t *params, const char *root, ModuleArgs_Type_t dtype, const char *valist) {
  int nvals = 0, status = 0;
  char *name, *next, *nptr, *tmplist;
  char key[CP_MAXNAMELEN], val[CP_MAXNAMELEN];
  void *actvals = NULL;
  LinkedList_t *listvals = NULL;
  ListNode_t *node = NULL;
  int dsize = 0;
  CmdParams_Arg_t *thisarg = NULL;
  ModuleArgs_Type_t origdtype = dtype;

  /* Need to store the actual array of values, not just the char * representation, and 
   * and not a string parameter per value. */

  /* there is no limit to the size of bytes that an individual argument value
   * can have - but since the arguments we're dealing with here are either
   * ints, floats, or doubles, 128 bytes should be sufficient. */
  listvals = list_llcreate(kARGSIZE, NULL);

  tmplist = malloc (strlen (valist) + 1);
  strcpy (tmplist, valist);
  str_compress (tmplist);			     /*  remove white space  */
  name = strip_delimiters (tmplist);			  /*  (), [], or {}  */
  if (!name) return -1;

  switch (dtype)
  {
     case ARG_INTS:
       /* make this an array of 64-bit ints */
       dtype = ARG_INT;
       dsize = sizeof(int64_t);
       break;
     case ARG_FLOATS:
       dtype = ARG_FLOAT;
       dsize = sizeof(float);
       break;
     case ARG_DOUBLES:
       dtype = ARG_DOUBLE;
       dsize = sizeof(double);
       break;
     default:
       fprintf(stderr, "Unexpected type '%d'\n", (int)dtype);
  }

  /* dtype has been converted from the array type to the type of a single element
   * (eg, ARG_INTS to ARG_INT). */
  /*
   *  name should contain a comma separated list of entities of the given type
   */
  char valbuf[kARGSIZE];
  next = name;
  while (next) {
     nptr = next;
     if ((next = (char *)strchr (nptr, ',')) != NULL) {
        *next = 0;
        next++;
     }
     if (!strlen (nptr)) continue;
     /*  nptr now points to entity  */
     snprintf(key, sizeof(key), "%s_%d_value", root, nvals);
     thisarg = cmdparams_set(params, key, nptr);
     thisarg->type = dtype;

     /* Be careful - inserttail is going to copy kARGSIZE bytes into a new list node,
      * so you can't use nptr as the final argument to inserttail, since nptr
      * might point to something less than kARGSIZE bytes.
      */
     snprintf(valbuf, sizeof(valbuf), "%s", nptr);
     list_llinserttail(listvals, valbuf);

     nvals++;
  }

  /* Create yet another argument to hold the number of items in the array. Make it have type ARG_INT. */
  sprintf (key, "%s_nvals", root);
  sprintf (val, "%d", nvals);
  thisarg = cmdparams_set (params, key, val);
  thisarg->type = ARG_INT;
  free (tmplist);

  /* Create actvals, an array of the data values - this gets saved as a field in the root argument structure. */
  int ival = 0;
  actvals = malloc(dsize * nvals);
  list_llreset(listvals);
  while ((node = list_llnext(listvals)) != NULL)
  {
     if ((status = cmdparams_conv2type(node->data, 
                                       dtype, 
                                       (char *)(actvals) + dsize * ival, 
                                       dsize)) != CMDPARAMS_SUCCESS)
     {
        break;
     }

     ival++;
  }

  if (!status)
  {
     thisarg = (CmdParams_Arg_t *)hcon_lookup(params->args, root); /* should find root, got parsed in cmdparams_parsetokens() */
     if (thisarg)
     {
        thisarg->type = origdtype;
        if (thisarg->actvals)
        {
           /* For whatever reason, the caller re-parsed the root argument - free the old array, then 
            * use the new one. */
           free(thisarg->actvals);
           thisarg->actvals = NULL;
           thisarg->nelems = 0;
        }
        
        thisarg->actvals = actvals;
        thisarg->nelems = ival;
     }
     else
     {
        fprintf(stderr, "Cannot locate expected argument'%s'.\n", root);
        status = CMDPARAMS_FAILURE;
     }
  }

  if (listvals)
  {
     list_llfree(&listvals);
  }

  return status;
}

static int parse_numerated (CmdParams_t *parms, const char *argname, char *klist, const char *val) {
/*
 *  Parses an entry in the args list of type ARG_NUME. Ensures that
 *  user-provided value (val) is a member of the enumeration (klist).
 *  Returns CMDPARAMS_SUCCESS on success.
 */
  int maxlen;
  char *next, *nptr, *tmp;
  int foundit;
  char intrep[8];
  int item;
  CmdParams_Arg_t *thisarg = NULL;

  if (!klist) return CMDPARAMS_SUCCESS; /* If no klist provided, then whatever val is is acceptable, 
                                         * AND val is not converted to its enum value. */
  maxlen = strlen (klist);
  tmp = malloc (maxlen + 1);
  strcpy (tmp, klist);
  str_compress (tmp);				     /*  remove white space  */
  next = tmp;

  foundit = -1;
  item = 0;
  while (next) {
    nptr = next;
    if ((next = (char *)strchr (nptr, ',')) != NULL) {
      *next = 0;
      next++;
    }
    if (!strlen (nptr)) continue;
					      /*  nptr now points to entity  */
    if (strcmp(nptr, val) == 0)
    {
       foundit = item;
    }

    item++;
  }
  free (tmp);

  if (foundit < 0) 
  {
     fprintf (stderr, "Parameter \"%s\" is out of permissible range:\n  [%s]\n", argname, klist);
     return CMDPARAMS_FAILURE;
  }
  else
  {
     sprintf (intrep, "%d", foundit);

     /* replace the enumeration key string (eg, "red") with the index value (eg, 3). */
     thisarg = GetCPArg(parms, argname, NULL);
     XASSERT(thisarg);
     free(thisarg->strval);
     thisarg->strval = strdup(intrep);
     thisarg->type = ARG_NUME;
  }

  return CMDPARAMS_SUCCESS;
}

/* works on both float and doubles */
static int parse_range_float (CmdParams_t *parms, const char *name, const char *rangein) {
   /*
    *  Parse a numeric range identifier string according to the rule
    *    range = {'[' | '('}{number},{number}{']' | ')'}
    *    where the optional left (right) bracket or left (right) parenthesis
    *    specifies a closed or open interval for the minimum (maximum) (closed
    *    by default) and a missing (minimum) maximum number is interpreted as
    *    (-)dInfinity
    */

   /* can't modify rangein - this is a pointer into the global, static 
    * default argument array.
    */
   double minval = 0;
   double maxval = 0;
   int minisopen = 0;
   int maxisopen = 0;
   char buf[128];
   int nchars;
   char *pbuf = NULL;
   int err = 0;
   CmdParams_Arg_t *thisarg = NULL;

   if (rangein && *rangein)
   {
      char *range = strdup(rangein);
      char *pc = range;
      int st = 0; /* 0 - parsing optional left bracket 
                   * 1 - parsing min
                   * 2 - parsing max
                   * 3 - parsing optional right bracket
                   * 4 - all expected tokens parsed
                   */

      while (*pc)
      {
         if (*pc == ' ')
         {
            /* always skip spaces */
            pc++;
         }
         else if (st == 0)
         {
            if (*pc == '[')
            {
               pc++; /* by default, already a closed interval */
               *buf = '\0';
               pbuf = buf;
               nchars = 0;
               st = 1;
            }
            else if (*pc == '(')
            {
               pc++;
               minisopen = 1;
               *buf = '\0';
               pbuf = buf;
               nchars = 0;
               st = 1;
            }
            else 
            {
               /* assume that there is no bracket, so the min is closed */
               *buf = '\0';
               pbuf = buf;
               nchars = 0;
               st = 1;
            }
         }
         else if (st == 1)
         {
            if (*pc != ',')
            {
               if (nchars < sizeof(buf) - 1)
               {
                  *pbuf++ = *pc++;
                  nchars++;
               }
            }
            else
            {
               pc++;
               *pbuf = '\0';
               if (strlen(buf) > 0)
               {
                  sscanf(buf, "%lf", &minval);
               }
               else
               {
                  /* no min value, so min is -infinity */
                  minval = D_NEG_INF;
               }
               *buf = '\0';
               pbuf = buf;
               nchars = 0;
               st = 2;
            }
         }
         else if (st == 2)
         {
            if (*pc != ')' && *pc != ']')
            {
               if (nchars < sizeof(buf) - 1)
               {
                  *pbuf++ = *pc++;
                  nchars++;
               }
            }
            else
            {
               *pbuf = '\0';
               if (strlen(buf) > 0)
               {
                  sscanf(buf, "%lf", &maxval);
               }
               else
               {
                  maxval = D_INF;
               }
               *buf = '\0';
               pbuf = buf;
               nchars = 0;
               st = 3;
            }
         }
         else if (st == 3)
         {
            /* found either ')' or ']', so set maxisopen */
            if (*pc == ')')
            {
               maxisopen = 1;
            }

            *pc++;
            st = 4;
         }
         else if (st == 4)
         {
            /* if already found a right bracket, and now there is something
             * other than a space, then this is an error */
            err = 1;
         }
      }

      if (st == 0 || st == 1)
      {
         err = 1;
      }
      else if (st == 2)
      {
         *pbuf = '\0';
         if (strlen(buf) > 0)
         {
            sscanf(buf, "%lf", &maxval);
         }
         else
         {
            maxval = D_INF;
         }
      }
      else if (st == 3)
      {
         /* never found a right bracket - assume closed interval */
         maxisopen = 0;
      }
 
      /*  look for required comma separator and parse min and max vals separately  */
      /* This function doesn't look fully implemented,
         but unused min/max are causing compiler warnings. */
      if (range)
      {
         free(range);
      }
   

      if (!err)
      {
         double val = cmdparams_get_double(parms, name, NULL);
         thisarg = GetCPArg(parms, name, NULL);
         XASSERT(thisarg); /* must exist */
         thisarg->accessed = 0; /* cmdparams_get_double() sets the accessed flag */

         if (minisopen && !isinf(minval) && val <= minval ||
             !minisopen && !isinf(minval) && val < minval ||
             maxisopen && !isinf(maxval) && val >= maxval ||
             !maxisopen && !isinf(maxval) && val > maxval)
         {
            /* value lies outside range */
            fprintf (stderr, "invalid value '%f' for argument '%s' out of range.\n", val, name);
            err = CMDPARAMS_FAILURE;
         }
      }
   }
   else
   {
      /* no range provide - the argument value is acceptable */
   }

   return err;
}
				    /*  Parse command line and option files  */
int cmdparams_parse (CmdParams_t *parms, int argc, char *argv[]) {
   /* defps is provided by the module. cmdparams shouldn't assume that this structure
    * exists. Modules and cmdparams should be separate entities. Instead, the 
    * cmdparams_parse() function should have defps as a parameter.
    *
    * Another thing that doesn't make sense: if defps defines the type of an argument, then 
    * that argument should 'collapse' to that type - it shouldn't remain indeterminate (a string).
    * But all arguments remain strings.  So if you define 'myvar' to be of type ARG_INT, 
    * then on the cmd-line you provide myvar=3.9, cmdparams should at least issue a warning, 
    * and then it should round that value to something sensible (eg, 4). When you request the
    * int value for myvar, it should return 4. If you request the value as a double, cmdparams
    * should return 4.0, not 3.9 as it currently does.
    */
  ModuleArgs_t *defps = gModArgs;
  int status;
  CmdParams_Arg_t *thisarg = NULL;

 /* Initialize parms->args container. */
  parms->args = hcon_create(sizeof(CmdParams_Arg_t), kKEYSIZE, (void (*)(const void *))FreeArg, NULL, NULL, NULL, 0);

  if (!parms->args)
  {
     cmdparams_freeall(parms);
     return CMDPARAMS_OUTOFMEMORY;
  }

  /* keep track of the parsed default argument values - store pointers to the ModuleArgs_t structures. */
  parms->defps = hcon_create(sizeof(ModuleArgs_t *), kKEYSIZE, NULL, NULL, NULL, NULL, 0);

  if (!parms->defps)
  {
     cmdparams_freeall(parms);
     return CMDPARAMS_OUTOFMEMORY;
  }

  parms->szargs = 8;
  parms->argarr = malloc(parms->szargs * sizeof(CmdParams_Arg_t *));

  /* Put program path into parms->args */
  thisarg = cmdparams_set(parms, NULL, argv[0]);
  thisarg->cmdlinestr = strdup(argv[0]);
  thisarg->accessed = 1; /* cmdparams_getargument() should say this has been accessed already. */
  thisarg->type = ARG_STRING;

  /* save original cmd-line */
  int iarg = 0;
  XASSERT(parms->argv = malloc(sizeof(char *) * argc));
  memset(parms->argv, 0, sizeof(char *) * argc);
  while (iarg < argc)
  {
     parms->argv[iarg] = strdup(argv[iarg]);
     iarg++;
  }

  parms->argc = iarg;

  /*  Parse all command line tokens (including contents of @references) */
  if ((status = cmdparams_parsetokens (parms, argc-1, argv+1, 0)))
    return status;
  if (cmdparams_exists (parms, "H") &&
      cmdparams_get_int (parms, "H", NULL) != 0)
    return CMDPARAMS_QUERYMODE;
					  /* Parse declared argument values. */
  if (defps) 
  {
    while (defps->type != ARG_END)
    {
      if (defps->name && strlen (defps->name)) 
      {
         /* Don't allow modules to use reserved names */
         if (cmdparams_isreserved(parms, defps->name))
         {
            fprintf(stderr, 
                    "WARNING: Default argument list specifies reserved argument '%s'.\n",
                    defps->name);
         }

	if (defps->value == NULL || strlen (defps->value) == 0) 
        {
           /*  Flags do not need to be set or cleared in defaults list  */
	  if (defps->type == ARG_FLAG) {
	    defps++;
	    continue;
	  }

          /*  This parameter requires a value - no default  */
	  if (!cmdparams_exists (parms, defps->name)) 
          {
	    fprintf (stderr, "Mandatory parameter \"%s\" is missing.\n",
		defps->name);
	    return CMDPARAMS_NODEFAULT;
	  }
	} 
        else if (!cmdparams_exists (parms, defps->name))
        {	
           /*  Use specified default value if the argument value was not provided on the cmd-line */
           thisarg = cmdparams_set (parms, defps->name, defps->value);
           thisarg->type = defps->type;
        }

        /* At this point, every arg in the default list now exists in parms. */

        /*  Replace the value of an enumerated type with the string representation of
            its integer value in the enumeration list  */
        /*  may be unnecessary  */
	if (defps->type == ARG_NUME) 
        {
           const char *cfval = NULL;

           thisarg = GetCPArg(parms, defps->name, NULL);
           XASSERT(thisarg); /* must exist */
           cfval = thisarg->strval;
           if (parse_numerated (parms, defps->name, defps->range, cfval) != CMDPARAMS_SUCCESS)
           {
              return CMDPARAMS_FAILURE;
           }
	}

        /* Still in default-value handler */
	if (defps->type == ARG_FLOATS || 
	    defps->type == ARG_DOUBLES || 
	    defps->type == ARG_INTS) 
	{
           thisarg = GetCPArg(parms, defps->name, NULL);
           XASSERT(thisarg); /* must exist */
           if ((status = parse_array (parms, defps->name, defps->type, thisarg->strval)))
           {
              fprintf (stderr, "array parsing returned error\n");
              return CMDPARAMS_FAILURE;
           }
	} 
	else if (defps->type == ARG_FLOAT || 
                 defps->type == ARG_DOUBLE ||
                 defps->type == ARG_INT) 
	{
	   /*  Might want to check range of numeric (and time) type arguments here,
	       once a syntax has been established  */
	  if (defps->range && strlen (defps->range)) {
	    status = parse_range_float (parms, defps->name, defps->range);
	    if (status) 
            {
               fprintf (stderr, "invalid float/int module argument range specified.\n");
               return CMDPARAMS_FAILURE;
            }

            /*  check if isfinite (minvalid), minopen, etc. and compare as necessary  */
	  }
	}
      } 
      else if (defps->type == ARG_VOID) 
      {
        fprintf (stderr,
	    "Warning: Parsing of unnamed parameters in arguments list unimplemented\n");
      } 
      else 
      {
					       /*  This should never happen  */
        fprintf (stderr,
	    "Module Error: Unnamed parameters must be declared type VOID\n");
	return CMDPARAMS_FAILURE;
      }

      /* insert into parms->defps */
      hcon_insert(parms->defps, defps->name, &defps);

      defps++;
    }
	/*  Might want to report unparsed members following end declaration  */
  }

  return hcon_size(parms->args);  
}

void cmdparams_freeall (CmdParams_t *parms) {
  int i;

  /* args */
  hcon_destroy(&(parms->args));

  /* argarr */
  free(parms->argarr);
  parms->argarr = NULL;

  /* argv */
  for (i = 0; i < parms->argc; i++)
  {
     if (parms->argv[i])
     {
        free(parms->argv[i]);
     }
  }
  free(parms->argv);
  parms->argv = NULL;

  /* argc */
  parms->argc = 0;
  
  /* numargs */
  parms->numargs = 0;

  /* szargs */
  parms->szargs = 0;

  /* numunnamed */
  parms->numunnamed = 0;

  /* reserved */
  hcon_destroy(&(parms->reserved));

  /* default values */
  hcon_destroy(&(parms->defps));
}

/*
 *  Parse option file. 
 *  Each non-comment line is parsed as a command line.
 *  Empty lines and lines starting with '#' are ignored.
 */
#undef SKIPWS
#undef ISBLANK
#define SKIPWS(p) {while(*p && isspace(*p)) { ++p; }}
#define SKIPBS(p) {while(*p && !isspace(*p)) { ++p; }}

int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth) {
  FILE *fp;
  char *p;
  int argc;
  char **argv;
  char *linebuf;
  
  if (depth > CMDPARAMS_MAXNEST) {
    fprintf (stderr,"Error: Option include files are nested more than %d "
	"levels deep. Please check for circular references.\n",
	CMDPARAMS_MAXNEST);
    return -1;
  } else {
    if ((fp = fopen (filename, "r"))) {
      XASSERT((linebuf = malloc (2*CMDPARAMS_MAXLINE)));
      memset (linebuf, 0,2*CMDPARAMS_MAXLINE);
      XASSERT((argv = malloc (2*CMDPARAMS_MAXLINE*sizeof (char *))));
      while ((p = fgets(linebuf, CMDPARAMS_MAXLINE, fp))) {
	if (linebuf[0] != '#' && linebuf[0] != 0 && linebuf[0] != '\n') {
	  argc = 0;
	  p = linebuf;
	  SKIPWS(p);
	  while (*p) {					  /*  start of word  */
	    char quote = 0;
            if (*p == '"' || *p == '\'') {
				    /*  start of quoted word, remove quotes  */
	      quote= *p++;
	      argv[argc++] = p;
	      while (*p && *p!=quote) ++p;
	      if (*p != quote) {
		fprintf(stderr,
		    "Unbalanced quotes (%c) in command line file %s\n",
		    quote, filename);
		return -1;
	      }
	      *p++ = '\0';
	    } else {				 /*  start of unquoted word  */
	      argv[argc++] = p;
	      while (*p && *p != '=' && !isspace (*p)) ++p;
              if (*p == '=') {
			/*  can be quote or char after = and word continues  */
		p++;
		if (*p == '"' || *p == '\'') {
				    /*  start of quoted word, remove quotes  */
		  quote = *p;
		  while (*(p+1) && *(p+1) != quote) {
		    *p = *(p+1);
		    p++;
		  }
		  *p++ = '\0';
		  if (*p != quote) {
		    fprintf (stderr,
		        "Unbalanced quotes (%c) in command line file %s\n",
			quote, filename);
		    return -1;
		  }
		  p++;
		} else {	    /*  if char is next then word continies  */
                  if (*p && !isspace (*p))
	          SKIPBS(p);
                }
	      }
	    }
	    // SKIPBS(p);
	    if (*p) *p++ = 0;
	    SKIPWS(p);
	  }  
	  if (cmdparams_parsetokens (parms, argc, argv, depth+1)) {
	    free (linebuf);
	    free (argv);
	    fclose (fp);
	    return -1;
	  }
	}
      }
      free (linebuf);
      free (argv);
      fclose (fp);
    } else {
      fprintf (stderr, "Couldn't open option file '%s'\n", filename);
      return -1;
    }
  }
  return 0;
}

#undef SKIPWS
#undef ISBLANK
						/*  Add a new keyword  */

/* Now handles the special processing of ARG_NUME, ARG_INTS, ARG_FLOATS, ARG_DOUBLES, ARG_FLOAT, ARG_DOUBLE, ARG_INT 
 * IF the argument is a member of the default list (which specifies the argument type). */
CmdParams_Arg_t *cmdparams_set(CmdParams_t *parms, const char *name, const char *value) 
{
   /*  Insert name and value string in buffer  */
   CmdParams_Arg_t *ret = NULL;
   CmdParams_Arg_t arg;
   int setargarr = 0;

   /* Make a new arg structure */
   memset(&arg, 0, sizeof(arg));

   /* The type will be filled in by the default value structure. */

   arg.strval = strdup(value);
   
   if (name && strlen(name))
   {
      arg.name = strdup(name);

      if ((ret = hcon_lookup(parms->args, arg.name)))
      {
         /* If the argument already exists, then go ahead and modify its value (the second
          * cmdparams_set() call for a argument 'wins'). */
         FreeArg((void *)ret);
         *ret = arg;
      }
      else
      {
         hcon_insert(parms->args, name, &arg);
         ret = (CmdParams_Arg_t *)hcon_lookup(parms->args, name);
         setargarr = 1;
      }
   }
   else
   {
      /* If no name, then this is an unnamed arg; create a dummy name so it can be place into the parms->args container. */
      char namebuf[512];
      snprintf(namebuf, sizeof(namebuf), "%s_%03d", CMDPARAMS_MAGICSTR, parms->numunnamed);

      if (hcon_lookup(parms->args, namebuf))
      {
         /* This shouldn't happen - unnamed args never collide with each other. */
         fprintf(stderr, "Unexpected argument-name collision for '%s'.\n", namebuf);
      }
      else
      {
         hcon_insert(parms->args, namebuf, &arg);
         ret = (CmdParams_Arg_t *)hcon_lookup(parms->args, namebuf);
         setargarr = 1;
         ret->unnamednum = parms->numunnamed++;
      }
   }

   if (setargarr)
   {
      if (parms->numargs + 1 > parms->szargs)
      {
         /* expand argarr buffer */
         int origsz = parms->szargs;

         parms->szargs *= 2;
         parms->argarr = realloc(parms->argarr, parms->szargs * sizeof(CmdParams_Arg_t *));
         memset(&parms->argarr[origsz], 0, (parms->szargs - origsz) * sizeof(CmdParams_Arg_t *));
      }

      parms->argarr[parms->numargs++] = ret;
   }

   /* Don't free arg.strval - it belongs to parms->args now. */

   /* type-specific actions */
   ModuleArgs_Type_t argtype = ARG_END;
   ModuleArgs_t **pmodarg = NULL;

   if (parms->defps && name)
   {
      pmodarg = hcon_lookup(parms->defps, name);
   }

   if (pmodarg)
   {
      argtype = (*pmodarg)->type;

      if (argtype == ARG_INTS || argtype == ARG_FLOATS || argtype == ARG_DOUBLES)
      {
         parse_array(parms, name, argtype, value);
      }
      else if (argtype == ARG_NUME && (*pmodarg)->range)
      {
         parse_numerated(parms, name, (*pmodarg)->range, value);
      }
      else if ((argtype == ARG_FLOAT || argtype == ARG_DOUBLE || argtype == ARG_INT) && (*pmodarg)->range)
      {
         parse_range_float(parms, name, (*pmodarg)->range);
      }
   }

   return ret;
}
				  /*  determine if a flag or keyword exists  */
int cmdparams_exists (CmdParams_t *parms, char *name) {
   int exists = 0;
   CmdParams_Arg_t *arg = GetCPArg(parms, name, NULL);

   if (!arg)
   {
      if (getenv(name) != NULL)
      {
         /* It exists in the environment, so when the user calls cmdparams_get...()
          * it will exist.*/
         exists = 1;
      }
   }
   else
   {
      exists = 1;
   }

   return exists;
}
					 /*  remove a named flag or keyword  */
void cmdparams_remove (CmdParams_t *parms, char *name) {
   CmdParams_Arg_t *arg = NULL;
   CmdParams_Arg_t **old = NULL;
   CmdParams_Arg_t **newarr = NULL;
   int iarg;
  
   arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, name);
   old = parms->argarr;
   newarr = (CmdParams_Arg_t **)malloc(sizeof(CmdParams_Arg_t *) * parms->numargs);

   /* Unfortunately, need to do a linear search, but there won't be that many args */
   iarg = 0;
   while (*old != arg && iarg < parms->numargs)
   {
      newarr[iarg++] = *old++;
   }
   
   if (iarg == parms->numargs)
   {
      fprintf(stderr, "Internal error: missing argument in parms->argarr.\n");
   }

   /* iarg is the index of the arg being removed */
   old++;
   while (iarg < parms->numargs - 1)
   {
      newarr[iarg++] = *old++;
   }

   free(parms->argarr);
   parms->argarr = newarr;

   hcon_remove(parms->args, name);
   parms->numargs--;
}
		  /*  simple printing function used as argument to hash_map  */
static void Argprint(const void *data) 
{
   CmdParams_Arg_t *arg = (CmdParams_Arg_t *)data;

   char *name = arg->name;
   int unnamednum = arg->unnamednum;
   char *value = arg->strval;

   if (name)
   {
      printf("%s = %s\n", name, value);
   }
   else
   {
      printf("unnamed arg %d = %s\n", unnamednum, value);
   }
}

static char *argtypename (int type) {
  switch (type) {
    case (ARG_INT):
      return "int";
    case (ARG_INTS):
      return "int array";
    case (ARG_FLOAT):
      return "float";
    case (ARG_FLOATS):
      return "float array";
    case (ARG_DOUBLE):
      return "double";
    case (ARG_DOUBLES):
      return "double array";
    case (ARG_STRING):
      return "string";
    case (ARG_TIME):
      return "time";
    case (ARG_NUME):
      return "selection";
    case (ARG_DATASET):
      return "dataset descriptor";
    case (ARG_DATASERIES):
      return "dataseries name";
    case (ARG_NEWDATA):
      return "created data set";
    case (ARG_FLAG):
      return "flag";
    default:
      return "?";
  }
}
						    /*  print usage message  */
void cmdparams_usage (char *module) {
  ModuleArgs_t *arg = gModArgs;
  int flagct = 0, length = strlen (module) + 7;

  printf ("usage: %s", module);
  while (arg->type != ARG_END) {
    if (arg->type == ARG_FLAG) {
      if (!flagct) printf (" [-");
      printf ("%s", arg->name);
      flagct++;
    }
    arg++;
  }
  if (flagct) {
    printf ("]");
    length += 4 + flagct;
  }
  arg = gModArgs;
  while (arg->type != ARG_END) {
    if (arg->type != ARG_FLAG && arg->type != ARG_VOID) {
      printf (" %s=...", arg->name);
      length += strlen (arg->name) + 5;
      if (length > 64) {
        printf ("\n        ");
	length = 8;
      }
    }
    arg++;
  }
  printf ("\n");
  arg = gModArgs;
  while (arg->type != ARG_END) {
    if (arg->type == ARG_FLAG) {
      if (arg->description)
        printf ("  -%s:  %s\n", arg->name, arg->description);
    }
    arg++;
  }
  arg = gModArgs;
  while (arg->type != ARG_END) {
    if (arg->type != ARG_FLAG && arg->type != ARG_VOID) {
      if (arg->description && strlen (arg->description))
        printf ("  %s (%s): %s\n    ", arg->name, argtypename (arg->type),
	    arg->description);
      else printf ("  %s (%s): ", arg->name, argtypename (arg->type));
      if (arg->value && strlen (arg->value))
        printf ("default value: %s\n", arg->value);
      else printf ("(no default)\n");
      if (arg->type == ARG_NUME) printf ("    valid values: %s\n", arg->range);
    }
    arg++;
  }
}
		 /*  print all command flags, keywords and argument strings  */
void cmdparams_printall (CmdParams_t *parms) {
   hcon_map(parms->args, Argprint);
}

/* Return number of unnamed arguments  */
int cmdparams_numargs (CmdParams_t *parms) {
  return parms->numunnamed;
}

/*  Return program UNNAMED arguments strings by number  */
const char *cmdparams_getarg (CmdParams_t *parms, int num) {
  char namebuf[512];
  CmdParams_Arg_t *arg = NULL;

  snprintf(namebuf, sizeof(namebuf), "%s_%03d", CMDPARAMS_MAGICSTR, num);

  if (num < parms->numunnamed)
  {
     arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, namebuf);
  }
  
  if (arg)
  {
     arg->accessed = 1;
     return arg->strval;
  }
  else
  {
     return NULL;
  }
}

const char *cmdparams_getargument(CmdParams_t *parms, 
                                  int num, 
                                  const char **name, 
                                  const char **value, 
                                  const char **cmdlinestr, 
                                  int *accessed)
{
   const char *valuein = NULL;
   CmdParams_Arg_t *arg = NULL;

   if (name)
      {
         *name = NULL;
      }

      if (value)
      {
         *value = NULL;
      }

      if (cmdlinestr)
      {
         *cmdlinestr = NULL;
      }

      if (accessed)
      {
         *accessed = 0;
      }

   if (num < parms->numargs)
   {
      arg = parms->argarr[num];
      valuein = arg->strval;

      if (name)
      {
         *name = arg->name; /* NULL if unnamed. */
      }

      if (value)
      {
         *value = arg->strval;
      }

      if (cmdlinestr)
      {
         *cmdlinestr = arg->cmdlinestr;
      }

      if (accessed)
      {
         *accessed = arg->accessed;
      }
   }

   return valuein;
}


		      /*  Get values of keywords converted to various types  */
const char *cmdparams_get_str(CmdParams_t *parms, const char *name, int *status) {
   const char *value = NULL;
   CmdParams_Arg_t *arg = NULL;

   arg = GetCPArg(parms, name, NULL);
   if (arg)
   {
      value = arg->strval;
      arg->accessed = 1;
   }
   else
   {
      /*  No such value. Try to get it from the environment  */
      value = getenv(name);
      if (value != NULL) 
      {
         arg = cmdparams_set(parms, name, value);
         arg->accessed = 1;
      }
   }

   if (status)
     *status = (value) ? CMDPARAMS_SUCCESS : CMDPARAMS_UNKNOWN_PARAM;      
  
   return value;
}

int cmdparams_isflagset (CmdParams_t *parms, char *name) {
   CmdParams_Arg_t *arg = NULL;

   arg = GetCPArg(parms, name, NULL);

   if (arg)
   {
      arg->accessed = 1;
   }

   if (strlen(name) > 1)
   {
      return (arg != NULL);
   }

   if (arg)
   {
      return cmdparams_get_int(parms, name, NULL);
   }
   else
   {
      return 0;
   }
}
//xxxxx
int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  int64_t val;
  int8_t value = CP_MISSING_CHAR;

  str_value = cmdparams_get_str (parms, name, &stat);
  
  if (!stat) {
    val = (int64_t)strtod(str_value, &endptr);
    if ((val==0 && endptr==str_value) || (val < INT8_MIN || val > INT8_MAX)) {
      value = CP_MISSING_CHAR;
      stat = CMDPARAMS_INVALID_CONVERSION;
    } else {
      value = (int8_t) val;
      stat = CMDPARAMS_SUCCESS;
    }
  }
  if (status) *status = stat;

  return value;
}

int16_t cmdparams_get_int16 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  int64_t val;
  int16_t value = CP_MISSING_SHORT;

  str_value = cmdparams_get_str (parms, name, &stat);
  
  if (!stat) {
    val = (int64_t)strtod(str_value, &endptr);
    if ((val==0 && endptr==str_value) || (val < INT16_MIN || val > INT16_MAX)) {
      value = CP_MISSING_SHORT;
      stat = CMDPARAMS_INVALID_CONVERSION;
    } else {
      value = (int16_t) val;
      stat = CMDPARAMS_SUCCESS;
    }
  }
  if (status) *status = stat;

  return value;
}

int cmdparams_get_int (CmdParams_t *parms, char *name, int *status) {
  return cmdparams_get_int32 (parms, name, status);
}

static int cmdparams_get_ints(CmdParams_t *parms, char *name, int64_t **arr, int *status)
{
   int stat = CMDPARAMS_SUCCESS;
   int64_t *arrint = NULL;
   CmdParams_Arg_t *arg = NULL;
   int nelems = 0;

   if (arr)
   {
      arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, name);

      if (arg)
      {
         if (arg->type != ARG_INTS)
         {
            fprintf(stderr, "Argument '%s' is not an array of integers.\n", name);
            stat = CMDPARAMS_INVALID_CONVERSION;
         }
         else
         {
            arrint = arg->actvals;
            *arr = arrint;
         }
      }
      else
      {
         fprintf(stderr, "Unknown keyword name '%s'.\n", name);
         stat = CMDPARAMS_UNKNOWN_PARAM;
      }

      if (!stat)
      {
         nelems = arg->nelems;
      }
   }
   
   if (status)
   {
      *status = stat;
   }

   return nelems;
}

/* int arrays are stored as 64-bit numbers, so if a 32-bit int is desired, a 32-bit array
 * must be allocated (and freed by the caller). */
int cmdparams_get_intarr(CmdParams_t *parms, char *name, int **arr, int *status)
{
   int stat = CMDPARAMS_SUCCESS;
   int nelems = 0;
   int64_t *arr64 = NULL;
   int oneint64;
   int oneint;
   int iint;

   if (arr)
   {
      nelems = cmdparams_get_ints(parms, name, &arr64, &stat);
      if (stat == CMDPARAMS_SUCCESS)
      {
         /* arr64 is a pointer to the internal 64-bit array - don't free! */
         *arr = (int *)malloc(nelems * sizeof(int));
         
         /* must copy each 64-bit number into a 32-bit one manually - I guess check for overflow. */
         for (iint = 0; iint < nelems; iint++)
         {
            oneint64 = arr64[iint];

            if (oneint64 <= INT32_MAX && oneint64 >= INT32_MIN)
            {
               oneint = (int)oneint64;
               (*arr)[iint] = oneint;
            }
            else
            {
               stat = CMDPARAMS_INVALID_CONVERSION;
               break;
            }
         }
      }
   }

   if (status)
   {
      *status = stat;
   }

   return nelems;
}

/* To be consistent with cmdparams_get_intarr(), allocate the returned array. The caller
 * must free the returned array in parameter arr. */
int cmdparams_get_int64arr(CmdParams_t *parms, char *name, int64_t **arr, int *status)
{
   int stat = CMDPARAMS_SUCCESS;
   int nelems = 0;
   int64_t *arr64 = NULL;

   if (arr)
   {
      nelems = cmdparams_get_ints(parms, name, &arr64, &stat);
      if (stat == CMDPARAMS_SUCCESS)
      {
         /* arr64 is a pointer to the internal 64-bit array - don't free! */
         *arr = (int64_t *)malloc(nelems * sizeof(int64_t));
         memcpy(*arr, arr64, nelems * sizeof(int64_t));
      }
   }

   if (status)
   {
      *status = stat;
   }

   return nelems;
}

int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  int64_t val;
  int32_t value = CP_MISSING_INT;

  str_value = cmdparams_get_str (parms, name, &stat);

  if (!stat) {
    val = (int64_t)strtod(str_value, &endptr);
    if ((val==0 && endptr==str_value) || (val < INT32_MIN || val > INT32_MAX)) {
      value = CP_MISSING_INT;
      stat = CMDPARAMS_INVALID_CONVERSION;
    } else {
      value = (int32_t) val;
      stat = CMDPARAMS_SUCCESS;
    }
  }
  if (status) *status = stat;

  return value;  
}

int64_t cmdparams_get_int64 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  int64_t val;
  int64_t value = CP_MISSING_LONGLONG;

  str_value = cmdparams_get_str (parms, name, &stat);
 
  if (!stat) {
    val = (int64_t)strtod(str_value, &endptr);
    if ((val==0 && endptr==str_value)) {
      value = CP_MISSING_LONGLONG;
      stat = CMDPARAMS_INVALID_CONVERSION;
    } else {
      value = val;
      stat = CMDPARAMS_SUCCESS;
    }
  }
  if (status) *status = stat;

  return value;
}



float cmdparams_get_float (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  double val;
  float value = CP_MISSING_FLOAT;
  char *strvalNtsp = NULL;

  str_value = cmdparams_get_str (parms, name, &stat);
  if (!stat)
  {
     strvalNtsp = RemoveTrailSp(str_value, &stat);
  }
  
  if (!stat) {
    XASSERT(sizeof(int64_t) == sizeof(double));
    val = strtod (strvalNtsp, &endptr);
    if ((*endptr != '\0' || endptr==strvalNtsp) ||
       ((*(int64_t *)&val - *(int64_t *)&gHugeValF == 0 || 
	 *(int64_t *)&val - *(int64_t *)&gNHugeValF == 0) && 
	 errno == ERANGE)) {
      stat = CMDPARAMS_INVALID_CONVERSION;
      value = CP_MISSING_FLOAT;
    } else {
      value = (float) val;
      stat = CMDPARAMS_SUCCESS;
    }
  }
  if (status) *status = stat;

  if (strvalNtsp)
  {
     free(strvalNtsp);
  }

  return value;
}

double cmdparams_get_double (CmdParams_t *parms, const char *name, int *status) {
  int stat;
  const char *str_value;
  char *endptr;
  double val;
  double value = CP_MISSING_DOUBLE;
  char *strvalNtsp = NULL;

  str_value = cmdparams_get_str (parms, name, &stat);

  if (!stat)
  {
     strvalNtsp = RemoveTrailSp(str_value, &stat);
  }

  if (!stat) {
    XASSERT(sizeof(int64_t) == sizeof(double));
    val = strtod(strvalNtsp,&endptr);
    if ((*endptr != '\0' || endptr==strvalNtsp) ||
       ((*(int64_t *)&val - *(int64_t *)&gHugeVal == 0 || 
	 *(int64_t *)&val - *(int64_t *)&gNHugeVal == 0) && 
	 errno == ERANGE)) {
      stat = CMDPARAMS_INVALID_CONVERSION;
      value = CP_MISSING_DOUBLE;
    } else {
      stat = CMDPARAMS_SUCCESS;
      value = (double) val;
    }
  }
  if (status) *status = stat;

  if (strvalNtsp)
  {
     free(strvalNtsp);
  }

  return value;
}

int cmdparams_get_dblarr(CmdParams_t *parms, char *name, double **arr, int *status)
{
   int stat = CMDPARAMS_SUCCESS;
   double *arrint = NULL;
   CmdParams_Arg_t *arg = NULL;
   int nelems = 0;

   if (arr)
   {
      arg = (CmdParams_Arg_t *)hcon_lookup(parms->args, name);

      if (arg)
      {
         if (arg->type != ARG_DOUBLES)
         {
            fprintf(stderr, "Argument '%s' is not an array of doubles.\n", name);
            stat = CMDPARAMS_INVALID_CONVERSION;
         }
         else
         {
            arrint = (double *)arg->actvals;
            *arr = arrint;
         }
      }
      else
      {
         fprintf(stderr, "Unknown keyword name '%s'.\n", name);
         stat = CMDPARAMS_UNKNOWN_PARAM;
      }

      if (!stat)
      {
         nelems = arg->nelems;
      }
   }
   
   if (status)
   {
      *status = stat;
   }

   return nelems;
}

TIME cmdparams_get_time (CmdParams_t *parms, char *name, int *status) {
  int stat;
  const char *str_value;
  TIME value = CP_MISSING_TIME;

  str_value = cmdparams_get_str (parms, name, &stat);

  if (!stat) {
     char *tmp = strdup(str_value);
     value = sscan_time (tmp);
     double mtime = CP_MISSING_TIME;
     if (*(int64_t *)&value - *(int64_t *)&mtime == 0) {
        value = CP_MISSING_TIME;
        stat =  CMDPARAMS_INVALID_CONVERSION;
     }
     free(tmp);
  }

  if (status) *status = stat;

  return value;
}

void cmdparams_get_argv(CmdParams_t *params,  char *const **argv, int *argc)
{
   if (argv)
   {
      *argv = params->argv;
   }
   if (argc)
   {
      *argc = params->argc;
   }
}

static void FreeReserved(const void *pitem)
{
   if (pitem)
   {
      char *item = *((char **)pitem);
      if (item)
      {
         free(item);
      }
   }
}

void cmdparams_reserve(CmdParams_t *params, const char *reserved, const char *owner)
{
   if (reserved && owner)
   {
      if (params && params->reserved == NULL)
      {
         params->reserved = hcon_create(sizeof(char *), 
                                        kKEYSIZE, 
                                        (void (*)(const void *))FreeReserved, 
                                        NULL, 
                                        NULL, 
                                        NULL, 
                                        0);
      }

      char *orig = strdup(reserved);
      char *buf = orig;
      char *pc = orig;
      char *val = NULL;
      int adv = 1;
   
      if (orig && params->reserved)
      {
         while (adv)
         {
            if (*pc == '\0')
            {
               adv = 0;
            }
            else if (*pc == ',')
            {
               *pc = '\0';
            }

            if (*pc == '\0')
            {
               val = strdup(owner);
               if (strlen(buf) > 1)
               {
                  /* flags with names greater than a single char are case-insensitive */
                  hcon_insert_lower(params->reserved, buf, &val);
               }
               else
               {
                  hcon_insert(params->reserved, buf, &val);
               }

               if (adv)
               {
                  buf = pc + 1;
               }
            }

            if (adv)
            {
               pc++;
            }
         }

         free(orig);
      }
   }
}

int cmdparams_isreserved(CmdParams_t *params, const char *key)
{
   int ret = 0;

   if (params && params->reserved)
   {
      if (strlen(key) > 1)
      {
         ret = (hcon_member_lower(params->reserved, key));
      }
      else
      {
         ret = (hcon_member(params->reserved, key));
      }
   }

   return ret;
}

			/*  Get values of keywords converted to various types,
						 but without status options  */
const char *params_get_str (CmdParams_t *parms, char *name) {
  return cmdparams_get_str (parms, name, NULL);
}

int params_isflagset (CmdParams_t *parms, char *name) {
   return cmdparams_isflagset(parms, name);
}

int8_t params_get_int8 (CmdParams_t *parms, char *name) {
  return cmdparams_get_int8 (parms, name, NULL);
}

int16_t params_get_int16 (CmdParams_t *parms, char *name) {
  return cmdparams_get_int16 (parms, name, NULL);
}

int32_t params_get_int32 (CmdParams_t *parms, char *name) {
  return cmdparams_get_int32 (parms, name, NULL);
}

int64_t params_get_int64 (CmdParams_t *parms, char *name) {
  return cmdparams_get_int64 (parms, name, NULL);
}

char params_get_char (CmdParams_t *parms, char *name) {
  return cmdparams_get_int8 (parms, name, NULL);
}

short params_get_short (CmdParams_t *parms, char *name) {
  return cmdparams_get_int16 (parms, name, NULL);
}

int params_get_int (CmdParams_t *parms, char *name) {
  return cmdparams_get_int32 (parms, name, NULL);
}

float params_get_float (CmdParams_t *parms, char *name) {
  return cmdparams_get_float (parms, name, NULL);
}

double params_get_double (CmdParams_t *parms, char *name) {
  return cmdparams_get_double (parms, name, NULL);
}

TIME params_get_time (CmdParams_t *parms, char *name) {
  return cmdparams_get_time (parms, name, NULL);
}

CmdParams_Arg_t *cmdparams_getargstruct(CmdParams_t* parms, const char *name)
{
   return GetCPArg(parms, name, NULL);
}
