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
 *	double cmdparams_get_double (CmdParams_t *parms, char *name, int *status)
 *	float cmdparams_get_float (CmdParams_t *parms, char *name, int *status)
 *	int cmdparams_get_int (CmdParams_t *parms, char *name, int *status)
 *	int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status)
 *	int16_t cmdparams_get_int16 (CmdParams_t *parms, char *name, int *status)
 *	int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status)
 *	int64_t cmdparams_get_int64 (CmdParams_t *parms, char *name, int *status)
 *	char *cmdparams_get_str (CmdParams_t *parms, char *name, int *status)
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

static void ActValsFree(const void *data)
{
   if (data)
   {
      void *realdata = *((void **)data);
      free(realdata);
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
				  /* parse options given on the command line */
  arg = 0; 
  while (arg < argc) {
     /* save the original cmd-line */


    if (argv[arg][0] == '-' && !isdigit (argv[arg][1])) {
      if (argv[arg][1] == '-') {
        if (strcasecmp (argv[arg], "--help") == 0) return CMDPARAMS_QUERYMODE;
			/*  This and the next argument  "--name value"  */

        /* There may or may not be a value associated with a --<name> flag.
         * If not, then the argument is a flag with a name that has one or 
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
           cmdparams_set (parms, argv[arg]+2, escbuf);
           arg += 2;
        }
	else 
        {
           /* --X flags are case insensitive */
           char *fbuf = strdup(argv[arg] + 2);
           strtolower(fbuf);
           cmdparams_set(parms, fbuf, "1"); 
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
	  cmdparams_set (parms, flagbuf, "1");	  
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

             cmdparams_set (parms, argv[arg], escbuf);
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

           cmdparams_set (parms, argv[arg], escbuf);
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
	} else {		/*  An argument to pass on to the program  */
          if (parms->num_args >= parms->max_args) {
	    parms->max_args *= 2;
	    parms->args = realloc (parms->args, parms->max_args*sizeof (char*));
	  }
	  parms->args[parms->num_args++] = strdup (argv[arg]);
	  /* FIXME: Do your own buffer management, you lazy bum! */
	}
	++arg;
      }
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

static int cmdparams_initactvals(CmdParams_t *params)
{
   if (params && !params->actvals)
   {
      params->actvals = hcon_create(sizeof(void *), kKEYSIZE, ActValsFree, NULL, NULL, NULL, 0);
   }

   return (params->actvals == NULL);
}

static int cmdparams_conv2type(const char *sdata, 
                               ModuleArgs_Type_t dtype, 
                               char *bufout, 
                               int size)
{
   int status = CMDPARAMS_SUCCESS;
   char *endptr = NULL;
   int64_t intval;
   int int32val;
   float fval;
   double dval;

   switch (dtype)
   {
      case ARG_INT:
        intval = (int64_t)strtod(sdata, &endptr);
        if ((intval == 0 && endptr == sdata) || (intval < INT32_MIN || intval > INT32_MAX)) 
        {
           intval = CP_MISSING_INT;
           status = CMDPARAMS_INVALID_CONVERSION;
        } 
        else
        {
           int32val = (int32_t)intval;
           XASSERT(sizeof(int32val) <= size);
           memcpy(bufout, &int32val, sizeof(int32val));
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

static int parse_array (CmdParams_t *params, char *root, ModuleArgs_Type_t dtype, char *valist) {
  int nvals = 0, status = 0;
  char *name, *next, *nptr, *tmplist;
  char key[CP_MAXNAMELEN], val[CP_MAXNAMELEN];
  void *actvals = NULL;
  LinkedList_t *listvals = NULL;
  ListNode_t *node = NULL;
  int dsize = 0;

  /* Need to store the actual array of values, not just the char * representation, and 
   * and not a string parameter per value. */
  if (!params->actvals)
  {
     cmdparams_initactvals(params);
  }

  if (params->actvals)
  {
     /* there is no limit to the size of bytes that an individual argument value
      * can have - but since the arguments we're dealing with here are either
      * ints, floats, or doubles, 128 bytes should be sufficient. */
     listvals = list_llcreate(kARGSIZE, NULL);

     tmplist = malloc (strlen (valist) + 1);
     strcpy (tmplist, valist);
     str_compress (tmplist);			     /*  remove white space  */
     name = strip_delimiters (tmplist);			  /*  (), [], or {}  */
     if (!name) return -1;
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
        cmdparams_set (params, key, nptr);

        /* Be careful - inserttail is going to copy kARGSIZE bytes into a new list node,
         * so you can't use nptr as the final argument to inserttail, since nptr
         * might point to something less than kARGSIZE bytes.
         */
        snprintf(valbuf, sizeof(valbuf), "%s", nptr);
        list_llinserttail(listvals, valbuf);

        nvals++;
     }
     sprintf (key, "%s_nvals", root);
     sprintf (val, "%d", nvals);
     cmdparams_set (params, key, val);
     free (tmplist);


     switch(dtype)
     {
        case ARG_INTS:
          actvals = malloc(sizeof(int) * nvals);
          dtype = ARG_INT;
          dsize = sizeof(int);
          break;
        case ARG_FLOATS:
          actvals = malloc(sizeof(float) * nvals);
          dtype = ARG_FLOAT;
          dsize = sizeof(float);
          break;
        case ARG_DOUBLES:
          actvals = malloc(sizeof(double) * nvals);
          dtype = ARG_DOUBLE;
          dsize = sizeof(double);
          break;
        default:
          fprintf(stderr, "Unexpected type '%d'\n", (int)dtype);
     }

     int ival = 0;
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
        hcon_insert(params->actvals, root, &actvals);
     }

     if (listvals)
     {
        list_llfree(&listvals);
     }
  }

  return status;
}

static int parse_numerated (char *klist, char ***names) {
/*
 *  Parses an entry in the args list of type ARG_NUME and returns the number
 *    of possible values, determined from the range entry interpreted as a
 *    comma-separated set of strings, and a malloc'd array of strings
 *    corresponding to the enumeration choices
 */
  int found = 0, maxlen;
  char *next, *nptr, *tmp;

  if (!klist) return found;
  maxlen = strlen (klist);
  tmp = malloc (maxlen + 1);
  strcpy (tmp, klist);
  str_compress (tmp);				     /*  remove white space  */
  next = tmp;

  *names = (char **)malloc (maxlen * sizeof (char *));
  while (next) {
    nptr = next;
    if ((next = (char *)strchr (nptr, ',')) != NULL) {
      *next = 0;
      next++;
    }
    if (!strlen (nptr)) continue;
					      /*  nptr now points to entity  */
    (*names)[found] = (char *)malloc (strlen (nptr) + 1);
    strcpy ((*names)[found], nptr);
    found++;
  }
  free (tmp);

  return found;
}

/* works on both float and doubles */
static int parse_range_float (const char *rangein, double *min, double *max,
  int *minopen, int *maxopen) {
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
   if (min)
   {
      *min = minval;
   }

   if (max)
   {
      *max = maxval;
   }

   if (minopen)
   {
      *minopen = minisopen;
   }
   
   if (maxopen)
   {
      *maxopen = maxisopen;
   }

   if (range)
   {
      free(range);
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

  XASSERT(parms->buffer = list_llcreate(sizeof(char *), NULL));

  parms->head = 0;
  hash_init (&parms->hash, 503, 1, (int (*)(const void *, const void *))strcmp,
    hash_universal_hash);
  parms->num_args = 1;
  XASSERT(parms->args = malloc ((argc+1)*sizeof (char *)));
  parms->max_args = argc+1;
  parms->args[0] = strdup (argv[0]);

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
  if (defps) {
    while (defps->type != ARG_END) {
      if (defps->name && strlen (defps->name)) {
         /* Don't allow modules to use reserved names */
         if (cmdparams_isreserved(parms, defps->name))
         {
            fprintf(stderr, 
                    "WARNING: Default argument list specifies reserved argument '%s'.\n",
                    defps->name);
         }

	if (defps->value == NULL || strlen (defps->value) == 0) {
		/*  Flags do not need to be set or cleared in defaults list  */
	  if (defps->type == ARG_FLAG) {
	    defps++;
	    continue;
	  }
			   /*  This parameter requires a value - no default  */
	  if (!cmdparams_exists (parms, defps->name)) {
	    fprintf (stderr, "Mandatory parameter \"%s\" is missing.\n",
		defps->name);
	    return CMDPARAMS_NODEFAULT;
	  }
	} else if (!cmdparams_exists (parms, defps->name))	
					    /*  Use specified default value  */
	  cmdparams_set (parms, defps->name, defps->value);
/*  Replace the value of an enumerated type with the string representation of
				  its integer value in the enumeration list  */
/*  may be unnecessary  */
	if (defps->type == ARG_NUME) {
	  char **names;
	  char intrep[8];
	  char *cfval = cmdparams_get_str (parms, defps->name, &status);
	  int nval, nvals = parse_numerated (defps->range, &names);

	  for (nval = 0; nval < nvals; nval++)
          {
             if (!(strcmp (cfval, names[nval]))) break;
          }

	  if (nval >= nvals) {
	    fprintf (stderr,
	        "Parameter \"%s\" is out of permissible range:\n  [%s]\n",
		defps->name, defps->range);
	    return CMDPARAMS_FAILURE;
	  }
	  sprintf (intrep, "%d", nval);

          /* remove leak */
          if (names)
          {
             for (nval = 0; nval < nvals; nval++)
             {
                if (names[nval])
                {
                   free(names[nval]);
                   names[nval] = NULL;
                }
             }

             free(names);
          }

				 /*  Evidently unnecessary, but a good idea  */
	  cmdparams_remove (parms, defps->name);
	  cmdparams_set (parms, defps->name, intrep);
	}

	if (defps->type == ARG_FLOATS || 
	    defps->type == ARG_DOUBLES || 
	    defps->type == ARG_INTS) 
	{
           if ((status = parse_array (parms, defps->name, defps->type, 
				     cmdparams_get_str (parms, defps->name, NULL))))
            fprintf (stderr, "array parsing returned error\n");
	} 
	else if (defps->type == ARG_FLOAT || 
                 defps->type == ARG_DOUBLE ||
                 defps->type == ARG_INT) 
	{
	   /*  Might want to check range of numeric (and time) type arguments here,
	       once a syntax has been established  */
	  if (defps->range && strlen (defps->range)) {
	    double minvalid, maxvalid;
	    int minopen, maxopen;
	    status = parse_range_float (defps->range, &minvalid, &maxvalid,
                                        &minopen, &maxopen);
	    if (status) 
            {
               fprintf (stderr, "invalid float/int module argument range specified.\n");
               return CMDPARAMS_FAILURE;
            }
            else
            {
               double val = cmdparams_get_double(parms, defps->name, NULL);
               if (minopen && !isinf(minvalid) && val <= minvalid ||
                   !minopen && !isinf(minvalid) && val < minvalid ||
                   maxopen && !isinf(maxvalid) && val >= maxvalid ||
                   !maxopen && !isinf(maxvalid) && val > maxvalid)
               {
                  /* value lies outside range */
                  fprintf (stderr, "invalid value '%f' for argument '%s' out of range.\n", 
                           val, defps->name);
                  return CMDPARAMS_FAILURE;
               }
            }
            /*  check if isfinite (minvalid), minopen, etc. and compare as necessary  */
	  }
	}
      } else if (defps->type == ARG_VOID) {
        fprintf (stderr,
	    "Warning: Parsing of unnamed parameters in arguments list unimplemented\n");
      } else {
					       /*  This should never happen  */
        fprintf (stderr,
	    "Module Error: Unnamed parameters must be declared type VOID\n");
	return CMDPARAMS_FAILURE;
      }
      defps++;
    }
	/*  Might want to report unparsed members following end declaration  */
  }
  return hash_size (&parms->hash);  
}

void cmdparams_freeall (CmdParams_t *parms) {
  int i;
  hash_free (&parms->hash);

  if (parms->buffer)
  {
     list_llfree(&(parms->buffer));
  }
  
  for (i=0; i<parms->num_args; i++)
    free(parms->args[i]);
  free (parms->args);

  for (i = 0; i < parms->argc; i++)
  {
     if (parms->argv[i])
     {
        free(parms->argv[i]);
     }
  }
  free(parms->argv);

  hcon_destroy(&(parms->actvals));
  hcon_destroy(&(parms->reserved));
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
void cmdparams_set (CmdParams_t *parms, const char *name, const char *value) {
				/*  Insert name and value string in buffer  */
  char *hashname = strdup(name);
  char *hashvalu = strdup(value);
  list_llinserttail(parms->buffer, &hashname);
  list_llinserttail(parms->buffer, &hashvalu);
  hash_insert (&parms->hash, hashname, hashvalu);
}
				  /*  determine if a flag or keyword exists  */
int cmdparams_exists (CmdParams_t *parms, char *name) {
  char *value;
  value = cmdparams_get_str (parms, name, NULL);
  return (value != NULL);
}
					 /*  remove a named flag or keyword  */
void cmdparams_remove (CmdParams_t *parms, char *name) {
  hash_remove (&parms->hash, name);
}
		  /*  simple printing function used as argument to hash_map  */
static void print (char *name, char *value) {
  printf ("%s = %s\n", name, value);
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
  int i;
					      /*  print all named arguments  */
  printf ("****** Named command line keywords:    ******\n");
  hash_map (&parms->hash, (void (*)(const void *, const void *))print);
					    /*  print all unnamed arguments  */
  printf ("****** Unnamed command line arguments: ******\n");
  for (i=0; i<parms->num_args; i++)
    printf("$%d = %s\n", i, parms->args[i]);
}
	      /*  For the shy programmer: Return number of argument strings  */
int cmdparams_numargs (CmdParams_t *parms) {
  return parms->num_args;
}
			     /*  Return program arguments strings by number  */
char *cmdparams_getarg (CmdParams_t *parms, int num) {
  if (num < parms->num_args) return parms->args[num];
  else return NULL;
}
		      /*  Get values of keywords converted to various types  */
char *cmdparams_get_str (CmdParams_t *parms, char *name, int *status) {
  char *value;
  int arg_num;

  value = NULL;
  if (name[0] == '$') {
    if (sscanf (name+1, "%d", &arg_num) == 1)
      value = cmdparams_getarg (parms, arg_num);
  } else {
    value = (char *)hash_lookup (&parms->hash, name);
    if (value == NULL)  {
		      /*  No such value. Try to get it from the environment  */
      value = getenv (name);
      if (value!=NULL) cmdparams_set (parms, name, value);
    }
  }
  if (status)
    *status = (value) ? CMDPARAMS_SUCCESS : CMDPARAMS_UNKNOWN_PARAM;      
  return value;
}

int cmdparams_isflagset (CmdParams_t *parms, char *name) {
   if (strlen(name) > 1)
   {
      char *fbuf = strdup(name);
      int ret = 0;

      strtolower(fbuf);
      ret = cmdparams_exists(parms, fbuf);
      free(fbuf);
      return ret;
   }

  if (cmdparams_exists (parms, name)) {
    return cmdparams_get_int (parms, name, NULL);
  } else return 0;
}

int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  char *str_value, *endptr;
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
  char *str_value, *endptr;
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

int cmdparams_get_intarr(CmdParams_t *parms, char *name, int **arr, int *status)
{
   int stat = CMDPARAMS_SUCCESS;
   char buf[128];
   int **arrint = NULL;

   if (arr)
   {
      arrint = (int **)hcon_lookup(parms->actvals, name);
      *arr = *arrint;
   }

   if (status)
   {
      *status = stat;
   }

   snprintf(buf, sizeof(buf), "%s_nvals", name);
   return cmdparams_get_int(parms, buf, status);
}

int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status) {
  int stat;
  char *str_value, *endptr;
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
  char *str_value, *endptr;
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
  char *str_value, *endptr;
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

double cmdparams_get_double (CmdParams_t *parms, char *name, int *status) {
  int stat;
  char *str_value, *endptr;
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
   char buf[128];
   double **arrdbl = NULL;

   if (arr)
   {
      arrdbl = (double **)hcon_lookup(parms->actvals, name);
      *arr = *arrdbl;
   }

   if (status)
   {
      *status = stat;
   }

   snprintf(buf, sizeof(buf), "%s_nvals", name);
   return cmdparams_get_int(parms, buf, status);
}

TIME cmdparams_get_time (CmdParams_t *parms, char *name, int *status) {
  int stat;
  char *str_value;
  TIME value = CP_MISSING_TIME;

  str_value = cmdparams_get_str (parms, name, &stat);

  if (!stat) {
    value = sscan_time (str_value);
    double mtime = CP_MISSING_TIME;
    if (*(int64_t *)&value - *(int64_t *)&mtime == 0) {
      value = CP_MISSING_TIME;
      stat =  CMDPARAMS_INVALID_CONVERSION;
    }
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
char *params_get_str (CmdParams_t *parms, char *name) {
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
