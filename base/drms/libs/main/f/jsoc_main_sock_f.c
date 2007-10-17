#ifdef FLIB
#ifdef DEBUG_MEM
#include "xmem.h"
#endif
#include "cfortran.h"
#include "jsoc_main.h"
#include "inthandles.h"


			 /* Global structure holding command line parameters */
CmdParams_t cmdparams;
ModuleArgs_t module_args[];
	  /*  Declarations/Definitions of module name and default arguments  */

							    /*  Module name  */
typedef struct {
  char name[127];
} MNameB;
#define mnb COMMON_BLOCK(mnb,mname)
COMMON_BLOCK_DEF(MNameB,mnb);
MNameB mnb; /* struct holding module name Fortran string */

char *module_name;	         /* module name C string */
						 /* Default module arguments */
typedef struct {
  char args[128][927]; /* Assumes max args is 128. */
} MArgsB;
#define mab COMMON_BLOCK(mab,margs)
COMMON_BLOCK_DEF(MArgsB,mab);
MArgsB mab; /* default modules args C strings */

/*  DoIt() Module entry point - defined in <modulename>.c or <modulename>.f  */
PROTOCCALLSFFUN0(INT, DoIt, doit) /* return type, C function name, F function name*/
#define DoIt() CCALLSFFUN0(DoIt, doit) /* C function name, F function name */

/* *****Fortran-module handling functions***** */
/* Fortran-modules define default arguments an array (margs) of strings of length 927.
 * This array is defined in <modulename>.f  Each element of this array corresponds
 * to one argument and is a string representation of the ModuleArgs_t structure.
 * The final argument must be assigned the value "end" to indicate there are no
 * more arguments.  This array is global and is read in jsoc_main.c where it
 * is converted to a dynamic array of ModuleArgs_t structures (gModArgs).
 */

/* Map from argument type token (specified in .f) to ModuleArgs_t type. */
#define kMaxTokenSize            64

static HContainer_t *gMargTokenMap = NULL;

/* TokenToArgType() - Converts string representation of argument type to enum value.
 * Used when parsing margs  */
static ModuleArgs_Type_t TokenToArgType (const char *token) {
   if (gMargTokenMap == NULL) {
							     /*  Initialize. */
      gMargTokenMap = hcon_create(sizeof(int), kMaxTokenSize, NULL, NULL, NULL, NULL, 0);
      if (!gMargTokenMap) {
			       /*  couldn't create token map, can't proceed. */
	 fprintf (stderr, "Couldn't create token map, bailing out\n");
	 exit (1);
      }

      int value = ARG_END;
      int nArgTypes = 0;
      hcon_insert(gMargTokenMap, "end", &value);
      nArgTypes++;

      value = ARG_FLAG;
      hcon_insert(gMargTokenMap, "flag", &value);
      nArgTypes++;

      value = ARG_TIME;
      hcon_insert(gMargTokenMap, "time", &value);
      nArgTypes++;

      value = ARG_INT; 
      hcon_insert(gMargTokenMap, "int", &value);
      nArgTypes++;

      value = ARG_FLOAT;
      hcon_insert(gMargTokenMap, "float", &value);
      nArgTypes++;

      value = ARG_DOUBLE;
      hcon_insert(gMargTokenMap, "double", &value);
      nArgTypes++;

      value = ARG_STRING;
      hcon_insert(gMargTokenMap, "string", &value);
      nArgTypes++;

      value = ARG_VOID;
      hcon_insert(gMargTokenMap, "void", &value);
      nArgTypes++;

      value = ARG_INTS;
      hcon_insert(gMargTokenMap, "ints", &value);
      nArgTypes++;

      value = ARG_FLOATS;
      hcon_insert(gMargTokenMap, "floats", &value);
      nArgTypes++;

      value = ARG_DOUBLES;
      hcon_insert(gMargTokenMap, "doubles", &value);
      nArgTypes++;

      value = ARG_NUME;
      hcon_insert(gMargTokenMap, "nume", &value);
      nArgTypes++;

      value = ARG_DATASET;
      hcon_insert(gMargTokenMap, "dataset", &value);
      nArgTypes++;

      value = ARG_DATASERIES;
      hcon_insert(gMargTokenMap, "dataseries", &value);
      nArgTypes++;

      value = ARG_NEWDATA;
      hcon_insert(gMargTokenMap, "newdata", &value);
      nArgTypes++;

      if (nArgTypes != ARG_NUMARGS)
      {
	 /* couldn't create token map, can't proceed. */
	 fprintf(stderr, "Missing items in token map, bailing out\n");
	 exit(1);
      }
   }

   char t[kMaxTokenSize];
   strncpy(t, token, kMaxTokenSize - 1);
   t[kMaxTokenSize - 1] = '\0';
   strtolower(t);

   ModuleArgs_Type_t *atype = hcon_lookup(gMargTokenMap, t);
     
   if (atype)
   {
      return *atype;
   }
   else
   {
      fprintf(stderr, "Bad argument type in .f file\n");
      exit(1);
   }
}

/* C functions that wrap around the JSOC C API functions.  They are directly callable
 * from Fortran code.  Each then calls the corresponding JSOC C API function.  Results
 * are passed back to Fortran code by reference. */
void f_cmdparams_get_handle(pFHandleCmdParams handle)
{
   int err = 1;

   FHandleCmdParams cphandle = kgModCmdParams;
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)&cphandle, kFHandleTypeCmdParams);

   if (pCP == NULL)
   {
      /* The one global CmdParams_t has not been inserted into the Fortran-interface structure 
       * map.*/
      CmdParams_t *cp = GetGlobalCmdParams();
      const char *key = NULL;
      err = InsertJSOCStructure((void *)&cphandle, (void *)cp, kFHandleTypeCmdParams, &key);
   }
   else
   {
      err = 0;
   }

   if (!err)
   {
      *handle = kgModCmdParams;
   }
   else
   {
      *handle = -1;
   }
}

char *f_cmdparams_gethandle2()
{
   return "blahblah";
}

FCALLSCSUB1(f_cmdparams_get_handle, cpgethandle, cpgethandle, PINT)
FCALLSCFUN0(STRING, f_cmdparams_gethandle2, CPGETHANDLE2, cpgethandle2)

/* Top-level function that initializes all containers holding structures used within 
 * the Fortran doit function.  Each type of structure is contained within a single
 * container.  This function initializes each of these containers with a single
 * function call of the form FInit<structuretype>().*/
static void InitFortranInterfaces()
{
   InthandlesInit(sizeof(CmdParams_t *));
}

/* Top-level function that releases all memory allocated during initialization of 
 * the Fortran interface.  In particular, the C-structure containers are freed.  
 * For each type of structure's container, this function calls a function of the
 * form FTerm<structuretype>(). */
static void TerminateFortranInterfaces()
{
   InthandlesTerm();
}

void JSOCMAIN_F_Initialize(int *nargsOut)
{
  int iMarg = 0;
  /* Copy Fortran strings into C strings. */
  char cMArgs[128][928];
  char cMArgsCopy[128][928]; /* parsing tokens is destructive */

  module_name = (char *)malloc(128);

  FCB2CSTR(mnb.name, module_name, 0);
  FCB2CSTR(mab.args, cMArgs, 1);

  memcpy(cMArgsCopy, cMArgs, sizeof(cMArgsCopy));

  /* Must fill in gModArgs from margs */
  char *oneLine = NULL;
  char *thisToken = NULL;
  
  /* count args */
  ModuleArgs_Type_t argType = ARG_END;
  int nArgs = 0;
 
  while (1) {
     argType = TokenToArgType (strtok (cMArgsCopy[nArgs], ","));
     if (argType != ARG_END) nArgs++;
     else break;
  }

  gModArgs = (ModuleArgs_t *)malloc (sizeof(ModuleArgs_t) * (nArgs + 1));
  memset(gModArgs, 0, sizeof(ModuleArgs_t) * (nArgs +1));

  iMarg = 0;
  while (iMarg <= nArgs) {
     oneLine = cMArgs[iMarg];

     thisToken = strtok (oneLine, ",");
     
     if (thisToken) {
	gModArgs[iMarg].type = TokenToArgType (strcmp (thisToken, "\"\"") == 0 ? "" : thisToken);
	thisToken = strtok (NULL, ",");
     }
     
     if (thisToken) {
	gModArgs[iMarg].name = strdup (strcmp (thisToken, "\"\"") == 0 ? "" : thisToken);
	thisToken = strtok (NULL, ",");
     }
     
     if (thisToken) {
	gModArgs[iMarg].value = strdup (strcmp (thisToken, "\"\"") == 0 ? "" : thisToken);
	thisToken = strtok (NULL, ",");
     }
     
     if (thisToken) {
	gModArgs[iMarg].description = strdup (strcmp (thisToken, "\"\"") == 0 ? "" : thisToken);
	thisToken = strtok (NULL, ",");
     }
     
     if (thisToken) {
	gModArgs[iMarg].range = strdup (strcmp (thisToken, "\"\"") == 0 ? "" : thisToken);
	thisToken = strtok (NULL, ",");
     }

     iMarg++;
  }

  InitFortranInterfaces();

  if (nargsOut)
  {
     *nargsOut = nArgs;
  }
}

void JSOCMAIN_F_Terminate(int nArgs)
{
  TerminateFortranInterfaces();

  /* Free memory associated with allocated module_args. */
  int iMarg = 0;
  while (iMarg < nArgs)
  {
     ModuleArgs_t *oneArg = gModArgs + iMarg;
     
     free(oneArg->name);
     free(oneArg->value);
     free(oneArg->description);
     free(oneArg->range);

     iMarg++;
  }

  free(gModArgs);

  hcon_destroy(&gMargTokenMap);
}

int CallDoIt()
{
   return DoIt();
}

int main(int argc, char **argv) 
{
   int ret = 1;
   int nArgs = 0;

   JSOCMAIN_F_Initialize(&nArgs);
   ret = JSOCMAIN_Main(argc, argv, module_name, CallDoIt);
   JSOCMAIN_F_Terminate(nArgs);

   return ret;
}

#endif /* FLIB */
