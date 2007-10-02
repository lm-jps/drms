/*
 *  cmdparams_f.c
 *
 *  The Fortran interface to the cmdparams API.  This is implemented as a double
 *  wrapper around the DRMS API.  The outer wrapper is defined by macros in cfortran.h
 *  This header manages passing of function parameters and function names.  It 
 *  has platform-specific macros that do the correct thing for our environemnt.  The 
 *  inner wrapper are functions that convert DRMS C structure handles to the corresponding
 *  DRMS C structures.  
 *
 *  --Art Amezcua 8/9/2007
 */

#ifdef FLIB

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include "cmdparams.h"
#include "inthandles.h"
#include "cfortran.h"
#include "timeio.h"

/* ********** Fortran interface to JSOC C CmdParams API. ********** */

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

/* the ret parameter is set to the size of the Fortran string after spaces have been 
 * removed PLUS 1 for C's null character.  Upon return, the non-null chars are copied
 * back into Fortran's string buffer. */
void f_cmdparams_get_str(pFHandleCmdParams handle, 
			 const char *name, 
			 char *ret, 
			 int size, 
			 int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_str is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 char *str = cmdparams_get_str(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    strncpy(ret, str, size);
	 }
	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_isflagset(pFHandleCmdParams handle, const char *name, int *ret)
{
   *ret = 0;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_isflagset is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int ans = cmdparams_isflagset(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_cmdparams_get_int(pFHandleCmdParams handle, const char *name, int *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int ans = cmdparams_get_int(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_int8(pFHandleCmdParams handle, const char *name, int8_t *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int8 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int8_t ans = cmdparams_get_int8(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_int16(pFHandleCmdParams handle, const char *name, int16_t *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int16 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int16_t ans = cmdparams_get_int16(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_int32(pFHandleCmdParams handle, const char *name, int32_t *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int32 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int32_t ans = cmdparams_get_int32(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_int64(pFHandleCmdParams handle, const char *name, long long *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int64 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int64_t ans = cmdparams_get_int64(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_float(pFHandleCmdParams handle, char *name, float *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_float is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 float ans = cmdparams_get_float(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_double(pFHandleCmdParams handle, char *name, double *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_double is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 double ans = cmdparams_get_double(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_cmdparams_get_time(pFHandleCmdParams handle, char *name, double *ret, int *status)
{
   *status = CMDPARAMS_FAILURE;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_time is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 TIME ans = cmdparams_get_time(pCP, argname, status);
	 if (*status == CMDPARAMS_SUCCESS)
	 {
	    *ret = ans;
	 }

	 free(argname);
      }
      else
      {
	 *status = CMDPARAMS_OUTOFMEMORY;
      }
   }
}

void f_params_get_str(pFHandleCmdParams handle, const char *name, char *ret, int size)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! params_get_str is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 char *str = params_get_str(pCP, argname);
	 if (str)
	 {
	    strncpy(ret, str, size);
	 }

	 free(argname);
      }
   }
}

void f_params_isflagset(pFHandleCmdParams handle, const char *name, int *ret)
{
   *ret = 0;

   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_isflagset is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int ans = params_isflagset(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_int(pFHandleCmdParams handle, const char *name, int *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int ans = params_get_int(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_int8(pFHandleCmdParams handle, const char *name, int8_t *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int8 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int8_t ans = params_get_int8(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_int16(pFHandleCmdParams handle, const char *name, int16_t *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int16 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int16_t ans = params_get_int16(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_int32(pFHandleCmdParams handle, const char *name, int32_t *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int32 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int32_t ans = params_get_int32(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_int64(pFHandleCmdParams handle, const char *name, long long *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int64 is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 int64_t ans = params_get_int64(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_char(pFHandleCmdParams handle, char *name, signed char *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 char ans = params_get_char(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_short(pFHandleCmdParams handle, char *name, short *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_int is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 short ans = params_get_short(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_float(pFHandleCmdParams handle, char *name, float *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_float is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 float ans = params_get_float(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_double(pFHandleCmdParams handle, char *name, double *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_double is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 double ans = params_get_double(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

void f_params_get_time(pFHandleCmdParams handle, char *name, double *ret)
{
   CmdParams_t *pCP = (CmdParams_t *)GetJSOCStructure((void *)handle, kFHandleTypeCmdParams);

   if (pCP != NULL)
   {
      /* D'oh! cmdparams_get_time is expecting char *, not const char * -
       * it should be expecting the latter. */
      char *argname = strdup(name);

      if (argname)
      {
	 TIME ans = params_get_time(pCP, argname);
	 *ret = ans;

	 free(argname);
      }
   }
}

char *f_cmdparams_gethandle2()
{
   return "blahblah";
}

/* Macros defined by cfortran.h that magically handle parameter passing between 
 * Fortran and C.  These are actually function definitions, not protocols - so 
 * don't put them in cmdparams.h.  They define the functions named by
 * their third parameter plus an underscore (e.g., cpgethandle_). */
#ifdef ICCCOMP
#pragma warning (disable : 981)
#pragma warning (disable : 1418)
#endif
/* The use of cfortran.h for creating C wrapper functions called by Fortran code will 
 * cause this warning to display.  cfortran.h does  not provide a way to prototype 
 * these functions, it only provides a way to define them. */

FCALLSCFUN0(STRING, f_cmdparams_gethandle2, CPGETHANDLE2, cpgethandle2)

FCALLSCSUB1(f_cmdparams_get_handle, cpgethandle, cpgethandle, PINT)

FCALLSCSUB4(f_cmdparams_get_str, cpgetstr, cpgetstr, PINT, STRING, PSTRING, PINT)
FCALLSCSUB3(f_cmdparams_isflagset, cpflagset, cpflagset, PINT, STRING, PINT)
FCALLSCSUB4(f_cmdparams_get_int, cpgetint, cpgetint, PINT, STRING, PINT, PINT)
FCALLSCSUB4(f_cmdparams_get_int8, cpgetint8, cpgetint8, PINT, STRING, PBYTE, PINT)
FCALLSCSUB4(f_cmdparams_get_int16, cpgetint16, cpgetint16, PINT, STRING, PSHORT, PINT)
FCALLSCSUB4(f_cmdparams_get_int32, cpgetint32, cpgetint32, PINT, STRING, PINT, PINT)
FCALLSCSUB4(f_cmdparams_get_int64, cpgetint64, cpgetint64, PINT, STRING, PLONGLONG, PINT)
FCALLSCSUB4(f_cmdparams_get_float, cpgetfloat, cpgetfloat, PINT, STRING, PFLOAT, PINT)
FCALLSCSUB4(f_cmdparams_get_double, cpgetdouble, cpgetdouble, PINT, STRING, PDOUBLE, PINT)
FCALLSCSUB4(f_cmdparams_get_time, cpgettime, cpgettime, PINT, STRING, PDOUBLE, PINT)

FCALLSCSUB3(f_params_get_str, paramsgetstr, paramsgetstr, PINT, STRING, PSTRING)
FCALLSCSUB3(f_params_isflagset, paramsflagset, paramsflagset, PINT, STRING, PINT)
FCALLSCSUB3(f_params_get_int, paramsgetint, paramsgetint, PINT, STRING, PINT)
FCALLSCSUB3(f_params_get_int8, paramsgetint8, paramsgetint8, PINT, STRING, PBYTE)
FCALLSCSUB3(f_params_get_int16, paramsgetint16, paramsgetint16, PINT, STRING, PSHORT)
FCALLSCSUB3(f_params_get_int32, paramsgetint32, paramsgetint32, PINT, STRING, PINT)
FCALLSCSUB3(f_params_get_int64, paramsgetint64, paramsgetint64, PINT, STRING, PLONGLONG)
FCALLSCSUB3(f_params_get_char, paramsgetchar, paramsgetchar, PINT, STRING, PBYTE)
FCALLSCSUB3(f_params_get_short, paramsgetshort, paramsgetshort, PINT, STRING, PSHORT)
FCALLSCSUB3(f_params_get_float, paramsgetfloat, paramsgetfloat, PINT, STRING, PFLOAT)
FCALLSCSUB3(f_params_get_double, paramsgetdouble, paramsgetdouble, PINT, STRING, PDOUBLE)
FCALLSCSUB3(f_params_get_time, paramsgettime, paramsgettime, PINT, STRING, PDOUBLE)

#ifdef ICCCOMP
#pragma warning (default : 981)
#pragma warning (default : 1418)
#endif

#endif /* FLIB */
