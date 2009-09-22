/* dsdsapi.c */

#include <stdlib.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include "drms_dsdsapi.h"

const char *DSDS_PortNS[] =
{
   "dsds",
   "ds_mdi",
   NULL
};

void *DSDS_GetFPtr(void *hDSDS, const char *symbol)
{
   void *ret = NULL;
   char *msg = NULL;

   if (hDSDS)
   {
      dlerror();
      ret = dlsym(hDSDS, symbol);
      if ((msg = dlerror()) != NULL)
      {
	 /* symbol not found */
	 fprintf(stderr, "Symbol %s not found: %s.\n", symbol, msg);
      }
   }

   return ret;
}

int DSDS_IsDSDSSpec(const char *spec)
{
   return (strstr(spec, "prog:") == spec);
}

int DSDS_IsDSDSPort(const char *query)
{
   int isport = 0;
   int ins = 0;
   const char *portns = NULL;

   while ((portns = DSDS_PortNS[ins]) != NULL)
   {
      if (strstr(query, portns) == query)
      {
         isport = 1;
         break;
      }

      ins++;
   }

   return isport;
}

int DSDS_GetDSDSParams(DRMS_SeriesInfo_t *si, char *out)
{
   int err = 1;
   char buf[kDSDS_MaxHandle];
   snprintf(buf, sizeof(buf), "%s", si->description);
   char *lasts;
   char *ans;

   if (si)
   {
      if ((ans = strtok_r(buf, "[]", &lasts)) != NULL)
      {
	 snprintf(out, kDSDS_MaxHandle, "%s", ans);
	 err = 0;
      }
   }

   return err;
}

int DSDS_SetDSDSParams(void *hDSDS, DRMS_SeriesInfo_t *si, DSDS_Handle_t in)
{
   int err = 1;

   if (hDSDS && si)
   {
      pDSDSFn_DSDS_handle_todesc_t pFn_DSDS_handle_todesc = 
	(pDSDSFn_DSDS_handle_todesc_t)DSDS_GetFPtr(hDSDS, kDSDS_DSDS_HANDLE_TODESC);

      if (pFn_DSDS_handle_todesc)
      {
	  kDSDS_Stat_t dsdsStat;
	 (*pFn_DSDS_handle_todesc)(in, si->description, &dsdsStat);
	 err = (dsdsStat != kDSDS_Stat_Success);
      }
   }

   return err;
}

void *DSDS_GetLibHandle(const char *libname, kDSDS_Stat_t *status)
{
   kDSDS_Stat_t stat = kDSDS_Stat_Success;
   void *ret = NULL;
   char lpath[PATH_MAX];
   char *msg = NULL;

#ifdef DRMS_LIBDIR
   snprintf(lpath, 
            sizeof(lpath), 
            "%s/%s", 
            DRMS_LIBDIR, 
            libname);
   dlerror();
   ret = dlopen(lpath, RTLD_NOW);
   if ((msg = dlerror()) != NULL)
   {
      /* library not found */
      fprintf(stderr, "dlopen(%s) error: %s.\n", lpath, msg);
      if (ret)
      {
         dlclose(ret);
         ret = NULL;
      }
      stat = kDSDS_Stat_CantOpenLibrary;
   }

#else
   #error Ensure the DRMS_LIBDIR is defined in make_basic.mk
#endif

   if (status)
   {
      *status = stat;
   }

   return ret;
}
