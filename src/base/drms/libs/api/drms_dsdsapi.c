/* dsdsapi.c */

#include <stdlib.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include "drms_dsdsapi.h"

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
