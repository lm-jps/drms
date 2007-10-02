/* inthandles_f.c */
#ifdef FLIB
#include "xassert.h"
#include "jsoc.h"
#include "inthandles.h"
#include "hcontainer.h"

typedef const char *FHandleDRMS;
typedef FHandleDRMS *pFHandleDRMS;

static HContainer_t *fHandlesCmdParams = NULL;
static HContainer_t *fHandlesDRMS = NULL;


void InthandlesInit(int cpsize)
{
   if (!fHandlesCmdParams)
   {
      fHandlesCmdParams = hcon_create(cpsize, 
				      kMaxFHandleKey, 
				      NULL, /* cmdparams is static - don't free */
				      NULL, 
				      NULL, 
				      NULL, 
				      0);
      XASSERT(fHandlesCmdParams);
   }

   if (!fHandlesDRMS)
   {
      fHandlesDRMS = hcon_create(sizeof(void *), 
				 kMaxFHandleKey, 
				 NULL, /* Shouldn't ever let hcon free, APIs should free 
					* underlying DRMS data structures. */
				 NULL, 
				 NULL, 
				 NULL, 
				 0);
      XASSERT(fHandlesDRMS);
   }
}

void InthandlesTerm()
{
   if (fHandlesCmdParams)
   {
      hcon_destroy(&fHandlesCmdParams);
   }

   if (fHandlesDRMS)
   {
      hcon_destroy(&fHandlesDRMS);
   }
}

/* This API is called from within the JSOC Fortran API (C functions wrapping 
 * the JSOC C API).  Provided a handle, it returns the 
 * corresponding JSOC C structure needed.
*/
void *GetJSOCStructure(void *handle, FHandleType_t t)
{
   void *ret = NULL;
   char buf[kMaxFHandleKey];

   if (handle != NULL)
   {
      switch(t)
      {
	 case kFHandleTypeCmdParams:
	   if (fHandlesCmdParams)
	   {
	      FHandleCmdParams h = *((pFHandleCmdParams)handle);
	      void **ppCP = NULL;
	      snprintf(buf, sizeof(buf), "%lld", (long long)(h));
	      ppCP = (void **)hcon_lookup(fHandlesCmdParams, buf);
	      if (ppCP)
	      {
		 ret = (void *)*ppCP;
	      }
	   }
	   break;
	 case kFHandleTypeDRMS:
	   if (fHandlesDRMS)
	   {
	      FHandleDRMS h = handle;
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      void **st = (void **)hcon_lookup(fHandlesDRMS, buf);
	      if (st)
	      {
		 ret = *st;
	      }
	   }
	   break;
	 default:
	   fprintf(stderr, "Unknown Fortran handle type.\n");
      }
   }

   return ret;
}
/* Returns 0 if successful insert. */
int InsertJSOCStructure(void *handle, void *structure, FHandleType_t t, const char **keyout) 
{
   int ret = 1;
   char buf[kMaxFHandleKey];

   if (handle != NULL && structure != NULL) 
   {
      switch(t) 
      {
	 case kFHandleTypeCmdParams:
	   if (fHandlesCmdParams) 
	   {
	      FHandleCmdParams h = *((pFHandleCmdParams)handle);
	      snprintf(buf, sizeof(buf), "%lld", (long long)(h));
	      void *pCP = structure;	 
	      ret = hcon_insert(fHandlesCmdParams, buf, &pCP);
	      hcon_lookup_ext(fHandlesCmdParams, buf, keyout);
	   }
	   break;
	 case kFHandleTypeDRMS:
	   /* In this case, handle is a ptr, not a ptr to a ptr. */
	   if (fHandlesDRMS)
	   {
	      FHandleDRMS h = handle;
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      void **st = &structure;
	      ret = hcon_insert(fHandlesDRMS, buf, st);
	      st = hcon_lookup_ext(fHandlesDRMS, buf, keyout);
	   }
	   break;
	 default:
	   fprintf (stderr, "Unknown Fortran handle type.\n");
      }
   }

   return ret;
}

int RemoveJSOCStructure(void *handle, FHandleType_t t) 
{
   int ret = 1;
   char buf[kMaxFHandleKey];

   if (handle != NULL) 
   {
      switch(t) 
      {
	 case kFHandleTypeCmdParams:
	   if (fHandlesCmdParams) 
	   {
	      FHandleCmdParams h = *((pFHandleCmdParams)handle);
	      snprintf(buf, sizeof(buf), "%lld", (long long)(h));
	      hcon_remove(fHandlesCmdParams, buf);
	      ret = (hcon_lookup(fHandlesCmdParams, buf) != NULL);
	   }
	   break;
	 case kFHandleTypeDRMS:
	   if (fHandlesDRMS)
	   {
	      FHandleDRMS h = handle;
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      hcon_remove(fHandlesDRMS, buf);
	      ret = (hcon_lookup(fHandlesDRMS, buf) != NULL);
	   }
	   break;
	 default:
	   fprintf (stderr, "Unknown Fortran handle type.\n");
      }
   }
   return ret;
}

#endif // FLIB
