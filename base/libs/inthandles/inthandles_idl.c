/* inthandles_idl.c */
#ifdef IDLLIB
#include "xassert.h"
#include "jsoc.h"
#include "inthandles.h"
#include "hcontainer.h"

typedef const char *IDLHandleDRMS;
typedef IDLHandleDRMS *pIDLHandleDRMS;

static HContainer_t *idlHandlesDRMS = NULL; /* Handle/Structure container */


void InthandlesInit()
{
   if (!idlHandlesDRMS)
   {
      idlHandlesDRMS = hcon_create(sizeof(void *), 
				 kMaxIDLHandleKey, 
				 NULL, /* Shouldn't ever let hcon free, APIs should free 
					* underlying DRMS data structures. */
				 NULL, 
				 NULL, 
				 NULL, 
				 0);
      XASSERT(idlHandlesDRMS);
   }
}

/* Top-level function that releases all memory allocated during initialization of 
 * the IDL interface. 
 */
void InthandlesTerm()
{
   if (idlHandlesDRMS)
   {
      hcon_destroy(&idlHandlesDRMS);
   }
}


/* This API is called from within the JSOC IDL API (C functions wrapping 
 * the JSOC C API).  Provided a handle, it returns the 
 * corresponding JSOC C structure needed.
*/
void *GetJSOCStructure(void *handle, IDLHandleType_t t)
{
   void *ret = NULL;
   char buf[kMaxIDLHandleKey];

   if (handle != NULL)
   {
      switch(t)
      {
	 case kIDLHandleTypeDRMS:
	   /* handle is a ptr to a ptr*/
	   if (idlHandlesDRMS)
	   {
	      IDLHandleDRMS h = *((pIDLHandleDRMS)handle);
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      void **st = (void **)hcon_lookup(idlHandlesDRMS, buf);
	      if (st)
	      {
		 ret = *st;
	      }
	   }
	   break;
	 default:
	   fprintf(stderr, "Unknown IDL handle type.\n");
      }
   }

   return ret;
}
/* Returns 0 if successful insert. */
int InsertJSOCStructure(void *handle, void *structure, IDLHandleType_t t, const char **keyout) 
{
   int ret = 1;
   char buf[kMaxIDLHandleKey];

   if (handle != NULL && structure != NULL) 
   {
      switch(t) 
      {
	 case kIDLHandleTypeDRMS:
	   /* handle is a ptr to a ptr*/
	   if (idlHandlesDRMS)
	   {
	      IDLHandleDRMS h = *((pIDLHandleDRMS)handle);
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      void **st = &structure;
	      ret = hcon_insert(idlHandlesDRMS, buf, st);
	      st = hcon_lookup_ext(idlHandlesDRMS, buf, keyout);
	   }
	   break;
	 default:
	   fprintf (stderr, "Unknown IDL handle type.\n");
      }
   }

   return ret;
}

int RemoveJSOCStructure(void *handle, IDLHandleType_t t) 
{
   int ret = 1;
   char buf[kMaxIDLHandleKey];

   if (handle != NULL) 
   {
      switch(t) 
      {
	 case kIDLHandleTypeDRMS:
	   if (idlHandlesDRMS)
	   {
	      /* handle is a ptr to a ptr*/
	      IDLHandleDRMS h = *((pIDLHandleDRMS)handle);
	      snprintf(buf, sizeof(buf), "%s", (const char *)h);
	      hcon_remove(idlHandlesDRMS, buf);
	      ret = (hcon_lookup(idlHandlesDRMS, buf) != NULL);
	   }
	   break;
	 default:
	   fprintf (stderr, "Unknown IDL handle type.\n");
      }
   }
   return ret;
}

#endif //IDLLIB
