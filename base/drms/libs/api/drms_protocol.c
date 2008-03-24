#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"

static char *prot_string[] = {
   "generic", 
   "bin",
   "bin.gz",
   "fitz",
   "fits",
   "msi",
   "tas",
   "dsds",
   "local",
   "fitzdep",
   "fitsdep"
};

static char *prot_fileext[] = {
   "generic",   
   "bin",
   "bin.gz",
   "fits",
   "fits",
   "msi",
   "tas",
   "dsds",
   "local",
   "fitz",
   "fits"
};

static HContainer_t *gProtMap = NULL;

const int kMAXPROTSTR = 16;

DRMS_Protocol_t drms_str2prot (const char *str) {
   int status = DRMS_SUCCESS;
   DRMS_Protocol_t ret = DRMS_PROTOCOL_INVALID;
   DRMS_Protocol_t *ans = NULL;

   if (!gProtMap)
   {
      gProtMap = hcon_create(sizeof(DRMS_Protocol_t), kMAXPROTSTR, NULL, NULL, NULL, NULL, 0);      

      if (gProtMap)
      {
	 int iprot;
	 for (iprot = 0; status == DRMS_SUCCESS && iprot < DRMS_PROTOCOL_END; iprot++)
	 {
	    if (hcon_insert(gProtMap, prot_string[iprot], (void *)&iprot))
	    {
	       status = DRMS_ERROR_CANTCREATEHCON;
	       break;
	    }
	 }
      }
      else
      {
	 status = DRMS_ERROR_CANTCREATEHCON;
      }
   }

   if (status == DRMS_SUCCESS && gProtMap)
   {
      if ((ans = (DRMS_Protocol_t *)hcon_lookup(gProtMap, str)) != NULL)
      {
	 ret = *ans;
      }
   }

   return ret;
}

const char *drms_prot2str (DRMS_Protocol_t prot) {
   if (prot > DRMS_PROTOCOL_INVALID && prot < DRMS_PROTOCOL_END)
   {
      return prot_string[prot];
   }

   return NULL;
}

const char *drms_prot2ext (DRMS_Protocol_t prot) {
if (prot > DRMS_PROTOCOL_INVALID && prot < DRMS_PROTOCOL_END)
   {
      return prot_fileext[prot];
   }

 return NULL;
}

void drms_protocol_term() {
   if (gProtMap)
   {
      hcon_destroy(&gProtMap);
   }
}
