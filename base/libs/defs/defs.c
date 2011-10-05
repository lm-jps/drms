#include "jsoc.h"
#include "defs.h"
#include "hcontainer.h"

#define kMAXKEYSIZE 64
#define kMAXVALSIZE 128

HContainer_t *gDefs = NULL;

void InitGDefs()
{
   if (!gDefs)
   {
      gDefs = malloc(sizeof(HContainer_t));
      if (gDefs)
      {
         hcon_init(gDefs, kMAXVALSIZE, kMAXKEYSIZE, NULL, NULL);
      }
   }
}

void defs_term()
{
   if (gDefs)
   {
      hcon_destroy(&gDefs);
   }
}

const char *defs_getval(const char *key)
{
   const char *ret = NULL;

   if (gDefs && key)
   {
      ret = hcon_lookup(gDefs, key);
   }

   if (!ret)
   {
      fprintf(stderr, "DEF ERROR: Definition ID '%s' undefined.\n", key);
   }

   return ret;
}
