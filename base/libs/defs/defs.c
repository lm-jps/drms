#include "jsoc.h"
#include "defs.h"
#include "hcontainer.h"

#define kMAXKEYSIZE 64
#define kMAXVALSIZE 128

HContainer_t *gDefs = NULL;

static void InitGDefs()
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

/* Adds key-value pairs from filepath into gDefs */
int defs_register(const char *filepath)
{
   int stat = 1; /* default is error */
   char *key = NULL;
   char val[kMAXVALSIZE];

   if (filepath && *filepath)
   {
      FILE *fptr = fopen(filepath, "r");
      if (fptr)
      {
         char lineBuf[LINE_MAX];
         char *lasts = NULL;
         char *tmpstr = NULL;
         char *start = NULL;
         char *loc = NULL;

         while((fgets(lineBuf, LINE_MAX, fptr)))
         {
            tmpstr = strdup(lineBuf);

            if (tmpstr)
            {
               /* remove trailing \n */
               if (tmpstr[strlen(tmpstr) - 1] == '\n')
               {
                  tmpstr[strlen(tmpstr) - 1] = '\0';
               }

               /* remove whitespace */
               start = tmpstr;
               while (start && (*start == ' ' || *start == '\t' || *start == '\b'))
               {
                  start++;
               }

               if (!start || *start == '#' || *start == '\0')
               {
                  /* ignore ws line, comment line, empty line */
                  free(tmpstr);
                  continue;
               }

               loc = strchr(start, '=');
               if (loc)
               {
                  *loc = '\0';
                  start = strtok_r(start, " \t\b", &lasts);
                  if (start)
                  {
                     key = strdup(start);
                     if (key)
                     {

                        start = loc + 1;
                        while (start && (*start == ' ' || *start == '\t' || *start == '\b'))
                        {
                           start++;
                        }

                        if (start)
                        {
                           snprintf(val, sizeof(val), "%s", start);
                           if (val)
                           {
                              InitGDefs();
                              if (hcon_member(gDefs, key))
                              {
                                 fprintf(stderr, 
                                         "DEF WARNING: Can't register definition with id '%s' - this id already exists.\n", 
                                         key);
                              }
                              else
                              {
                                 hcon_insert(gDefs, key, val);
                              }
                           }
                        }

                        free(key);
                     }
                  }
               }
               else
               {
                  /* invalid line */
                  fprintf(stderr, "DEF WARNING: Invalid defs file line '%s', skipping\n.", tmpstr);
                  free(tmpstr);
                  continue;
               }

               free(tmpstr);
            }
         }

         stat = 0;
      }
   }

   return stat;
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
