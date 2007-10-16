/* drms_keymap.c */

#include "drms.h"

/* KeyMapClass table enum */
#define DEFKEYMAPCLASS(A,B,C) A,

typedef enum
{
   kKEYMAPCL_UNDEF = 0,
#include "defkeymapclass.h"
   kKEYMAPCL_NUMTABLESPLUSONE
} KeyMapClassTables_t;

#undef DEFKEYMAPCLASS

/* KeyMapClass tables */
#define DEFKEYMAPCLASS(A,B,C) C,

const char *KeyMapClassTables[] = 
{
   "",
#include "defkeymapclass.h"
   ""
};

#undef DEFKEYMAPCLASS

/* KeyMapClass enum to string identifier table */
#define DEFKEYMAPCLASS(A,B,C) B,

const char *KeyMapClassIDMap[] = 
{
   "",
#include "defkeymapclass.h"
   ""
};

#undef DEFKEYMAPCLASS

#define MAXCLKEY          128 /* key */
#define MAXMAPPINGKEY     64  /* This is an external keyword name */

/* Global containers. */
static HContainer_t *gClassTables = NULL;

DRMS_KeyMap_t *drms_keymap_create()
{
   DRMS_KeyMap_t *ret = NULL;

   DRMS_KeyMap_t *km = (DRMS_KeyMap_t *)malloc(sizeof(DRMS_KeyMap_t));
   if (km)
   {
       hcon_init(&(km->int2ext), MAXMAPPINGKEY, DRMS_MAXNAMELEN, NULL, NULL);
       hcon_init(&(km->ext2int), DRMS_MAXNAMELEN, MAXMAPPINGKEY, NULL, NULL);
       ret = km;
   }

   return ret;
}

void drms_keymap_destroy(DRMS_KeyMap_t **km)
{
   if (*km)
   {
      hcon_free(&((*km)->int2ext));
      hcon_free(&((*km)->ext2int));
      free(*km);
      *km = NULL;
   }
}

int drms_keymap_parsetable(DRMS_KeyMap_t *keymap, const char *text)
{
   int success = 1;

   if (keymap)
   {
      /* Read in keyword mappings from keyword map file. */
      char token[MAX(MAXMAPPINGKEY, DRMS_MAXNAMELEN)];
      char *pCh = NULL;
      char *fits = NULL;
      char *drms = NULL;
      char *textC = strdup(text);
      char *lasts = NULL;
      
      pCh = strtok_r(textC, "\n", &lasts);
      for (; success && pCh != NULL; pCh = strtok_r(NULL, "\n", &lasts))
      {
	 if (strlen(pCh) == 0)
	 {
	    /* skip empty lines */
	    continue;
	 }

	 if (pCh[0] == '#')
	 {
	    /* skip comments */
	    continue;
	 }

	 snprintf(token, sizeof(token), "%s", pCh);
	 pCh = strtok(token, " \t,");
	 if (pCh)
	 {
	    fits = pCh;
	    pCh = strtok(NULL, " \t,");
	    if (pCh)
	    {
	       drms = pCh;
	    }
	    else
	    {
	       fprintf(stderr, "Skipping bad line in map file:\n\t%s", textC);
	       continue;
	    }
	 }
	 else
	 {
	    /* skip whitespace lines */
	    continue;
	 }

	 if (fits && drms)
	 {
	       char drmsKeyName[DRMS_MAXNAMELEN];
	       snprintf(drmsKeyName, sizeof(drmsKeyName), "%s", drms);
	       hcon_insert(&(keymap->ext2int), fits, drmsKeyName);
	       hcon_insert(&(keymap->int2ext), drmsKeyName, fits);
	    }
	 }

      if (textC)
      {
	 free(textC);
      }
   }
   else
   {
      success = 0;
   }

   return success;
}

int drms_keymap_parsefile(DRMS_KeyMap_t *keymap, FILE *fPtr)
{
   int success = 1;

   char buf[8192];
   char lineBuf[LINE_MAX];
   long nRead = 0;
   int done = 0;

   /* The table should be fairly small fit entirely in the buffer. */
   while (!done && success)
   {
      while (success && !(done = (fgets(lineBuf, LINE_MAX, fPtr) == NULL)))
      {
	 if (strlen(lineBuf) + nRead < sizeof(buf))
	 {
	    nRead += strlen(lineBuf);
	    strcat(buf, lineBuf); 
	 }
	 else
	 {
	    break;
	 }
      }

      /* send buffer to drms_keymap_parsetable() */
      success = drms_keymap_parsetable(keymap, buf);
      buf[0] = '\0';
      nRead = 0;
   }

   return success;
}

static void KMFree(const void *val)
{
   DRMS_KeyMap_t *km = (DRMS_KeyMap_t *)val;
   
   if (km)
   {
      hcon_free(&(km->int2ext));
      hcon_free(&(km->ext2int));
   }
}

static int drms_keymap_initcltables()
{
   int ret = 0;

   if (!gClassTables)
   {
      gClassTables = hcon_create(sizeof(DRMS_KeyMap_t), MAXCLKEY, KMFree, NULL, NULL, NULL, 0);
   }

   if (gClassTables)
   {
      int ok = 1;
      int i = kKEYMAPCL_UNDEF + 1;
      for (; ok && i < kKEYMAPCL_NUMTABLESPLUSONE; i++)
      {
	 DRMS_KeyMap_t *km = drms_keymap_create();
	 if (km)
	 {
	    /* Parse keyword mapping table */
	    ok = drms_keymap_parsetable(km, KeyMapClassTables[i]);

	    if (ok)
	    {
	       ok = !hcon_insert(gClassTables, KeyMapClassIDMap[i], km);
	       free(km); /* don't deep-free since gClassTables points to malloc'd mem */
	    }
	    else
	    {
	       /* deep-free since km never got copied into gClassTables */
	       drms_keymap_destroy(&km);
	    }
	 }
	 else
	 {
	    ok = 0;
	 }
      }

      ret = ok;
   }

   if (!ret && gClassTables)
   {
      hcon_destroy(&gClassTables);
   }
   
   return ret;
}

int drms_keymap_init()
{
   return drms_keymap_initcltables();
}

static void drms_keymap_termcltables()
{
   if (gClassTables)
   {
      hcon_destroy(&gClassTables);
   }
}

void drms_keymap_term()
{
   drms_keymap_termcltables();
}

const char *drms_keymap_extname(DRMS_KeyMap_t *keymap, const char *intName)
{
   return (const char*)hcon_lookup(&(keymap->int2ext), intName);
}

const char *drms_keymap_classidextname(DRMS_KeyMapClass_t classid, const char *intName)
{
   const char *ret = NULL;

   /* First, get class string id */
   const char *strid = KeyMapClassIDMap[classid];

   /* Then, get external name from map. */
   DRMS_KeyMap_t *km = hcon_lookup(gClassTables, strid);
   if (km)
   {
      ret = (const char *)hcon_lookup(&(km->int2ext), intName);
   }

   return ret;
}

const char *drms_keymap_classextname(const char *class, const char *intName)
{
   const char *ret = NULL;

   /* Get external name from map. */
   DRMS_KeyMap_t *km = hcon_lookup(gClassTables, class);
   if (km)
   {
      ret = (const char *)hcon_lookup(&(km->int2ext), intName);
   }

   return ret;
}

const char *drms_keymap_intname(DRMS_KeyMap_t *keymap, const char *extName)
{
   return (const char*)hcon_lookup(&(keymap->ext2int), extName);
}

const char *drms_keymap_classidintname(DRMS_KeyMapClass_t classid, const char *extName)
{
   const char *ret = NULL;

   /* First, get class string id */
   const char *strid = KeyMapClassIDMap[classid];

   /* Then, get external name from map. */
   DRMS_KeyMap_t *km = hcon_lookup(gClassTables, strid);
   if (km)
   {
      ret = (const char *)hcon_lookup(&(km->ext2int), extName);
   }

   return ret;
}

const char *drms_keymap_classintname(const char *class, const char *extName)
{
   const char *ret = NULL;

   /* Get external name from map. */
   DRMS_KeyMap_t *km = hcon_lookup(gClassTables, class);
   if (km)
   {
      ret = (const char *)hcon_lookup(&(km->ext2int), extName);
   }

   return ret;
}
