/* keymap.c
 *
 * Functions to map an internal DRMS keyword to an external FITS keyword,
 * and vice versa.
 *
 * Added documenting comments - Art Amezcua, 11/29/2007
 *
 */

#include "keymap.h"
#include <sys/param.h>
#include "drms_types.h"

/* The keymap data used to be defined in defkeymapclass.h, and
 * included throught the #include directive, but this confused
 * doxygen, so removed it.  The ramifications of this change
 * are that you have to be careful to keep the enum and string
 * definitions in synch.
 */

/* KeyMapClass tables */
const char *KeyMapClassTables[] =
{
   "",
   "",
   "",
   "BUNIT\tbunit_ssw\n" \
   "BMAJ Bmajor_ssw\n" \
   "noexist,intKey3_ssw\n",
   "LONPOLE\tlongitude_pole_gng\n" \
   "BUNIT\tbunit_gng\n" \
   "EQUINOX, intKey_gng\n",
   ""
};

/* KeyMapClass enum to string identifier table */
const char *KeyMapClassIDMap[] =
{
   "",
   "dsds",
   "local",
   "ssw",
   "gng",
   ""
};

#define MAXCLKEY          128 /* key */
#define MAXMAPPINGKEY     64  /* This is an external keyword name */

/* Global containers. */
static HContainer_t *gClassTables = NULL;

/* Internal functions */
static void KMFree(const void *val);
static int exputl_keymap_initcltables();
static void exputl_keymap_termcltables();

/* KMFree() - This function frees all HContainer_t memory associated with a
 * drms keymap. Used as the deep_free function in the call to hcon_create()
 * when the global class table (gClassTables) is created.
 */
static void KMFree(const void *val)
{
   Exputl_KeyMap_t *km = (Exputl_KeyMap_t *)val;

   if (km)
   {
      hcon_free(&(km->int2ext));
      hcon_free(&(km->ext2int));
   }
}

/* exputl_keymap_initcltables() - JSOC defines several default keyword mappings. Each
 * such default mapping is considered a mapping "class". For each mapping class
 * defined above, this function reads the mapping data and creates
 * a Exputl_Keymap_t structure. These Exputl_Keymap_t structures are saved in a global
 * container - gClassTables. Once exputl_keymap_initcltables() completes, programs
 * then have access to these classes with the exputl_keymap_classidextname(),
 * exputl_keymap_classextname(), exputl_keymap_classidintname(), and
 * exputl_keymap_classintname() functions.
 */
static int exputl_keymap_initcltables()
{
   int ret = 0;

   if (!gClassTables)
   {
      gClassTables = hcon_create(sizeof(Exputl_KeyMap_t), MAXCLKEY, KMFree, NULL, NULL, NULL, 0);
   }

   if (gClassTables)
   {
      int ok = 1;
      int i = kKEYMAPCLASS_DEFAULT + 1;
      for (; ok && i < kKEYMAPCLASS_NUMTABLESPLUSONE; i++)
      {
	 Exputl_KeyMap_t *km = exputl_keymap_create();
	 if (km)
	 {
	    /* Parse keyword mapping table */
	    ok = exputl_keymap_parsetable(km, KeyMapClassTables[i]);

	    if (ok)
	    {
	       ok = !hcon_insert(gClassTables, KeyMapClassIDMap[i], km);
	       free(km); /* don't deep-free since gClassTables points to malloc'd mem */
	    }
	    else
	    {
	       /* deep-free since km never got copied into gClassTables */
	       exputl_keymap_destroy(&km);
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

/* exputl_keymap_termcltables() - Free all memory allocated by the exputl_keymap_initcltables()
 * function call.
 */
static void exputl_keymap_termcltables()
{
   if (gClassTables)
   {
      hcon_destroy(&gClassTables);
   }
}
/* exputl_keymap_create() - Allocate an empty Exputl_Keymap_t and return a pointer to it if
 * memory was successfully allocated. It is the caller's responsiblity to free the returned
 * Exputl_Keymap_t by calling exputl_keymap_destroy().
 */
Exputl_KeyMap_t *exputl_keymap_create()
{
   Exputl_KeyMap_t *ret = NULL;

   Exputl_KeyMap_t *km = (Exputl_KeyMap_t *)malloc(sizeof(Exputl_KeyMap_t));
   if (km)
   {
      hcon_init(&(km->int2ext), MAXMAPPINGKEY, DRMS_MAXKEYNAMELEN, NULL, NULL);
      hcon_init(&(km->ext2int), DRMS_MAXKEYNAMELEN, MAXMAPPINGKEY, NULL, NULL);
      ret = km;
   }

   return ret;
}

/* exputl_keymap_destroy() - Free all allocated memory associated with a Exputl_Keymap_t.
 * The Exputl_Keymap_t pointer contained in km is set to NULL.
 */
void exputl_keymap_destroy(Exputl_KeyMap_t **km)
{
   if (*km)
   {
      hcon_free(&((*km)->int2ext));
      hcon_free(&((*km)->ext2int));
      free(*km);
      *km = NULL;
   }
}

/* exputl_keymap_parsetable() - Parse a buffer containing FITS-keyword-name-to-DRMS-keyword-name
 * mappings. The buffer, <text>, can contain zero or more mappings, each of the form
 * <fitsname>(<whitespace> | ',')<drmsname>. Each pair of mappings must be separated by
 * a newline character ('\n'). Comments or emtpy strings may appear between newline characters
 * as well. If <keymap> is not NULL, the resulting set of mappings is used to
 * initialize <keymap>.
 */
int exputl_keymap_parsetable(Exputl_KeyMap_t *keymap, const char *text)
{
   int success = 1;

   if (keymap)
   {
      /* Read in keyword mappings from keyword map file. */
      char token[MAX(MAXMAPPINGKEY, DRMS_MAXKEYNAMELEN)];
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
	    char drmsKeyName[DRMS_MAXKEYNAMELEN];
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
/* exputl_keymap_parsefile() - Parse a file containing FITS-keyword-name-to-DRMS-keyword-name
 * mappings. <fPtr> must contain a valid file pointer. If <keymap> is not NULL, the
 * resulting set of mappings is used to initialize <keymap>. This function
 * calls exputl_keymap_parsetable() to parse the content of the file to which <fPtr>
 * refers.
 */
int exputl_keymap_parsefile(Exputl_KeyMap_t *keymap, FILE *fPtr)
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

      /* send buffer to exputl_keymap_parsetable() */
      success = exputl_keymap_parsetable(keymap, buf);
      buf[0] = '\0';
      nRead = 0;
   }

   return success;
}

/* exputl_keymap_init() - This function calls all keymap initialization functions().
 * This function should be called during program initialization.
 */
int exputl_keymap_init()
{
   BASE_Cleanup_t cu;
   cu.item = NULL;
   cu.free = exputl_keymap_term;
   base_cleanup_register("keymapclasstables", &cu);

   return exputl_keymap_initcltables();
}

/* exputl_keymap_term() - This function calls all keymap termination functions().
 * This function should be called immediately prior to program termination..
 */
void exputl_keymap_term(void *data)
{
   exputl_keymap_termcltables();
}

const char *exputl_keymap_extname(Exputl_KeyMap_t *keymap, const char *intName)
{
   return (const char*)hcon_lookup(&(keymap->int2ext), intName);
}

const char *exputl_keymap_classidextname(Exputl_KeyMapClass_t classid, const char *intName)
{
   const char *ret = NULL;

   if (gClassTables || exputl_keymap_init())
   {
      /* First, get class string id */
      const char *strid = KeyMapClassIDMap[classid];

      /* Then, get external name from map. */
      Exputl_KeyMap_t *km = hcon_lookup(gClassTables, strid);
      if (km)
      {
         ret = (const char *)hcon_lookup(&(km->int2ext), intName);
      }
   }

   return ret;
}

const char *exputl_keymap_classextname(const char *class, const char *intName)
{
   const char *ret = NULL;

   if (gClassTables || exputl_keymap_init())
   {
      /* Get external name from map. */
      Exputl_KeyMap_t *km = hcon_lookup(gClassTables, class);
      if (km)
      {
         ret = (const char *)hcon_lookup(&(km->int2ext), intName);
      }
   }

   return ret;
}

const char *exputl_keymap_intname(Exputl_KeyMap_t *keymap, const char *extName)
{
   return (const char*)hcon_lookup(&(keymap->ext2int), extName);
}

const char *exputl_keymap_classidintname(Exputl_KeyMapClass_t classid, const char *extName)
{
   const char *ret = NULL;

   if (gClassTables || exputl_keymap_init())
   {
      /* First, get class string id */
      const char *strid = KeyMapClassIDMap[classid];

      /* Then, get external name from map. */
      Exputl_KeyMap_t *km = hcon_lookup(gClassTables, strid);
      if (km)
      {
         ret = (const char *)hcon_lookup(&(km->ext2int), extName);
      }
   }

   return ret;
}

const char *exputl_keymap_classintname(const char *class, const char *extName)
{
   const char *ret = NULL;

   if (gClassTables || exputl_keymap_init())
   {
      /* Get external name from map. */
      Exputl_KeyMap_t *km = hcon_lookup(gClassTables, class);
      if (km)
      {
         ret = (const char *)hcon_lookup(&(km->ext2int), extName);
      }
   }

   return ret;
}

const char *exputl_keymap_getclname(Exputl_KeyMapClass_t clid)
{
   return KeyMapClassIDMap[clid];
}
