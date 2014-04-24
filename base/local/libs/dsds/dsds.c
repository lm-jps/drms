/* dsds.c - interface to DSDS system. Composes, without any other files, libdsds.so.  
 * Dynamically links to libsoi.so, and JSOC dynamically links to libdsds.so. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <dlfcn.h>
#ifdef GCCCOMP
#define __USE_GNU
#endif
#include <search.h>
#include "drms_dsdsapi.h"
#include "drms_types.h"
#include "drms_protocol.h"
#include "hcontainer.h"
#include "list.h"

/* from libsoi */
#include "soi.h"

void *gHandleSOI = NULL;
long long gSeriesGuid = 1;
static HContainer_t *gHandleVDSCache = NULL;

const char kDSDS_GenericSeriesName[] = "dsds_series";
#define kLIBSOI "libsoi.so"
#define kDATAFILE "DATAFILE"

/* libsoi API function names */
typedef enum Soifn_enum
{
   kSOI_VDS_OPEN = 0,
   kSOI_VDS_CLOSE,
   kSOI_VDS_SELECT_HDR,
   kSOI_VDS_SELECT_REC,
   kSOI_VDS_LAST_RECORD,
   kSOI_NEWKEYLIST,
   kSOI_FREEKEYLIST,
   kSOI_SETKEY_STR,
   kSOI_SETKEY_INT,
   kSOI_GETKEY_STR,
   kSOI_GETKEY_INT,
   kSOI_PARSE_LIST,
   kSOI_KEYITERATE,
   kSOI_SDS_DATA,
   kSOI_SDS_FREE_DATA,
   kSOI_SDS_DATATYPE,
   kSOI_SDS_DATA_LENGTH,
   kSOI_SDS_NUMBYTES,
   kSOI_SDS_RANK,
   kSOI_SDS_LENGTH,
   kSOI_SDS_FIRST_ATTR,
   kSOI_SDS_NEXT_ATTR,
   kSOI_SDS_LAST_ATTR,
   kSOI_SDS_ATTRNAME,
   kSOI_SDS_ATTRTYPE,
   kSOI_SDS_ATTRVALUE,
   kSOI_SDS_ATTRCOMMENT,
   kSOI_SDS_GET_FITS_HEAD,
   kSOI_SDS_FREE,
   kSOI_SDS_READ_FITS,
   kSOI_NFNS
} Soifn_t;

char *SoifnNames[] =
{
   "vds_open",
   "vds_close",
   "VDS_select_hdr",
   "VDS_select_rec",
   "vds_last_record",
   "newkeylist",
   "freekeylist",
   "setkey_str",
   "setkey_int",
   "getkey_str",
   "getkey_int",
   "parse_list",
   "keyiterate",
   "sds_data",
   "sds_free_data",
   "sds_datatype",
   "sds_data_length",
   "sds_numbytes",
   "sds_rank",
   "sds_length",
   "sds_first_attr",
   "sds_next_attr",
   "sds_last_attr",
   "sds_attrname",
   "sds_attrtype",
   "sds_attrvalue",
   "sds_attrcomment",
   "sds_get_fits_head",
   "sds_free",
   "sds_read_fits",
   ""
};

/* Function pointer typedefs */
typedef VDS *(*pSOIFn_vds_open_t)(KEY *, char *);
typedef int (*pSOIFn_vds_close_t)(VDS **);
typedef SDS *(*pSOIFn_VDS_select_hdr_t)(VDS *, int, int);
typedef SDS *(*pSOIFn_VDS_select_rec_t)(VDS *, int, int);
typedef int (*pSOIFn_vds_last_record_t)(VDS *);
typedef KEY *(*pSOIFn_newkeylist_t)(void);
typedef void (*pSOIFn_freekeylist_t)(KEY **);
typedef void (*pSOIFn_setkey_str_t)(KEY **, char *, char *);
typedef void (*pSOIFn_setkey_int_t)(KEY **, char *, int);
typedef char *(*pSOIFn_getkey_str_t)(KEY *, const char *);
typedef int (*pSOIFn_getkey_int_t)(KEY *, const char *);
typedef int (*pSOIFn_parse_list_t)(KEY **, char *);
typedef int (*pSOIFn_keyiterate_t)(void (*action)(), KEY *overlist);
typedef void *(*pSOIFn_sds_data_t)(SDS *);
typedef void (*pSOIFn_sds_free_data_t)(SDS *);
typedef int (*pSOIFn_sds_datatype_t)(SDS *);
typedef long (*pSOIFn_sds_data_length_t)(SDS *);
typedef int (*pSOIFn_sds_numbytes_t)(SDS *);
typedef int (*pSOIFn_sds_rank_t)(SDS *);
typedef int *(*pSOIFn_sds_length_t)(SDS *);
typedef ATTRIBUTES *(*pSOIFn_sds_first_attr_t)(SDS *);
typedef ATTRIBUTES *(*pSOIFn_sds_next_attr_t)(ATTRIBUTES *);
typedef ATTRIBUTES *(*pSOIFn_sds_last_attr_t)(SDS *);
typedef char *(*pSOIFn_sds_attrname_t)(ATTRIBUTES *);
typedef int (*pSOIFn_sds_attrtype_t)(ATTRIBUTES *);
typedef void *(*pSOIFn_sds_attrvalue_t)(ATTRIBUTES *);
typedef char *(*pSOIFn_sds_attrcomment_t)(ATTRIBUTES *);
typedef SDS * (*pSOIFn_sds_get_fits_head_t)(char *);
typedef void (*pSOIFn_sds_free_t)(SDS **);
typedef SDS * (*pSOIFn_sds_read_fits_t)(FILE *);

/* Global function pointers - don't assign more than once */
void **gpFn = NULL;

/* Global hash tables */ 
struct hsearch_data *ftable = NULL; /* function pointers */
HContainer_t *htable = NULL;

const int kDSDS_MaxFunctions = 128;
const int kDSDS_MaxKeyName = 1024;
const int kDSDS_MaxHandles = 4096;

const int kDSDS_MaxVDSs = 256;

/* Forward decls */
void DSDS_free_keylistarr(DSDS_KeyList_t ***pklarr, int n);
void DSDS_free_segarr(DRMS_Segment_t **psarr, int n);

static void *GetSOI(kDSDS_Stat_t *stat)
{
    static int attempted = 0;
    kDSDS_Stat_t status;
    
    if (!attempted && !gHandleSOI)
    {
        /* Get handle to libdsds.so */
        gHandleSOI = DSDS_GetLibHandle(kLIBSOI, &status);
        if (status == kDSDS_Stat_CantOpenLibrary)
        {
            status = kDSDS_Stat_NoSOI;
        }
        
        attempted = 1;
    }
    
    if (gHandleSOI)
    {
        status = kDSDS_Stat_Success;
    }
    else
    {
        status = kDSDS_Stat_NoSOI;
    }
    
    if (stat)
    {
        *stat = status;
    }
    
    return gHandleSOI;
}

static void *GetSOIFPtr(void *hSOI, Soifn_t fnNum)
{
    void *ret = NULL;
    char *msg = NULL;
    int err = 0;
    
    if (hSOI)
    {
        if (!ftable)
        {
            ftable = (struct hsearch_data *)malloc(sizeof(struct hsearch_data));
            gpFn = (void **)malloc(sizeof(void *) * kDSDS_MaxFunctions);
            memset(ftable, 0, sizeof(struct hsearch_data));
            memset(gpFn, 0, sizeof(void *) * kDSDS_MaxFunctions);
            
            if (hcreate_r(kDSDS_MaxFunctions, ftable))
            {
                ENTRY entry;
                ENTRY *entryRet = NULL;
                int nfn;
                
                for (nfn = 0; err == 0 && nfn < kSOI_NFNS; nfn++)
                {
                    entry.key = SoifnNames[nfn];
                    entry.data = &(gpFn[nfn]); /* data is address of global function pointer */
                    if (!hsearch_r(entry, ENTER, &entryRet, ftable))
                    {
                        err = 1;
                    }
                }
            }
        }
    }
    
    if (!err && ftable)
    {
        ENTRY entrySearch;
        ENTRY *entryFound = NULL;
        
        entrySearch.key = SoifnNames[fnNum];
        hsearch_r(entrySearch, FIND, &entryFound, ftable);
        if (!entryFound)
        {
            fprintf(stderr, "Symbol %s not found in ftable.\n", SoifnNames[fnNum]);
            err = 1;
        }
        else
        {
            void **pFn = entryFound->data;
            
            if (*pFn == NULL)
            {
                /* This function isn't in ftable yet. */
                dlerror();
                ret = dlsym(hSOI, SoifnNames[fnNum]);
                if ((msg = dlerror()) != NULL)
                {
                    /* symbol not found */
                    fprintf(stderr, "Symbol %s not found: %s.\n", SoifnNames[fnNum], msg);
                    ret = NULL;
                    err = 1;
                }
                
                if (ret)
                {
                    *pFn = ret;
                }
            }
            else
            {
                ret = *pFn;
            }
        }
    }
    
    return ret;
}

static void freeVDS(const void *value)
{
    kDSDS_Stat_t status = kDSDS_Stat_Success;
    
    VDS *tofree = *((VDS **)value);
    if (tofree)
    {
        void *hSOI = GetSOI(&status);
        
        if (hSOI)
        {
            pSOIFn_vds_close_t pFn_vds_close = (pSOIFn_vds_close_t)GetSOIFPtr(hSOI, kSOI_VDS_CLOSE);
            
            if (pFn_vds_close)
            {
                (*pFn_vds_close)(&tofree);
            }
        }
    }
}

/* Will create the cache if it has not been created already. */
static HContainer_t *getVDSCache(kDSDS_Stat_t *rstat)
{
    kDSDS_Stat_t status = kDSDS_Stat_Success;
    
    if (!gHandleVDSCache)
    {
        /* Map the hparams handle string to the address of the VDS. */
        gHandleVDSCache = hcon_create(sizeof(VDS *), kDSDS_MaxHandle, freeVDS, NULL, NULL, NULL, 0);
        if (!gHandleVDSCache)
        {
            status = kDSDS_Stat_NoMemory;
        }
    }
    
    if (rstat)
    {
        *rstat = status;
    }
    
    return gHandleVDSCache;
}

static void purgeVDSCache()
{
    /* Randomly dump half of the cache entries. */
    int initSize;
    HIterator_t iter;
    const char *key = NULL;
    int ielem;
    
    if (gHandleVDSCache)
    {
        initSize = hcon_size(gHandleVDSCache);
        hiter_new(&iter, gHandleVDSCache);
        ielem = 0;
        
        while (hcon_size(gHandleVDSCache) > initSize / 2)
        {
            /* hacky, hacky! */
            key = iter.elems[ielem]->key;
            hcon_remove(gHandleVDSCache, key);
            ielem++;
        }
    }
}

static int isfullVDSCache()
{
    if (gHandleVDSCache)
    {
        return hcon_size(gHandleVDSCache) >= kDSDS_MaxVDSs;
    }
    
    return 0;
}

static int insertVDSIntoCache(DSDS_Handle_t handle, VDS *vds)
{
    kDSDS_Stat_t status = kDSDS_Stat_Success;
    HContainer_t *cache = NULL;
    
    cache = getVDSCache(&status);
    
    if (cache && status == kDSDS_Stat_Success)
    {
        if (isfullVDSCache())
        {
            purgeVDSCache();
        }
        
        if (hcon_insert(cache, handle, &vds) != 0)
        {
            status = kDSDS_Stat_VDSCache;
        }
    }
    
    return status;
}

static VDS *getVDSFromCache(DSDS_Handle_t handle)
{
    if (gHandleVDSCache)
    {
        VDS **pVds = (VDS **)hcon_lookup(gHandleVDSCache, handle);
        
        if (pVds)
        {
            return *(pVds);
        }
    }
    
    return NULL;
}

/* Maintain a set of open VDSs. */
static VDS *getVDS(KEY *keylist, const char *dsname, DSDS_Handle_t handle, kDSDS_Stat_t *rstat)
{
    VDS *vds = NULL;
    kDSDS_Stat_t status;
    void *hSOI = NULL;
    
    status = kDSDS_Stat_Success;
    
    vds = getVDSFromCache(handle);
    
    if (!vds)
    {
        hSOI = GetSOI(&status);
        
        if (hSOI)
        {
            pSOIFn_vds_open_t pFn_vds_open = (pSOIFn_vds_open_t)GetSOIFPtr(hSOI, kSOI_VDS_OPEN);
            vds = (*pFn_vds_open)(keylist, dsname);
            
            if (vds)
            {
                /* If this fails, don't care. This just means that caching didn't work. */
                insertVDSIntoCache(handle, vds);
            }
        }
    }
    
    if (rstat)
    {
        *rstat = status;
    }
    
    return vds;
}

static void removeVDSFromCache(DSDS_Handle_t handle)
{
    if (gHandleVDSCache)
    {
        hcon_remove(gHandleVDSCache, handle);
    }
}

static void destroyVDSCache()
{
    if (gHandleVDSCache)
    {
        hcon_destroy(&gHandleVDSCache);
    }
}

/* Return a pointer to JSOC code so that when the underlying structure is deleted
 * the handle is also deleted.  That way the user won't accidentally use an 
 * invalid handle. 
 *
 * User:
 *   DSDS_Handle_t h = (*pFn_CreateFunc)(param);
 *   ...
 *   (*pFn_DestroyFunc)(&h);
 *   // now h == NULL;
*/
static int GenerateHandle(const char *desc, void *structure, DSDS_pHandle_t out)
{
    char buf[kDSDS_MaxHandle];
    snprintf(buf, sizeof(buf), "%s:%p", desc, structure);
    int err = 0;
    
    /* put in hash for later retrieval to free structure */
    if (!htable)
    {
        htable = hcon_create(kDSDS_MaxHandle, kDSDS_MaxHandle, NULL, NULL, NULL, NULL, 0);
    }
    
    if (htable)
    {
        err = hcon_insert(htable, buf, buf);
        
        if (!err)
        {
            void *item = hcon_lookup(htable, buf);
            if (item)
            {
                *out = (DSDS_Handle_t)item;
            }
            else
            {
                err = 1;
            }
        }
    }
    else
    {
        err = 1;
    }
    
    return err;
}

static DSDS_Handle_t FindHandle(const char *desc)
{
    DSDS_Handle_t ret = NULL;
    
    if (htable)
    {
        void *item = hcon_lookup(htable, desc);
        if (item)
        {
            ret = (DSDS_Handle_t)item;
        }
    }
    
    return ret;
}

static int GetStructure(DSDS_Handle_t handle, void **out)
{
    void *structure = NULL;
    char *handleStr = NULL;
    char *pCh = NULL;
    int err = 1;
    
    if (handle && out && htable)
    {
        /* Check for valid handle */
        void *item = hcon_lookup(htable, handle);
        
        if (!item)
        {
            fprintf(stderr, "Invalid DSDS_Handle %s.\n", handle);
            err = 1;
        }
        else
        {
            handleStr = strdup((const char *)handle);
            if (handleStr)
            {
                pCh = strchr(handleStr, ':');
                if (pCh)
                {
                    pCh++;
                    if (pCh < handleStr + strlen(handleStr))
                    {
                        if (sscanf(pCh, "%p", &structure) == 1)
                        {
                            *out = structure;
                            err = 0;
                        }
                    }
                }
            }
        }
    }
    
    if (handleStr)
    {
        free(handleStr);
    }
    
    return err;
}

static void DestroyHandle(DSDS_pHandle_t h)
{
    if (h && *h)
    {
        void *item = hcon_lookup(htable, *h);
        if (item)
        {
            KEY *keylist = NULL;
            pSOIFn_freekeylist_t pFn_freekeylist = NULL;
            void *hSOI = NULL;
            kDSDS_Stat_t status;
            
            hSOI = GetSOI(&status);
            
            if (hSOI)
            {
                pFn_freekeylist = (pSOIFn_freekeylist_t)GetSOIFPtr(hSOI, kSOI_FREEKEYLIST);
            }
            
            /* Free the open VDS for this handle. */
            if (pFn_freekeylist)
            {
                /* Free KEY-list structure. */
                GetStructure(*h, (void **)&keylist);
                (*pFn_freekeylist)(&keylist);
            }
            
            /* Remove handle from table of known handles. */
            hcon_remove(htable, *h);
            free(*h);
            *h = NULL;
        }
    }
}

static void Printkey(KEY *key)
{
   printf ("%s:\t", key->name);
   switch(key->type) {
      case KEYTYP_STRING:
	printf ("KEYTYP_STRING\t");
	printf ("%s\n", (char *) key->val);
	break;
      case KEYTYP_BYTE:
	printf ("KEYTYP_BYTE\t");
	printf ("%d\n", *(char *)key->val);
	break;
      case KEYTYP_INT:
	printf ("KEYTYP_INT\t");
	printf ("%d\n", *(int *)key->val);
	break;
      case KEYTYP_FLOAT:
	printf ("KEYTYP_FLOAT\t");
	printf ("%13.6e\n", *(float *)key->val);
	break;
      case KEYTYP_DOUBLE:
	printf ("KEYTYP_DOUBLE\t");
	printf ("%23.16e\n", *(double *)key->val);
	break;
      case KEYTYP_TIME:
	printf ("KEYTYP_TIME\t");
	printf ("%23.16e\n", *(TIME *)key->val);
	break;
      case KEYTYP_SHORT:
	printf ("KEYTYP_SHORT\t");
	printf ("%d\n", *(short *)key->val);
	break;
      case KEYTYP_LONG:
	printf ("KEYTYP_LONG\t");
	printf ("%ld\n", *(long *)key->val);
	break;
      case KEYTYP_UBYTE:
	printf ("KEYTYP_UBYTE\t");
	printf ("%d\n", *(unsigned char *)key->val);
	break;
      case KEYTYP_USHORT:
	printf ("KEYTYP_USHORT\t");
	printf ("%d\n", *(unsigned short *)key->val);
	break;
      case KEYTYP_UINT:
	printf ("KEYTYP_UINT\t");
	printf ("%u\n", *(unsigned int *)key->val);
	break;
      case KEYTYP_ULONG:
	printf ("KEYTYP_ULONG\t");
	printf ("%lu\n", *(unsigned long *)key->val);
	break;
      default:
	printf ("(void)\n");
   }
}

/* Turns the "in=prog:..." parameter into a keylist that vds_open() can use. */
static KEY *CreateSOIKeylist(const char *progspec, LinkedList_t **wdlist, kDSDS_Stat_t *stat)
{
   kDSDS_Stat_t status = kDSDS_Stat_Success;
   void *hSOI = GetSOI(&status);
   KEY *ret = NULL;
   int err = NO_ERROR;

   if (hSOI)
   {
      pSOIFn_newkeylist_t pFn_newkeylist = 
	(pSOIFn_newkeylist_t)GetSOIFPtr(hSOI, kSOI_NEWKEYLIST);
      pSOIFn_freekeylist_t pFn_freekeylist = 
	(pSOIFn_freekeylist_t)GetSOIFPtr(hSOI, kSOI_FREEKEYLIST);
      pSOIFn_setkey_str_t pFn_setkey_str = 
	(pSOIFn_setkey_str_t)GetSOIFPtr(hSOI, kSOI_SETKEY_STR);
      pSOIFn_setkey_int_t pFn_setkey_int = 
	(pSOIFn_setkey_int_t)GetSOIFPtr(hSOI, kSOI_SETKEY_INT);
      pSOIFn_getkey_int_t pFn_getkey_int = 
	(pSOIFn_getkey_int_t)GetSOIFPtr(hSOI, kSOI_GETKEY_INT);
      pSOIFn_parse_list_t pFn_parse_list = 
	(pSOIFn_parse_list_t)GetSOIFPtr(hSOI, kSOI_PARSE_LIST);
      pSOIFn_keyiterate_t pFn_keyiterate = 
	(pSOIFn_keyiterate_t)GetSOIFPtr(hSOI, kSOI_KEYITERATE);

      KEY *dslist = NULL;
      char *spec = strdup(progspec);

      if (spec && pFn_parse_list && pFn_newkeylist && 
          pFn_freekeylist && pFn_setkey_str && 
	  pFn_getkey_int && pFn_setkey_int)
      {
	 dslist = (*pFn_newkeylist)();

	 (*pFn_setkey_str)(&dslist, "in", spec);
	 if ((err = (*pFn_parse_list)(&dslist, "in")) != NO_ERROR)
	 {
	    fprintf(stderr, "Error calling parse_list(): %d.\n", err);
	    status = kDSDS_Stat_APIRetErr;
	 }

	 /* If spec is a 'prog' spec, then call peq to obtain certain 
	  * keys. */
         if (DSDS_IsDSDSSpec(spec) || DSDS_IsDSDSPort(spec))
	 {
	    /* Call peq to add keys for %s_wd, %s_level_sn, and %s_series_sn.  The only
	     * way to get this information is by communicating with dsds.  And there are
	     * are currently only two ways of doing this - using pe or peq. It should be
	     * possible to copy code from SOI here, but that would involve copying not 
	     * only the code in peq, but also dependent libraries (libdsds for one). 
	     * It is probably best to simply call peq, pipe back the results here, and 
	     * parse out the three needed keys (three keys per series).
	     */
	  
	    // int outfd[2]; /* parent writes to this pipe */
	    int infd[2];  /* child writes to this pipe */
	    int oldstdin, oldstdout;

	    // pipe(outfd);
	    pipe(infd);

	    oldstdin = dup(0);  /* current stdin */
	    oldstdout = dup(1); /* current stdout */

	    // close(0);
	    // close(1);

	    // dup2(outfd[0], 0); /* make the read end of outfd pipe as stdin */
	    dup2(infd[1], 1);  /* make the write end of infd as stdout */

	    if(!fork())
	    {
	       /* child */
	       char **argv = malloc(sizeof(char *) * 3);
	       argv[0] = "peq";
	       argv[1] = spec;
	       argv[2] = NULL;
	       
	       /* doesn't need to use the pipe directly - stdout goes to infd[1] */
	       // close(outfd[0]);
	       // close(outfd[1]);
	       close(infd[0]);
	       close(infd[1]);

	       execvp(argv[0], argv);
	    }
	    else
	    {
	       /* parent */
	       close(0); // Restore the original std fds of parent
	       close(1);
	       dup2(oldstdin, 0);
	       dup2(oldstdout, 1);

	       // close(outfd[0]); 
	       close(infd[1]); /* the child uses this pipe for writing */

	       char buf;
	       char lineBuf[LINE_MAX];
	       long nRead = 0;
	       int nGot = 0;
	       int nds = 0;

	       while (read(infd[0], (void *)&buf, 1) > 0)
	       {
		  if (buf == '\n')
		  {
		     if (nRead < LINE_MAX && nRead > 0)
		     {
			/* check for one of the three keys */
			char *keystr = NULL;
			char *typstr = NULL;
			char *valstr = NULL;
			int valint = 0;
			char *colon = NULL;
			long long ds = 0;

			lineBuf[nRead] = '\0';

                        if (strstr(lineBuf, "is not on-line"))
                        {
                           /* DSDS data are on tape, but not on disk - error out. */
                           status = kDSDS_Stat_DSDSOffline;
                           break;
                        }

			keystr = strtok(lineBuf, " \t");			

			if (keystr)
			{
			   if (strstr(keystr, "_wd:") && sscanf(keystr, "in_%lld_wd", &ds))
			   {			   
			      /* found a decimal number inside */
			      if ((colon = strchr(keystr, ':')) != NULL)
			      {
				 *colon = '\0';

				 typstr = strtok(NULL, " \t");
				 if (typstr)
				 {
				    if (!strcmp(typstr, "KEYTYP_STRING"))
				    {
				       valstr = strtok(NULL, " \t");				 
				       (*pFn_setkey_str)(&dslist, keystr, valstr);

                                       /* save this path to return with segment */
                                       if (wdlist && !*wdlist)
                                       {
                                          *wdlist = list_llcreate(PATH_MAX, NULL);
                                       }

                                       if (*wdlist)
                                       {
                                          list_llinserthead(*wdlist, valstr);
                                       }

				       nGot++;
				    }
				 }
			      }
			   }
			   else if ((strstr(keystr, "_level_sn:") && 
				     sscanf(keystr, "in_%lld_level_sn", &ds)) || 
				    (strstr(keystr, "_series_sn:") && 
				     sscanf(keystr, "in_%lld_series_sn", &ds)))
			   {
			      if ((colon = strchr(keystr, ':')) != NULL)
			      {
				 *colon = '\0';

				 typstr = strtok(NULL, " \t");
				 if (typstr)
				 {
				    if (!strcmp(typstr, "KEYTYP_INT"))
				    {
				       valstr = strtok(NULL, " \t");
				       valint = atoi(valstr);
				       (*pFn_setkey_int)(&dslist, keystr, valint);
				       nGot++;
				    }
				 }
			      }
			   }
                        }

                        nRead = 0;
		     }
		  }
		  else if (nRead < LINE_MAX)
		  {
		     lineBuf[nRead] = buf;
		     nRead++;
		  }
	       }

	       close(infd[0]);

               if (status == kDSDS_Stat_Success)
               {
                  if (DSDS_IsDSDSSpec(spec))
                  {
                     /* there should be three keys per vds - for the DSDSPort case, 
                        in_%lld_level_sn is missing (don't know why). */
                     nds = (*pFn_getkey_int)(dslist, "in_nsets");	       
                     if (nGot != 3 * nds)
                     {
                        status = kDSDS_Stat_PeqError;
                     }
                  }
               }
	    }
	 } /* 'prog' spec or DSDSPort spec */

         if (status == kDSDS_Stat_Success)
         {
            /* At this point, the soikeylist is "good" if the spec was a "prog" spec, 
             * or a spec that is a VDS directory.  But it is BAD if it is a 
             * spec that resolves into a SUMS dir containing a VDS directory.
             * Fortuantely, we now have, thanks to peq, a SUMS directory containing 
             * a VDS directory, so we can use that to create a "good" soikeylist.
             */
            if (DSDS_IsDSDSPort(spec))
            {
               (*pFn_freekeylist)(&dslist);
               dslist = (*pFn_newkeylist)();

               /* *wdlist contains the SUMS directory that has the VDS directory */
               if (wdlist && *wdlist)
               {
                  char *sumsdir = (char *)(list_llgethead(*wdlist)->data);
                  char *tmp = NULL;

                  /* OMG - don't forget to add a trailing '/' otherwise 
                   * VDS panics and doesn't know what to do.
                   */
                  if (sumsdir[strlen(sumsdir) - 1] != '/')
                  {
                     tmp = malloc(strlen(sumsdir) + 1);
                     if (tmp)
                     {
                        sprintf(tmp, "%s/", sumsdir);
                     }
                  }
                  else
                  {
                     tmp = strdup(sumsdir);
                  }
            
                  if (tmp)
                  {
                     (*pFn_setkey_str)(&dslist, "in", tmp);
                     free(tmp);
                     tmp = NULL;
                  }

                  if ((err = (*pFn_parse_list)(&dslist, "in")) != NO_ERROR)
                  {
                     fprintf(stderr, "Error calling parse_list(): %d.\n", err);
                     status = kDSDS_Stat_APIRetErr;
                  }
               }
               else
               {
                  status = kDSDS_Stat_PeqError;
                  fprintf(stderr, "peq failed to find the SUMS directory containing the data files.\n");
               }
            } /* DSDSPort spec */
            else if (!DSDS_IsDSDSSpec(spec))
            {
               /* Must be a local directory that has an overview.fits file in it - 
                * spec is the file dir containing fits fits. */
               if (wdlist && !*wdlist)
               {
                  *wdlist = list_llcreate(PATH_MAX, NULL);
               }

               if (*wdlist)
               {
                   char tbuf[PATH_MAX];
                   
                   snprintf(tbuf, sizeof(tbuf), "%s", spec);
                  list_llinserthead(*wdlist, tbuf);
               }
            }
         }
      }
      else
      {
	 status = kDSDS_Stat_MissingAPI;
      }
   
      if (status == kDSDS_Stat_Success)
      {
	 ret = dslist;
	 // (*pFn_keyiterate)(Printkey, ret);
      }

      if (spec)
      {
	 free(spec);
      }
   }

   if (stat)
   {
      *stat = status;
   }

   return ret;
}

/* IMPORTANT NOTE: SDS has a type SDS_LONG, which is truly 
 * the built-in type 'long'.  This means 32-bits on 32-bit machines, 
 * and 64-bits on 64-bit machines. */
static DRMS_Type_t SOITypeToDRMSType(int soiType)
{
   DRMS_Type_t drmsType;
     
   switch (soiType)
   {
      case SDS_LOGICAL:
      case SDS_BYTE:
      case SDS_UBYTE:
	drmsType = DRMS_TYPE_CHAR;
	break;
      case SDS_SHORT:
      case SDS_USHORT:
	drmsType = DRMS_TYPE_SHORT;
	break;
      case SDS_INT:
      case SDS_UINT:
	drmsType = DRMS_TYPE_INT;
	break;
      case SDS_LONG:
      case SDS_ULONG:
	drmsType = DRMS_TYPE_LONGLONG;
	break;
      case SDS_FLOAT:
	drmsType = DRMS_TYPE_FLOAT;
	break;
      case SDS_DOUBLE:
	drmsType = DRMS_TYPE_DOUBLE;
	break;
      case SDS_TIME:
	drmsType = DRMS_TYPE_TIME;
	break;
      case SDS_STRING:
	drmsType = DRMS_TYPE_STRING;
	break;
      default:
	drmsType = DRMS_TYPE_RAW;
   }

   return drmsType;
}

/* Returns the equivalent drms type. */


/* IMPORTANT NOTE: SDS has a type SDS_LONG, which is truly 
 * the built-in type 'long'.  This means 32-bits on 32-bit machines, 
 * and 64-bits on 64-bit machines. */
int PolyValueToDRMSValue(int soiType, void *val, DRMS_Type_Value_t *value)
{
   int error = 0;

   switch (soiType)
   {
      case SDS_LOGICAL:
	value->char_val = *((char *)val);
	break;
      case SDS_BYTE:
	value->char_val = *((signed char *)val);
	break;
      case SDS_UBYTE:
	value->char_val = *((unsigned char *)val);
	break;
      case SDS_SHORT:
	value->short_val = *((short *)val);
	break;
      case SDS_USHORT:
	value->short_val = *((unsigned short *)val);
	break;
      case SDS_INT:
	value->int_val = *((int *)val);
	break;
      case SDS_UINT:
	value->int_val = *((unsigned int *)val);
	break;
      case SDS_LONG:
	/* val is of type 'long', which may be either 32-bits or 64-bits, depending on
	 * architecture */
	{
	  long longval = *((long *)val);
	  value->longlong_val = (long long)longval;
	}
	break;
      case SDS_ULONG:
	/* val is of type 'unsigned long', which may be either 32-bits or 64-bits, depending on
	 * architecture */
	{
	  long longval = *((unsigned long *)val);
	  value->longlong_val = (long long)longval;
	}
	break;
      case SDS_FLOAT:
	value->float_val = *((float *)val);
	break;
      case SDS_DOUBLE:
	value->double_val = *((double *)val);
	break;
      case SDS_TIME:
	value->time_val = *((double *)val);
	break;
      case SDS_STRING:
	value->string_val = strdup((char *)val);
	if (value->string_val == NULL)
	{
	   fprintf(stderr, "alloc failure in PolyValueToDRMSValue().\n");
	   error = 1;
	}
	break;
      default:
	fprintf(stderr, "Invalid soi type: %d\n", soiType);
	error = 1;
   }

   return error;
}

static int GetKWFormat(char *buf, int size, DRMS_Type_t drmsType)
{
   int error = 0;
   char formatStr[64];

   switch (drmsType)
   {
      case DRMS_TYPE_CHAR:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_SHORT:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_INT:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_LONGLONG:
	formatStr[0] = '%';
	formatStr[1] = 'l';
	formatStr[2] = 'l';
	formatStr[3] = 'd';
	formatStr[4] = '\0';
	break;
      case DRMS_TYPE_FLOAT:
	formatStr[0] = '%';
	formatStr[1] = 'f';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_DOUBLE:
	formatStr[0] = '%';
	formatStr[1] = 'f';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_TIME:
	snprintf(formatStr, sizeof(formatStr), "%s", "UTC");
	break;
      case DRMS_TYPE_STRING:
	formatStr[0] = '%';
	formatStr[1] = 's';
	formatStr[2] = '\0';
	break;
      default:
	fprintf(stderr, "Invalid drms type: %d\n", (int)drmsType);
	error = 1;
   }

   if (!error)
   {
      snprintf(buf, size, "%s", formatStr);
   }

   return error;
}

static long long NumRecords(void *hSOI, int nds, KEY *params, kDSDS_Stat_t *stat)
{
   long long nRecs = 0;
   char dsname[kDSDS_MaxKeyName];
   char key[kDSDS_MaxKeyName];
   int ds;
   int fsn = 0;
   int lsn = 0;
   kDSDS_Stat_t status = kDSDS_Stat_Success;
   VDS *vds = NULL;

   pSOIFn_vds_open_t pFn_vds_open = 
     (pSOIFn_vds_open_t)GetSOIFPtr(hSOI, kSOI_VDS_OPEN);
   pSOIFn_vds_close_t pFn_vds_close = 
     (pSOIFn_vds_close_t)GetSOIFPtr(hSOI, kSOI_VDS_CLOSE);
   pSOIFn_vds_last_record_t pFn_vds_last_record =
     (pSOIFn_vds_last_record_t)GetSOIFPtr(hSOI, kSOI_VDS_LAST_RECORD);
   pSOIFn_getkey_int_t pFn_getkey_int = 
     (pSOIFn_getkey_int_t)GetSOIFPtr(hSOI, kSOI_GETKEY_INT);

   if (pFn_vds_open && pFn_vds_close && pFn_vds_last_record &&
       pFn_getkey_int)
   {
      /* loop through datasets to determine number of records */
      for (ds = 0; ds < nds; ds++) 
      {
	 sprintf(dsname, "in_%d", ds);
	 vds = (*pFn_vds_open)(params, dsname);

	 if (vds)
	 {
	    sprintf(key, "%s_fsn", dsname); 
	    fsn = (*pFn_getkey_int)(params, key);
	    sprintf(key, "%s_lsn", dsname); 
	    lsn = (*pFn_getkey_int)(params, key);

	    if (lsn == -1)
	    {
	       lsn = (*pFn_vds_last_record)(vds);
	    }

	    nRecs += (lsn - fsn + 1);

	    (*pFn_vds_close)(&vds);
	 }
      }
   }
   else
   {
      status = kDSDS_Stat_MissingAPI;
   }

   if (stat)
   {
      *stat = status;
   }
   
   return nRecs;
}

static void MakeDRMSSeriesName(void *hSOI,
			       char *drmsSeriesName, 
			       int size, 
			       KEY *params, 
			       const char *dsname,
			       kDSDS_Stat_t *stat)
{
   kDSDS_Stat_t status = kDSDS_Stat_Success;

   char drmsLev[DRMS_MAXSERIESNAMELEN];
   char key[kDSDS_MaxKeyName];
   const char *dsdsNsPrefix = NULL;

   pSOIFn_getkey_str_t pFn_getkey_str = 
     (pSOIFn_getkey_str_t)GetSOIFPtr(hSOI, kSOI_GETKEY_STR);

   if (pFn_getkey_str)
   {
      dsdsNsPrefix = DSDS_GetNsPrefix();

      snprintf(key, sizeof(key), "%s_prog", dsname);
      char *prog = (*pFn_getkey_str)(params, key);
      snprintf(key, sizeof(key), "%s_series", dsname);
      char *dsdsSeries = (*pFn_getkey_str)(params, key);
      snprintf(key, sizeof(key), "%s_level", dsname);
      char *level = (*pFn_getkey_str)(params, key);
      char *dot = NULL;

      if (prog && dsdsSeries && level)
      {
	 snprintf(drmsLev, sizeof(drmsLev), "%s", level);

	 if ((dot = strchr(drmsLev, '.')) != NULL)
	 {
	    *dot = '_';
	 }

	 snprintf(drmsSeriesName,
		  size,
		  "%s_%s.%s_%s__%lld",
                  dsdsNsPrefix,
		  prog,
		  dsdsSeries,
		  drmsLev,
		  gSeriesGuid);
      }
      else
      {
	 /* If the dataset being opened resides in a directory, and did not come
	  * from the SOI database, then use a generic, but unique name. */
	 snprintf(drmsSeriesName, size, "%s.%s%lld", dsdsNsPrefix, kDSDS_GenericSeriesName, gSeriesGuid);
      }

      gSeriesGuid++;
   }
   else
   {
      status = kDSDS_Stat_MissingAPI;
   }

   if (stat)
   {
      *stat = status;
   }
}

static void FreeDSDSKeyList(DSDS_KeyList_t **list)
{
    DSDS_KeyList_t *pList = *list;
    DSDS_KeyList_t *nElem = NULL;
    
    while (pList)
    {
        nElem = pList->next;
        
        /* need to free malloc'd mem */
        if (pList->elem)
        {
            if (pList->elem->info->type == DRMS_TYPE_STRING && pList->elem->value.string_val)
            {
                free(pList->elem->value.string_val);
            }
            
            if (pList->elem->info)
            {
                free (pList->elem->info);
            }
            
            free(pList->elem);
        }
        free(pList);
        pList = nElem;
    }
    
    *list = NULL;
}

/* returns number of attributes processed */
static int LoopAttrs(void *hSOI, 
		     SDS *sds, 
		     DSDS_KeyList_t *pHead, 
		     kDSDS_Stat_t *stat,
                     char **datafile,
                     int dfnlen)
{
   kDSDS_Stat_t status = kDSDS_Stat_Success;
   int nAttrs = 0;
   
   if (sds && pHead && hSOI)
   {
      pSOIFn_sds_first_attr_t pFn_sds_first_attr =
	(pSOIFn_sds_first_attr_t)GetSOIFPtr(hSOI, kSOI_SDS_FIRST_ATTR);
      pSOIFn_sds_next_attr_t pFn_sds_next_attr =
	(pSOIFn_sds_next_attr_t)GetSOIFPtr(hSOI, kSOI_SDS_NEXT_ATTR);
      pSOIFn_sds_last_attr_t pFn_sds_last_attr =
	(pSOIFn_sds_last_attr_t)GetSOIFPtr(hSOI, kSOI_SDS_LAST_ATTR);
      pSOIFn_sds_attrname_t pFn_sds_attrname =
	(pSOIFn_sds_attrname_t)GetSOIFPtr(hSOI, kSOI_SDS_ATTRNAME);
      pSOIFn_sds_attrtype_t pFn_sds_attrtype =
	(pSOIFn_sds_attrtype_t)GetSOIFPtr(hSOI, kSOI_SDS_ATTRTYPE);
      pSOIFn_sds_attrvalue_t pFn_sds_attrvalue =
	(pSOIFn_sds_attrvalue_t)GetSOIFPtr(hSOI, kSOI_SDS_ATTRVALUE);
      pSOIFn_sds_attrcomment_t pFn_sds_attrcomment =
	(pSOIFn_sds_attrcomment_t)GetSOIFPtr(hSOI, kSOI_SDS_ATTRCOMMENT);

      if (pFn_sds_first_attr &&
	  pFn_sds_next_attr && pFn_sds_last_attr &&
	  pFn_sds_attrname && pFn_sds_attrtype &&
	  pFn_sds_attrvalue && pFn_sds_attrcomment)
      {
	 DSDS_KeyList_t *pKL = pHead;
	 DSDS_KeyList_t *pPrevKL = NULL;

	 char *attrName = NULL;
	 int attrType;
	 void *attrVal = NULL;
	 char *attrComment = NULL;
	 DRMS_Keyword_t *drmskey = NULL;
      
	 /* loop through attributes */
         if (datafile && *datafile)
         {
            (*datafile)[0] = '\0';
         }

	 nAttrs = 0;
	 ATTRIBUTES *attr = (*pFn_sds_first_attr)(sds);
	 ATTRIBUTES *lastAttr = (*pFn_sds_last_attr)(sds);

	 while (attr)
	 {
	    if (pKL->elem != NULL)
	    {
	       pKL->next = (DSDS_KeyList_t *)malloc(sizeof(DSDS_KeyList_t));

	       if (!(pKL->next))
	       {
		  status = kDSDS_Stat_NoMemory;
		  break;
	       }

	       pPrevKL = pKL;
	       pKL = pKL->next;
	       pKL->next = NULL;
	    }

	    attrName = (*pFn_sds_attrname)(attr);

	    if (attrName && *attrName)
	    {
	       attrType = (*pFn_sds_attrtype)(attr);
	       attrVal = (*pFn_sds_attrvalue)(attr);
	       attrComment = (*pFn_sds_attrcomment)(attr);

               /* Check to see if a data file exists - if not no DRMS segment will be created */
               if (datafile && 
                   *datafile &&
                   strcmp(attrName, kDATAFILE) == 0 && 
                   attrType == SDS_STRING && 
                   strlen(attrVal) > 0)
               {
                  snprintf(*datafile, dfnlen, "%s", (char *)attrVal);
               }

	       /* make a drms keyword (make stand-alone keyword->info) */
	       drmskey = (DRMS_Keyword_t *)malloc(sizeof(DRMS_Keyword_t));
	       memset(drmskey, 0, sizeof(DRMS_Keyword_t));
	       drmskey->info = 
		 (DRMS_KeywordInfo_t *)malloc(sizeof(DRMS_KeywordInfo_t));
	       memset(drmskey->info, 0, sizeof(DRMS_KeywordInfo_t));

	       snprintf(drmskey->info->name, DRMS_MAXKEYNAMELEN, "%s", attrName);
	       drmskey->info->type = SOITypeToDRMSType(attrType);
	       if (GetKWFormat(drmskey->info->format, 
			       DRMS_MAXFORMATLEN, 
			       drmskey->info->type))
	       {
		  status = kDSDS_Stat_TypeErr;
	       }
	       else
	       {
		  if (attrComment)
		  {
		     snprintf(drmskey->info->description, 
			      DRMS_MAXCOMMENTLEN,
			      "%s",
			      attrComment);
		  }
		  
		  if (PolyValueToDRMSValue(attrType, 
					   attrVal, 
					   &(drmskey->value)))
		  {
		     status = kDSDS_Stat_TypeErr;
		  }
	       }

	       if (status != kDSDS_Stat_Success)
	       {
		  /* A bad key - delete and continue */
		  fprintf(stderr, "A bad fits keyword encountered.\n"
			  "Some keywords may not have been ingested.\n");

		  if (drmskey)
		  {
		     if (drmskey->info)
		     {
			free(drmskey->info);
		     }

		     free(drmskey);
		  }

		  free(pKL);
		  pKL = pPrevKL;
		  pKL->next = NULL;
	       }
	       else
	       {
		  pKL->elem = drmskey;
		  nAttrs++;
	       }

	       if (attr == lastAttr)
	       {
		  break;
	       }
	    }

	    attr = (*pFn_sds_next_attr)(attr);

	 } /* attr loop */
      }
      else
      {
	 status = kDSDS_Stat_MissingAPI;
      }
   }

   if (stat)
   {
      *stat = status;
   }

   return nAttrs;
}

static void FillDRMSSeg(void *hSOI, 
			SDS *sds, 
			DRMS_Segment_t *segout,
			const char *segname,
			DRMS_Protocol_t protocol,
                        const char *filename,
			kDSDS_Stat_t *stat)
{
   kDSDS_Stat_t status = kDSDS_Stat_InvalidParams;

   if (!hSOI)
   {
      status = kDSDS_Stat_NoSOI;
   }
   else if (!segout)
   {
      status = kDSDS_Stat_InvalidParams;
   }
   else
   {
      pSOIFn_sds_rank_t pFn_sds_rank = 
	(pSOIFn_sds_rank_t)GetSOIFPtr(hSOI, kSOI_SDS_RANK);
      pSOIFn_sds_length_t pFn_sds_length = 
	(pSOIFn_sds_length_t)GetSOIFPtr(hSOI, kSOI_SDS_LENGTH);
      pSOIFn_sds_datatype_t pFn_sds_datatype = 
	(pSOIFn_sds_datatype_t)GetSOIFPtr(hSOI, kSOI_SDS_DATATYPE);

      if (pFn_sds_rank && pFn_sds_length && pFn_sds_datatype)
      {
	 /* One segment per record only! */
	 int rank = (*pFn_sds_rank)(sds);
	 int *dims = (*pFn_sds_length)(sds);
	 memset(segout, 0, sizeof(DRMS_Segment_t));
	 segout->info = (DRMS_SegmentInfo_t *)malloc(sizeof(DRMS_SegmentInfo_t));

	 if (segout->info)
	 {
	    memset(segout->info, 0, sizeof(DRMS_SegmentInfo_t));
	    snprintf(segout->info->name, DRMS_MAXSEGNAMELEN, "%s", segname);
	    segout->info->segnum = 0; /* only one segment */
	    segout->info->type = SOITypeToDRMSType((*pFn_sds_datatype)(sds));
	    segout->info->naxis = rank;
	    segout->info->protocol = protocol;
	    segout->info->scope = DRMS_VARIABLE;

	    memcpy(segout->axis, dims, sizeof(int) * rank);

	    /* IMPORTANT: If bscale OR bzero are present, SOI code (sds_read_fits()) will 
	     * convert data to either float or double.  bzero and bscale are needed
	     * to do the scaling part of the conversion.  If |bitpix| > 16, 
	     * then conversion to double happens, else conversion to float. 
	     * sds_get_fits_head() will always give you the raw data type. 
	     * So, adjust for this now.  */
	    
	    /* SOI will convert to either float or double */ 	 
	    if (segout->info->type == DRMS_TYPE_INT || segout->info->type == DRMS_TYPE_LONGLONG)
	    {
	       segout->info->type = DRMS_TYPE_DOUBLE;
	    }
	    else if (segout->info->type == DRMS_TYPE_CHAR || segout->info->type == DRMS_TYPE_SHORT)
	    {
	       segout->info->type = DRMS_TYPE_FLOAT;
	    }

            if (filename && *filename)
            {
               snprintf(segout->filename, DRMS_MAXPATHLEN, "%s", filename);
            }

	    status = kDSDS_Stat_Success;
	 }
	 else
	 {
	    status = kDSDS_Stat_NoMemory;
	 }
      }
      else
      {
	 status = kDSDS_Stat_MissingAPI;
      }
   }

   if (stat)
   {
      *stat = status;
   }
}

static DRMS_Array_t *CreateDRMSArray(void *hSOI, SDS *sds, kDSDS_Stat_t *stat)
{
   DRMS_Array_t *ret = NULL;
   DRMS_Array_t *local = NULL;
   kDSDS_Stat_t status = kDSDS_Stat_Success;

   if (hSOI)
   {
      pSOIFn_sds_data_t pFn_sds_data =
	(pSOIFn_sds_data_t)GetSOIFPtr(hSOI, kSOI_SDS_DATA);
      pSOIFn_sds_datatype_t pFn_sds_datatype =
	(pSOIFn_sds_datatype_t)GetSOIFPtr(hSOI, kSOI_SDS_DATATYPE);
      pSOIFn_sds_data_length_t pFn_sds_data_length =
	(pSOIFn_sds_data_length_t)GetSOIFPtr(hSOI, kSOI_SDS_DATA_LENGTH);
      pSOIFn_sds_numbytes_t pFn_sds_numbytes =
	(pSOIFn_sds_numbytes_t)GetSOIFPtr(hSOI, kSOI_SDS_NUMBYTES);
      pSOIFn_sds_rank_t pFn_sds_rank =
	(pSOIFn_sds_rank_t)GetSOIFPtr(hSOI, kSOI_SDS_RANK);
      pSOIFn_sds_length_t pFn_sds_length =
	(pSOIFn_sds_length_t)GetSOIFPtr(hSOI, kSOI_SDS_LENGTH);

      if (pFn_sds_data && pFn_sds_datatype &&
	  pFn_sds_data_length && pFn_sds_numbytes &&
	  pFn_sds_rank && pFn_sds_length)
      {

	 void *data = (*pFn_sds_data)(sds);

	 /* create DRMS_Array_t */ 
	 local = (DRMS_Array_t *)calloc(1, sizeof(DRMS_Array_t));
	 if (local)
	 {
	    int datasize = (*pFn_sds_numbytes)(sds);
	    long long datalen = (*pFn_sds_data_length)(sds) * datasize;
	    local->type = SOITypeToDRMSType((*pFn_sds_datatype)(sds));
	    local->naxis = (*pFn_sds_rank)(sds);

	    if (local->naxis < DRMS_MAXRANK)
	    {
	       int *dims = (*pFn_sds_length)(sds);
	       memcpy(local->axis, dims, sizeof(int) * local->naxis);

	       if (data)
	       {
		  local->data = calloc(1, datalen);
		  if (local->data)
		  {
		     memcpy(local->data, data, datalen);
		  }
		  else
		  {
		     status = kDSDS_Stat_NoMemory;
		  }
	       }
	    }
	    else
	    {
	       fprintf(stderr, "Unsupported data rank %d.\n", local->naxis);
	       status = kDSDS_Stat_InvalidRank;
	    }

	    if (status == kDSDS_Stat_Success)
	    {
	       /* SDS's always created with bzero == 0.0 and bscale = 1.0. */
	       local->bzero = 0.0;
	       local->bscale = 1.0;
	       local->israw = 1; /* bzero and bscale have NOT been applied to data.
				  * Point is moot because drms_segment_read() will
				  * change this to 0 if the type requested is 
				  * anything but DRMS_TYPE_RAW, otherwise it
				  * is set to 1. */
	       /* The rest are irrelevant - except parent_segment, which is 
		* is set in drms_segment_read(). */

	       /* Set offset multiplier. */
	       int i;
	       local->dope[0] = datasize;
	       for (i = 1; i < local->naxis; i++)
	       {
		  local->dope[i] = local->dope[i-1] * local->axis[i-1];
	       }

	       ret = local;
	    }
	    else
	    {
	       if (local->data)
	       {
		  free(local->data);
		  local->data = NULL;
	       }

	       free(local);
	       local = NULL;
	    }
	 }
	 else
	 {
	    status = kDSDS_Stat_NoMemory;
	 }
      }
      else
      {
	 status = kDSDS_Stat_MissingAPI;
      }
   } /* hSOI */

   if (stat)
   {
      *stat = status;
   }

   return ret;
}


/* Creates a prototype DRMS_RecordSet_t that contains containers for keywords and segments 
 *   Cannot call any DRMS functions from here, so just create structures.
 *   Create keywords using SDS headers (don't read data now).
 *   Create segments using SDS information.  Must save keylist for future calls to
 *     drms_segment_read() (must call getkey_int(keylist, "in_nsets"), vds_open(keylist,
 *     dsname), etc.).
 *
 *   In record keywords, store dsname ("in_%d") and sn.  Then in drms_segment_read()
 *   use vds_open(keylist, dsname) and vds_select(vds, 0, sn) to read in data.
 *
 *   Make one record per fits file.  All records go in one series.
 */
long long DSDS_open_records(const char *dsspec, 
			    char *drmsSeries,
			    DSDS_pHandle_t hparams,
			    DSDS_KeyList_t ***keys,
			    DRMS_Segment_t **segs,
			    kDSDS_Stat_t *stat)
{
    kDSDS_Stat_t status = kDSDS_Stat_Success;
    void *hSOI = GetSOI(&status);
    long long nRecs = 0; /* No. of SOI records (some may have no data (SDSs)) */
    long long nDRMSRecs = 0; /* No. of DRMS records (all have data (at least one SDS)) */
    char drmsSeriesName[DRMS_MAXSERIESNAMELEN];
    KEY *params = NULL;
    char datafile[PATH_MAX];
    char datapath[PATH_MAX];
    
    *drmsSeries = '\0';
    *keys = NULL;
    *segs = NULL;
    int keepsegs = 0;
    
    if (hSOI)
    {
        pSOIFn_getkey_str_t pFn_getkey_str = 
        (pSOIFn_getkey_str_t)GetSOIFPtr(hSOI, kSOI_GETKEY_STR);
        pSOIFn_getkey_int_t pFn_getkey_int = 
        (pSOIFn_getkey_int_t)GetSOIFPtr(hSOI, kSOI_GETKEY_INT);
        pSOIFn_vds_last_record_t pFn_vds_last_record =
        (pSOIFn_vds_last_record_t)GetSOIFPtr(hSOI, kSOI_VDS_LAST_RECORD);
        pSOIFn_vds_open_t pFn_vds_open = 
        (pSOIFn_vds_open_t)GetSOIFPtr(hSOI, kSOI_VDS_OPEN);
        pSOIFn_vds_close_t pFn_vds_close = 
        (pSOIFn_vds_close_t)GetSOIFPtr(hSOI, kSOI_VDS_CLOSE);
        pSOIFn_VDS_select_hdr_t pFn_VDS_select_hdr =
        (pSOIFn_VDS_select_hdr_t)GetSOIFPtr(hSOI, kSOI_VDS_SELECT_HDR);
        pSOIFn_sds_datatype_t pFn_sds_datatype =
        (pSOIFn_sds_datatype_t)GetSOIFPtr(hSOI, kSOI_SDS_DATATYPE);
        
        if (pFn_getkey_str && 
            pFn_getkey_int && pFn_vds_last_record && pFn_vds_open &&
            pFn_vds_close && pFn_VDS_select_hdr && pFn_sds_datatype)
        {
            VDS *vds = NULL;
            SDS *sds = NULL;
            int nds = 0;
            int ds = 0;
            char dsname[kDSDS_MaxKeyName];
            char key[kDSDS_MaxKeyName];
            
            int fsn = 0;
            int lsn = 0;
            int sn = 0;
            
            LinkedList_t *wdlist = NULL;
            
            params = CreateSOIKeylist(dsspec, &wdlist, &status);
            
            if (status == kDSDS_Stat_Success)
            {
                nds = (*pFn_getkey_int)(params, "in_nsets");
                nRecs = NumRecords(hSOI, nds, params, &status);
                
                if (status == kDSDS_Stat_Success)
                {
                    /* can now malloc structures */
                    if (keys && segs)
                    {
                        /* The actual number of drms records created is <= nRecs, so
                         * some of these DSDS_KeyList_ts may not get used. But *keys
                         * will get freed by DSDS_free_keylistarr() */
                        
                        /* If the SDS has a protocol of FITS_MERGE, then the SDS file contains
                         * data for more than one record (like a TAS file in DRMS). So, we don't
                         * know at this point how many DRMS segments we want to create. We have
                         * to first call vds_open() to get that information. */
                        *keys = (DSDS_KeyList_t **)malloc(sizeof(DSDS_KeyList_t *) * nRecs);
                        *segs = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t) * nRecs);
                        if (*keys && *segs)
                        {
                            memset(*keys, 0, sizeof(DSDS_KeyList_t *) * nRecs);
                            memset(*segs, 0, sizeof(DRMS_Segment_t) * nRecs);
                        }
                        else
                        {
                            status = kDSDS_Stat_NoMemory;
                        }
                    }
                    else
                    {
                        status = kDSDS_Stat_InvalidParams;
                    }
                }
            }
            
            /* loop through datasets (directories) */
            int iRec = 0;
            for (ds = 0; status == kDSDS_Stat_Success && ds < nds; ds++) 
            {
                sprintf(dsname, "in_%d", ds);
                if (ds == 0)
                {
                    /* Create DRMS (temporary) series name - this series won't be saved to dbase. */
                    /* Must make a new series each time DSDS_open_records() is called 
                     * since the caller could make many calls to DSDS_open_records() requesting 
                     * the same DSDS series.  If each subsequent requests retrieve records with 
                     * differing keyword sets, then the cached template record will not
                     * be accurate from call to call. */
                    MakeDRMSSeriesName(hSOI, 
                                       drmsSeriesName, 
                                       sizeof(drmsSeriesName), 
                                       params, 
                                       dsname,
                                       &status);
                }
                
                vds = (*pFn_vds_open)(params, dsname);
                
                if (vds)
                {
                    sprintf(key, "%s_fsn", dsname); 
                    fsn = (*pFn_getkey_int)(params, key);
                    sprintf(key, "%s_lsn", dsname); 
                    lsn = (*pFn_getkey_int)(params, key);
                    
                    if (lsn == -1)
                    {
                        lsn = (*pFn_vds_last_record)(vds);
                    }
                    
                    /* get series_num */
                    snprintf(key, sizeof(key), "%s_series_sn", dsname);
                    int series_num = (*pFn_getkey_int)(params, key);
                    
                    /* loop through records (fits files) within a dataset */
                    DRMS_Keyword_t *drmskey = NULL;
                    
                    for (sn = fsn; status == kDSDS_Stat_Success && sn <= lsn; sn++)
                    {
                        DSDS_KeyList_t *pKL = NULL;
                        
                        sds = (*pFn_VDS_select_hdr)(vds, 0, sn);
                        
                        if (sds)
                        {
                            /* make primary index keywords - series_num and sn */
                            (*keys)[nDRMSRecs] = (DSDS_KeyList_t *)malloc(sizeof(DSDS_KeyList_t));
                            pKL = (*keys)[nDRMSRecs];
                            pKL->next = NULL;
                            
                            drmskey = (DRMS_Keyword_t *)malloc(sizeof(DRMS_Keyword_t));
                            memset(drmskey, 0, sizeof(DRMS_Keyword_t));
                            drmskey->info = (DRMS_KeywordInfo_t *)malloc(sizeof(DRMS_KeywordInfo_t));
                            memset(drmskey->info, 0, sizeof(DRMS_KeywordInfo_t));
                            
                            snprintf(drmskey->info->name, DRMS_MAXKEYNAMELEN, "%s", kDSDS_SERIES_NUM);
                            drmskey->info->type = DRMS_TYPE_INT;
                            GetKWFormat(drmskey->info->format, DRMS_MAXFORMATLEN, drmskey->info->type);
                            
                            snprintf(drmskey->info->description, 
                                     DRMS_MAXCOMMENTLEN,
                                     "%s",
                                     "Identifies dataset.");
                            
                            (drmskey->value).int_val = series_num;
                            
                            pKL->elem = drmskey;
                            
                            /* make primary index keywords - sn */
                            pKL->next = (DSDS_KeyList_t *)malloc(sizeof(DSDS_KeyList_t));
                            pKL = pKL->next;
                            pKL->next = NULL;
                            
                            drmskey = (DRMS_Keyword_t *)malloc(sizeof(DRMS_Keyword_t));
                            memset(drmskey, 0, sizeof(DRMS_Keyword_t));
                            drmskey->info = (DRMS_KeywordInfo_t *)malloc(sizeof(DRMS_KeywordInfo_t));
                            memset(drmskey->info, 0, sizeof(DRMS_KeywordInfo_t));
                            
                            snprintf(drmskey->info->name, DRMS_MAXKEYNAMELEN, "%s", kDSDS_RN);
                            drmskey->info->type = DRMS_TYPE_INT;
                            GetKWFormat(drmskey->info->format, DRMS_MAXFORMATLEN, drmskey->info->type);
                            
                            snprintf(drmskey->info->description, 
                                     DRMS_MAXCOMMENTLEN,
                                     "%s",
                                     "Identifies record within dataset.");
                            
                            (drmskey->value).int_val = sn;
                            
                            pKL->elem = drmskey;
                            
                            /* make keyword needed for reading fits file - ds */
                            pKL->next = (DSDS_KeyList_t *)malloc(sizeof(DSDS_KeyList_t));
                            pKL = pKL->next;
                            pKL->next = NULL;
                            
                            drmskey = (DRMS_Keyword_t *)malloc(sizeof(DRMS_Keyword_t));
                            memset(drmskey, 0, sizeof(DRMS_Keyword_t));
                            drmskey->info = (DRMS_KeywordInfo_t *)malloc(sizeof(DRMS_KeywordInfo_t));
                            memset(drmskey->info, 0, sizeof(DRMS_KeywordInfo_t));
                            
                            snprintf(drmskey->info->name, DRMS_MAXKEYNAMELEN, "%s", kDSDS_DS);
                            drmskey->info->type = DRMS_TYPE_INT;
                            GetKWFormat(drmskey->info->format, DRMS_MAXFORMATLEN, drmskey->info->type);
                            
                            snprintf(drmskey->info->description, 
                                     DRMS_MAXCOMMENTLEN,
                                     "%s",
                                     "Identifies virtual dataset.");
                            
                            (drmskey->value).int_val = ds;
                            
                            pKL->elem = drmskey;
                            
                            
                            /* loop through attributes */
                            datafile[0] = '\0';
                            int dataexist = 0;
                            char *pdf = datafile;
                            LoopAttrs(hSOI, sds, pKL, &status, &pdf, sizeof(datafile));
                            
                            if (strlen(datafile) > 0)
                            {
                                /* There is a data file.  There may be a single record's data
                                 * per FITS file, in which case sds->filename contains
                                 * the FITS file path (protocol RDB.FITS).  Or, there may be multiple 
                                 * records' data per FITS file, in which case 
                                 * vds->filename contains the FITS file path (protocol 
                                 * RDB.FITS_MERGE).
                                 */
                                dataexist = 1;
                                keepsegs = 1; /* There is at least one data file present in the series*/
                                
                                if (sds->filename && strlen(sds->filename) > 0)
                                {
                                    snprintf(datapath, sizeof(datapath), "%s", sds->filename);
                                }
                                else if (vds->filename && strlen(vds->filename) > 0)
                                {
                                    snprintf(datapath, sizeof(datapath), "%s", vds->filename);
                                }
                                else
                                {
                                    /* error */
                                    status = kDSDS_Stat_UnkFITSpath;
                                }
                            }
                            
                            /* Must make segment now */
                            DRMS_Segment_t *drmsseg = &((*segs)[nDRMSRecs]);
                            kDSDS_Stat_t segstatus = kDSDS_Stat_Success;
                            
                            /* It is possible that VDS will create a record with no segment.  This is accomplished 
                             * by creating a .record.rdb file, but no .fits file.  When this happens, we
                             * basically have something analogous to a DRMS record with no segment.  Leave the 
                             * segment undefined (the struct is zero'd out) so that callers of DSDS_open_records()
                             * know there is no data segment. */
                            if (status == kDSDS_Stat_Success && dataexist)
                            {
                                FillDRMSSeg(hSOI, 
                                            sds, 
                                            drmsseg, 
                                            kDSDS_Segment, 
                                            DRMS_DSDS, 
                                            datapath, 
                                            &segstatus);
                            }
                            
                            if (segstatus != kDSDS_Stat_Success)
                            {
                                /* free rec and its keyword list */
                                FreeDSDSKeyList(&((*keys)[nDRMSRecs]));
                                status = segstatus;
                            }
                            
                            if (status == kDSDS_Stat_Success)
                            {
                                nDRMSRecs++;
                            }
                        }
                        
                        if (status == kDSDS_Stat_Success)
                        { 
                            iRec++;
                        }
                    } /* record loop */
                    
                    (*pFn_vds_close)(&vds); /* frees sds's */
                } /* vds */
            } /* dataset loop */
            
            list_llfree(&wdlist);
        }
        else
        {
            status = kDSDS_Stat_MissingAPI;
        }
    }
    
    if (status == kDSDS_Stat_Success)
    {
        snprintf(drmsSeries, DRMS_MAXSERIESNAMELEN, "%s", drmsSeriesName);
        GenerateHandle("DSDS_KEY", params, hparams);
        
        if (!keepsegs && segs)
        {
            DSDS_free_segarr(segs, nDRMSRecs);
        }
    }
    else
    {
        if (keys)
        {
            DSDS_free_keylistarr(keys, nDRMSRecs);
        }
        if (segs)
        {
            DSDS_free_segarr(segs, nDRMSRecs);
        }
    }
    
    if (stat)
    {
        *stat = status;
    }
    
    return nDRMSRecs;
}

void DSDS_free_keylist(DSDS_KeyList_t **pkl)
{
   if (pkl)
   {
      FreeDSDSKeyList(pkl);
   }
}

void DSDS_free_keylistarr(DSDS_KeyList_t ***pklarr, int n)
{
   int iRec;

   if (pklarr)
   {
      DSDS_KeyList_t **arr = *pklarr;

      if (arr)
      {
	 for (iRec = 0; iRec < n; iRec++)
	 {
	    if (arr[iRec])
	    {
	       FreeDSDSKeyList(&(arr[iRec]));
	    }
	 }

	 free(arr);
      }

      *pklarr = NULL;
   }
}

void DSDS_free_seg(DRMS_Segment_t **seg)
{
   if (seg && *seg)
   {
      if ((*seg)->info)
      {
	 free((*seg)->info);
      }

      free(*seg);
      *seg = NULL;
   }
}

/* frees array of DRMS_Segment_ts that were malloc'd as a continguous block. */
void DSDS_free_segarr(DRMS_Segment_t **psarr, int n)
{
   if (psarr && *psarr)
   {
      int i;
      for (i = 0; i < n; i++)
      {
	 DRMS_Segment_t *seg = (*psarr + i);

	 /* need to free malloc'd mem within each segment */
	 if (seg->info)
	 {
	    free(seg->info);
	 }
      }

      free(*psarr);
   }

   *psarr = NULL;
}

void DSDS_steal_seginfo(DRMS_Segment_t *thief, DRMS_Segment_t *victim)
{
   /* Not only purloins seg->info, but copies all of seg too */
   if (thief && victim)
   {
      *thief = *victim;
      victim->info = NULL;
      victim->record = NULL;
   }
}

/* Ideally the DRMS module will call this at shutdown, but the logic to figure out whether or not a VDS cache exists in a module plugin
 * would be a little convoluted, so we'll probably simply not clean up the cache. However, when drms_close_records() is called, 
 * the associated open VDSs will be freed and removed from the cache. So at shutdown, there will be an empty cache that doesn't get
 * freed. */
void DSDS_free_vdscache()
{
    destroyVDSCache();
}

/* keylist must come from series (all recs have the same keylist).  So, in
 * drms_segment_read(), get record, then seriesinfo, then keylist handle.
 *
 * If no paramsDesc is provided, then create an SDS from filename, if it 
 * exists, otherwise, fail.
 * 
 * paramsDesc is a reference to the KEY list that vds_open() uses to identify
 * the VDS that is to be opened.
 */
DRMS_Array_t *DSDS_segment_read(char *paramsDesc, int ds, int rn, const char *filename, kDSDS_Stat_t *stat)
{
    kDSDS_Stat_t status = kDSDS_Stat_Success;
    void *hSOI = GetSOI(&status);
    DRMS_Array_t *ret = NULL;
    DRMS_Array_t *local = NULL;
    
    if (hSOI)
    {
        if (!paramsDesc)
        {
            pSOIFn_sds_read_fits_t pFn_sds_read_fits =
            (pSOIFn_sds_read_fits_t)GetSOIFPtr(hSOI, kSOI_SDS_READ_FITS);
            pSOIFn_sds_free_t pFn_sds_free =
            (pSOIFn_sds_free_t)GetSOIFPtr(hSOI, kSOI_SDS_FREE);
            
            if (pFn_sds_read_fits && pFn_sds_free)
            {
                FILE *fp = fopen(filename, "r");
                if (fp)
                {
                    SDS *sds = (*pFn_sds_read_fits)(fp);
                    if (sds)
                    {
                        fclose(fp);
                        fp = NULL;
                        local = CreateDRMSArray(hSOI, sds, &status);
                        (*pFn_sds_free)(&sds);
                    }
                }
            }
            else
            {
                status = kDSDS_Stat_MissingAPI;
            }
        }
        else
        {
            pSOIFn_vds_open_t pFn_vds_open =
            (pSOIFn_vds_open_t)GetSOIFPtr(hSOI, kSOI_VDS_OPEN);
            pSOIFn_VDS_select_rec_t pFn_VDS_select_rec =
            (pSOIFn_VDS_select_rec_t)GetSOIFPtr(hSOI, kSOI_VDS_SELECT_REC);
            pSOIFn_vds_close_t pFn_vds_close =
            (pSOIFn_vds_close_t)GetSOIFPtr(hSOI, kSOI_VDS_CLOSE);
            pSOIFn_sds_free_data_t pFn_sds_free_data =
            (pSOIFn_sds_free_data_t)GetSOIFPtr(hSOI, kSOI_SDS_FREE_DATA);
            
            /* Need something here to distinguish a FITS_MERGE VDS from a non-FITS_MERGE one. If the VDS is a 
             * FITS_MERGE VDS, then attempt to fetch the VDS from the container of open VDSs. If the VDS isn't
             * in this container, then open the VDS and put it in the container (flushing some chunk of the 
             * cache if the cache is full). */
            
            
            SDS *sds = NULL;
            VDS *vds = NULL;
            KEY *keylist = NULL;
            char dsname[kDSDS_MaxKeyName];
            
            if (pFn_vds_open && pFn_VDS_select_rec && pFn_vds_close && pFn_sds_free_data)
            {
                DSDS_Handle_t hparams = FindHandle(paramsDesc);
                GetStructure(hparams, (void **)&keylist);
                sprintf(dsname, "in_%d", ds);
                vds = getVDS(keylist, dsname, hparams, NULL); /* Ignore error code, I guess. */

                if (vds)
                {
                    sds = (*pFn_VDS_select_rec)(vds, 0, rn);
                    
                    if (sds)
                    {
                        local = CreateDRMSArray(hSOI, sds, &status);
                        
                        /* Can't free the entire SDS, as I'd like, because there is some difficult-to-see connection between
                         * the containing VDS and the SDSs. And there doesn't seem to be a way to tell VDS to clean-up its
                         * VDS. So we're stuck with a leak. */
                        (*pFn_sds_free_data)(sds);
                    }
                    
                    /* Used to call vds_close() here, but do not do that any more. Instead, the VDS will be closed when drms_close_records() is called. */
                }
            }
            else
            {
                status = kDSDS_Stat_MissingAPI;
            }
        }  
    }
    
    if (status == kDSDS_Stat_Success)
    {
        ret = local;
    }
    
    if (stat)
    {
        *stat = status;
    }
    
    return ret;
}

void DSDS_free_array(DRMS_Array_t **arr)
{
   if (arr && *arr)
   {
      if ((*arr)->data)
      {
	 free((*arr)->data);
      }

      free(*arr);
      *arr = NULL;
   }
}

void DSDS_handle_todesc(DSDS_Handle_t handle, char *desc, kDSDS_Stat_t *stat)
{
    int error = 1;
    
    /* First check validity of handle */
    if (handle)
    {
        if (FindHandle(handle))
        {
            error = 0;
            snprintf(desc, kDSDS_MaxHandle, "[%s]", handle);
        }
    }
    
    if (stat)
    {
        if (error)
        {
            *stat = kDSDS_Stat_InvalidHandle;
        }
        else
        {
            *stat = kDSDS_Stat_Success;
        }
    }
}

void DSDS_free_handle(DSDS_pHandle_t pHandle)
{
   /* Free the associated open VDS, if one exists, from the VDS cache. */
   removeVDSFromCache(*pHandle);

   DestroyHandle(pHandle);
}

/* returns number of keywords */
int DSDS_read_fitsheader(const char *file, 
			 DSDS_KeyList_t **keylist,
			 DRMS_Segment_t **seg,
			 const char *segname,
			 kDSDS_Stat_t *stat)
{
   kDSDS_Stat_t status = kDSDS_Stat_InvalidFITS;
   DSDS_KeyList_t *retlist = NULL;
   DRMS_Segment_t *retseg = NULL;
   int nKeys = 0;

   void *hSOI = GetSOI(&status);

   if (hSOI)
   {
      pSOIFn_sds_get_fits_head_t pFn_sds_get_fits_head = 
	(pSOIFn_sds_get_fits_head_t)GetSOIFPtr(hSOI, kSOI_SDS_GET_FITS_HEAD);
      pSOIFn_sds_free_t pFn_sds_free = 
	(pSOIFn_sds_free_t)GetSOIFPtr(hSOI, kSOI_SDS_FREE);

      if (pFn_sds_get_fits_head && pFn_sds_free)
      {
	 char *fitsfile = strdup(file);
	 SDS *sds = (*pFn_sds_get_fits_head)(fitsfile);
	 if (sds)
	 {
	    /* create keyword list */
	    retlist = (DSDS_KeyList_t *)malloc(sizeof(DSDS_KeyList_t));
	    if (retlist)
	    {
	       retlist->elem = NULL;
	       retlist->next = NULL;

	       /* loop through attributes */
	       nKeys = LoopAttrs(hSOI, sds, retlist, &status, NULL, 0);
	    }
	    else
	    {
	       status = kDSDS_Stat_NoMemory;
	    }

	    retseg = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t));
            memset(retseg, 0, sizeof(DRMS_Segment_t));
	    if (retseg)
	    {
               FillDRMSSeg(hSOI, sds, retseg, segname, DRMS_LOCAL, fitsfile, &status);
            }
	    else
	    {
	       status = kDSDS_Stat_NoMemory;
	    }

	    (*pFn_sds_free)(&sds);
	 }

	 if (fitsfile)
	 {
	    free(fitsfile);
	 }
      }
      else
      {
	 status = kDSDS_Stat_MissingAPI;
      }
   }

   if (stat)
   {
      *stat = status;
   }

   if (status == kDSDS_Stat_Success)
   {
      if (keylist)
      {
	 *keylist = retlist;
      }
      if (seg)
      {
	 *seg = retseg;
      }
   }
   else
   {
      if (retlist)
      {
	 free(retlist);
      }
      if (retseg)
      {
	 free(retseg);
      }
   }

   return nKeys;
}
