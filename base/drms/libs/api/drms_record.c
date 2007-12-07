// #define DEBUG
#include <dirent.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "drms_dsdsapi.h"

#ifndef DEBUG
#undef TIME
#define TIME(code) code
#endif

#define kMAXRSETS 128
#define kMAXRSETSPEC (DRMS_MAXNAMELEN + DRMS_MAXQUERYLEN + 128)

static void *ghDSDS = NULL;
static int gAttemptedDSDS = 0;

typedef enum
{
   kRSParseState_Begin = 0,   /* First char of the input string */
   kRSParseState_NameChar,    /* Parsing a drms series name */
   kRSParseState_EndName,     /* Remove trailing whitespace from drms series name */
   kRSParseState_FiltChar,    /* Parsing a filter (stuff between '[' and ']') */
   kRSParseState_EndFilt,     /* A ']' seen */
   kRSParseState_QuerySQL,    /* A "[?" seen */
   kRSParseState_EndQuerySQL, /* A "?]" seen */
   kRSParseState_EndRS,       /* Another RS to follow (comma seen) */
   kRSParseState_LastRS,      /* No RS's to follow */
   kRSParseState_BeginDSDS,   /* A '{' seen */
   kRSParseState_BeginAtFile, /* A '@' - file that contains queries */
   kRSParseState_AtFile,      /* Parsing "@file". */
   kRSParseState_EndAtFile,   /* A comma or end of input seen while parsing "@file". */
   kRSParseState_DSDS,        /* Parsing 'prog' specification */
   kRSParseState_TrailDSDS,   /* Trailing spaces after 'prog' specification and before '}' */
   kRSParseState_EndDSDS,     /* A '}' seen */
   kRSParseState_End,         /* Only spaces or end of input */
   kRSParseState_Done,         /* Parsing done */
   kRSParseState_Error,
} RSParseState_t;

static int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopySegments(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyLinks(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyKeywords(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyPrimaryIndex(DRMS_Record_t *target, DRMS_Record_t *source);
static int ParseRecSetDesc(const char *recsetsStr, char ***sets, int *nsets);
static int FreeRecSetDescArr(char ***sets, int nsets);

/* drms_open_records() helpers */
static int IsLocalSpec(const char *recSetSpec, 
		       DSDS_KeyList_t ***klarrout, 
		       DRMS_Segment_t **segarrout,
		       int *nRecsout,
		       char **pkeysout,
		       int *status);
static void AddLocalPrimekey(DRMS_Record_t *template, int *status);
static int CreateRecordProtoFromFitsAgg(DRMS_Env_t *env,
					DSDS_KeyList_t **keylistarr, 
					DRMS_Segment_t *segarr,
					int nRecs,
					char **pkeyarr,
					int nPKeys,
					DRMS_KeyMapClass_t fitsclass,
					DRMS_Record_t **proto,
					DRMS_Segment_t **segout,
					int *status);
static void AdjustRecordProtoSeriesInfo(DRMS_Env_t *env, 
					DRMS_Record_t *proto, 
					const char *seriesName);
static void AllocRecordProtoSeg(DRMS_Record_t *template, DRMS_Segment_t *seg, int *status);
static void SetRecordProtoPKeys(DRMS_Record_t *template, 
				char **pkeyarr, 
				int nkeys, 
				int *status);
static DRMS_Record_t *CacheRecordProto(DRMS_Env_t *env, 
				       DRMS_Record_t *proto, 
				       const char *seriesName, 
				       int *status);
static DRMS_RecordSet_t *CreateRecordsFromDSDSKeylist(DRMS_Env_t *env,
						      int nRecs,
						      DRMS_Record_t *cached,
						      DSDS_KeyList_t **klarr,
						      DRMS_Segment_t *segarr,
						      int pkeysSpecified,
						      DRMS_KeyMapClass_t fitsclass,
						      int *status);
static DRMS_RecordSet_t *OpenLocalRecords(DRMS_Env_t *env, 
					  DSDS_KeyList_t ***klarr,
					  DRMS_Segment_t **segarr,
					  int nRecs,
					  char **pkeysout,
					  int *status);
static void RSFree(const void *val);
/* end drms_open_records() helpers */


/* A valid local spec is:
 *   ( <file> | <directory> ) { '[' <keyword1>, <keyword2>, <...> ']' }
 *
 * where () denotes grouping, {} denotes optional, and '' denotes literal
 */
static int IsLocalSpec(const char *recSetSpecIn, 
		       DSDS_KeyList_t ***klarrout, 
		       DRMS_Segment_t **segarrout,
		       int *nRecsout,
		       char **pkeysout,
		       int *status)
{
   int isLocSpec = 0;
   struct stat stBuf;
   int lstat = DRMS_SUCCESS;
   char *lbrack = NULL;
   int keylistSize = sizeof(char) * (strlen(recSetSpecIn) + 1);
   char *keylist = malloc(keylistSize);
   char *recSetSpec = strdup(recSetSpecIn);

   /* There could be an optional clause at the end of the file/directory spec. */
   if ((lbrack = strchr(recSetSpec, '[')) != NULL)
   {
      if (strchr(lbrack, ']'))
      {
	 *lbrack = '\0';
	 snprintf(keylist, keylistSize, "%s", lbrack + 1);
	 *(strchr(keylist, ']')) = '\0';
	 *pkeysout = strdup(keylist);
	 free(keylist);
      }
   }
   else
   {
      *pkeysout = NULL;
   }

   /* Basically, if recSetSpec is a FITS file, directory of FITS files, or 
    * a link to either of these, then it refers to a set of local files */
   if (recSetSpec && !stat(recSetSpec, &stBuf) && klarrout && segarrout)
   {
      if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode) || S_ISDIR(stBuf.st_mode))
      {
	 /* ensure these files are all fits files - okay to use libdsds.so for now 
	  * but this should be divorced from Stanford-specific code in the future */
	 if (!gAttemptedDSDS && !ghDSDS)
	 {
	    char lpath[PATH_MAX];
	    const char *root = getenv(kJSOCROOT);
	    const char *mach = getenv(kJSOC_MACHINE);
	    if (root && mach)
	    {
	       char *msg = NULL;
	       snprintf(lpath, 
			sizeof(lpath), 
			"%s/lib/%s/libdsds.so", 
			root, 
			mach);
	       dlerror();
	       ghDSDS = dlopen(lpath, RTLD_NOW);
	       if ((msg = dlerror()) != NULL)
	       {
		  /* dsds library not found */
		  fprintf(stderr, "dlopen(%s) error: %s.\n", lpath, msg);
		  lstat = DRMS_ERROR_CANTOPENLIBRARY;
	       }
	    }

	    gAttemptedDSDS = 1;
	 }

	 if (lstat == DRMS_SUCCESS && ghDSDS)
	 {
	    DSDS_KeyList_t *kl = NULL;
	    DRMS_Segment_t *seg = NULL;
	    int iRec = 0;

	    pDSDSFn_DSDS_read_fitsheader_t pFn_DSDS_read_fitsheader = 
	      (pDSDSFn_DSDS_read_fitsheader_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_READ_FITSHEADER);
	    pDSDSFn_DSDS_free_keylist_t pFn_DSDS_free_keylist = 
	      (pDSDSFn_DSDS_free_keylist_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLIST);
	    pDSDSFn_DSDS_steal_seginfo_t pFn_DSDS_steal_seginfo =
	      (pDSDSFn_DSDS_steal_seginfo_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_STEAL_SEGINFO);
	    pDSDSFn_DSDS_free_seg_t pFn_DSDS_free_seg = 
	      (pDSDSFn_DSDS_free_seg_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_SEG);

	    /* for each file, check for a valid FITS header */
	    if (pFn_DSDS_read_fitsheader && pFn_DSDS_free_keylist &&
		pFn_DSDS_steal_seginfo && pFn_DSDS_free_seg)
	    {
	       kDSDS_Stat_t dsdsStat;
	       int fitshead = 1;

	       if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
	       {
		  (*pFn_DSDS_read_fitsheader)(recSetSpec, &kl, &seg, kLocalSegName, &dsdsStat);

		  if (dsdsStat == kDSDS_Stat_Success)
		  {
		     *klarrout = (DSDS_KeyList_t **)malloc(sizeof(DSDS_KeyList_t *));
		     *segarrout = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t));

		     (*klarrout)[0] = kl;
		     (*pFn_DSDS_steal_seginfo)(&((*segarrout)[0]), seg);
		     /* For local data, add filepath for use with later calls to 
		      * drms_segment_read(). */
		     snprintf((*segarrout)[0].filename, 
			      DRMS_MAXSEGFILENAME, 
			      "%s", 
			      recSetSpec);
		     (*pFn_DSDS_free_seg)(&seg);
		     *nRecsout = 1;
		  }
		  else
		  {
		     fitshead = 0;
		  }
	       }
	       else 
	       {
		  struct dirent **fileList = NULL;
		  int nFiles = -1;

		  if ((nFiles = scandir(recSetSpec, &fileList, NULL, NULL)) > 0 && 
		      fileList != NULL)
		  {
		     int fileIndex = 0;
		     iRec = 0;

		     /* nFiles is the maximum number of fits files possible */
		     *klarrout = (DSDS_KeyList_t **)malloc(sizeof(DSDS_KeyList_t *) * nFiles);
		     *segarrout = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t) * nFiles);

		     while (fileIndex < nFiles)
		     {
			struct dirent *entry = fileList[fileIndex];
			if (entry != NULL)
			{
			   char *oneFile = entry->d_name;
			   char dirEntry[PATH_MAX] = {0};
			   snprintf(dirEntry, 
				    sizeof(dirEntry), 
				    "%s%s%s", 
				    recSetSpec, 
				    recSetSpec[strlen(recSetSpec) - 1] == '/' ? "" : "/",
				    oneFile);
			   if (*dirEntry !=  '\0' && !stat(dirEntry, &stBuf));
			   {
			      if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
			      {
				 (*pFn_DSDS_read_fitsheader)(dirEntry, 
							     &kl, 
							     &seg, 
							     kLocalSegName, 
							     &dsdsStat);

				 if (dsdsStat == kDSDS_Stat_Success)
				 {
				    (*klarrout)[iRec] = kl;
				    (*pFn_DSDS_steal_seginfo)(&((*segarrout)[iRec]), seg);
				    /* For local data, add filepath for use with later calls to 
				     * drms_segment_read(). */
				    snprintf((*segarrout)[iRec].filename, 
					     DRMS_MAXSEGFILENAME, 
					     "%s", 
					     dirEntry);
				    (*pFn_DSDS_free_seg)(&seg);
				    iRec++;
				 }
				 else
				 {
				    fitshead = 0;
				    break;
				 }
			      }
			   }

			   free(entry);
			}
		 
			fileIndex++;
		     } /* while */

		     if (nRecsout)
		     {
			*nRecsout = iRec;
		     }
	      
		     free(fileList);
		  }
	       }

	       isLocSpec = fitshead;
	    }
	 }
      }	 
   }
   

   if (recSetSpec)
   {
      free(recSetSpec);
   }

   if (status)
   {
      *status = lstat;
   }

   return isLocSpec;
}

static void AddLocalPrimekey(DRMS_Record_t *template, int *status)
{
   int stat = DRMS_SUCCESS;
   char drmsKeyName[DRMS_MAXNAMELEN];
   DRMS_Keyword_t *tKey = NULL;

   snprintf(drmsKeyName, sizeof(drmsKeyName), kLocalPrimekey);

   /* insert into template */
   XASSERT(tKey = 
	   hcon_allocslot_lower(&(template->keywords), drmsKeyName));
   memset(tKey, 0, sizeof(DRMS_Keyword_t));
   XASSERT(tKey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
   memset(tKey->info, 0, sizeof(DRMS_KeywordInfo_t));
	    
   if (tKey && tKey->info)
   {
      /* record */
      tKey->record = template;

      /* keyword info */
	      
      //memcpy(tKey->info, sKey->info, sizeof(DRMS_KeywordInfo_t));
      snprintf(tKey->info->name, 
	       DRMS_MAXNAMELEN,
	       "%s",
	       drmsKeyName);

      tKey->info->type = DRMS_TYPE_LONGLONG;
      strcpy(tKey->info->format, "%lld");

      /* default value - missing */
      drms_missing(tKey->info->type, &(tKey->value));
   }
   else
   {
      stat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = stat;
   }
}

static int CreateRecordProtoFromFitsAgg(DRMS_Env_t *env,
					DSDS_KeyList_t **keylistarr, 
					DRMS_Segment_t *segarr,
					int nRecs,
					char **pkeyarr,
					int nPKeys,
					DRMS_KeyMapClass_t fitsclass,
					DRMS_Record_t **proto,
					DRMS_Segment_t **segout,
					int *status)
{
   DRMS_Record_t *template = NULL;
   DRMS_Segment_t *seg = NULL;
   int iRec = 0;
   int stat = DRMS_SUCCESS;
   int pkeysSpecified = (pkeyarr != NULL);
	   
   if (stat == DRMS_SUCCESS)
   {
      XASSERT(template = calloc(1, sizeof(DRMS_Record_t)));
      XASSERT(template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));
   }

   if (template && template->seriesinfo)
   {
      char drmsKeyName[DRMS_MAXNAMELEN];
      DRMS_Keyword_t *sKey = NULL;
      DRMS_Keyword_t *tKey = NULL;

      template->env = env;
      template->init = 1;
      template->recnum = 0;
      template->sunum = -1;
      template->sessionid = 0;
      template->sessionns = NULL;
      template->su = NULL;
      
      /* Initialize container structure. */
      hcon_init(&template->segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_segment_struct, 
		(void (*)(const void *, const void *)) drms_copy_segment_struct);
      /* Initialize container structures for links. */
      hcon_init(&template->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_link_struct, 
		(void (*)(const void *, const void *)) drms_copy_link_struct);
      /* Initialize container structure. */
      hcon_init(&template->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_keyword_struct, 
		(void (*)(const void *, const void *)) drms_copy_keyword_struct);

      /* Loop through DSDS records - put a superset of keywords in rec->keywords and 
       * ensure segments match */
      for (iRec = 0; iRec < nRecs; iRec++)
      {
	 /* multiple keywords per record - walk keylistarr */
	 DSDS_KeyList_t *kl = keylistarr[iRec];
	 DRMS_Segment_t *oneSeg = &(segarr[iRec]); 

	 if (kl && oneSeg)
	 {
	    while (kl != NULL && ((sKey = kl->elem) != NULL))
	    {
	       /* generate a valid drms keyword (fits might not be valid) */
	       if (!drms_keyword_getintname_ext(sKey->info->name, 
						&fitsclass,
						NULL, 
						drmsKeyName, 
						sizeof(drmsKeyName)))
	       {
		  *drmsKeyName = '\0';
		  stat = DRMS_ERROR_INVALIDDATA;
		  break;
	       }

	       if (!hcon_lookup_lower(&(template->keywords), drmsKeyName))
	       {
		  /* insert into template */
		  XASSERT(tKey = 
			  hcon_allocslot_lower(&(template->keywords), drmsKeyName));
		  memset(tKey, 0, sizeof(DRMS_Keyword_t));
		  XASSERT(tKey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
		  memset(tKey->info, 0, sizeof(DRMS_KeywordInfo_t));
	    
		  if (tKey && tKey->info)
		  {
		     /* record */
		     tKey->record = template;

		     /* keyword info */
		     memcpy(tKey->info, sKey->info, sizeof(DRMS_KeywordInfo_t));
		     snprintf(tKey->info->name, 
			      DRMS_MAXNAMELEN,
			      "%s",
			      drmsKeyName);

		     /* default value - missing */
		     drms_missing(tKey->info->type, &(tKey->value));
		  }
		  else
		  {
		     stat = DRMS_ERROR_OUTOFMEMORY;
		  }
	       }
		     
	       kl = kl->next;
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* one segment per record */
	       if (!seg)
	       {
		  seg = oneSeg;
	       }
	       else if (!drms_segment_segsmatch(seg, oneSeg))
	       {
		  stat = DRMS_ERROR_INVALIDDATA;
	       }
	    }
	 }
      } /* iRec */

      /* Add a keyword that will serve as primary key - not sure what to use here. 
       * What about an increasing integer? */
      if (fitsclass == kKEYMAPCLASS_LOCAL)
      {
	 if (!pkeysSpecified)
	 {
	    AddLocalPrimekey(template, &stat);
	 }
	 else
	 {
	    /* pkeysSpecified - ensure they exist; if kLocalPrimekey is specified, but
	     * doesn't exist, add it */
	    int iKey;
	    for (iKey = 0; iKey < nPKeys && stat == DRMS_SUCCESS; iKey++)
	    {
	       if (!hcon_member_lower(&(template->keywords), pkeyarr[iKey]))
	       {
		  if (strcasecmp(pkeyarr[iKey], kLocalPrimekey) == 0)
		  {
		     /* user specified kLocalPrimekey, but it doesn't 
		      * exist in the local data being read in.  This is 
		      * a special keyword that may have gotten added the last 
		      * time this code was run, so add it now. */
		     AddLocalPrimekey(template, &stat);
		  }
		  else
		  {
		     /* error - user specified a prime keyword that doesn't exist
		      * in the local data being read in. */
		     stat = DRMS_ERROR_INVALIDDATA;
		     fprintf(stderr, 
			     "keyword %s doesn't exist in fits file header\n", 
			     pkeyarr[iKey]);
		  }
	       }
	    }
	 }
      }
   }

   if (status)
   {
      *status = stat;
   }

   if (stat == DRMS_SUCCESS)
   {
      *proto = template;
      *segout = seg;
   }

   return iRec;
}

static void AdjustRecordProtoSeriesInfo(DRMS_Env_t *env, 
					DRMS_Record_t *proto, 
					const char *seriesName) 
{
   /* series info */
   char *user = getenv("USER");
   snprintf(proto->seriesinfo->seriesname,
	    DRMS_MAXNAMELEN,
	    "%s",
	    seriesName);

   strcpy(proto->seriesinfo->author, "unknown");
   strcpy(proto->seriesinfo->owner, "unknown");

   if (user)
   {
      if (strlen(user) < DRMS_MAXCOMMENTLEN)
      {
	 strcpy(proto->seriesinfo->author, user);
      }
		    
      if (strlen(user) < DRMS_MAXNAMELEN)
      {
	 strcpy(proto->seriesinfo->owner, user);
      }
   }

   /* discard "Owner", fill it with the dbuser */
   if (env->session->db_direct) 
   {
      strcpy(proto->seriesinfo->owner, env->session->db_handle->dbuser);
   }
}

static void AllocRecordProtoSeg(DRMS_Record_t *template, DRMS_Segment_t *seg, int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_Segment_t *tSeg = NULL;

   XASSERT(tSeg = hcon_allocslot_lower(&(template->segments), seg->info->name));
   memset(tSeg, 0, sizeof(DRMS_Segment_t));
   XASSERT(tSeg->info = malloc(sizeof(DRMS_SegmentInfo_t)));
   memset(tSeg->info, 0, sizeof(DRMS_SegmentInfo_t));

   if (tSeg && tSeg->info)
   {
      /* record */
      tSeg->record = template;
	       
      /* segment info*/
      memcpy(tSeg->info, seg->info, sizeof(DRMS_SegmentInfo_t));

      /* axis is allocated as static array */
      memcpy(tSeg->axis, seg->axis, sizeof(int) * DRMS_MAXRANK);
   }
   else
   {
      stat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = stat;
   }
}

static void SetRecordProtoPKeys(DRMS_Record_t *template, 
				char **pkeyarr, 
				int nkeys, 
				int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_Keyword_t *pkey = NULL;
   int ikey = 0;

   for (; ikey < nkeys; ikey++)
   {
      pkey = hcon_lookup_lower(&(template->keywords), pkeyarr[ikey]);
      if (pkey != NULL)
      {
	 template->seriesinfo->pidx_keywords[ikey] = pkey;
      }
      else
      {
	 stat = DRMS_ERROR_INVALIDKEYWORD;
	 break;
      }
   }

   template->seriesinfo->pidx_num = ikey;

   if (status)
   {
      *status = stat;
   }
}

static DRMS_Record_t *CacheRecordProto(DRMS_Env_t *env, 
				       DRMS_Record_t *proto, 
				       const char *seriesName, 
				       int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_Record_t *cached = NULL;

   if (hcon_member_lower(&(env->series_cache), seriesName))
   {
      fprintf(stderr,"drms_open_dsdsrecords(): "
	      "ERROR: Series '%s' already exists.\n", seriesName);
      drms_free_template_record_struct(proto);
      stat = DRMS_ERROR_INVALIDDATA;
   }
   else
   {
      cached = (DRMS_Record_t *)hcon_allocslot_lower(&(env->series_cache), seriesName);
      drms_copy_record_struct(cached, proto);  

      for (int i = 0; i < cached->seriesinfo->pidx_num; i++) 
      {
	 cached->seriesinfo->pidx_keywords[i] = 
	   drms_keyword_lookup(cached, 
			       proto->seriesinfo->pidx_keywords[i]->info->name, 
			       0);
      }

      drms_free_record_struct(proto);
   }

   if (status)
   {
      *status = stat;
   }
   
   return cached;
}

static DRMS_RecordSet_t *CreateRecordsFromDSDSKeylist(DRMS_Env_t *env,
						      int nRecs,
						      DRMS_Record_t *cached,
						      DSDS_KeyList_t **klarr,
						      DRMS_Segment_t *segarr,
						      int setPrimeKey,
						      DRMS_KeyMapClass_t fitsclass,
						      int *status)
{
   int stat = DRMS_SUCCESS;
   int iRec;
   DRMS_RecordSet_t *rset = (DRMS_RecordSet_t *)malloc(sizeof(DRMS_RecordSet_t));
   rset->n = nRecs;
   rset->records = (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nRecs);
   memset(rset->records, 0, sizeof(DRMS_Record_t *) * nRecs);

   int iNewRec = 1;
   int primekeyCnt = 0;
   for (iRec = 0; stat == DRMS_SUCCESS && iRec < nRecs; iRec++)
   {
      DSDS_KeyList_t *kl = klarr[iRec];
      DRMS_Segment_t *sSeg = &(segarr[iRec]);
      DRMS_Segment_t *tSeg = NULL;

      if (kl)
      {
	 rset->records[iRec] = drms_alloc_record2(cached, iNewRec, &stat);
	 if (stat == DRMS_SUCCESS)
	 {
	    rset->records[iRec]->sessionid = env->session->sessionid;  
	    rset->records[iRec]->sessionns = strdup(env->session->sessionns);
	    rset->records[iRec]->lifetime = DRMS_TRANSIENT;
	 }

	 /* populate the new records with information from keylistarr and segarr */

	 DRMS_Keyword_t *sKey = NULL;
	 char drmsKeyName[DRMS_MAXNAMELEN];

	 while (stat == DRMS_SUCCESS && kl && ((sKey = kl->elem) != NULL))
	 {
	    if (!drms_keyword_getintname_ext(sKey->info->name, 
					     &fitsclass,
					     NULL, 
					     drmsKeyName, 
					     sizeof(drmsKeyName)))
	    {
	       *drmsKeyName = '\0';
	       stat = DRMS_ERROR_INVALIDDATA;
	       break;
	    }

	    stat = drms_setkey(rset->records[iRec], 
			       drmsKeyName,
			       sKey->info->type,
			       &(sKey->value));
	    kl = kl->next;
	 } /* while */

	 if (fitsclass == kKEYMAPCLASS_LOCAL)
	 {
	    /* set and increment primekey (if no prime key specified by user ) */
	    if (setPrimeKey)
	    {
	       stat = drms_setkey_longlong(rset->records[iRec], 
					   kLocalPrimekey,
					   primekeyCnt);
	       primekeyCnt++;
	    }

	    /* enter segment file names (kKEYMAPCLASS_LOCAL) */
	    if ((tSeg = 
		 hcon_lookup_lower(&(rset->records[iRec]->segments), kLocalSegName)) != NULL)
	    {
	       snprintf(tSeg->filename, DRMS_MAXSEGFILENAME, "%s", sSeg->filename);
	    }
	    else
	    {
	       stat = DRMS_ERROR_INVALIDDATA;
	       break;
	    }
	 }

	 rset->records[iRec]->readonly = 1;
	 iNewRec++;
      }
   } /* for iRec */

   if (status)
   {
      *status = stat;
   }

   return rset;
}

static DRMS_RecordSet_t *OpenLocalRecords(DRMS_Env_t *env, 
					  DSDS_KeyList_t ***klarr,
					  DRMS_Segment_t **segarr,
					  int nRecs,
					  char **pkeys,
					  int *status)
{
   DRMS_RecordSet_t *rset = NULL;
   int stat = DRMS_SUCCESS;

   if (!gAttemptedDSDS && !ghDSDS)
   {
      char lpath[PATH_MAX];
      const char *root = getenv(kJSOCROOT);
      const char *mach = getenv(kJSOC_MACHINE);
      if (root && mach)
      {
	 char *msg = NULL;
	 snprintf(lpath, 
		  sizeof(lpath), 
		  "%s/lib/%s/libdsds.so", 
		  root, 
		  mach);
	 dlerror();
	 ghDSDS = dlopen(lpath, RTLD_NOW);
	 if ((msg = dlerror()) != NULL)
	 {
	    /* library not found */
	    fprintf(stderr, "dlopen(%s) error: %s.\n", lpath, msg);
	    stat = DRMS_ERROR_CANTOPENLIBRARY;
	 }
      }

      gAttemptedDSDS = 1;
   }

   if (stat == DRMS_SUCCESS && ghDSDS)
   {
      pDSDSFn_DSDS_free_keylistarr_t pFn_DSDS_free_keylistarr = 
	(pDSDSFn_DSDS_free_keylistarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLISTARR);

      if (pFn_DSDS_free_keylistarr)
      {
	 char seriesName[DRMS_MAXNAMELEN];
	 int nPkeys = 0;
	
	 /* make record prototype from this morass of information */
	 DRMS_Record_t *proto = NULL;
	 DRMS_Segment_t *seg = NULL;
	 DRMS_Record_t *cached = NULL;


	 DRMS_KeyMapClass_t fitsclass = kKEYMAPCLASS_LOCAL;
	 char drmsKeyName[DRMS_MAXNAMELEN];
	 char *pkeyarr[DRMS_MAXPRIMIDX] = {0};
	 int setPrimeKey = 0;

	 if (pkeys && *pkeys)
	 {
	    char *pkeyname = strtok(*pkeys, ",");
	    nPkeys = 0;
	    while(pkeyname != NULL)
	    {
	       if (!drms_keyword_getintname_ext(pkeyname, 
						&fitsclass,
						NULL, 
						drmsKeyName, 
						sizeof(drmsKeyName)))
	       {
		  *drmsKeyName = '\0';
		  stat = DRMS_ERROR_INVALIDDATA;
		  break;
	       }

	       if (strcasecmp(drmsKeyName, kLocalPrimekey) == 0)
	       {
		  setPrimeKey = 1;
	       }

	       pkeyname = strtok(NULL, ",");
	       pkeyarr[nPkeys] = strdup(drmsKeyName);
	       nPkeys++;
	    }
	 }
	 else
	 {
	    pkeyarr[0] = strdup(kLocalPrimekey);
	    nPkeys = 1;
	    setPrimeKey = 1;
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    CreateRecordProtoFromFitsAgg(env, 
					 *klarr, 
					 *segarr, 
					 nRecs, 
					 pkeyarr,
					 nPkeys,
					 kKEYMAPCLASS_LOCAL,
					 &proto,
					 &seg,
					 &stat);
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* xxxx Get series name - this should be "su_tmp.<GUID>.  The GUID should
	     * be stored in libdrmsserver.a.  It should be a monotonically
	     * increasing integer that lives for the life of libdrmsserver.a. 
	     *
	     * I believe this is the way to achieve this:
	     * send_string
	     */

	    long long guid = -1;

#ifdef DRMS_CLIENT
	       drms_send_commandcode(env->session->sockfd, DRMS_GETTMPGUID);
	       guid = Readlonglong(env->session->sockfd);
#else
	       /* has direct access to drms_server.c. */
	       guid = drms_server_gettmpguid(NULL);
#endif /* DRMS_CLIENT */

	       snprintf(seriesName, sizeof(seriesName), "su_tmp.%lld", guid);

	    /* Adjust seriesinfo */
	    AdjustRecordProtoSeriesInfo(env, proto, seriesName);
	      
	    /* alloc segments */
	    AllocRecordProtoSeg(proto, seg, &stat);
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* primary index */
	    SetRecordProtoPKeys(proto, pkeyarr, nPkeys, &stat);
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* place proto in cache */
	    cached = CacheRecordProto(env, proto, seriesName, &stat);
	 }

	 /* create a new record (read-only) for each record */
	 if (stat == DRMS_SUCCESS)
	 {
	    rset = CreateRecordsFromDSDSKeylist(env,
						nRecs, 
						cached, 
						*klarr,
						*segarr,
						setPrimeKey,
						kKEYMAPCLASS_LOCAL,
						&stat);
	 }

	 /* libdsds created the keylists in the array, it needs to clean them */
	 (*pFn_DSDS_free_keylistarr)(klarr, nRecs);

	 /* segarr was created by DRMS, clean segments here */
	 if (segarr && *segarr)
	 {
	    int i;
	    for (i = 0; i < nRecs; i++)
	    {
	       seg = (*segarr + i);

	       /* need to free malloc'd mem within each segment */
	       if (seg->info)
	       {
		  free(seg->info);
	       }
	    }

	    free(*segarr);
	 }

	 *segarr = NULL;
	 
	 /* free primary key list */
	 if (pkeys && *pkeys)
	 {
	    free(*pkeys);
	    *pkeys = NULL;
	 }

	 for (nPkeys = 0; nPkeys < DRMS_MAXPRIMIDX; nPkeys++)
	 if (pkeyarr[nPkeys])
	 {
	    free(pkeyarr[nPkeys]);
	 }
      }
   }
   else
   {
      fprintf(stdout, "Your JSOC environment does not support DSDS database access.\n");
      stat = DRMS_ERROR_NODSDSSUPPORT;
   }

   if (status)
   {
      *status = stat;
   }

   /* Clean-up in the case of an error happens in calling function */

   return rset;
}

/* Deep-frees recordsets in realSets container */
static void RSFree(const void *val)
{
   DRMS_RecordSet_t **realPtr = (DRMS_RecordSet_t **)val;
   DRMS_RecordSet_t *theRS = NULL;

   if (realPtr)
   {
      theRS = *realPtr;
      if (theRS)
      {
	 drms_close_records(theRS, DRMS_FREE_RECORD);
	 /* free(theRS); - don't free, drms_close_records() already does this! */
      }
   }
}

/*********************** Public functions *********************/

DRMS_RecordSet_t *drms_open_localrecords(DRMS_Env_t *env, const char *dsRecSet, int *status)
{
   DRMS_RecordSet_t *rs = NULL;
   int stat = DRMS_SUCCESS;
   DSDS_KeyList_t **klarr = NULL;
   DRMS_Segment_t *segarr = NULL;
   int nRecsLocal = 0;
   char *pkeys = NULL;

   if (IsLocalSpec(dsRecSet, &klarr, &segarr, &nRecsLocal, &pkeys, &stat))
   {
      rs = OpenLocalRecords(env, &klarr, &segarr, nRecsLocal, &pkeys, &stat);
   }

   if (status)
   {
      *status = stat;
   }

   return rs;
}

/* dsRecSet may resolve into more than one record (fits file) */
DRMS_RecordSet_t *drms_open_dsdsrecords(DRMS_Env_t *env, const char *dsRecSet, int *status)
{
   DRMS_RecordSet_t *rset = NULL;
   int stat = DRMS_SUCCESS;

   if (!gAttemptedDSDS && !ghDSDS)
   {
      char lpath[PATH_MAX];
      const char *root = getenv(kJSOCROOT);
      const char *mach = getenv(kJSOC_MACHINE);
      if (root && mach)
      {
	 char *msg = NULL;
	 snprintf(lpath, 
		  sizeof(lpath), 
		  "%s/lib/%s/libdsds.so", 
		  root, 
		  mach);
	 dlerror();
	 ghDSDS = dlopen(lpath, RTLD_NOW);
	 if ((msg = dlerror()) != NULL)
	 {
	    /* library not found */
	    fprintf(stderr, "dlopen(%s) error: %s.\n", lpath, msg);
	    stat = DRMS_ERROR_CANTOPENLIBRARY;
	 }
      }

      gAttemptedDSDS = 1;
   }

   if (stat == DRMS_SUCCESS && ghDSDS)
   {
      pDSDSFn_DSDS_open_records_t pFn_DSDS_open_records = 
	(pDSDSFn_DSDS_open_records_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_OPEN_RECORDS);
      pDSDSFn_DSDS_free_keylistarr_t pFn_DSDS_free_keylistarr = 
	(pDSDSFn_DSDS_free_keylistarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLISTARR);
      pDSDSFn_DSDS_free_segarr_t pFn_DSDS_free_segarr = 
	(pDSDSFn_DSDS_free_segarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_SEGARR);

      if (pFn_DSDS_open_records && pFn_DSDS_free_keylistarr && pFn_DSDS_free_segarr)
      {
	 char seriesName[DRMS_MAXNAMELEN];
	 DSDS_Handle_t hparams;
	 DSDS_KeyList_t **keylistarr;
	 DRMS_Segment_t *segarr;
	 kDSDS_Stat_t dsdsStat;
	 long long nRecs = 0; /* info returned from libdsds.so */

	 /* Returns one keylist per record and one segment per record. Even though
	  * this could involve a lot of alloc'd memory, and it isn't 100% necessary 
	  * to do this to pass the needed information from libdsds.so here, it
	  * does make keyword and segment analysis easier since the functions for 
	  * doing the analysis are available in the module, but not in libdsds.so.
	  * And, eventually it IS necessary to have n keywords and one segment 
	  * per record - that is the way DRMS works. */
	 nRecs = (*pFn_DSDS_open_records)(dsRecSet, 
					  seriesName, 
					  &hparams, 
					  &keylistarr, 
					  &segarr, 
					  &dsdsStat);
	 if (dsdsStat == kDSDS_Stat_Success)
	 {
	    /* make record prototype from this morass of information */
	    DRMS_Record_t *template = NULL;
	    DRMS_Segment_t *seg = NULL;
	    DRMS_Record_t *cached = NULL;

	    CreateRecordProtoFromFitsAgg(env, 
					 keylistarr, 
					 segarr, 
					 nRecs, 
					 NULL,
					 0,
					 kKEYMAPCLASS_DSDS,
					 &template,
					 &seg,
					 &stat);

	    if (stat == DRMS_SUCCESS)
	    {
	       /* Adjust seriesinfo */
	       AdjustRecordProtoSeriesInfo(env, template, seriesName);
	       DSDS_SetDSDSParams(ghDSDS, template->seriesinfo, hparams);
	      
	       /* alloc segments */
	       AllocRecordProtoSeg(template, seg, &stat);
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* primary index - series_num and rn */
	       char *pkeyarr[2] = {kDSDS_SERIES_NUM, kDSDS_RN};
	       SetRecordProtoPKeys(template, pkeyarr, 2, &stat);
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* place proto in cache */
	       cached = CacheRecordProto(env, template, seriesName, &stat);
	    }

	    /* create a new record (read-only) for each record */
	    rset = CreateRecordsFromDSDSKeylist(env,
						nRecs, 
						cached, 
						keylistarr,
						segarr,
						0,
						kKEYMAPCLASS_DSDS,
						&stat);

	    /* clean up - let libdsds clean up the stuff it created */
	    (*pFn_DSDS_free_keylistarr)(&keylistarr, nRecs);
	    (*pFn_DSDS_free_segarr)(&segarr, nRecs);
	 }
	 else
	 {
	    stat = DRMS_ERROR_LIBDSDS;
	 }
      }
   }
   else
   {
      fprintf(stdout, "Your JSOC environment does not support DSDS database access.\n");
      stat = DRMS_ERROR_NODSDSSUPPORT;
   }

   if (status)
   {
      *status = stat;
   }

   /* Clean-up in the case of an error happens in calling function */

   return rset;
}

/* recordsetname is a comma-separated list of recordsets.  
 * Must surround DSDS queries with '{' and '}'. 
 */
DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, 
				    int *status)
{
  DRMS_RecordSet_t *rs = NULL;
  DRMS_RecordSet_t *ret = NULL;
  int i, stat, filter, mixed;
  char *query=0, *seriesname=0;
  HContainer_t *realSets = NULL;
  int nRecs = 0;
  int j = 0;
  char buf[64];
  DSDS_KeyList_t **klarr = NULL;
  DRMS_Segment_t *segarr = NULL;
  int nRecsLocal = 0;
  char *pkeys = NULL;

  /* recordsetname is a list of comma-separated record sets
   * commas may appear within record sets, so need to use a parsing 
   * mechanism more sophisticated than strtok() */
  char **sets = NULL;
  int nsets = 0;
  stat = ParseRecSetDesc(recordsetname, &sets, &nsets);

  if (stat == DRMS_SUCCESS)
  {
     int iSet;

     CHECKNULL_STAT(env,status);

     for (iSet = 0; stat == DRMS_SUCCESS && iSet < nsets; iSet++)
     {
	char *oneSet = sets[iSet];

	if (oneSet && strlen(oneSet) > 0)
	{
	   if (IsLocalSpec(oneSet, &klarr, &segarr, &nRecsLocal, &pkeys, &stat))
	   {
	      rs = OpenLocalRecords(env, &klarr, &segarr, nRecsLocal, &pkeys, &stat);
	      if (stat)
		goto failure;
	   }
	   else if (DSDS_IsDSDSSpec(oneSet))
	   {
	      rs = drms_open_dsdsrecords(env, oneSet, &stat);
	      if (stat)
		goto failure;
	   }
	   else
	   {
	      TIME(stat = drms_recordset_query(env, oneSet, &query, &seriesname, 
					       &filter, &mixed));
	      if (stat)
		goto failure;

#ifdef DEBUG  
	      printf("seriesname = %s\n",seriesname);
	      printf("query = %s\n",query);
#endif

	      TIME(rs = drms_retrieve_records(env, seriesname, query, filter, mixed, &stat));
#ifdef DEBUG
	      printf("rs=%p, env=%p, seriesname=%s, filter=%d, stat=%d\n  query=%s\n",rs,env,seriesname,filter,stat,query);
#endif
	      if (stat)
		goto failure;

	      free(query);
	      query = NULL;
	      free(seriesname); 
	      seriesname = NULL;
	      
	      for (i=0; i<rs->n; i++)
	      {
		 rs->records[i]->lifetime = DRMS_PERMANENT; 
	      }
	   } /* drms records*/

	   if (stat)
	   {
	      goto failure;
	   }

	   if (nsets == 1)
	   {
	      /* optimize */
	      ret = rs;
	   }
	   else
	   {
	      if (!realSets)
	      {
		 realSets = hcon_create(sizeof(DRMS_RecordSet_t *), 
					64,
					NULL,
					NULL,
					NULL,
					NULL,
					0);
	      }
	   
	      /* save rs - combine at the end */
	      snprintf(buf, sizeof(buf), "%d", iSet);
	      hcon_insert(realSets, buf, &rs);
	      nRecs += rs->n;
	   }

	   rs = NULL;
	}
     } /* iSet */

     /* create the record set structure to return if necessary */
     if (!ret)
     {
	ret = (DRMS_RecordSet_t *)malloc(sizeof(DRMS_RecordSet_t));
	if (ret)
	{
	   ret->records = NULL;
	   ret->n = 0;
	}
     }

     if (ret)
     {
	if (nsets > 1)
	{
	   if (realSets && realSets->num_total > 0)
	   {
	      if (ret)
	      {
		 ret->records = (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nRecs);
		 ret->n = nRecs;

		 /* retain record set order */
		 j = 0;
		 DRMS_RecordSet_t **prs = NULL;
		 DRMS_RecordSet_t *oners = NULL;

		 for (iSet = 0; iSet < nsets; iSet++)
		 {
		    snprintf(buf, sizeof(buf), "%d", iSet);
		    if ((prs = hcon_lookup(realSets, buf)) != NULL)
		    {
		       oners = *prs;
		       if (oners)
		       {
			  /* Move oners to return RecordSet */
			  for (i = 0; i < oners->n; i++)
			  {
			     ret->records[j] = oners->records[i];
			     j++;
			  }
		       }

		       oners = NULL;
		    }
		    else
		    {
		       stat = DRMS_ERROR_INVALIDDATA;
		    }
		 } /* iSet */
	      }
	   }
	}
     }
     else
     {
	stat = DRMS_ERROR_OUTOFMEMORY;
     }

     if (realSets)
     {	
	hcon_destroy(&realSets);
     }

     FreeRecSetDescArr(&sets, nsets);

     if (status)
       *status = stat;

     return ret;
  }

 failure:
  if (query)
  {
     free(query);
  }

  if (seriesname)
  {
     free(seriesname);
  }

  FreeRecSetDescArr(&sets, nsets);

  if (rs)
  {
     RSFree(&rs);
  }

  if (ret)
  {
     RSFree(&ret);
  }
  
  if (realSets)
  {
     hcon_map(realSets, RSFree);
     hcon_destroy(&realSets);
  }

  if (status)
    *status = stat;
  return NULL;
}

/* Create n new records by calling drms_create_record n times.  */
DRMS_RecordSet_t *drms_create_records(DRMS_Env_t *env, int n, char *series,
				      DRMS_RecLifetime_t lifetime, int *status)
{
  DRMS_Record_t *template;

  /* Get template record for the series. */
  if ((template = drms_template_record(env, series, status)) == NULL)
    return NULL;
  return drms_create_records_fromtemplate(env, n, template, lifetime, status);
}

/* Create n new records by calling drms_create_record n times.  */
DRMS_RecordSet_t *drms_create_records_fromtemplate(DRMS_Env_t *env, int n,  
						   DRMS_Record_t *template,     
						   DRMS_RecLifetime_t lifetime, 
						   int *status)
{
  int i=0, stat;
  DRMS_RecordSet_t *rs=NULL;
  long long *recnum=NULL;
  HIterator_t hit;
  DRMS_StorageUnit_t **su;
  DRMS_Segment_t *seg;
  int *slotnum;
  char filename[DRMS_MAXPATHLEN];
  char *series;

  CHECKNULL_STAT(env,status);
  CHECKNULL_STAT(template,status);

  if (n<1)
  {
    if (status)
      *status = DRMS_ERROR_BADRECORDCOUNT;
    return NULL;
  }

  /* Allocate the outer record set structure. */
  XASSERT( rs = malloc(sizeof(DRMS_RecordSet_t)) );
  XASSERT( rs->records = malloc(n*sizeof(DRMS_Record_t *)) );
  rs->n = n;
  series = template->seriesinfo->seriesname;

  /* Get unique sequence numbers from the database server. */
  if ((recnum = drms_alloc_recnum(env->session, series, lifetime, n)) == NULL)
  {
    stat = DRMS_ERROR_BADSEQUENCE;
    goto failure;
  }

  /* Allocate data structures and populate them with default values
     from the series template. */
  for (i=0; i<n; i++)
  {
    if ((
	 rs->records[i] = drms_alloc_record2(template, recnum[i], &stat)
	 ) == NULL)
      goto failure;    

    rs->records[i]->readonly = 0;
    rs->records[i]->sessionid = env->session->sessionid;  
    rs->records[i]->sessionns = strdup(env->session->sessionns);
    rs->records[i]->lifetime = lifetime;
  }

  /* If this series has data segments associated with it then allocate 
     a storage unit slot for each record to hold them. */
  if ( drms_record_numsegments(rs->records[0]) )
  {
    XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
    XASSERT(slotnum = malloc(n*sizeof(int)));
    
    if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, su)))
      goto failure;    
    
    for (i=0; i<n; i++)      
    {      
      rs->records[i]->slotnum = slotnum[i];
      rs->records[i]->su = su[i];
      su[i]->refcount++;
#ifdef DEBUG
      printf("record[%d]->su = %p\n",i,su[i]);
#endif
      rs->records[i]->sunum = rs->records[i]->su->sunum;
      if (slotnum[i]==0)
      {
	/* This is the first record in a new storage unit. Create empty TAS
	   files for each data segment stored in TAS format. */
	hiter_new(&hit, &rs->records[i]->segments);
	while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
	{
	  if (seg->info->protocol == DRMS_TAS)
	  {
	    drms_segment_filename(seg, filename);
	    seg->axis[seg->info->naxis] = rs->records[i]->seriesinfo->unitsize;
	    seg->blocksize[seg->info->naxis] = 1; 
#ifdef DEBUG
	    printf("creating new tasfile '%s'\n",filename);
#endif
	    drms_tasfile_create(filename, DRMS_COMP_RICE, seg->info->type, 
				seg->info->naxis+1, seg->axis, seg->blocksize,
				NULL);
	    seg->axis[seg->info->naxis] = 0;
	    seg->blocksize[seg->info->naxis] = 0; 
	  }
	}
      }
    }
    free(su);
    free(slotnum);
  }
  else
    for (i=0; i<n; i++)      
       rs->records[i]->su = NULL;
  

  if (status)
    *status = 0;
  free(recnum);
  return rs;

 failure:
  if (rs)
    drms_close_records(rs, DRMS_FREE_RECORD);
  if (recnum)
    free(recnum);
  if (status)
    *status = stat;
  return NULL;
}

/* This copies an existing set of records, but ensures that there are no 
 * pointers between these records and anything else in DRMS (like the record
 * cache).  The created record structures must be cleaned up by calling 
 * either drms_free_template_record_struct() or drms_free_record_struct().
 * Use the former in all cases, except if you pass the records to the
 * drms_create_series() call.  If drms_create_series() succeeds, then clean
 * up the passed in record by calling drms_free_record_struct().
 */
DRMS_RecordSet_t *drms_create_recprotos(DRMS_RecordSet_t *recset, int *status)
{
   DRMS_RecordSet_t *detached;
   DRMS_Record_t *recSource = NULL;
   DRMS_Record_t *recTarget = NULL;

   int nRecs = recset->n;

   XASSERT(detached = malloc(sizeof(DRMS_RecordSet_t)) );
   XASSERT(detached->records = malloc(nRecs * sizeof(DRMS_Record_t *)));

   if (detached && detached->records)
   {
      *status = DRMS_SUCCESS;
   }
   else
   {
      *status = DRMS_ERROR_OUTOFMEMORY;
   }

   int idx = 0;
   for (; *status == DRMS_SUCCESS && idx < nRecs; idx++) 
   {
      recSource = recset->records[idx];
      recTarget = drms_create_recproto(recSource, status);

      if (*status == DRMS_SUCCESS)
      {
	 /* Insert record into return set. */
	 detached->records[idx] = recTarget;
      }
   }
   
   return detached;
}

void drms_destroy_recprotos (DRMS_RecordSet_t **protos) {
  if (protos && *protos) {
    int idx, nRecs = (*protos)->n;
    for (idx = 0; idx < nRecs; idx++) 
      drms_destroy_recproto ((DRMS_Record_t **)((*protos)->records)[idx]);
  }
}

DRMS_Record_t *drms_create_recproto(DRMS_Record_t *recSource, int *status)
{
   DRMS_Record_t *detached = NULL;
   DRMS_Record_t *recTarget = NULL;

   XASSERT(recTarget = calloc(1, sizeof(DRMS_Record_t)));
   XASSERT(recTarget->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));

   if (recTarget && recTarget->seriesinfo)
   {
      recTarget->env = recSource->env;
      recTarget->init = 1;
      recTarget->recnum = 0;
      recTarget->sunum = -1;
      recTarget->sessionid = 0;
      recTarget->sessionns = NULL;
      recTarget->su = NULL;
      
      /* Initialize container structure. */
      hcon_init(&recTarget->segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_segment_struct, 
		(void (*)(const void *, const void *)) drms_copy_segment_struct);
      /* Initialize container structures for links. */
      hcon_init(&recTarget->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_link_struct, 
		(void (*)(const void *, const void *)) drms_copy_link_struct);
      /* Initialize container structure. */
      hcon_init(&recTarget->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_keyword_struct, 
		(void (*)(const void *, const void *)) drms_copy_keyword_struct);
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopySeriesInfo(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to create series info.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopySegments(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy segments.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyLinks(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy links.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyKeywords(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy keywords.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyPrimaryIndex(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy primary index.\n");
	 }
      }
      
      if (*status != DRMS_SUCCESS)
      {
	 hcon_free(&recTarget->segments);
	 hcon_free(&recTarget->links);
	 hcon_free(&recTarget->keywords);
	 free(recTarget);
      }
      else
      {
	 detached = recTarget;
      }
   }
   else
   {
      *status = DRMS_ERROR_OUTOFMEMORY;
   }

   return detached;
}

void drms_destroy_recproto(DRMS_Record_t **proto)
{
   /* If proto is in series cache, shallow-free, else deep-free. */
   if (proto)
   {
      DRMS_Record_t *prototype = *proto;
      char *series = prototype->seriesinfo->seriesname;
      DRMS_Env_t *env = prototype->env;
      DRMS_Record_t *rec = hcon_lookup_lower(&(env->series_cache), series);
      int deep = 1;

      if (rec)
      {
	 if (rec->seriesinfo == prototype->seriesinfo)
	 {
	    /* prototype has been cached */
	    deep = 0;
	 }
      }

      if (deep)
      {
	 drms_free_template_record_struct(prototype);
      }
      else
      {
	 drms_free_record_struct(prototype);
      }

      free(prototype);

      *proto = NULL;
   }
}


/* Create n new records by calling drms_create_record n times.  */
DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *rs_in, 
				     DRMS_RecLifetime_t lifetime,  
				     DRMS_CloneAction_t mode, int *status)
{
  int i, stat, first, last, n, n_total;
  DRMS_RecordSet_t *rs_out=NULL;
  DRMS_Record_t *rec_in, *rec_out;
  long long *recnum=NULL;
  HIterator_t hit_in,hit_out;
  DRMS_StorageUnit_t **su;
  DRMS_Segment_t *seg_in, *seg_out;
  int *slotnum;
  char dir_in[DRMS_MAXPATHLEN], dir_out[DRMS_MAXPATHLEN];
  char command[2*DRMS_MAXPATHLEN+20], filename[DRMS_MAXPATHLEN];
  char *series;
  DRMS_Env_t *env;
  DRMS_Array_t *arr;

  CHECKNULL_STAT(rs_in,status);
  n_total = rs_in->n;
  if (n_total<1)
  {
    if (status)
      *status = DRMS_ERROR_BADRECORDCOUNT;
    return NULL;
  }
  CHECKNULL_STAT(rs_in->records,status);
  CHECKNULL_STAT(rs_in->records[0],status);
  env = rs_in->records[0]->env;
  
  /* Allocate the outer record set structure. */
  XASSERT( rs_out = malloc(sizeof(DRMS_RecordSet_t)) );
  XASSERT( rs_out->records = malloc(n_total*sizeof(DRMS_Record_t *)) );
  rs_out->n = n_total;
  
  /* Outer loop over runs of input records from the same series. */
  first = 0;
  while(first<n_total)
  {
    series = rs_in->records[first]->seriesinfo->seriesname;
    last = first+1;
    while(last<n_total && 
	  !strcmp(series, rs_in->records[last]->seriesinfo->seriesname))
      ++last;
    n = last-first;
    
    /* Get unique sequence numbers from the database server. */
    if ((recnum = drms_alloc_recnum(env->session, series, lifetime, n)) == NULL)
    {
      stat = DRMS_ERROR_BADSEQUENCE;
      goto failure;
    }

    /* Allocate data structures and populate them with values from the 
       input records. */
    for (i=0; i<n; i++)
    {
      rs_out->records[first+i] = drms_alloc_record2(rs_in->records[first+i], 
						    recnum[i], &stat);
      if (rs_out->records[first+i] == NULL)
	goto failure;    
      
      rs_out->records[first+i]->readonly = 0;
      rs_out->records[first+i]->sessionid = env->session->sessionid;      
      rs_out->records[first+i]->sessionns = strdup(env->session->sessionns);   
      rs_out->records[first+i]->lifetime = lifetime;
    }

    /* Process nonlink data segments if this series has any associated
       with it. */ 
    if ( drms_record_num_nonlink_segments(rs_out->records[first]) )
    {
      switch(mode)
      {
      case DRMS_SHARE_SEGMENTS:
	for (i=0; i<n; i++)    
          if ( rs_out->records[first+i]->su )
	    ++rs_out->records[first+i]->su->refcount;
	break;
      case DRMS_COPY_SEGMENTS:
	XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
	XASSERT(slotnum = malloc(n*sizeof(int)));
      
	/* Allocate new SU slots for copies of data segments. */
	if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, 
				  su)))
	  goto failure;    
      
	/* Copy record directories and TAS data segments (slices)
	   record-by-record. */
	for (i=0; i<n; i++)      
	{     
	  rec_in = rs_in->records[first+i];
	  rec_out = rs_out->records[first+i];
	    
	  rec_out->slotnum = slotnum[i];
	  rec_out->su = su[i];
	  su[i]->refcount++;
	  rec_out->sunum = su[i]->sunum;

	  /* Copy record directory. */
	  drms_record_directory(rec_in, dir_in, 1);
	  drms_record_directory(rec_out, dir_out, 1);
	  DIR *dp;
	  struct dirent *dirp;
	  if ((dp = opendir(dir_in)) == NULL) {
	    fprintf(stderr, "Can't open %s\n", dir_in);
	    goto failure;
	  }
	  while ((dirp = readdir(dp)) != NULL) {
	    if (!strcmp(dirp->d_name, ".") ||
		!strcmp(dirp->d_name, "..")) 
	      continue;
	    sprintf(command,"cp -rf %s/* %s", dir_in, dir_out);
#ifdef DEBUG
	    printf("executing %s\n",command);
#endif
	    if(system(command))
	      {
		fprintf(stderr, "ERROR: system command to copy record directory"
			" failed.\n");
		goto failure;
	      }
	    break;
	  }
	  closedir(dp);
	  /* Loop over segments and copy any TAS segments one by one. */
	  hiter_new(&hit_in, &rec_in->segments);
	  hiter_new(&hit_out, &rec_out->segments);
	  while ((seg_in = (DRMS_Segment_t *)hiter_getnext(&hit_in)) && 
		 (seg_out = (DRMS_Segment_t *)hiter_getnext(&hit_out)))
	  {
	    if (seg_out->info->protocol == DRMS_TAS)
	    {
	      if (slotnum[i]==0)
	      {
		/* This is the first record in a new storage unit. 
		   Create an empty TAS file. */
		drms_segment_filename(seg_out, filename);
		seg_out->axis[seg_out->info->naxis] = rec_out->seriesinfo->unitsize;
		seg_out->blocksize[seg_out->info->naxis] = 1; 
		drms_tasfile_create(filename, DRMS_COMP_RICE, seg_out->info->type, 
				    seg_out->info->naxis+1, seg_out->axis, 
				    seg_out->blocksize, NULL);
		seg_out->axis[seg_out->info->naxis] = 0;
		seg_out->blocksize[seg_out->info->naxis] = 0; 
	      }
	      if ((arr = drms_segment_read(seg_in,DRMS_TYPE_RAW,&stat))==NULL)
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to read "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_in->info->name, rec_in->seriesinfo->seriesname, 
			rec_in->recnum);
		goto failure;
	      }
	      if (drms_segment_write(seg_out, arr, 0))
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to write "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_out->info->name, rec_out->seriesinfo->seriesname, 
			rec_out->recnum);
		goto failure;
	      }
	      drms_free_array(arr);
	    }
	  }
	}
	free(su);
	free(slotnum);
	break;
      }
    }
    else
      for (i=0; i<n; i++)      
	rs_out->records[first+i]->su = NULL;
    first = last;
  }
  

  if (status)
    *status = 0;
  free(recnum);
  return rs_out;

 failure:
  fprintf(stderr,"FAILURE in clone_records.\n");
  drms_close_records(rs_out, DRMS_FREE_RECORD);
  if (recnum)
    free(recnum);
  if (status)
    *status = stat;
  return NULL;
}

/* Call drms_close_record for each record in a record set. */
int drms_close_records(DRMS_RecordSet_t *rs, int action)
{
  int i, status=0;
  DRMS_Record_t *rec;
  
  CHECKNULL(rs);
  status = 0;
  switch(action)
  {
  case DRMS_FREE_RECORD:
    for (i=0; i<rs->n; i++)
    {
      rec = rs->records[i];
      /* If this record was temporarily created by this session then
	 free its storage unit slot. */
      if (!rec->readonly && rec->su && rec->su->refcount==1)
      {
	if ((status = drms_freeslot(rec->env, rec->seriesinfo->seriesname, 
				    rec->su->sunum, rec->slotnum)))
	{
	  fprintf(stderr,"ERROR in drms_close_record: drms_freeslot failed with "
		  "error code %d\n", status);
	  break;
	}
      }
    }
    break;
  case DRMS_INSERT_RECORD:
    for (i=0; i<rs->n; i++)
    {
      if (rs->records[i]->readonly)
      {
	fprintf(stderr,"ERROR in drms_close_record: trying to commit a "
		"read-only record.\n");
	status = DRMS_ERROR_COMMITREADONLY;
	break;
      }
    }
    if (!status)
    {
#ifdef DEBUG
      printf("Inserting records...\n");
#endif
      if ((status = drms_insert_records(rs)))
      {
	fprintf(stderr,"ERROR in drms_close_record: Could not insert record "
		"in the database.\ndrms_insert_records failed with error "
		"code %d\n",status);
      }
    }
    break;
  default:
    fprintf(stderr,"ERROR in drms_close_record: Action flag (%d) "
	    "is invalid.\n",action);
    status = DRMS_ERROR_INVALIDACTION;
  }
  drms_free_records(rs);
  return status;
}

/* Call drms_free_record for each record in a record set. */
void drms_free_records(DRMS_RecordSet_t *rs)
{
  int i;

  if (!rs)
    return;
  for (i=0; i<rs->n; i++)
    drms_free_record(rs->records[i]);
  if (rs->n>0 && rs->records)
    free(rs->records);
  rs->n = 0;
  free(rs);
}


/*********************** Primary functions *********************/

/* Create a new record from the specified series, assign it a new unique 
   record number and a storage unit slot, possibly allocating a new storage 
   unit if no free slots exist. The keywors, links and data segments will
   be initialized with default values from the series template. */
DRMS_Record_t *drms_create_record(DRMS_Env_t *env, char *series, 
				  DRMS_RecLifetime_t lifetime, int *status)
{
  DRMS_RecordSet_t *rs;
  DRMS_Record_t *rec;
  
  rs = drms_create_records(env, 1, series, lifetime, status);
  if (rs && (rs->n==1))
  {
    rec = rs->records[0]; 
    free(rs->records);
    free(rs);
    return rec;
  }
  else
    return NULL;
}

/*  Create a new record by cloning the record pointed to by "oldrec", 
    and assign it a new unique record number. The keywords, links will be 
    initialized with values from the oldrec. If mode=DRMS_SHARE_SEGMENTS 
    the record will inherit the storage unit slot of the old record "oldrec" 
    and consequently all files that might be in its storage unit directory. 
    If mode=DRMS_COPY_SEGMENTS a new storage unit slot is allocated for the 
    new record and all files in the storage unit od "oldrec" are copied
    to the new storage unit directory. */
DRMS_Record_t *drms_clone_record(DRMS_Record_t *oldrec, 
				 DRMS_RecLifetime_t lifetime, 
				 DRMS_CloneAction_t mode, int *status)
{
  DRMS_RecordSet_t rs_old,*rs;
  DRMS_Record_t *rec;

  rs_old.n = 1;
  rs_old.records = alloca(sizeof(DRMS_Record_t *));
  rs_old.records[0] = oldrec;
  rs = drms_clone_records(&rs_old, lifetime, mode, status);
  if (rs && (rs->n==1))
  {
    rec = rs->records[0]; 
    free(rs->records);
    free(rs);
    return rec;
  }
  else
    return NULL;
}



/* Close the record pointed to by "rec". 

   If action=DRMS_FREE_RECORD the storage unit slot reserved for the 
   record is freed and record meta-data is discarded. 

   If action=DRMS_INSERT_RECORD the records's  meta-data is inserted into 
   the database. The next time drms_commit is called (e.g. when the DRMS 
   session finishes successfully) the meta-data will be commited to the
   database and the data-segment files  in the record's storage unit will 
   be commited to SUMS for archiving (if applicable).

   In all cases the record data structure is freed from memory.
*/
int drms_close_record(DRMS_Record_t *rec, int action)
{
  DRMS_RecordSet_t *rs_old = malloc(sizeof(DRMS_RecordSet_t));

  rs_old->n = 1;
  rs_old->records = malloc(sizeof(DRMS_Record_t *));
  rs_old->records[0] = rec;
  return drms_close_records(rs_old, action);
}


/* Call drms_close_record for all records in the record cache. */
int drms_closeall_records(DRMS_Env_t *env, int action)
{
  HIterator_t hit;
  DRMS_Record_t *rec;
  int status;

  CHECKNULL(env);
  status = 0;
  hiter_new(&hit, &env->record_cache); 
  while( (rec = (DRMS_Record_t *)hiter_getnext(&hit)) )
  {   
    if (action == DRMS_INSERT_RECORD && !rec->readonly)
      status = drms_close_record(rec, action);
    else
      drms_free_record(rec);
     if (status)
      break;
  }
  return status;
}


/* Remove a record from the record cache and free any memory 
   allocated for it. */
void drms_free_record(DRMS_Record_t *rec)
{
   char hashkey[DRMS_MAXHASHKEYLEN];
   XASSERT(rec);
#ifdef DEBUG
   printf("freeing '%s':%lld\n", rec->seriesinfo->seriesname, rec->recnum);
#endif
   drms_make_hashkey(hashkey, rec->seriesinfo->seriesname, rec->recnum);
   /* NOTICE: refcount on rec->su will be decremented when hcon_remove calls
      drms_free_record_struct via its deep_free callback. */
   hcon_remove(&rec->env->record_cache, hashkey); 
}



/* Retrieve a data records with known series and unique record number. 
   If it is already in the dataset cache, simply return a pointer to its 
   data structure, otherwise retrieve it from the database. In the latter 
   case, add it to the record cache and hash tables for fast future
   retrieval. */
DRMS_Record_t *drms_retrieve_record(DRMS_Env_t *env, const char *seriesname, 
				    long long recnum, int *status)
{
  int stat;
  DRMS_Record_t *rec;
  char hashkey[DRMS_MAXHASHKEYLEN];

  CHECKNULL_STAT(env,status);
  
#ifdef DEBUG
  printf("[Trying to retrieve dataset (series=%s, recnum=%lld).\n",seriesname, recnum);
#endif
  drms_make_hashkey(hashkey, seriesname, recnum);

  if ( (rec = hcon_lookup(&env->record_cache, hashkey)) == NULL )
  {
    /* Set up data structure for dataset based on series template. */
    if ((rec = drms_alloc_record(env, seriesname, recnum, &stat)) == NULL)
    {
      if (status)
	*status = stat;
      return NULL;
    }
#ifdef DEBUG
    printf("Allocated the following template for dataset (%s,%lld):\n",
	   seriesname, recnum);
    drms_print_record(rec);
#endif

    /* Fill result into dataset structure. */
    if ((stat = drms_populate_record(rec, recnum)))
    {
      if (status)
	*status = stat;      
      goto bailout; /* Query result was inconsistent with series template. */
    }

    /* Mark as read-only. */
    rec->readonly = 1;

    if (status)
      *status = DRMS_SUCCESS;
    return rec;
  }
  else
  {
#ifdef DEBUG
    printf("Found dataset in cache.\n");    
#endif
    if (status)
      *status = DRMS_SUCCESS;
    return rec;
  }

  bailout:
  drms_free_record(rec);
  return NULL;  
}




/* Retrieve a set of data records from a series satisfying the condition
   given in the string "where", which must be valid SQL WHERE clause wrt. 
   the main record table for the series.
  */
DRMS_RecordSet_t *drms_retrieve_records(DRMS_Env_t *env, 
					const char *seriesname, 
					char *where, int filter, int mixed,
                                        int *status)
{
  int i,throttled;
  int stat = 0;
  long long recnum;
  DRMS_RecordSet_t *rs;
  DB_Binary_Result_t *qres;
  DRMS_Record_t *template;
  char hashkey[DRMS_MAXHASHKEYLEN];
  char *field_list, *query;
  char *series_lower;
  long long recsize, limit;
  char pidx_names[1024]; // comma separated pidx keyword names
  char *p;

#ifdef DEBUG
  printf("ENTER drms_retrieve_records, env=%p, status=%p\n",env,status);
#endif
  CHECKNULL_STAT(env,status);
  
  if ((template = drms_template_record(env,seriesname,status)) == NULL)
    return NULL;
  drms_link_getpidx(template); /* Make sure links have pidx's set. */
  field_list = drms_field_list(template, NULL);
  recsize = drms_record_memsize(template);
  limit  = (long long)((0.4e6*env->query_mem)/recsize);
#ifdef DEBUG
  printf("limit  = (%f / %lld) = %lld\n",0.4e6*env->query_mem, recsize, limit);
#endif
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  if (template->seriesinfo->pidx_num>0) {
    p = pidx_names;
    p += sprintf(p, "%s.%s", series_lower, template->seriesinfo->pidx_keywords[0]->info->name);
    for (i = 1; i < template->seriesinfo->pidx_num; i++) {
      p += sprintf(p, ", %s.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name);
    }
  }

  /* Do query to retrieve record meta-data. */
  XASSERT( (query = malloc(strlen(field_list)+DRMS_MAXQUERYLEN)) );
  p = query;

  if (filter && template->seriesinfo->pidx_num>0) { // query on the lastest version
    if (mixed) {
      // query on both prime and non-prime keys
      p += sprintf(p, "select %s from %s, (select q2.max1 as max from  (select max(recnum) as max1, min(q1.max) as max2 from %s, (select %s, max(recnum) from %s where %s group by %s) as q1 where %s.%s = q1.%s", field_list, series_lower, series_lower, pidx_names, series_lower, where, pidx_names, series_lower, template->seriesinfo->pidx_keywords[0]->info->name, template->seriesinfo->pidx_keywords[0]->info->name);
      for (i = 1; i < template->seriesinfo->pidx_num; i++) {
	p += sprintf(p, " and %s.%s = q1.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name, template->seriesinfo->pidx_keywords[i]->info->name);
      }
      p += sprintf(p, " group by %s) as q2 where max1 = max2) as q3 where %s.recnum = q3.max order by %s", pidx_names, series_lower, pidx_names);
    } else {
      // query only on prime keys
      p += sprintf(p, "select %s from %s where recnum in (select max(recnum) from %s where 1=1 ", field_list, series_lower, series_lower);
      if (where && *where) {
	p += sprintf(p, " and %s", where);
      }
      p += sprintf(p, " group by %s ) order by %s", pidx_names, pidx_names);
    }
  } else { // query on all records including all versions
    p += sprintf(p, "select %s from %s where 1 = 1", field_list, series_lower);
    if (where && *where) {
      p += sprintf(p, " and %s", where);
    }
    if (template->seriesinfo->pidx_num>0) {
      p += sprintf(p, " order by %s", pidx_names);
    } else {
      p += sprintf(p, " order by recnum");
    }
  }
  p += sprintf(p, " limit %lld", limit);
#ifdef DEBUG
  printf("query = '%s'\n",query);
  printf("\nMemory used = %Zu\n\n",xmem_recenthighwater());
#endif

  TIME(qres = drms_query_bin(env->session, query));
  if (qres == NULL)
  {
    stat = DRMS_ERROR_QUERYFAILED;
    fprintf(stderr, "Failed in drms_retrieve_records, query = '%s'\n",query);
    goto bailout1;
  }
#ifdef DEBUG
  db_print_binary_result(qres);
  printf("\nMemory used after query = %Zu\n\n",xmem_recenthighwater());
  printf("number of record returned = %d\n",qres->num_rows);
#endif
  throttled = (qres->num_rows == limit);

  /* Filter query result and initialize record data structures 
     from template. */  
  XASSERT(rs = malloc(sizeof(DRMS_RecordSet_t)));
  if (qres->num_rows<1)
  {
    rs->n = 0;
    rs->records = NULL;    
  }
  else
  {
    /* Allocate data structures and copy default values from template. */
#ifdef DEBUG
    PushTimer();
#endif
    rs->n = qres->num_rows;
    XASSERT(rs->records=malloc(rs->n*sizeof(DRMS_Record_t*)));
    for (i=0; i<rs->n; i++)
    {
#ifdef DEBUG
      printf("Memory used = %Zu\n",xmem_recenthighwater());      
#endif
      recnum = db_binary_field_getlonglong(qres, i, 0);
      drms_make_hashkey(hashkey, seriesname, recnum);
      
      if ( (rs->records[i] = hcon_lookup(&env->record_cache, hashkey)) == NULL )
      {
	/* Allocate a slot in the hash indexed record cache. */
	rs->records[i] = hcon_allocslot(&env->record_cache, hashkey);
	/* Populate the slot with values from the template. */
	drms_copy_record_struct(rs->records[i], template);
	/* Set pidx in links */
	drms_link_getpidx(rs->records[i]);
	/* Set new unique record number. */
	rs->records[i]->recnum = recnum;
      } else {
	// the old record is going to be overridden
	free(rs->records[i]->sessionns);
      }
      rs->records[i]->readonly = 1;
    }


    /* Populate dataset structures with data from query result. */
#ifdef DEBUG
    printf("Time to allocate record structures = %f\n",PopTimer());
    printf("\nMemory used before populate= %Zu\n\n",xmem_recenthighwater());
#endif
    TIME(if ((stat = drms_populate_records(rs, qres)))
    {
      goto bailout; /* Query result was inconsistent with series template. */
    } );
#ifdef DEBUG
    printf("\nMemory used after populate= %Zu\n\n",xmem_recenthighwater());
#endif
  }
  db_free_binary_result(qres);   
  free(query);
  free(field_list);
  free(series_lower);
  if (status)
  {
    if (!throttled)
      *status = DRMS_SUCCESS;
    else {
      fprintf(stderr, "Query truncated\n");
      *status = DRMS_QUERY_TRUNCATED;
    }
  }
  return rs;
 bailout:
  free(series_lower);
  db_free_binary_result(qres);   
  for (i=0;i<rs->n;i++)
    drms_free_records(rs);
  free(rs);
 bailout1:
  free(query);
  free(field_list);
  if (status)
    *status = stat;
  return NULL;  
}



/* Allocate a new record data structure and initialize it with
   meta-data from the given series template. "recnum" must contain
   a unique record number to be assigned to the new record. 
*/
DRMS_Record_t *drms_alloc_record2(DRMS_Record_t *template,
				  long long recnum, int *status)
{
  DRMS_Record_t *rec;
  char hashkey[DRMS_MAXHASHKEYLEN];
  char *series;
  DRMS_Env_t *env;

  CHECKNULL_STAT(template,status);
  env = template->env;
  series = template->seriesinfo->seriesname;

  /* Build hashkey <seriesname>_<recnum> */
  drms_make_hashkey(hashkey, series, recnum);

  /* Allocate a slot in the hash indexed record cache. */
  rec = (DRMS_Record_t *)hcon_allocslot(&env->record_cache, hashkey);
  rec->su = NULL;
  /* Populate the slot with values from the template. */
  drms_copy_record_struct(rec, template);
  
  /* Set pidx in links */
  drms_link_getpidx(rec);
  
  /* Set new unique record number. */
  rec->recnum = recnum;
  if (status)
    *status = DRMS_SUCCESS;

  return rec;
}


/* Just a wrapper for drms_alloc_record2 that looks up the 
   template from the series name. */
DRMS_Record_t *drms_alloc_record(DRMS_Env_t *env, const char *series, 
				 long long recnum, int *status)
{
  DRMS_Record_t *template;

  CHECKNULL_STAT(env,status);
  /* Get template record for the series. */
  if ((template = drms_template_record(env, series,status)) == NULL)
    return NULL;
  return drms_alloc_record2(template, recnum, status);
}



/* 
   Return a template record for the given series. If one already exist
   in the series_cache return that one. Otherwise, build a template record 
   the given series from scratch, i.e. directly from the series meta-data 
   contained in the global database tables DRMS_MASTER_SERIES_TABLE, 
   DRMS_MASTER_KEYWORD_TABLE, DRMS_MASTER_LINK_TABLE, and 
   DRMS_MASTER_SEGMENT_TABLE (see drms_env.h for macro definition). 
   The data structure is allocated and populated with default values 
   from the database.
*/
DRMS_Record_t *drms_template_record(DRMS_Env_t *env, const char *seriesname, 
				    int *status)
{
  int stat;
  DB_Binary_Result_t *qres;
  DRMS_Record_t *template;
  char *p, *q, query[DRMS_MAXQUERYLEN], buf[DRMS_MAXPRIMIDX*DRMS_MAXNAMELEN];
  DRMS_Keyword_t *kw;
  XASSERT(env);
  XASSERT(seriesname);

#ifdef DEBUG
    printf("Getting template for series '%s'\n",seriesname);
#endif
  if ( (template = hcon_lookup_lower(&env->series_cache, seriesname)) == NULL )
  {
    if (status) {
      *status = DRMS_ERROR_UNKNOWNSERIES;
    }
    return NULL;
  }

  if (template->init == 0)
  {
#ifdef DEBUG
    printf("Building new template for series '%s'\n",seriesname);
#endif
    /* The series template for this series was not initialized yet.
       Retrieve its info and insert it in the the hash table
       for future reference. */

    /* Set up top level fields. */
    template->env = env;
    template->init = 1;
    template->recnum = 0;
    template->sunum = -1LL;
    template->sessionid = 0;
    template->sessionns = NULL;
    template->su = NULL;
    XASSERT(template->seriesinfo = malloc(sizeof(DRMS_SeriesInfo_t)));
    /* Populate series info part */
    char *namespace = ns(seriesname);
    sprintf(query, "select seriesname, description, author, owner, "
	    "unitsize, archive, retention, tapegroup, primary_idx "
	    "from %s.%s where seriesname ~~* '%s'", 
	    namespace, DRMS_MASTER_SERIES_TABLE, seriesname);
    free(namespace);
#ifdef DEBUG
    printf("query string '%s'\n",query);
#endif
    
    if ((qres = drms_query_bin(env->session, query)) == NULL)
    {
      printf("Failed to retrieve series information for series %s.\n",
	     seriesname);
      stat = DRMS_ERROR_QUERYFAILED;
      goto bailout;
    }
    if (qres->num_cols != 9 || qres->num_rows != 1)
    {
      printf("Invalid sized query result for global series information for"
	     " series %s.\n", seriesname);      
      db_free_binary_result(qres);
      stat = DRMS_ERROR_BADQUERYRESULT;
      goto bailout;
    }
    db_binary_field_getstr(qres, 0, 0, DRMS_MAXNAMELEN, 
			   template->seriesinfo->seriesname);
    db_binary_field_getstr(qres, 0, 1, DRMS_MAXCOMMENTLEN, 
			   template->seriesinfo->description);
    db_binary_field_getstr(qres, 0, 2, DRMS_MAXCOMMENTLEN, 
			   template->seriesinfo->author);
    db_binary_field_getstr(qres, 0, 3, DRMS_MAXNAMELEN, 
			   template->seriesinfo->owner);
    template->seriesinfo->unitsize = db_binary_field_getint(qres, 0, 4);
    template->seriesinfo->archive = db_binary_field_getint(qres, 0, 5);
    template->seriesinfo->retention = db_binary_field_getint(qres, 0, 6);
    template->seriesinfo->tapegroup = db_binary_field_getint(qres, 0, 7);

    /* Populate series info segments, keywords, and links part */
    if ((stat=drms_template_segments(template)))
      goto bailout;
    if ((stat=drms_template_links(template)))
      goto bailout;
    if ((stat=drms_template_keywords(template)))
      goto bailout;

    /* Set up primary index list. */
    if ( !db_binary_field_is_null(qres, 0, 8) )
    {
      db_binary_field_getstr(qres, 0, 8, DRMS_MAXPRIMIDX*DRMS_MAXNAMELEN, buf);
      p = buf;
#ifdef DEBUG
      printf("Primary index string = '%s'\n",p);
#endif
      template->seriesinfo->pidx_num = 0;
      while(*p)
      {
	XASSERT(template->seriesinfo->pidx_num < DRMS_MAXPRIMIDX);
	while(*p && isspace(*p))
	  ++p;
	q = p;
	while(*p && !isspace(*p) && *p!=',')
	  ++p;	       
	*p++ = 0;
	
#ifdef DEBUG
	printf("adding primary key '%s'\n",q);
#endif
	kw = hcon_lookup_lower(&template->keywords,q);
	XASSERT(kw);
	template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
	kw; 
      }
#ifdef DEBUG
      { int i;
	printf("Primary indices: ");
	for (i=0; i<template->seriesinfo->pidx_num; i++)
	  printf("'%s' ",(template->seriesinfo->pidx_keywords[i])->info->name); 
      }
      printf("\n");    
#endif
    }
    else
      template->seriesinfo->pidx_num = 0;
    db_free_binary_result(qres);   
  }
  if (status)
    *status = DRMS_SUCCESS;
  return template;

 bailout:
  if (status)
    *status = stat;
  return NULL;
}


/* Free the body of a template record data structure. */
void drms_free_template_record_struct(DRMS_Record_t *rec)
{
  /* Free malloc'ed data segments, links and keywords. */
  XASSERT(rec);
  if ( rec->init == 1 ) /* Don't try to free uninitialized templates. */
  {
    (rec->links).deep_free = (void (*)(const void *)) drms_free_template_link_struct;
    hcon_free(&rec->links);

    (rec->segments).deep_free = (void (*)(const void *)) drms_free_template_segment_struct;
    hcon_free(&rec->segments);

    (rec->keywords).deep_free = (void (*)(const void *)) drms_free_template_keyword_struct;
    hcon_free(&rec->keywords);

    free(rec->seriesinfo);
  }
}

/* Free the body of a record data structure. */
void drms_free_record_struct(DRMS_Record_t *rec)
{
  /* Free malloc'ed data segments, links and keywords. */
  XASSERT(rec);
  if ( rec->init == 1 ) /* Don't try to free uninitialized templates. */
  {
    hcon_free(&rec->links);
    hcon_free(&rec->segments);
    hcon_free(&rec->keywords);
    free(rec->sessionns);
    if (rec->env && !rec->env->session->db_direct && rec->su)
    {
      --rec->su->refcount;
      if (rec->su->refcount == 0)
      {
#ifdef DEBUG
	printf("drms_free_record_struct: freeing unit sunum=%lld, sudir='%s'\n",
	       rec->su->sunum,rec->su->sudir);
#endif
	drms_freeunit(rec->env, rec->su);
      }
    }
  }
}

/* Copy the body of a record structure. */
void drms_copy_record_struct(DRMS_Record_t *dst, DRMS_Record_t *src)
{
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Segment_t *seg;

  XASSERT(dst && src);
  /* Copy fields in the main structure and 
     series info. */
  *dst = *src;
  /* Copy fields in segments, links and keywords. */
  hcon_copy(&dst->keywords, &src->keywords);
  hcon_copy(&dst->links, &src->links);
  hcon_copy(&dst->segments, &src->segments);

  /* Set parent pointers. */
  hiter_new(&hit, &dst->links);
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
    link->record = dst;
  hiter_new(&hit, &dst->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
    key->record = dst;
  hiter_new(&hit, &dst->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
    seg->record = dst;

  /* FIXME: Should we copy su struct here? */
}


/* Populate the keyword, link, and data descriptors in the
   record structure with the result from a database query. */ 
int drms_populate_record(DRMS_Record_t *rec, long long recnum)
{
  int stat;
  char *query, *field_list, *seriesname;
  DRMS_Env_t *env;
  DB_Binary_Result_t *qres;
  DRMS_RecordSet_t rs;
  char *series_lower;

  CHECKNULL(rec);
  env = rec->env;
  seriesname = rec->seriesinfo->seriesname;
  field_list = drms_field_list(rec, NULL);
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  /* Do query. */
  XASSERT( (query = malloc(strlen(field_list)+10*DRMS_MAXNAMELEN)) );
  sprintf(query, "select %s from %s where recnum=%lld", 
	  field_list, series_lower, recnum);

  qres = drms_query_bin(env->session, query);
  if (qres == NULL)
  {
    stat = DRMS_ERROR_QUERYFAILED;
    goto bailout1;
  }
  else if (qres->num_rows != 1)
  {
    stat = DRMS_ERROR_BADQUERYRESULT;
    goto bailout2;
  }
  XASSERT(rs.records=malloc(sizeof(DRMS_Record_t*)));
  rs.n = 1;
  rs.records[0] = rec;

  stat = drms_populate_records(&rs,qres);
  db_free_binary_result(qres);      
  free(query);
  free(field_list);
  free(rs.records);
  free(series_lower);
  return stat;
  
 bailout2:
  db_free_binary_result(qres);      
 bailout1:
  free(series_lower);
  free(query);
  free(field_list);
  return 1;
}
/*
 *  Input: Recordset with initialized record templates, query result.
 *  Populate the keyword, link, and data descriptors in the
 *  record structure with the result from a database query
 */ 
int drms_populate_records (DRMS_RecordSet_t *rs, DB_Binary_Result_t *qres) {
  int row, col, i;
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Segment_t *seg;
  DRMS_Record_t *rec;
  //  DRMS_Env_t *env;
  DB_Type_t column_type;
  int segnum;
  char *record_value;

  CHECKNULL(rs);
  CHECKNULL(qres);

  if (rs->n != qres->num_rows)
    return DRMS_ERROR_BADQUERYRESULT;

  for (row=0; row<qres->num_rows; row++) {
    rec = rs->records[row];
    col = 0;
    /* Absolute record number. */
    rec->recnum = db_binary_field_getlonglong(qres, row, col++);
    /* Set up storageunit info. */
    rec->sunum = db_binary_field_getlonglong(qres, row, col++);
    /* Storage unit slot number */
    rec->slotnum = db_binary_field_getint(qres, row, col++);

    /* Session ID of creating session. */
    rec->sessionid = db_binary_field_getint(qres, row, col++);

    /* Session namespace of creating session.*/
    rec->sessionns = strdup(db_binary_field_get(qres, row, col++));

    /* Populate Links. */
    if (hcon_size(&rec->links) > 0) {
      hiter_new(&hit, &rec->links);
      while ((link = (DRMS_Link_t *)hiter_getnext (&hit))) {
	link->record = rec; /* parent link. */
	if (link->info->type == STATIC_LINK)
	  link->recnum = db_binary_field_getlonglong(qres, row, col++);
	else {
					      /*  Oh crap! A dynamic link... */
	  link->isset = db_binary_field_getint(qres, row, col);
	  col++;
		/*  There is a field for each keyword in the primary index
				   of the target series...walk through them  */
	  for (i = 0; i < link->info->pidx_num; i++, col++) {
	    column_type = db_binary_column_type (qres, col);
	    record_value = db_binary_field_get (qres, row, col);
	    drms_copy_db2drms (link->info->pidx_type[i], &link->pidx_value[i],
		column_type, record_value);
	  }
	}
      }
    }

    /* Populate keywords. */
    if (hcon_size(&rec->keywords) > 0) {
      hiter_new(&hit, &rec->keywords);
      while ((key = (DRMS_Keyword_t *)hiter_getnext(&hit)) ) {
	key->record = rec; /* parent link. */
	if (likely (!key->info->islink && !key->info->isconstant)) {
		/*  Copy value only if not null. Otherwise use default value
					already set from the record template  */
	  if (!qres->column[col].is_null[row]) {
	    column_type = db_binary_column_type (qres, col);
	    record_value = db_binary_field_get (qres, row, col);
	    drms_copy_db2drms (key->info->type, &key->value, 
		column_type, record_value);
	  }
	  col++;
	}
      }
    }

    /* Segment fields. */
    if (hcon_size(&rec->segments) > 0)
    {
      hiter_new(&hit, &rec->segments); /* Iterator for segment container. */
      segnum = 0;
      while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
      {
	seg->record = rec; /* parent link. */
	/* Segment file name stored as column "sg_XXX_file */
	db_binary_field_getstr(qres, row, col++, DRMS_MAXSEGFILENAME, seg->filename);
	if (seg->info->scope==DRMS_VARDIM)
	{
	  /* segment dim names are stored as columns "sg_XXX_axisXXX" */	
	  for (i=0; i<seg->info->naxis; i++)
	    seg->axis[i] = db_binary_field_getint(qres, row, col++);
	}
	seg->info->segnum = segnum++;
      }
    }
  }
  return 0;
}





/* 
   Build a list of fields corresponding to all the columns in
   the main series table in which the attributes of the record
   is stored. The fields are stored listed in the following order:

   recnum, sumid, slotnum, <keywords names>, <link names>

*/
char *drms_field_list(DRMS_Record_t *rec, int *num_cols) 
{
  int i,len=0;
  char *buf, *p;
  HIterator_t hit;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  int ncol=0, segnum;
  DRMS_Segment_t *seg;


  XASSERT(rec);
  /**** First get length of string buffer required. ****/
  /* Fixed fields. */
  ncol=0;
  len = strlen("recnum");
  len += strlen("sunum")+2;  
  len += strlen("slotnum")+2;
  len += strlen("sessionid")+2; 
  len += strlen("sessionns")+2;
  ncol += 5;
  /* Link fields. */
  hiter_new(&hit, &rec->links); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      len += strlen(link->info->name)+5;
      ++ncol;
    }
    else /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	len += strlen(link->info->name) + 11;
	++ncol;
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++) {
	len += strlen (link->info->name);
	len += strlen (link->info->pidx_name[i]) + 6;
	++ncol;
      }
    }
  }
  /* Keyword fields. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !key->info->isconstant )
    {
      len += strlen(key->info->name)+5;
      ++ncol;
    }
  }
  /* Segment fields. */
  hiter_new(&hit, &rec->segments); /* Iterator for segment container. */
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    /* Segment file name stored as column "sg_XXX_file */
    len += 13;
    ++ncol; 
    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sg_XXX_axisXXX" */	
      len += 16*seg->info->naxis;      
      ncol += seg->info->naxis;
    }
  }
  /* Malloc string buffer. */
  XASSERT( buf = malloc(len+1) );
  //  printf("ncol = %d\n",ncol);
  /**** Finally construct string by copying names of segments, links,
	and keywords into the string buffer. ****/
  p = buf;
  /* Fixed fields. */
  p += sprintf(p,"%s","recnum"); 
  p += sprintf(p,", %s","sunum"); 
  p += sprintf(p,", %s","slotnum"); 
  p += sprintf(p,", %s","sessionid"); 
  p += sprintf(p,", %s","sessionns");
  /* Link fields. */
  hiter_new(&hit, &rec->links); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
      p += sprintf(p,", ln_%s",link->info->name);
    else  /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	p += sprintf(p,", ln_%s_isset", link->info->name);
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
	p += sprintf(p,", ln_%s_%s",link->info->name, link->info->pidx_name[i]);
    }
  }
  /* Keyword fields. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !key->info->isconstant )
      p += sprintf(p,", %s",key->info->name);
  }
  /* Segment fields. */
  hiter_new(&hit, &rec->segments); /* Iterator for segment container. */
  segnum = 0;
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    p += sprintf(p, ", sg_%03d_file", segnum);
    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
	p += sprintf(p,", sg_%03d_axis%03d",segnum,i);
    }
    segnum++;
  }
  buf[len] = 0; /* Hopefully we got the length right! */

  if (num_cols)
    *num_cols = ncol;
#ifdef DEBUG
  printf("Constructed field list '%s'\n",buf);
#endif
  return buf;
}



/* Insert multiple records via bulk insert interface. */
int drms_insert_records(DRMS_RecordSet_t *recset)
{
  DRMS_Env_t *env;
  int col,row,i,num_args, num_rows;
  char *query, *field_list, *seriesname, *p, *series_lower;
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Record_t *rec;
  DRMS_Segment_t *seg;
  DB_Type_t *intype;
  char **argin;
  int status;
  int *sz;

  CHECKNULL(recset);

  /* Variables to save typing... */
  if ((num_rows = recset->n) < 1)
    return 0;

  /* Use the first record to get series wide information. */
  rec = recset->records[0];
  env = rec->env;
  seriesname = rec->seriesinfo->seriesname;
  series_lower = strdup(seriesname);
  strtolower(series_lower);
  /* Get list of column names. */
  field_list = drms_field_list(rec, &num_args);


  /* Construct SQL string. */
  XASSERT( (query = malloc(strlen(field_list)+10*DRMS_MAXNAMELEN)) );
  p = query;
  p += sprintf(p, "%s (%s)",  series_lower, field_list);
#ifdef DEBUG
  printf("Table and column specifier for bulk insert = '%s'\n",query);
#endif

  XASSERT(argin = malloc(num_args*sizeof(void *)));
  XASSERT(sz = malloc(num_args*sizeof(int)));
  XASSERT(intype = malloc(num_args*sizeof(DB_Type_t)));
    
  /* Get type and size information for record attributes. */
  col = 0;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* recnum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sunum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_INT);  /* slotnum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sessionid */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_STRING);   /* sessionns */
  XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));

  /* Loop through Links. */
  hiter_new(&hit, &rec->links);/* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      intype[col]  = drms2dbtype(DRMS_TYPE_LONGLONG);
      XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
      col++;
    }
    else /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
      {
	intype[col] = drms2dbtype(link->info->pidx_type[i]);
	if (link->info->pidx_type[i] == DRMS_TYPE_STRING )
	{
	  XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));
	}
	else
	{
	  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	  col++;
	}
      }
    }
  }

  /* Loop through Keywords. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !key->info->isconstant )
    {
      intype[col] = drms2dbtype(key->info->type);
      if ( key->info->type == DRMS_TYPE_STRING )
      {
	XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));
      }
      else
      {
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
    }    
  }

  /* Loop through Segment fields. */
  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    /* segment names are stored as columns "sg_XXX_file */	
    intype[col] = drms2dbtype(DRMS_TYPE_STRING);
    XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));

    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      /* segment dim values are stored in columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
      {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
    }
  }

  for (col=0; col<num_args; col++)
    sz[col] = db_sizeof(intype[col]);
    

  /* Now copy the actual values from the records into the argument arrays. */
  for (row=0; row<num_rows; row++)
  {
    rec = recset->records[row];
    col = 0;
    memcpy(argin[col]+row*sz[col], &rec->recnum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sunum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->slotnum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sessionid, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sessionns, sz[col]);
    col++;

    /* Loop through Links. */
    hiter_new(&hit, &rec->links);/* Iterator for link container. */
    while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
    {
      if (link->info->type == STATIC_LINK)
      {
	memcpy(argin[col]+row*sz[col], &link->recnum, sz[col]);
	col++;
      }
      else /* Oh crap! A dynamic link... */
      {
	memcpy(argin[col]+row*sz[col], &link->isset, sz[col]);
	col++;
	/* There is a field for each keyword in the primary index
	   of the target series...walk through them. */
	for (i=0; i<link->info->pidx_num; i++)
	{
	  if (link->info->pidx_type[i] == DRMS_TYPE_STRING )
	  {
	    memcpy(argin[col]+row*sz[col], &link->pidx_value[i].string_val, sz[col]);
	    col++;
	  }
	  else
	  {
	    memcpy(argin[col]+row*sz[col], 
		   drms_addr(link->info->pidx_type[i], &link->pidx_value[i]), sz[col]);
	    col++;
	  }
	}
      }
    }

    /* Loop through Keywords. */
    hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
    while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
    {
      if (!key->info->islink && !key->info->isconstant )
      {
	if (key->info->type == DRMS_TYPE_STRING )
	{
	  memcpy(argin[col]+row*sz[col], &key->value.string_val, sz[col]);
	  col++;
	}
	else
	{
	  memcpy(argin[col]+row*sz[col], 
		 drms_addr(key->info->type, &key->value), sz[col]);
	  col++;
	}
      }    
    }

    /* Loop through Segment fields. */
    hiter_new(&hit, &rec->segments);
    while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
    {
      // This is a hack to get around the problem that
      // seg->filename is an array, not a pointer.  
      ((char **)argin[col])[row] = seg->filename;
      col++;
      if (seg->info->scope==DRMS_VARDIM)
      {
	/* segment dim names are stored as columns "sgXXX_axisXXX" */	
	/* segment dim values are stored in columns "sgXXX_axisXXX" */	
	for (i=0; i<seg->info->naxis; i++)
	{
	  memcpy(argin[col]+row*sz[col], &seg->axis[i], sz[col]);
	  col++;
	}
      }
    }
  }
#ifdef DEBUG
  printf("col = %d, num_args=%d\n",col,num_args);
#endif
  XASSERT(col==num_args);
  status = drms_bulk_insert_array(env->session, query, num_rows, num_args,
				  intype, (void **)argin);

  for (col=0; col<num_args; col++)
    free(argin[col]);
  free(argin);
  free(intype);
  free(query);
  free(sz);
  free(field_list);
  free(series_lower);
  return status;
}




/* Return an estimate of the size of a record's data segment files in bytes. */
long long drms_record_size(DRMS_Record_t *rec)
{
  long long  size;
  DRMS_Segment_t *seg;
  HIterator_t hit; 
  
  CHECKNULL(rec);
  size = 0;
  /* Sum up the sizes of the data segments.  */
  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    //    if (seg->info->scope != DRMS_CONSTANT)
    size +=  drms_segment_size(seg,NULL);
    
  }
  return size;
}

/* "Pretty" print the fields of a record structure and its keywords, links, 
   and data segments. */
void  drms_print_record(DRMS_Record_t *rec)
{
  int i;
  const int fwidth=17;
  HIterator_t hit;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;

  if (rec==NULL)
  {
    fprintf(stderr,"NULL pointer in drms_print_record.\n");
    return;
  }
  printf("================================================================================\n");
  printf("%-*s:\t%s\n",fwidth,"Series name",rec->seriesinfo->seriesname);
  printf("%-*s:\t%lld\n",fwidth,"Record #",rec->recnum);
  printf("%-*s:\t%lld\n",fwidth,"Storage unit #",rec->sunum);
  if (rec->su)
  {
    printf("%-*s:\t%s\n",fwidth,"Storage unit dir",rec->su->sudir);
  }
  printf("%-*s:\t%d\n",fwidth,"Storage unit slot #",rec->slotnum);
  printf("%-*s:\t%lld\n",fwidth,"Session ID",rec->sessionid);
  printf("%-*s:\t%s\n",fwidth,"Session Namespace",rec->sessionns);
  printf("%-*s:\t%d\n",fwidth,"Readonly",rec->readonly);
  printf("%-*s:\t%s\n",fwidth,"Description",rec->seriesinfo->description);
  printf("%-*s:\t%s\n",fwidth,"Author",rec->seriesinfo->author);
  printf("%-*s:\t%s\n",fwidth,"Owner",rec->seriesinfo->owner);
  printf("%-*s:\t%d\n",fwidth,"Unitsize",rec->seriesinfo->unitsize);
  printf("%-*s:\t%d\n",fwidth,"Archive",rec->seriesinfo->archive);
  printf("%-*s:\t%d\n",fwidth,"Retention",rec->seriesinfo->retention);
  printf("%-*s:\t%d\n",fwidth,"Tapegroup",rec->seriesinfo->tapegroup);

  for (i=0; i<rec->seriesinfo->pidx_num; i++)
    printf("%-*s %d:\t%s\n",fwidth,"Primary index",i,
	   (rec->seriesinfo->pidx_keywords[i])->info->name);

  hiter_new(&hit, &rec->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    printf("%-*s '%s':\n",13,"Keyword",key->info->name);
    drms_keyword_print(key);
  }

  hiter_new(&hit, &rec->links); 
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    printf("%-*s '%s':\n",13,"Link",link->info->name);
    drms_link_print(link);
  }

  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    printf("%-*s '%s':\n",fwidth,"Segment",seg->info->name);
      drms_segment_print(seg);
  }
  printf("================================================================================\n");
}

int drms_record_numkeywords(DRMS_Record_t *rec)
{
  if (rec)
      return hcon_size(&rec->keywords); 
  else
    return 0;
}

int drms_record_numlinks(DRMS_Record_t *rec)
{
  if (rec)
    return hcon_size(&rec->links); 
  else
    return 0;
}

int drms_record_numsegments(DRMS_Record_t *rec)
{
  if (rec)
    return hcon_size(&rec->segments); 
  else
      return 0;
}

int drms_record_num_nonlink_segments(DRMS_Record_t *rec)
{
  HIterator_t hit;
  DRMS_Segment_t *seg;
  int count = 0;
  if (rec) {
    hiter_new(&hit, &rec->segments); 
    while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) ) {
      if (!seg->info->islink) {
	count++;
      }
    }
  }
  return count;
}

/* Return the path to the Storage unit slot directory associateed with
   a record. If no storage unit slot has been assigned to the record yet,
   an empty string is returned. */
void drms_record_directory (DRMS_Record_t *rec, char *dirname, int retrieve) {
  int stat;
  if (drms_record_numsegments (rec) <= 0) {
#ifdef DEBUG
    fprintf (stderr, "ERROR: Calling drms_record_directory is valid only for "
	"records containing data segments.");
#endif
    dirname[0] = 0;
    return;
  }

  if (rec->sunum != -1LL && rec->su == NULL) {
#ifdef DEBUG
    printf ("Getting SU for record %s:#%lld, sunum=%lld\n",
	   rec->seriesinfo->seriesname,rec->recnum, rec->sunum);
#endif
    if ((rec->su = drms_getunit (rec->env, rec->seriesinfo->seriesname,
	rec->sunum, retrieve, &stat)) == NULL) {
      if (stat) 
	fprintf (stderr, "ERROR in drms_record_directory: Cannot retrieve "
	    "storage unit. stat = %d\n", stat);
      dirname[0] = '\0';
      return;
    }
    rec->su->refcount++;
#ifdef DEBUG    
    printf("Retrieved unit sunum=%lld, sudir=%s\n",
	   rec->su->sunum, rec->su->sudir);
#endif
  }
  CHECKSNPRINTF(snprintf(dirname, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT, 
			 rec->su->sudir, rec->slotnum), DRMS_MAXPATHLEN);
}



/* Ask DRMS to open a file in the storage unit slot directory associated with
   a data record. If mode="w" or mode="a" and the record has not been assigned 
   a storage unit slot, one is allocated. It is an error to call with 
   mode="r" if the record has not been assigned a storage unit slot. 
   In this case a NULL pointer is returned. */

FILE *drms_record_fopen(DRMS_Record_t *rec, char *filename, const char *mode)
{
  int stat;
  int newslot = 0;
  char path[DRMS_MAXPATHLEN];
  FILE *fp;
    
  if (drms_record_numsegments(rec) < 0)
  {
    fprintf(stderr,"ERROR: Calling drms_record_fopen is only for records "
	    "containing data segments.");
    return NULL;
  }
  switch(mode[0])
  {
  case 'w':
  case 'a':
    if (rec->su==NULL)
    {
      if ((stat = drms_newslots(rec->env, 1, rec->seriesinfo->seriesname, 
				&rec->recnum, rec->lifetime, &rec->slotnum, 
				&rec->su)))
      {
	fprintf(stderr,"ERROR in drms_record_fopen: drms_newslot"
		" failed with error code %d.\n",stat);
	return NULL;
      }
      rec->sunum = rec->su->sunum;
      newslot = 1;
    }
    break;
  case 'r':
    /* First check if there is a SU associated with this record. 
       If so get it from SUMS if that has not already been done. */  
    if (rec->sunum != -1LL && rec->su==NULL)
    {
      if ((rec->su = drms_getunit(rec->env, rec->seriesinfo->seriesname, 
				  rec->sunum, 1, &stat)) == NULL)
      {
	if (stat)
	  fprintf(stderr,"ERROR in drms_record_fopen: Cannot open file for "
		  "reading in non-existent storage unit slot. stat = %d\n", stat);
	return NULL;
      }
      rec->su->refcount++;
    }
  }

  CHECKSNPRINTF(snprintf(path,DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT "/%s",
			 rec->su->sudir, rec->slotnum, filename), DRMS_MAXPATHLEN);

  if (!(fp = fopen(path, mode)))
  {
    perror("ERROR in drms_record_fopen: fopen failed with");
    if (newslot) /* Ooops! Never mind about that slot! */
      drms_freeslot(rec->env, rec->seriesinfo->seriesname, 
		    rec->su->sunum, rec->slotnum);
  }
  return fp;
}



/* Estimate size used by record meta-data. */
long long drms_record_memsize( DRMS_Record_t *rec)
{
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  //  DRMS_Segment_t *seg;
  long long size;

  size = sizeof(DRMS_Record_t) + DRMS_MAXHASHKEYLEN;
  //  printf("struct size = %lld\n",size);
  /* Get type and size information for record attributes. */
  /* Loop through Links. */
  hiter_new(&hit, &rec->links);/* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    size += sizeof(DRMS_Link_t) +  DRMS_MAXNAMELEN;
    if (link->info->type != STATIC_LINK)
      size += link->info->pidx_num*10;/* NOTE: Just a wild guess. */
  }
  //  printf("link size = %lld\n",size);

  /* Loop through Keywords. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    size += sizeof(DRMS_Keyword_t) +  DRMS_MAXNAMELEN + 1;
    if (!key->info->islink && !key->info->isconstant )
    {
      if ( key->info->type == DRMS_TYPE_STRING )
      {
	if (key->value.string_val)
	  size += strlen(key->value.string_val); 
	else 
	  size += 40;
      }    
    }
  }
  //printf("keyword size = %lld\n",size);

  /* Loop through Segment fields. */
  size += hcon_size(&rec->segments) * (sizeof(DRMS_Segment_t) + DRMS_MAXNAMELEN);
  //printf("segment size = %lld\n",size);

  return size;
}

int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source)
{
   memcpy(target->seriesinfo, source->seriesinfo, sizeof(DRMS_SeriesInfo_t));
   memset(target->seriesinfo->pidx_keywords, 0, sizeof(DRMS_Keyword_t *) * DRMS_MAXPRIMIDX);

   return DRMS_SUCCESS;
}

int CopySegments(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_segment_prototypes(target, source, &status);

   return status;
}

int CopyLinks(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_link_prototypes(target, source, &status);

   return status;
}

int CopyKeywords(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_keyword_prototypes(target, source, &status);

   return status;
}

/* target->keywords must exist at this point! */
/* target->seriesinfo must exist at this point! */
int CopyPrimaryIndex(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;

   if (target != NULL && target->seriesinfo != NULL)
   {
      target->seriesinfo->pidx_num = source->seriesinfo->pidx_num;

      int idx = 0;
      for (; status == DRMS_SUCCESS && idx < source->seriesinfo->pidx_num; idx++)
      {
	 char *name = source->seriesinfo->pidx_keywords[idx]->info->name;
	 DRMS_Keyword_t *key = hcon_lookup_lower(&target->keywords, name);
	 if (key != NULL)
	 {
	    target->seriesinfo->pidx_keywords[idx] = key;
	 }
	 else
	 {
	    status = DRMS_ERROR_INVALIDKEYWORD;
	 }
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDRECORD;
   }
      
   return status;
}

/* Caller owns sets. */
static int ParseRecSetDesc(const char *recsetsStr, char ***sets, int *nsets)
{
   int status = DRMS_SUCCESS;
   RSParseState_t state = kRSParseState_Begin;
   char *rsstr = strdup(recsetsStr);
   char *pc = rsstr;
   char **intSets = NULL;
   int count = 0;
   char buf[kMAXRSETSPEC];
   char *pcBuf = buf;
   char **multiRS = NULL;
   int countMultiRS = 0;
   char *endInput = rsstr + strlen(rsstr); /* points to null terminator */

   *nsets = 0;

   if (rsstr)
   {
      while (pc <= endInput && state != kRSParseState_Done)
      {
	 switch (state)
	 {
	    case kRSParseState_Error:
	      if (status == DRMS_SUCCESS)
	      {
		 state = kRSParseState_Done;
		 status = DRMS_ERROR_INVALIDDATA;
	      }
	      break;
	    case kRSParseState_Begin:
	      if (pc < endInput)
	      {
		 if (*pc == ' ' || *pc == '\t')
		 {
		    /* skip whitespace */
		    pc++;
		 }		
		 else if (*pc == '[' || *pc == ']' || *pc == ',')
		 {
		    state = kRSParseState_Error;
		 }
		 else
		 {
		    if (*pc == '{')
		    {
		       state = kRSParseState_BeginDSDS;
		       pc++;
		    }
		    else if (*pc == '@')
		    {
		       /* text file that contains one or more recset queries */
		       /* recursively parse those queries! */
		       state = kRSParseState_BeginAtFile;
		       pc++;
		    }
		    else
		    {
		       state = kRSParseState_NameChar;
		    }

		    intSets = (char **)malloc(sizeof(char *) * kMAXRSETS);
		    if (!intSets)
		    {
		       state = kRSParseState_Error;
		       status = DRMS_ERROR_OUTOFMEMORY;
		    }
		    else
		    {
		       memset(intSets, 0, sizeof(char *) * kMAXRSETS);
		    }
		 }
	      }
	      break;
	    case kRSParseState_NameChar:
	      if (pc < endInput)
	      {
		 if (*pc == '{')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == '[')
		 {
		    if (pc + 1 < endInput && *(pc + 1) == '?')
		    {
		       state = kRSParseState_QuerySQL;
		       *pcBuf++ = *pc++;
		       *pcBuf++ = *pc++;
		    }
		    else
		    {
		       state = kRSParseState_FiltChar;
		       *pcBuf++ = *pc++;
		    }
		 }
		 else if (*pc == ']')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == ',')
		 {
		    if (pc + 1 < endInput)
		    {
		       state = kRSParseState_EndRS;
		       pc++;
		    }
		    else
		    {
		       state = kRSParseState_Error;
		    }
		 }
		 else if (*pc == ' ' || *pc == '\t')
		 {
		    /* whitespace between the series name and a filter is allowed */
		    state = kRSParseState_EndName;
		    *pcBuf++ = *pc++;
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_LastRS;
	      }
	      break;
	    case kRSParseState_EndName:
	      if (pc < endInput)
	      {
		 if (*pc == ' ' || *pc == '\t')
		 {
		    *pcBuf++ = *pc++;
		 }
		 else if (*pc == '[')
		 {
		    if (pc + 1 < endInput && *(pc + 1) == '?')
		    {
		       state = kRSParseState_QuerySQL;
		       *pcBuf++ = *pc++;
		       *pcBuf++ = *pc++;
		    }
		    else
		    {
		       state = kRSParseState_FiltChar;
		       *pcBuf++ = *pc++;
		    }
		 }
		 else
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 state = kRSParseState_End;
	      }
	      break;
	    case kRSParseState_FiltChar:
	      if (pc < endInput)
	      {
		 if (*pc == '[')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == ']')
		 {
		    state = kRSParseState_EndFilt;
		    *pcBuf++ = *pc++;
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 /* didn't finish filter */
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_EndFilt:
	      if (pc < endInput)
	      {
		 if (*pc == '[')
		 {
		    if (pc + 1 < endInput && *(pc + 1) == '?')
		    {
		       state = kRSParseState_QuerySQL;
		       *pcBuf++ = *pc++;
		       *pcBuf++ = *pc++;
		    }
		    else
		    {
		       state = kRSParseState_FiltChar;
		       *pcBuf++ = *pc++;
		    }
		 }
		 else if (*pc == ',')
		 {
		    if (pc + 1 < endInput)
		    {
		       state = kRSParseState_EndRS;
		       pc++;
		    }
		 }
		 else if (*pc == ' ' || *pc == '\t')
		 {
		    state = kRSParseState_LastRS;
		 }
		 else
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 state = kRSParseState_LastRS;
	      }
	      break;
	    case kRSParseState_QuerySQL:
	      if (pc < endInput)
	      {
		 if (*pc == '?')
		 {
		    state = kRSParseState_EndQuerySQL;
		    *pcBuf++ = *pc++;
		 }
		 else
		 {
		    /* whitespace okay in sql query 
		     *   see drms_names.c */
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 /* didn't finish query */
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_EndQuerySQL:
	      /* closing '?' seen, pc pointing at next char */
	      if (pc < endInput)
	      {
		 if (*pc == ']')
		 {
		    state = kRSParseState_EndFilt;
		    *pcBuf++ = *pc++;
		 }
		 else
		 {
		    /* no space allowed between '?' and ']' 
		     *   see drms_names.c */
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 /* didn't end with ']' */
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_EndRS:
	      /* pc points to next RS (or whatever is after ',') */
	      /* buf does not contain the comma or whitespace */
	      *pcBuf = '\0';
	      pcBuf = buf;

	      if (!multiRS)
	      {
		 intSets[count] = strdup(buf);
		 count++;
	      }
	      else
	      {
		 int iSet;
		 for (iSet = 0; iSet < countMultiRS; iSet++)
		 {
		    intSets[count] = multiRS[iSet];
		    count++;
		 }

		 free(multiRS); /* don't deep-free; intSets now owns strings */
		 multiRS = NULL;
		 countMultiRS = 0;
	      }

	      if (pc < endInput)
	      {
		 if (*pc == ' ' || 
		     *pc == '\t'|| 
		     *pc == '[' || 
		     *pc == ']' || 
		     *pc == ',')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == '{')
		 {
		    state = kRSParseState_BeginDSDS;
		    pc++;
		 }
		 else if (*pc == '@')
		 {
		    state = kRSParseState_BeginAtFile;
		    pc++;
		 }
		 else
		 {
		    state = kRSParseState_NameChar;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_LastRS:
	      /* pc points to endInput or whitespace */ 
	      *pcBuf = '\0';
	      pcBuf = buf;

	      if (!multiRS)
	      {		
		 intSets[count] = strdup(buf);
		 count++;
	      }
	      else
	      {
		 int iSet;
		 for (iSet = 0; iSet < countMultiRS; iSet++)
		 {
		    intSets[count] = multiRS[iSet];
		    count++;
		 }

		 free(multiRS); /* don't deep-free; intSets now owns strings */
		 multiRS = NULL;
		 countMultiRS = 0;
	      }
	      
	      if (pc < endInput)
	      {
		 if (*pc == ' ' || *pc == '\t')
		 {
		    state = kRSParseState_End;
		 }
		 else
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 state = kRSParseState_End;
	      }
	      break;
	    case kRSParseState_BeginAtFile:
	      if (pc < endInput)
	      {
		 if (*pc == ',')
		 {
		    state = kRSParseState_Error;
		 }
		 else
		 {
		    state = kRSParseState_AtFile; 
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_AtFile:
	      if (pc < endInput)
	      {
		 if (*pc == ',')
		 {
		    state = kRSParseState_EndAtFile;
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_EndAtFile;
	      }
	      break;
	    case kRSParseState_EndAtFile:
	      {
		 char lineBuf[LINE_MAX];
		 char **setsAtFile = NULL;
		 int nsetsAtFile = 0;
		 int iSet = 0;
		 struct stat stBuf;
		 FILE *atfile = NULL;

		 /* finished reading an AtFile filename - read file one line at a time,
		  * parsing each line recursively. */
		 if (multiRS)
		 {
		    state = kRSParseState_Error;
		    break;
		 }
		 else
		 {
		    multiRS = (char **)malloc(sizeof(char *) * kMAXRSETS);

		    /* buf has filename */
		    *pcBuf = '\0';
		    if (buf && !stat(buf, &stBuf))
		    {
		       if (S_ISREG(stBuf.st_mode))
		       {
			  /* read a line */
			  if ((atfile = fopen(buf, "r")) == NULL)
			  {
			     fprintf(stderr, "Cannot open @file %s for reading, skipping.\n", buf);
			  }
			  else
			  {
			     int len = 0;
			     while (!(fgets(lineBuf, LINE_MAX, atfile) == NULL))
			     {
				/* strip \n from end of lineBuf */
				len = strlen(lineBuf);

				/* skip empty lines*/
				if (len > 1)
				{
				   if (lineBuf[len - 1] == '\n')
				   {
				      lineBuf[len - 1] = '\0';
				   }
			
				   status = ParseRecSetDesc(lineBuf, &setsAtFile, &nsetsAtFile);
				   if (status == DRMS_SUCCESS)
				   {
				      /* add all nsetsAtFile recordsets to multiRS */
				      for (iSet = 0; iSet < nsetsAtFile; iSet++)
				      {
					 multiRS[countMultiRS] = strdup(setsAtFile[iSet]);
					 countMultiRS++;
				      }
				   }
				   else
				   {
				      state = kRSParseState_Error;
				      break;
				   }

				   FreeRecSetDescArr(&setsAtFile, nsetsAtFile);
				}
			     }
			  }
		       }
		       else
		       {
			  fprintf(stderr, "@file %s is not a regular file, skipping.\n", buf);
		       }
		    }
		    else
		    {
		       fprintf(stderr, "Cannot find @file %s, skipping.\n", buf);
		    }
		 }
	      }

	      if (pc < endInput)
	      {
		 if (*pc == ',')
		 {
		    /* AtFile filename was terminated by comma */
		    if (pc + 1 < endInput)
		    {
		       state = kRSParseState_EndRS;
		       pc++;
		    }
		    else
		    {
		       state = kRSParseState_Error;
		    }
		 }		 
	      }
	      else
	      {
		 state = kRSParseState_LastRS;
	      }
	      break;
	    case kRSParseState_BeginDSDS:
	      if (pc < endInput)
	      {
		 if (*pc == ' ' || *pc == '\t')
		 {
		    /* skip whitespace */
		    pc++;
		 }
		 else if (*pc == '[' || *pc == ']' || *pc == ',')
		 {
		    state = kRSParseState_Error;
		 }
		 else
		 {
		    state = kRSParseState_DSDS;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_DSDS:
	      if (pc < endInput)
	      {
		 if (*pc == '{')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == ' ' || *pc == '\t')
		 {
		    /* trailing whitespace */
		    state = kRSParseState_TrailDSDS;
		 }
		 else if (*pc == '}')
		 {
		    state = kRSParseState_EndDSDS;
		    pc++;
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_TrailDSDS:
	      if (pc < endInput)
	      {
		 if (*pc == ' ' || *pc == '\t')
		 {
		    pc++;
		 }
		 if (*pc == '}')
		 {
		    state = kRSParseState_EndDSDS;
		    pc++;
		 }
		 else
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_EndDSDS:
	      if (pc < endInput)
	      {
		 if (*pc == ',')
		 {
		    if (pc + 1 < endInput)
		    {
		       state = kRSParseState_EndRS;
		       pc++;
		    }
		    else
		    {
		       state = kRSParseState_Error;
		    }
		 }
		 else
		 {
		    state = kRSParseState_LastRS;
		 }
	      }
	      else
	      {
		 state = kRSParseState_LastRS;
	      }
	      break;
	    case kRSParseState_End:
	      if (*pc == ' ' || *pc == '\t')
	      {
		 /* pc points to whitespace*/
		 pc++;
	      }
	      else if (pc != endInput)
	      {
		 state = kRSParseState_Error;
	      }
	      else
	      {
		 state = kRSParseState_Done;
	      }
	      break;
	    default:
	      state = kRSParseState_Error;
	 }
      }
      
      free(rsstr);
   } /* rsstr */

   if (status == DRMS_SUCCESS && state == kRSParseState_Done && count > 0)
   {
      *sets = (char **)malloc(sizeof(char *) * count);

      if (*sets)
      {
	 *nsets = count;
	 memcpy(*sets, intSets, sizeof(char *) * count);
      }
      else
      {
	 status = DRMS_ERROR_OUTOFMEMORY;
      }
   }

   if (intSets)
   {
      free(intSets);
   }

   return status;
}

int FreeRecSetDescArr(char ***sets, int nsets)
{
   int error = 0;

   if (sets)
   {
      int iSet;
      char **setArr = *sets;

      if (setArr)
      {
	 for (iSet = 0; iSet < nsets; iSet++)
	 {
	    char *oneSet = setArr[iSet];

	    if (oneSet)
	    {
	       free(oneSet);
	    }
	 }

	 free(setArr);
      }

      *sets = NULL;
   }

   return error;
}

int drms_recproto_setseriesinfo(DRMS_Record_t *rec, 
				int *unitSize, 
				int *bArchive, 
				int *nDaysRetention,
				int *tapeGroup,
				const char *description)
{
   int status = DRMS_NO_ERROR;

   if (rec && rec->seriesinfo)
   {
      if (unitSize)
      {
	 rec->seriesinfo->unitsize = *unitSize;
      }

      if (bArchive)
      {
	 rec->seriesinfo->archive = *bArchive;
      }

      if (nDaysRetention)
      {
	 rec->seriesinfo->retention = *nDaysRetention;
      }
      
      if (tapeGroup)
      {
	 rec->seriesinfo->tapegroup = *tapeGroup;
      }

      if (description && strlen(description) < DRMS_MAXCOMMENTLEN)
      {
	 strcpy(rec->seriesinfo->description, description);
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDDATA;
   }

   return status;
}

static int IsFileOrDir(const char *q)
{
   int ret = 0;
   char *lbrack = NULL;
   char *query = strdup(q);
   struct stat stBuf;

   if (query)
   {
      if ((lbrack = strchr(query, '[')) != NULL)
      {
	 *lbrack = '\0';      
      }

      if (!stat(query, &stBuf))
      {
	 if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode) || S_ISDIR(stBuf.st_mode))
	 {
	    ret = 1;
	 }
      }

      free(query);
   }

   return ret;
}

DRMS_RecordQueryType_t drms_record_getquerytype(const char *query)
{
   DRMS_RecordQueryType_t ret;
   const char *pQ = (*query == '{') ? query + 1 : query;

   if (DSDS_IsDSDSSpec(pQ))
   {
      ret = kRecordQueryType_DSDS;
   }
   else if (IsFileOrDir(query))
   {
      ret = kRecordQueryType_LOCAL;
   }
   else
   {
      ret = kRecordQueryType_DRMS;
   }

   return ret;
}

char *drms_record_jsoc_version(DRMS_Env_t *env, DRMS_Record_t *rec) {
  char *jsocversion = 0;
  char query[DRMS_MAXQUERYLEN];
  snprintf(query, DRMS_MAXQUERYLEN, "select jsoc_version from %s.drms_session where sessionid = %lld", rec->sessionns, rec->sessionid);
  DB_Text_Result_t *qres;
  if ((qres = drms_query_txt(env->session, query)) && qres->num_rows>0) {
    if (qres->field[0][0][0]) {
      XASSERT(jsocversion = malloc(strlen(qres->field[0][0])+1));
      strcpy(jsocversion, qres->field[0][0]);
    }
  }
  db_free_text_result(qres);
  return jsocversion;
}
