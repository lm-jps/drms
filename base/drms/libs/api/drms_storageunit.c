//#define DEBUG

#include <dirent.h>
#include "drms.h"
#include "drms_priv.h"
#include "db.h"
#include "xmem.h"
#include "util.h"
#include "list.h"
#include "printk.h"

#ifdef DRMS_CLIENT
#define DEFS_CLIENT
#endif
#include "drmssite_info.h"

#ifdef DEFS_CLIENT
#undef DEFS_CLIENT
#endif

#define SUMIN(a,b)  ((a) > (b) ? (b) : (a))

//igor VSO HTTP request to JMD start
#if defined(JMD_IS_INSTALLED) && JMD_IS_INSTALLED
    #include <curl/curl.h>
    #include <curl/types.h>
    #include <curl/easy.h>

    struct POSTState
    {
        char session_id[256];
        int  no_submitted;
    };

    int session_status (char *session);
    size_t parse_session_state(void *buffer, size_t size, size_t nmemb, void *userp);

    void populate_with_sunums(DRMS_Env_t *env, HContainer_t *postmap, char * seriesname, int n, long long sulist[]);
    struct PassSunumList
    {
        char series[500];
        char **sunumlist;
        long long *sunumarr;
        int  n;
        int  ncount;
        char id[256];
        int   submitted;
        int  sizeadded;
    };

    void add_sunum_to_POST(DRMS_Env_t *env, HContainer_t *postmap, char *seriesname, long long sunum);
    size_t create_post_msg(HContainer_t *postmap, char **ptr);
    size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp);
    int send_POST_request(char * postrequeststr, curl_off_t postsize, struct POSTState *ps);
    void free_post_request(HContainer_t *postmap);

    size_t read_callback(void *ptr, size_t size, size_t nmemb, void *userp);
#endif // JMD Support
//igor VSO HTTP request to JMD end

#define kEXTREMOTESUMS "remotesums_master.pl"
#define kSUNUMLISTSIZE 248576

struct SUList_struct
{
  char *str;
  size_t size;
};

typedef struct SUList_struct SUList_t;

static char *EndsWith(const char *str, const char *suffix, int caseInsensitive)
{
   if (!str || !suffix)
   {
      return NULL;
   }

   size_t lenstr = strlen(str);
   size_t lensuffix = strlen(suffix);
   int found = 0;

   if (lensuffix > lenstr)
   {
      return NULL;
   }

   if (caseInsensitive)
   {
      found = (strncasecmp(str + lenstr - lensuffix, suffix, lensuffix) == 0);
   }
   else
   {
      found = (strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0);
   }

   if (found)
   {
      return (char *)str + lenstr - lensuffix;
   }
   else
   {
      return NULL;
   }
}

static int EmptyDir(const char *dir, int depth)
{
  struct stat stBuf;
  struct dirent **fileList = NULL;
  int nfiles = 0;
  int notempty = 0;

  notempty = (!stat(dir, &stBuf) && S_ISDIR(stBuf.st_mode) && (nfiles = scandir(dir, &fileList, NULL, NULL)) > 0 && fileList);

  if (notempty)
  {
     /* Don't count "." and ".." */
     int ifile;
     struct dirent *entry = NULL;
     char fbuf[PATH_MAX];
     char subdir[PATH_MAX];

     notempty = 0;
     ifile = 0;

     while (ifile < nfiles)
     {
        entry = fileList[ifile];

        if (entry != NULL)
        {
           if (!notempty)
           {   
              char *oneFile = entry->d_name;

              if (strcmp(oneFile, ".") != 0 && strcmp(oneFile, "..") != 0)
              {
                 /* There is no good place to put this, but this is the most efficient place (since we already have 
                  * to iterate through all files in the SU and this function is always called before committing an SU).
                  * Delete empty TAS files (TAS files that were created, but no slices were ever written to them). TAS
                  * files live in the sudir, not in any slot dir. This is depth == 0. */

                 /* Look for a TAS-file-virgin-file pair. If found, delete them both and go onto the next file.
                  * Because we are iterating through a list that we are also deleting from, we have to check for the
                  * existence of the files in the following code before doing anything with the files.
                  * If there is no TAS-file-virgin-file pair, fall through to the code below. */
                 
                 snprintf(fbuf, sizeof(fbuf), "%s/%s", dir, oneFile);
                 if (stat(fbuf, &stBuf))
                 {
                    /* oneFile no longer exists - go onto the next file. */
                    free(entry);
                    ifile++;
                    continue;
                 }

                 if (depth == 0 && EndsWith(oneFile, ".tas.virgin", 1) || EndsWith(oneFile, ".tas", 1))
                 {
                    char *dup = strdup(oneFile);
                    char *loc = NULL;

                    if (dup)
                    {
                       if (loc = EndsWith(dup, ".virgin", 1))
                       {
                          /* We're looking at a .virgin file. Find the corresponding TAS file. */
                          *loc = '\0';
                          snprintf(fbuf, sizeof(fbuf), "%s/%s", dir, dup);
                          if (!stat(fbuf, &stBuf))
                          {
                             /* Found the empty TAS file; delete it. */
                             unlink(fbuf);

                             /* Delete the .virgin file. */
                             snprintf(fbuf, sizeof(fbuf), "%s/%s", dir, oneFile);
                             unlink(fbuf);
                             
                             free(entry);
                             ifile++;
                             continue;
                          }
                       }
                       else if (loc = EndsWith(dup, ".tas", 1))
                       {
                          /* We're looking at a TAS file. Find the corresponding .virgin file, if it exists. */
                          snprintf(fbuf, sizeof(fbuf), "%s/%s.virgin", dir, dup);
                          if (!stat(fbuf, &stBuf))
                          {
                             /* Found the corresponding .virgin file; delete it and the TAS file. */
                             unlink(fbuf);

                             /* Delete the TAS file. */
                             snprintf(fbuf, sizeof(fbuf), "%s/%s", dir, oneFile);
                             unlink(fbuf);

                             free(entry);
                             ifile++;
                             continue;
                          }
                       }

                       free(dup);
                    }
                    else
                    {
                       /* It isn't that important if we don't clean up the TAS files. Don't sweat this - if we get
                        * here, though, most likely the code will fail elsewhere (at a place where code dereferences
                        * a NULL pointer probably). */
                    }
                 }

                 snprintf(subdir, sizeof(subdir), "%s/%s", dir, oneFile);
                 
                 if (!stat(subdir, &stBuf))
                 {
                    if (S_ISDIR(stBuf.st_mode))
                    {
                       notempty = !EmptyDir(subdir, depth + 1);
                    }
                    else
                    {
                       notempty = 1;
                    }
                 }
              }
           }

           free(entry);
        }

        ifile++;
     }

     free(fileList);
  }

  return !notempty;
}

/* Allocate a storage unit of the indicated size from SUMS and return
   its sunum and directory. */
#ifndef DRMS_CLIENT
long long drms_su_alloc(DRMS_Env_t *env, uint64_t size, char **sudir, int *tapegroup, int *status)
{
  int stat;
  DRMS_SumRequest_t *request, *reply;
  request = malloc(sizeof(DRMS_SumRequest_t));
  XASSERT(request);
  long long sunum;

  //  printf("************** HERE I AM *******************\n");

  /* No free slot was found, allocate a new storage unit from SUMS. */
  request->opcode = DRMS_SUMALLOC;
  request->dontwait = 0;
  request->reqcnt = 1;
  request->bytes = (double)size;

  if (tapegroup)
  {
     request->group = *tapegroup;
  }
  else
  {
     request->group = -1;
  }

  if (request->bytes <=0 )
  {
    fprintf(stderr,"Invalid storage unit size %lf\n",request->bytes);
    return 0;
  }

  drms_lock_server(env);

  if (!env->sum_thread) {
    if((stat = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
			      (void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", stat);
      drms_unlock_server(env);
      return 1;
    }
  }

  /* Submit request to sums server thread. */
  tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

  /* Wait for reply. FIXME: add timeout. */
  /* PERFORMANCE BOTTLENECK
   * If there are many drms sessions happening, SUMS grinds to a halt right here.
   * tqueueAdd causes the SUMS thread to call SUM_open(), but SUM_open() calls 
   * back up when there are a lot of transactions. As a result, SUM_open() may not
   * return for a while. After it returns it puts the result of the request into
   * the queue with its own tqueueAdd. The following tqueueDel has been blocked
   * the whole time waiting for the SUMS thread to call tqueueAdd. */
  drms_unlock_server(env);
  tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&reply);

  if (reply->opcode)
  {
      if (reply->opcode == -2)
      {
          fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
          stat = DRMS_ERROR_PENDINGTAPEREAD;
      }
      else
      {
          fprintf(stderr,"SUM ALLOC failed with error code %d.\n",reply->opcode);
          stat = reply->opcode;
      }
      sunum = 0;
      if (sudir)
          *sudir = NULL;
  }
  else
  {
    stat = DRMS_SUCCESS;
    sunum = reply->sunum[0];
    if (sudir)
      *sudir =  reply->sudir[0];
#ifdef DEBUG
    printf("Allocated Storage unit #%lld, dir='%s'\n",sunum, reply->sudir[0]);
#endif
  }

  if (status)
    *status = stat;
  free(reply);
  return sunum;   
}
#endif

#ifndef DRMS_CLIENT
int drms_su_alloc2(DRMS_Env_t *env, 
                   uint64_t size, 
                   long long sunum, 
                   char **sudir, 
                   int *tapegroup,
                   int *status)
{
   int stat;
   DRMS_SumRequest_t *request = NULL;
   DRMS_SumRequest_t *reply = NULL;
   request = malloc(sizeof(DRMS_SumRequest_t));
   XASSERT(request);

   request->opcode = DRMS_SUMALLOC2;
   request->dontwait = 0;
   request->reqcnt = 1;
   request->bytes = (double)size;
   request->sunum[0] = sunum;

   if (tapegroup)
   {
      request->group = *tapegroup;
   }
   else
   {
      request->group = -1;
   }

   if (request->bytes <=0 )
   {
      fprintf(stderr,"Invalid storage unit size %lf\n",request->bytes);
      return 0;
   }

   drms_lock_server(env);
   if (!env->sum_thread) 
   {
      if((stat = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
                                (void *) env))) 
      {
         fprintf(stderr,"Thread creation failed: %d\n", stat);
         drms_unlock_server(env);
         return 1;
      }
   }

   /* Submit request to sums server thread. */
   tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

   drms_unlock_server(env);
   /* Wait for reply. FIXME: add timeout. */
   tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&reply);

   if (reply->opcode)
   {
       if (reply->opcode == -2)
       {
           fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
           stat = DRMS_ERROR_PENDINGTAPEREAD;
       }
       else
       {
           fprintf(stderr,"SUM ALLOC2 failed with error code %d.\n",reply->opcode);
           stat = reply->opcode;
       }
       
       if (sudir)
       {
           *sudir = NULL;
       }
   }
   else
   {
      stat = DRMS_SUCCESS;
      if (sudir)
      {
         *sudir = reply->sudir[0];
      }
   }

   if (status)
   {
      *status = stat;
   }

   if (reply)
   {
      free(reply);
   }

   return stat;
}
#endif /* DRMS_CLIENT */

/* Get a new empty slot for a record from "series". If no storage units 
   from this series with empty slots are currently open, allocate a new 
   one from SUMS. When an appropriate storate unit is found, a new
   slot number (slot) is assigned corresponding to the 
   subdirectory printf("slot%04d",slot) which gets created.
   
   Returns storage unit struct (which contains sunum and directory), and 
   slot number.
*/
#ifndef DRMS_CLIENT
static int drms_su_newslots_internal(DRMS_Env_t *env, int n, char *series, 
                                     long long *recnum, DRMS_RecLifetime_t lifetime,
                                     int *slotnum, DRMS_StorageUnit_t **su,
                                     int createslotdirs,
                                     int gotosums)
{
  int i, status, slot;
  HContainer_t *scon; 
  HIterator_t hit; 
  long long sunum;
  char slotdir[DRMS_MAXPATHLEN+40], hashkey[DRMS_MAXHASHKEYLEN], *sudir = NULL;
  DRMS_Record_t *template=NULL;



  XASSERT(env->session->db_direct==1);
  
  /* Look up container of storage units for this series. */
  scon = hcon_lookup(&env->storageunit_cache, series);
  if (scon == NULL) /* Make new container for this series. */
  {
    scon = hcon_allocslot(&env->storageunit_cache, series);
    hcon_init(scon, sizeof(DRMS_StorageUnit_t), DRMS_MAXHASHKEYLEN,
	      (void (*)(const void *)) drms_su_freeunit, NULL);
  }
  
  /* Now iterate through all storage units for this series open for writing 
     and try to find one with a free slot.   */
  XASSERT(scon != NULL);
  slot = -1;
  status = -1;
  hiter_new(&hit, scon);  
  while( (su[0] = (DRMS_StorageUnit_t *)hiter_getnext(&hit)) )
  {
    if ( su[0]->mode == DRMS_READWRITE && su[0]->nfree > 0 )
    { 
      slot = 0;
      break;
    }
  }
  hiter_free(&hit);

  /* Start allocating slots (requesting new storage units from SUMS 
     as necessary. */
#ifdef DEBUG
  printf("n = %d\n",n);
#endif
  for (i=0; i<n; i++)
  {    
    if (slot >= 0)
    {     
      if (i>0) /* If previous storage unit still has room, use it. */
	su[i] = su[i-1];
      XASSERT(su[i]->nfree>0);
      while( su[i]->state[slot] != DRMS_SLOT_FREE ) 
	++slot;
    }
    if (slot == -1) /* Out of slots: It's time to allocate a new SU. */
    {
       /* ART - if gotosums == 0, then we cannot talk to SUMS, and we cannot allocate
        * the requested new slots, so we have to error out. */
       if (gotosums == 0)
       {
          fprintf(stderr, "drms_su_newslots_internal() failure - no existing slots available, need to fetch a new one from SUMS, but SUMS access is disabled (gotosums == 0).\n");
          status = DRMS_ERROR_NEEDSUMS;
          goto bail;
       }

      if (template==NULL) /* Only look up the template once... */
      {
	if ((template = drms_template_record(env, series, &status)) == NULL)
	{
	  fprintf(stderr,"ERROR: failed to look up series %s template in "
		  "drms_su_newslots()\n", series);
	  fprintf(stderr, "num items in series_cache %d\n", hcon_size(&(env->series_cache)));
	  hcon_printf(stderr, &(env->series_cache));
	  goto bail;
	}
      }

        /* Do not use drms_su_size(). This function includes linked segments when calculating the size of an SU, 
         * but when you allocate an SU, i.e., when creating records, you do not need to allocate space for linked segments
         * since they reside in different SUs. Instead, simply provide a small number as an estimate (e.g., 100MB).
         * SUMS actually does not do much with this argument. It is only used to ensure that at least that many
         * bytes exist on the file system before an allocation attempt is made.
         */
      sunum = drms_su_alloc(env, 
                            104857600,
                            &sudir,
                            &(template->seriesinfo->tapegroup), 
                            &status);
      if (status)
      {
	if (sudir)
	  free(sudir);
	goto bail;
      }	  
      sprintf(hashkey,DRMS_SUNUM_FORMAT, sunum);
      /* Insert new entry in hash table. */
      /* This allocates a new DRMS_StorageUnit_t. */
      su[i] = hcon_allocslot(scon, hashkey);
#ifdef DEBUG
      printf("Got su[i] = %p. Now has %d slots from '%s'\n",su[i], 
	     hcon_size(scon), series);
#endif

      /* Initialize storage unit struct. */
      su[i]->sunum = sunum;
      strncpy(su[i]->sudir, sudir, sizeof(su[i]->sudir));
      free(sudir);
      su[i]->mode = DRMS_READWRITE;
      su[i]->seriesinfo = template->seriesinfo;
      su[i]->nfree = su[i]->seriesinfo->unitsize;
      su[i]->state = malloc(su[i]->nfree);
      XASSERT(su[i]->state);
      memset(su[i]->state, DRMS_SLOT_FREE, su[i]->nfree);
      su[i]->recnum = malloc(su[i]->nfree*sizeof(long long));
      XASSERT(su[i]->recnum);
      memset(su[i]->recnum, 0, su[i]->nfree*sizeof(long long));
      su[i]->refcount = 0;
      /* This is a fresh storage unit. Assign slot 0 to the caller. */
      slot = 0;
    }
    slotnum[i] = slot;
    if (lifetime == DRMS_TRANSIENT)
      su[i]->state[slot] = DRMS_SLOT_TEMP;
    else
      su[i]->state[slot] = DRMS_SLOT_FULL;
    su[i]->recnum[slot] = recnum[i]; 
    ++slot;
    su[i]->nfree--; /* Slots are numbered starting at 0. */
    if (su[i]->nfree == 0)
      slot = -1;
    /*
#ifdef DEBUG
    printf("su[i]->sunum = %d\n",su[i]->sunum);
    printf("su[i]->sudir = %s\n",su[i]->sudir);
    printf("su[i]->nfree = %d\n",su[i]->nfree);
    printf("su->seriesinfo->unitsize = %d\n",su[i]->seriesinfo->unitsize);
    printf("slotnum[i]   = %d\n",slotnum[i]);
    printf("next slot    = %d\n",slot);
#endif
    */
  }
  /* Create the slot directory. */ 
  if (createslotdirs)
  {
     for (i=0; i<n; i++)	
     {
        if (su[i])
	{
           CHECKSNPRINTF(snprintf(slotdir, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT,
                                  su[i]->sudir,slotnum[i]), DRMS_MAXPATHLEN);
#ifdef DEBUG
           printf("su->sudir = '%s', slotdir = '%s'\n",su[i]->sudir, slotdir);
#endif
      
           if (mkdir(slotdir,0777))
           {
	      fprintf(stderr,"ERROR: drms_newslot could not create record "
		      "directory '%s'.\n",slotdir);
	      perror("mkdir call failed with error");
	      status = DRMS_ERROR_MKDIRFAILED;
           } 
	}
     }
  }

  status = DRMS_SUCCESS;
 bail:
  return status;
}

int drms_su_newslots(DRMS_Env_t *env, int n, char *series, 
                     long long *recnum, DRMS_RecLifetime_t lifetime,
                     int *slotnum, DRMS_StorageUnit_t **su,
                     int createslotdirs)
{
   /* This call will result in a SUMS requests in some cases. */
   return drms_su_newslots_internal(env, n, series, recnum, lifetime, slotnum, su, createslotdirs, 1);
}

int drms_su_newslots_nosums(DRMS_Env_t *env, int n, char *series, 
                            long long *recnum, DRMS_RecLifetime_t lifetime,
                            int *slotnum, DRMS_StorageUnit_t **su,
                            int createslotdirs)
{
   /* Does not allow SUMS access (if SUMS access is required for successful completion, then an error
    * code is returned). */
   return drms_su_newslots_internal(env, n, series, recnum, lifetime, slotnum, su, createslotdirs, 0);
}
#endif

/* Get the actual storage unit directory from SUMS. */
/* The su may NOT have seriesinfo (this function could be called with only an SUNUM known) */
#ifndef DRMS_CLIENT
struct DRMS_RsHandle_struct
{
    DB_Handle_t *dbh;
    char *requestsTable;
    char *seqTable;
};

typedef struct DRMS_RsHandle_struct DRMS_RsHandle_t;

static DRMS_RsHandle_t *crankUpRemoteSums(DRMS_Env_t *env, int *status)
{
    int rsStatus = DRMS_SUCCESS;
    char *nspace = NULL;
    char *table = NULL;
    char hostPort[128] = {0};
    char seqTable[128] = {0};
    DRMS_RsHandle_t *handle = NULL;
    DRMS_RsHandle_t *rv = NULL;
    
    
    /* The remote-sums components must be present if the JMD is not installed. Ensure this is the case. */
    
    /* Create handle. */
    handle = calloc(1, sizeof(DRMS_RsHandle_t));
    if (!handle)
    {
        rsStatus = DRMS_ERROR_OUTOFMEMORY;
    }
    
    /* requests table */
    if (!rsStatus)
    {
        if (get_namespace(RS_REQUEST_TABLE, &nspace, &table))
        {
            rsStatus = DRMS_ERROR_OUTOFMEMORY;
        }
        else
        {
            if (!drms_query_tabexists(env->session, nspace, table, &rsStatus))
            {
                printkerr("Cannot locate remote-sums requests table %s.\n", RS_REQUEST_TABLE);
                rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
            }
            else
            {
                handle->requestsTable = strdup(RS_REQUEST_TABLE);
                if (!handle->requestsTable)
                {
                    rsStatus = DRMS_ERROR_OUTOFMEMORY;
                }
            }
        }
    }
    
    /* requests table sequence */
    if (!rsStatus)
    {
        snprintf(seqTable, sizeof(seqTable), "%s_seq", table);
        if (!drms_query_tabexists(env->session, nspace, seqTable, &rsStatus))
        {
            printkerr("Cannot locate remote-sums requests table sequence table %s_seq.\n", RS_REQUEST_TABLE);
            rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
        }
        else
        {
            char tBuf[128];

            snprintf(tBuf, sizeof(tBuf), "%s.%s", nspace, seqTable);
            handle->seqTable = strdup(tBuf);
            if (!handle->seqTable)
            {
                rsStatus = DRMS_ERROR_OUTOFMEMORY;
            }
        }
    }
    
    /* rsumsd.py */
    if (!rsStatus)
    {
        /* The lockfile must be present, it must have the rsumsd.py PID in it, and the PID must exist (/proc/<pid> must exist).
         * I miss Python. */
        struct stat stBuf;
        FILE *fptr = NULL;
        
        if (stat(RS_LOCKFILE, &stBuf) || !S_ISREG(stBuf.st_mode))
        {
            printkerr("rsumsd.py is not running - cannot find PID file %s.\n", RS_LOCKFILE);
            rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
        }
        else
        {
            /* Extract the PID from the file. */
            fptr = fopen(RS_LOCKFILE, "r");
            if (!fptr)
            {
                printkerr("Cannot open remote-sums PID file %s.\n", RS_LOCKFILE);
                rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
            }
            else
            {
                char lineBuf[LINE_MAX];
                
                /* Will truncate the line after LINE_MAX - 1 chars, so the PID string must be short. */
                if (!fgets(lineBuf, sizeof(lineBuf), fptr))
                {
                    printkerr("remote-sums PID file %s is missing PID.\n", RS_LOCKFILE);
                    rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
                }
                else
                {
                    char procBuf[PATH_MAX];
                    
                    /* Assume linux! */
                    if (lineBuf[strlen(lineBuf) - 1] == '\n')
                    {
                        lineBuf[strlen(lineBuf) - 1] = '\0';
                    }
                    
                    snprintf(procBuf, sizeof(procBuf), "/proc/%s", lineBuf);
                    /* Now check for the existence of this DIRECTORY. */
                    if (stat(procBuf, &stBuf) || !S_ISDIR(stBuf.st_mode))
                    {
                        printkerr("rsumsd.py is not running - cannot find running process %s.\n", procBuf);
                        rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
                    }
                }
                
                /* FINALLY! */
                fclose(fptr);
            }
        }
    }
    
    if (!rsStatus)
    {
        snprintf(hostPort, sizeof(hostPort), "%s:%d", RS_DBHOST, RS_DBPORT);
        
        if ((handle->dbh = db_connect(hostPort, env->session->db_handle->dbuser, NULL, RS_DBNAME, 1)) == NULL)
        {
            printkerr("Couldn't connect to remote-sums database (host=%s, user=%s, db=%s).\n", hostPort, env->session->db_handle->dbuser, RS_DBNAME);
            rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
        }
    }
    
    if (!rsStatus)
    {
        rv = handle;
    }
    
    if (nspace)
    {
        free(nspace);
    }
    
    if (table)
    {
        free(table);
    }
    
    if (status)
    {
        *status = rsStatus;
    }
    
    return rv;
}

static void shutDownRemoteSums(DRMS_RsHandle_t **pHandle)
{
    if (pHandle)
    {
        DRMS_RsHandle_t *handle = *pHandle;
        
        if (handle)
        {
            if (handle->seqTable)
            {
                free(handle->seqTable);
                handle->seqTable = NULL;
            }
            
            if (handle->requestsTable)
            {
                free(handle->requestsTable);
                handle->requestsTable = NULL;
            }
            
            if (handle->dbh)
            {
                db_disconnect(&handle->dbh);
            }
            
            free(handle);
        }
        
        *pHandle = NULL;
    }
}

static int processRemoteSums(DRMS_RsHandle_t *handle, int64_t *sunums, unsigned int nsunums)
{
    int rsStatus = DRMS_SUCCESS;
    char idBuf[128];
    
    if (!rsStatus)
    {
        /* Insert a new request record into the rsumsd.py request table. Requests look like:
         *
         *   INSERT into drms.rs_requests(requestid, starttime, sunums, status, errmsg) VALUES (10, '2014-10-31 08:00', '545196869,545196870,545196871', 'N', '')
         */
        
        /* Make string out of the array of SUNUMs. */
        char *sunumList = NULL;
        size_t szList = 128;
        
        sunumList = calloc(szList, sizeof(char));
        
        if (!sunumList)
        {
            rsStatus = DRMS_ERROR_OUTOFMEMORY;
        }
        else
        {
            int iSunum;
            char numBuf[64];
            
            for (iSunum = 0; iSunum < nsunums; iSunum++)
            {
                if (sunums[iSunum] >= 0)
                {
                    if (iSunum > 0)
                    {
                        sunumList = base_strcatalloc(sunumList, ",", &szList);
                    }
                    
                    snprintf(numBuf, sizeof(numBuf), "%lld", (long long)(sunums[iSunum]));
                    sunumList = base_strcatalloc(sunumList, numBuf, &szList);
                }
            }
            
            if (strlen(sunumList) > 0)
            {
                /* Needs to big enough to operate on a chunk of SUNUMs (up to 512 of them). */
                char *cmd = NULL;
                size_t szCmd;
                
                szCmd = strlen(sunumList) + 256;
                cmd = calloc(szCmd, sizeof(char));
                
                if (!cmd)
                {
                    rsStatus = DRMS_ERROR_OUTOFMEMORY;
                }
                else
                {
                    /* Get request ID. */
                    long long nextID;
                    
                    /* Must make a NEW db connection since the remote-sums tables may live in a different database. Have to
                     * combine the host and port. This is the correct way to obtain the user name - this code is only available
                     * to server (non-socket-module) code. */
                    if (!handle || !handle->seqTable || !handle->requestsTable || !handle->dbh)
                    {
                        printkerr("Invalid remote-sums handle.\n");
                        rsStatus = DRMS_ERROR_REMOTESUMS_MISSING;
                    }
                    else
                    {
                        /* You actually provide the parent table, not the sequence table, to db_sequence_getnext(). */
                        nextID = db_sequence_getnext(handle->dbh, handle->requestsTable);
                        snprintf(idBuf, sizeof(idBuf), "%lld", nextID);
                        
                        cmd = base_strcatalloc(cmd, "INSERT INTO ", &szCmd);
                        cmd = base_strcatalloc(cmd, handle->requestsTable, &szCmd);
                        cmd = base_strcatalloc(cmd, "(requestid, starttime, sunums, status, errmsg) VALUES (", &szCmd);
                        cmd = base_strcatalloc(cmd, idBuf, &szCmd);
                        /* Use the localtimestamp() function for the starttime column. */
                        cmd = base_strcatalloc(cmd, ", localtimestamp(0), '", &szCmd);
                        cmd = base_strcatalloc(cmd, sunumList, &szCmd);
                        cmd = base_strcatalloc(cmd, "', 'N', '')", &szCmd);
                        
                        /* Execute the SQL. */
                        if (db_dms(handle->dbh, NULL, cmd))
                        {
                            printkerr("Failure inserting record for new remote-sums request: %s.\n", cmd);
                            rsStatus = DRMS_ERROR_REMOTESUMS_REQUEST;
                        }
                    }
                    
                    free(cmd);
                }
            }
            
            free(sunumList);
        }
    }
    
    if (!rsStatus)
    {
        char dbCmd[256];
        DB_Text_Result_t *dbRes = NULL;
        const char *reqStatus = NULL;
        const char *reqErrmsg = NULL;
        time_t timeStart;
        
        /* Poll for results from rsumds.py. Time-out in case rsumsd.py disappears. rsumsd.py has its own time-out for request processing, but
         * if it disappears, then this client will run forever. */
        timeStart = time(NULL);
        while (1)
        {
            /* Time-out after 12 hours. */
            if (time(NULL) > timeStart + 12 * 60 * 60)
            {
                rsStatus = DRMS_REMOTESUMS_TRYLATER;
                break;
            }
            
            /* Read request record from database using handle->dbh. */
            snprintf(dbCmd, sizeof(dbCmd), "SELECT status,errmsg FROM %s WHERE requestid = %s", handle->requestsTable, idBuf);
            dbRes = db_query_txt(handle->dbh, dbCmd);
            
            if (dbRes && dbRes->num_rows == 1 && dbRes->num_cols == 2)
            {
                /* rsumsd.py finished processing the request. */
                reqStatus = dbRes->field[0][0];
                reqErrmsg = dbRes->field[0][1];
                
                if (*reqStatus != 'N')
                {
                    if (*reqStatus == 'E')
                    {
                        printkerr(reqErrmsg);
                        rsStatus = DRMS_ERROR_REMOTESUMS_REQUEST;
                    }
                    
                    /* Delete request from the requests table. */
                    snprintf(dbCmd, sizeof(dbCmd), "DELETE FROM %s WHERE requestid = %s", handle->requestsTable, idBuf);
                    
                    /* Execute the SQL. */
                    if (db_dms(handle->dbh, NULL, dbCmd))
                    {
                        printkerr("Failure deleting record for completed remote-sums request: %s.\n", dbCmd);
                        rsStatus = DRMS_ERROR_REMOTESUMS_REQUEST;
                    }
                    
                    break;
                }
            }
            else
            {
                printkerr("Error checking on remote-sums request %s.\n", idBuf);
                rsStatus = DRMS_ERROR_REMOTESUMS_REQUEST;
                break;
            }
            
            if (dbRes)
            {
                db_free_text_result(dbRes);
                dbRes = NULL;
            }
            
            sleep(1);
        }
    }
    
    return rsStatus;
}

int drms_su_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retrieve)
{  
  int sustatus = DRMS_SUCCESS;
  DRMS_SumRequest_t *request, *reply;
  int tryagain;
  int natts;
  int16_t stagingRet = INT16_MIN;

  drms_lock_server(env);

  if (!env->sum_thread) {
    int status;
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
				(void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);
      drms_unlock_server(env);
      return 1;
    }
  }

  tryagain = 1;
  natts = 1;
  while (tryagain && natts < 3)
  {
     tryagain = 0;

     request = malloc(sizeof(DRMS_SumRequest_t));
     XASSERT(request);
     request->opcode = DRMS_SUMGET;
     request->reqcnt = 1;
     request->sunum[0] = su->sunum;
     request->mode = NORETRIEVE + TOUCH;
     if (retrieve) 
       request->mode = RETRIEVE + TOUCH;

     request->dontwait = 0;

      /* Use the Sget retention value here. This is the upper half of the 32-bit retention value in su->seriesinfo->retention
       * (actually the lower 15 bits of the upper 16 bits). This can be overriden by the value in env->retention (a positive
       * 15-bit number - never negative). To tell SUMS that we want to set the retention to set this value, but only if
       * the current retention value is smaller that this value, make the sign of this 15-bit number negative).
       * If this value is 0, then use the STDRETENTION value.
       */      
      if (env->retention != INT16_MIN)
      {
          /* The user set the DRMS_RETENTION argument. It overrides all other ways of specifying the retention time. */
          request->tdays = -1 * abs(env->retention); // env->retention should not be negative, but be careful.
      }
      else if ((stagingRet = drms_series_getstagingretention(su->seriesinfo)) != INT16_MIN)
      {
         /* Look at the lower 15 bits of the upper 16 bits of the series retention time, if the series info exists. It could 
          * be that this function was called without knowing the series that contains the requested SU. */
         if (stagingRet == 0)
         {
            /* If the staging retention time is 0, then use the STDRETENTION time. */
            request->tdays = -1 * abs(STDRETENTION);
         }
         else
         {
            request->tdays = -1 * stagingRet;
         }
      }
      else
      {
          /* The user did not set the DRMS_RETENTION argument, and we couldn't fetch the value from the database. */
          request->tdays = -1 * abs(STDRETENTION);
      }

     /* Submit request to sums server thread. */
     tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

     /***** DON'T USE request AFTER THIS POINT - drms_sums_thread() frees it *****/

     /* Wait for reply. FIXME: add timeout. 
      * This could take a long time if SUMS has to fetch from tape, so release env lock temporarily. */
     drms_unlock_server(env);
     tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);
     drms_lock_server(env);

     if (reply->opcode != 0)
     {
         if (reply->opcode == 3)
         {
             /* We waited over two hours for a tape fetch to complete, then we timed-out. */
             free(reply);
             drms_unlock_server(env);
             return DRMS_ERROR_SUMSTRYLATER;
         }
         else if (reply->opcode == -2)
         {
             fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
             free(reply);
             drms_unlock_server(env);
             return DRMS_ERROR_PENDINGTAPEREAD;
         }
         else
         {
             fprintf(stderr, "SUM GET failed with error code %d.\n", reply->opcode);
             free(reply);
             drms_unlock_server(env);
             return DRMS_ERROR_SUMGET;
         }
     }
     else
     {
        su->sudir[0] = '\0';

        if (strlen(reply->sudir[0]) > 0)
        {
           snprintf(su->sudir, sizeof(su->sudir), "%s", reply->sudir[0]);
           free(reply->sudir[0]);
        }
        else if (retrieve && natts < 2 && su->sunum >= 0 && drms_su_isremotesu(su->sunum))
        {
           /* This sudir is 
            * POSSIBLY owned by a remote sums.  We invoke remote
            * sums only if the retrieve flag is set.
            *
            * Since the retrieve flag was set, we assume that any empty string
            * SUDIR denotes a SUNUM that belongs to a different SUMS (or
            * it is an invalid SUNUM, in which case the storage unit is
            * is invalid - for now, we punt on this possibility and assume
            * that an empty dir denotes a remote SUNUM).
            */

#if defined(JMD_IS_INSTALLED) && JMD_IS_INSTALLED
           //ISS VSO HTTP JMD START {
           struct POSTState ps;
           char *postrequeststr=NULL;

           HContainer_t *postmap = hcon_create(sizeof(struct PassSunumList *), 128, NULL, NULL, NULL, NULL, 0);
           char *sname = su->seriesinfo ? su->seriesinfo->seriesname : "unknown";
           long long sunum = su->sunum;
           add_sunum_to_POST(env, postmap,sname,sunum);

           int inprogress=-1;
           int ntries = 0;
           int totaltries=24;
           int waittime=10;
           while ((inprogress = session_status(ps.session_id)) > 0 && ntries < totaltries)
           {
              sleep(waittime);
              ntries++;
           }

           //free any post structures
           free_post_request(postmap);
           free(postrequeststr);

           if (0 != inprogress)
           {
              /* JMD has not finished the request, so the caller should not attempt to use the SU(s) they want to use. */
              sustatus = DRMS_REMOTESUMS_TRYLATER;
              tryagain = 0;
           }
           //ISS VSO HTTP JMD END }
#else
            /* Begin remote sums. */
            {
                int64_t *sunums = NULL;
                sunums = calloc(1, sizeof(int64_t));
                DRMS_RsHandle_t *rsHandle = NULL;
                
                if (!sunums)
                {
                    sustatus = DRMS_ERROR_OUTOFMEMORY;
                    tryagain = 0;
                }
                else
                {
                    sunums[0] = su->sunum;
                    rsHandle = crankUpRemoteSums(env, &sustatus);
                    
                    if (!sustatus && rsHandle)
                    {
                        sustatus = processRemoteSums(rsHandle, sunums, 1);
                        
                        if (!sustatus)
                        {
                            tryagain = 1; /* This causes the SUM_get() to be retried. */
                        }
                        else
                        {
                            tryagain = 0;
                        }
                    }
                    else
                    {
                        if (!sustatus)
                        {
                            sustatus = DRMS_ERROR_REMOTESUMS_INITIALIZATION;
                        }
                        
                        tryagain = 0;
                    }
                    
                    if (rsHandle)
                    {
                        shutDownRemoteSums(&rsHandle);
                    }
                    
                    free(sunums);
                    sunums = NULL;
                }
            }
#endif /* JMD_IS_INSTALLED */
        }
        else
        {
            /* Some kind of problem with the sunum - set the sudir field to '\0'. This should already be the case,
             * but make sure. */
            (su->sudir)[0] = '\0';
        }
     }

     free(reply);
     natts++;
  }

  drms_unlock_server(env);

  return sustatus;
}
#endif


/* Get the actual storage unit directory from SUMS. */
#ifndef DRMS_CLIENT
int drms_su_getsudirs(DRMS_Env_t *env, int n, DRMS_StorageUnit_t **su, int retrieve, int dontwait)
{  
  int sustatus = DRMS_SUCCESS;
  DRMS_SumRequest_t *request, *reply;
  DRMS_StorageUnit_t **workingsus = NULL;
  DRMS_StorageUnit_t **rsumssus = NULL;
  //LinkedList_t *retrysunums = NULL;
  LinkedList_t *retrysus = NULL;
  int nretrySUNUMS = 0;
  int workingn;
  int tryagain;
  int natts;
  int16_t maxRet;
  int16_t stagingRet = INT16_MIN;

  drms_lock_server(env);

  if (!env->sum_thread) {
    int status;
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
				(void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);
      drms_unlock_server(env);
      return 1;
    }
  }

  int isu;
  int iSUMSsunum;
  DRMS_StorageUnit_t *onesu = NULL;

    /* SUMS does not support dontwait == 1, so force dontwait to be 0 (deprecate the dontwait parameter). */
    dontwait = 0;
    
  for (isu = 0; isu < n; isu++)
  {
     onesu = su[isu];

     /* Set all returned sudirs to empty strings - used as a flag to know what has been processed. */
     *(onesu->sudir) = '\0';
  }

  /* There is a maximum no. of SUs that can be requested from SUMS, MAXSUMREQCNT. So, loop. */
  int start = 0;
  int end = SUMIN(MAXSUMREQCNT, n); /* index of SU one past the last one to be processed */

  workingsus = su;
  workingn = n;

  tryagain = 1;
  natts = 1;
  maxRet = -1;

  if (env->retention != INT16_MIN)
  {
     /* The user set the DRMS_RETENTION argument. It overrides all other ways of specifying the retention time. */
     maxRet = (int16_t)abs(env->retention); // env->retention should not be negative, but be careful.
  }

  while (tryagain && natts < 3)
  {
     tryagain = 0;

     /* Ask SUMS for ALL SUS in workingsus (in chunks of MAXSUMREQCNT) */
     while (start < workingn)
     {
        /* create SUMS request (apparently, SUMS frees this request) */
        request = malloc(sizeof(DRMS_SumRequest_t));
        XASSERT(request);

        request->opcode = DRMS_SUMGET;
        request->reqcnt = end - start;
         
        for (isu = start, iSUMSsunum = 0; isu < end; isu++, iSUMSsunum++) 
        {
           request->sunum[iSUMSsunum] = workingsus[isu]->sunum;
           if (maxRet == -1)
           {
              /* Look at the lower 15 bits of the upper 16 bits of the series retention time, if the series info exists. It could
               * be that this function was called without knowing the series that contains the requested SU. */
              stagingRet = drms_series_getstagingretention(workingsus[isu]->seriesinfo);
              if (stagingRet != INT16_MIN)
              {
                 if (stagingRet > maxRet)
                 {
                    maxRet = stagingRet;
                 }
              }
           }
        }
        
        request->mode = NORETRIEVE + TOUCH;
        if (retrieve) 
          request->mode = RETRIEVE + TOUCH;

        request->dontwait = dontwait;
         
        /* Use the Sget retention value here. This is the upper half of the 32-bit retention value in su->seriesinfo->retention
         * (actually the lower 15 bits of the upper 16 bits). This can be overriden by the value in env->retention (a positive
         * 15-bit number - never negative). To tell SUMS that we want to set the retention to set this value, but only if
         * the current retention value is smaller that this value, make the sign of this 15-bit number negative).
         * If this value is 0, then use the STDRETENTION value.
         */
        if (maxRet != -1 && maxRet != 0)
        {
           /* Look at the lower 15 bits of the upper 16 bits of the series retention time, if the series info exists. It could
            * be that this function was called without knowing the series that contains the requested SU. */
           request->tdays = -1 * maxRet;
        }
        else
        {
           /* The user did not set the DRMS_RETENTION argument, and we couldn't fetch the value from the database. */
           request->tdays = -1 * abs(STDRETENTION);
        }

        /* Submit request to sums server thread. */
        tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) request);
  
        /* Wait for reply. FIXME: add timeout. */
        if (!dontwait) 
        {
           /* If and only if user wants to wait for the reply, then return back 
            * to user all SUDIRs found. */
           /* Could take a while for SUMS to respond (it it has to fetch from tape), 
            * so release env lock temporarily. */
           drms_unlock_server(env);
           tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);
           drms_lock_server(env);
     
           if (reply->opcode != 0)
           {
               if (reply->opcode == 3)
               {
                   free(reply);
                   drms_unlock_server(env);
                   
                   /* We waited over two hours for a tape fetch to complete, then we timed-out. */
                   return DRMS_ERROR_SUMSTRYLATER;
               }
               else if (reply->opcode == -2)
               {
                   fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
                   free(reply);
                   drms_unlock_server(env);
                   return DRMS_ERROR_PENDINGTAPEREAD;
               }
               else if (reply->opcode == -3)
               {
                   fprintf(stderr, "Failure setting sum-get-pending flag.\n");
                   free(reply);
                   drms_unlock_server(env);
                   return DRMS_ERROR_SUMGET;
               }
               else if (reply->opcode == -4)
               {
                  fprintf(stderr, "Failure UNsetting sum-get-pending flag.\n");
                  free(reply);
                  drms_unlock_server(env);
                  return DRMS_ERROR_SUMGET;
               }
               else
               {
                   fprintf(stderr, "SUM GET failed with error code %d.\n", reply->opcode);
                   free(reply);
                   drms_unlock_server(env);
                   return DRMS_ERROR_SUMGET;
               }
           }
           else
           {
              retrysus = list_llcreate(sizeof(DRMS_StorageUnit_t *), NULL);

              for (isu = start, iSUMSsunum = 0; isu < end; isu++, iSUMSsunum++)
              {
                 if (strlen(reply->sudir[iSUMSsunum]) > 0)
                 {
                    /* For these SUNUMs, we have SUDIRs, so we can provide these 
                     * back to the caller.  */
                    strncpy(workingsus[isu]->sudir, 
                            reply->sudir[iSUMSsunum], 
                            DRMS_MAXPATHLEN);
                 }
                 else if (retrieve && natts < 2 && workingsus[isu]->sunum >= 0 && drms_su_isremotesu(workingsus[isu]->sunum))
                 {
                    /* Count the sudirs that are empty string.  Each of these is 
                     * POSSIBLY owned by a remote sums.  We invoke remote
                     * sums only if the retrieve flag is set.
                     *
                     * Since the retrieve flag was set, we assume that any empty string
                     * SUDIR denotes a SUNUM that belongs to a different SUMS (or
                     * it is an invalid SUNUM, in which case the storage unit is
                     * is invalid - for now, we punt on this possibility and assume
                     * that an empty dir denotes a remote SUNUM).
                     */
                    snprintf(workingsus[isu]->sudir, DRMS_MAXPATHLEN, "%s", "rs"); /* flag to indicate
                                                                                    * remotesums
                                                                                    * processing
                                                                                    */
                    list_llinserttail(retrysus, &(workingsus[isu]));
                 }
                 else
                 {
                     /* Some kind of problem with the sunum - set the sudir field to '\0'. This should already be the case, 
                      * but make sure. */
                     (workingsus[isu]->sudir)[0] = '\0';
                 }

                 free(reply->sudir[iSUMSsunum]);
              }
           }

           free(reply);
        } /* !dontwait*/

        start = end;
        end = SUMIN(MAXSUMREQCNT + start, workingn);
     } /* while */

     /* At this point, there may be some off-site SUNUMs - if so, run master script
      * and then retry. */
     if (natts < 2 && retrysus && list_llgetnitems(retrysus) > 0)
     {
         ListNode_t *node = NULL;
#if defined(JMD_IS_INSTALLED) && JMD_IS_INSTALLED
        //ISS VSO HTTP JMD START {
        /* iterate through each sunum */
        DRMS_StorageUnit_t *rsu = NULL;

        list_llreset(retrysus);

        struct POSTState ps;
        char *postrequeststr=NULL; //this pointer is allocated

        HContainer_t *postmap = hcon_create(sizeof(struct PassSunumList *), 128, NULL, NULL, NULL, NULL, 0);
        while ((node = list_llnext(retrysus)) != NULL)
        {
           rsu = *((DRMS_StorageUnit_t **)(node->data));
           char *sname = rsu->seriesinfo ? rsu->seriesinfo->seriesname : "unknown";
           long long sunum = rsu->sunum;
           add_sunum_to_POST(env, postmap,sname,sunum);
        }

        int postsize = create_post_msg(postmap,&postrequeststr);
        send_POST_request(postrequeststr,postsize,&ps);

        int inprogress=-1;
        int ntries = 0;
        int totaltries=40;
        int waittime=10;
        while ((inprogress = session_status(ps.session_id)) > 0 && ntries < totaltries) 
        {
           sleep(waittime);
           ntries++;
        }

        //free any post structures
        free_post_request(postmap);
        free(postrequeststr);

        if (0 == inprogress)
        {
           /* JMD worked - need to call SUM_get() one more time
            * with SUNUMS that failed originally */
           nretrySUNUMS = list_llgetnitems(retrysus);

           start = 0;
           /* index of SU one past the last one to be processed */
           end = SUMIN(MAXSUMREQCNT, nretrySUNUMS);
           rsumssus = (DRMS_StorageUnit_t **)malloc(sizeof(DRMS_StorageUnit_t *) * nretrySUNUMS);

           isu = 0;

           while (node = list_llgethead(retrysus))
           {
              onesu = (DRMS_StorageUnit_t *)malloc(sizeof(DRMS_StorageUnit_t));
              onesu->sunum = (*(DRMS_StorageUnit_t **)(node->data))->sunum;
              *(onesu->sudir) = '\0';
              rsumssus[isu] = onesu;
              isu++;
              list_llremove(retrysus, node);
              list_llfreenode(&node);
           }

           if (retrysus)
           {
              list_llfree(&retrysus);
           }

           workingsus = rsumssus;
           workingn = nretrySUNUMS;
           natts++;
        }
        //ISS VSO HTTP JMD END }
#else
         /* Remote SUMS */
         {
             int64_t *sunums = NULL;
             DRMS_RsHandle_t *rsHandle = NULL;
             DRMS_StorageUnit_t *rsu = NULL;
             
             nretrySUNUMS = list_llgetnitems(retrysus);
             sunums = calloc(SUMIN(MAXSUMREQCNT, nretrySUNUMS), sizeof(int64_t));
             
             if (!sunums)
             {
                 sustatus = DRMS_ERROR_OUTOFMEMORY;
                 tryagain = 0;
             }
             else
             {
                 rsHandle = crankUpRemoteSums(env, &sustatus);
                 
                 if (!sustatus && rsHandle)
                 {
                     /* Iterate through SUs. We have to chunk them before calling processRemoteSums(). */
                     for (list_llreset(retrysus), isu = 0, iSUMSsunum = 0; ((node = list_llnext(retrysus)) != NULL); isu++)
                     {
                         rsu = *((DRMS_StorageUnit_t **)(node->data));
                         sunums[iSUMSsunum++] = rsu->sunum;
                         
                         if (iSUMSsunum > 0 && (iSUMSsunum % MAXSUMREQCNT == 0 || isu == nretrySUNUMS - 1))
                         {
                             /* if isu == nretrySUNUMS - 1, then about to exit loop. */
                             sustatus = processRemoteSums(rsHandle, sunums, iSUMSsunum);
                             free(sunums);
                             sunums = NULL;
                             
                             iSUMSsunum = 0;
                             
                             if (sustatus)
                             {
                                 break;
                             }
                             
                             if (isu < nretrySUNUMS - 1)
                             {
                                 sunums = calloc(SUMIN(MAXSUMREQCNT, nretrySUNUMS - isu - 1), sizeof(int64_t));
                                 
                                 if (!sunums)
                                 {
                                     sustatus = DRMS_ERROR_OUTOFMEMORY;
                                     break;
                                 }
                             }
                         }
                     }
                 }
                 else
                 {
                     if (!sustatus)
                     {
                         sustatus = DRMS_ERROR_REMOTESUMS_INITIALIZATION;
                     }
                 }
                 
                 if (rsHandle)
                 {
                     shutDownRemoteSums(&rsHandle);
                 }
                 
                 if (!sustatus)
                 {
                     /* The remote-sums request was successfully processed. Retry all the SUs tagged for retry. */
                     tryagain = 1;
                     
                     start = 0;
                     /* index of SU one past the last one to be processed */
                     end = SUMIN(MAXSUMREQCNT, nretrySUNUMS);
                     rsumssus = (DRMS_StorageUnit_t **)malloc(sizeof(DRMS_StorageUnit_t *) * nretrySUNUMS);
                     
                     isu = 0;
                     while (node = list_llgethead(retrysus))
                     {
                         onesu = (DRMS_StorageUnit_t *)malloc(sizeof(DRMS_StorageUnit_t));
                         onesu->sunum = (*(DRMS_StorageUnit_t **)(node->data))->sunum;
                         *(onesu->sudir) = '\0';
                         rsumssus[isu] = onesu;
                         isu++;
                         list_llremove(retrysus, node);
                         list_llfreenode(&node);
                     }
                     
                     if (retrysus)
                     {
                         list_llfree(&retrysus);
                     }
                     
                     workingsus = rsumssus;
                     workingn = nretrySUNUMS;
                     natts++;
                 }
                 else
                 {
                     tryagain = 0;
                 }
             }
         }
#endif /* JMD_IS_INSTALLED */
     } /* code to set up parent/child and call remotesums_master.pl */
  } /* while - retry */

  if (retrieve && rsumssus && nretrySUNUMS > 0)
  {
     /* Merge results of remotesums request with original su array */
     for (isu = 0, iSUMSsunum = 0; isu < n; isu++)
     {
        if (strcmp(su[isu]->sudir, "rs") == 0)
        {
           /* This SUNUM was sent to remotesums. */
           snprintf(su[isu]->sudir, DRMS_MAXPATHLEN, "%s", rsumssus[iSUMSsunum]->sudir);
           iSUMSsunum++;
        }
     }
  }

  if (rsumssus)
  {
     for (isu = 0; isu < nretrySUNUMS; isu++)
     {
        if (rsumssus[isu])
        {
           free(rsumssus[isu]);
        }
     }

     free(rsumssus);
  }

  if (retrysus)
  {
     list_llfree(&retrysus);
  }

  drms_unlock_server(env);

  return sustatus;
}
#endif

#ifndef DRMS_CLIENT
int drms_su_setretention(DRMS_Env_t *env, int16_t newRetention, int nsus, long long *sunums)
{
    int drmsStatus;
    int isu;
    int start;
    DRMS_SumRequest_t *request = NULL;
    DRMS_SumRequest_t *reply = NULL;
    int szChunk;
    
    drmsStatus = DRMS_SUCCESS;
    
    drms_lock_server(env);
    
    if (!env->sum_thread)
    {
        int libStat;
        if (libStat = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, (void *)env))
        {
            fprintf(stderr,"Thread creation failed: %d\n", libStat);
            drmsStatus = DRMS_ERROR_CANTCREATETHREAD;
        }
    }
    
    if (drmsStatus == DRMS_SUCCESS)
    {
        HContainer_t *map = NULL;
        int yep;
        char key[128];
        
        map = hcon_create(sizeof(SUM_info_t *), 128, NULL, NULL, NULL, NULL, 0);
        
        if (!map)
        {
            drmsStatus = DRMS_ERROR_OUTOFMEMORY;
        }
        else
        {
            yep = 1;
            start = 0;
            
            while (start < nsus)
            {
                /* create SUMS request (SUMS frees this request) */
                request = malloc(sizeof(DRMS_SumRequest_t));
                XASSERT(request);
                
                request->opcode = DRMS_SUMGET;
                
                for (isu = start, szChunk = 0; szChunk < MAXSUMREQCNT && isu < nsus; isu++)
                {
                    /* Some of these SUs may not even belong to the local SUMS. We don't care if that happens. We
                     * want to modify the retention of local SUs only. */
                    
                    /* Do not send SUNUMs with a value of -1 to SUMS, and don't send duplicates. */
                    snprintf(key, sizeof(key), "%llu", (unsigned long long)sunums[isu]); /* -1 converted to ULLONG_MAX. */
                    if (sunums[isu] >= 0 && !hcon_member(map, key))
                    {
                        hcon_insert(map, key, &yep);
                        request->sunum[szChunk] = sunums[isu];
                        szChunk++;
                    }
                }
                
                start += szChunk; /* for next while-loop iteration */
                
                request->reqcnt = szChunk;
                
                /* If a requested SU is offline, bring it online and set its retention. */
                request->mode = RETRIEVE + TOUCH;
                request->dontwait = 0;
                /* newRetention can be positive or negative. A positive number will potentially result in a decrease of the
                 * retention value. */
                request->tdays = newRetention;
                
                /* Submit request to sums server thread. */
                tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) request);
                
                /* Wait for reply. FIXME: add timeout. */
                
                /* If and only if user wants to wait for the reply, then return back
                 * to user all SUDIRs found. */
                /* Could take a while for SUMS to respond (it it has to fetch from tape),
                 * so release env lock temporarily. */
                drms_unlock_server(env);
                tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);
                drms_lock_server(env);
                
                if (reply->opcode != 0)
                {
                    if (reply->opcode == 3)
                    {
                        /* We waited over six hours for a tape fetch to complete, then we timed-out. */
                        drmsStatus = DRMS_ERROR_SUMSTRYLATER;
                    }
                    else if (reply->opcode == -2)
                    {
                        fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
                        drmsStatus = DRMS_ERROR_PENDINGTAPEREAD;
                    }
                    else if (reply->opcode == -3)
                    {
                        fprintf(stderr, "Failure setting sum-get-pending flag.\n");
                        drmsStatus = DRMS_ERROR_SUMGET;
                    }
                    else if (reply->opcode == -4)
                    {
                        fprintf(stderr, "Failure UNsetting sum-get-pending flag.\n");
                        drmsStatus = DRMS_ERROR_SUMGET;
                    }
                    else
                    {
                        fprintf(stderr, "SUM GET failed with error code %d.\n", reply->opcode);
                        drmsStatus = DRMS_ERROR_SUMGET;
                    }
                }
                else
                {
                    /* success setting new retention */
                    
                    /* free returned SUDIRs (which are not needed by this call) */
                    if (reply->sudir)
                    {
                        for (isu = 0; isu < szChunk; isu++)
                        {
                            if (reply->sudir[isu])
                            {
                                free(reply->sudir[isu]);
                            }
                        }
                    }
                }
                
                free(reply);
            }
            
            hcon_destroy(&map);
        }
    }
    
    drms_unlock_server(env);
    
    return drmsStatus;
}
#endif

#ifndef DRMS_CLIENT
static void SUFreeInfo(const void *value)
{
   SUM_info_t *tofree = *((SUM_info_t **)value);
   if (tofree)
   {
      free(tofree);
   }
}

/* info contains nsunums SUM_info_t pointers */
int drms_su_getinfo(DRMS_Env_t *env, long long *sunums, int nsunums, SUM_info_t **info)
{
   int status = DRMS_SUCCESS;
   DRMS_SumRequest_t *request = NULL;
   DRMS_SumRequest_t *reply = NULL;
   HContainer_t *map = NULL;
   int isunum;
   int iinfo;
   int nReqs;
   char key[128];
   SUM_info_t *nulladdr = NULL;
   SUM_info_t **pinfo = NULL;

   drms_lock_server(env);

   if (!env->sum_thread) 
   {

      if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
                                  (void *) env)) != DRMS_SUCCESS) 
      {
         fprintf(stderr,"Thread creation failed: %d\n", status);
         drms_unlock_server(env);
         return 1;
      }
   }

   /* There might be more than MAXSUMREQCNT sunums, and there might be duplicates. 
    * Store unique values. */
   map = hcon_create(sizeof(SUM_info_t *), 128, SUFreeInfo, NULL, NULL, NULL, 0);

   for (nReqs = 0, isunum = 0; isunum < nsunums; isunum++)
   {
      if (nReqs == 0)
      {
         request = (DRMS_SumRequest_t *)malloc(sizeof(DRMS_SumRequest_t));
         XASSERT(request);
         request->opcode = DRMS_SUMINFO;
         request->dontwait = 0;
      }

      snprintf(key, sizeof(key), "%llu", (unsigned long long)sunums[isunum]); /* -1 converted to ULLONG_MAX. */
      if (!hcon_member(map, key))
      {
         /* sunums[isnum] could be equal to -1. The code in drms_server.c will handle these, not 
          * sending them onto sums. */
         request->sunum[nReqs] = (uint64_t)sunums[isunum]; /* -1 converted to ULLONG_MAX. */
         hcon_insert(map, key, &nulladdr);
         nReqs++;
      }
      
      /* ART - Although SUMS will handle up to MAXSUMREQCNT SUNUMs, the keylist 
       * code used by SUMS is inefficient - the optimal batch size is 64. */
      if (nReqs == MAXSUMREQCNT || (isunum + 1 == nsunums && nReqs > 0))
      {
         request->reqcnt = nReqs;

         /* Submit request to sums server thread. */
         tqueueAdd(env->sum_inbox, (long)pthread_self(), (char *)request);
         drms_unlock_server(env);
         tqueueDel(env->sum_outbox,  (long)pthread_self(), (char **)&reply);
         drms_lock_server(env);
     
         if (reply->opcode != 0)
         {
             hcon_destroy(&map);
             
             if (reply->opcode == -2)
             {
                 fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
                 if (reply)
                 {
                     free(reply);
                 }
                 drms_unlock_server(env);
                 return DRMS_ERROR_PENDINGTAPEREAD;
             }
             
             fprintf(stderr, "SUMINFO failed with error code %d.\n", reply->opcode);
             
             if (reply)
             {
                 free(reply);
             }
             
             drms_unlock_server(env);
             return 1;
         }
         else
         {
            SUM_info_t *retinfo = NULL;
            
            /* reply->surdir now has pointers to the SUM_info_t structs */
            for (iinfo = 0; iinfo < nReqs; iinfo++)
            {
               /* NOTE - if an SUNUM is unknown, the SUM_info_t returned from SUM_infoEx() will have the sunum set 
                * to -1.  But drms_server.c will overwrite that -1 with the SUNUM requested. BUT, there could have
                * been a sunum of -1 passed to this function to begin with. In that case, the -1 got passed to 
                * drms_server.c, and drms_server.c will return an info struct for that sunum, but it will
                * be all zeros (except for the sunum, which will be -1). */
               retinfo = (SUM_info_t *)reply->sudir[iinfo];
               snprintf(key, sizeof(key), "%llu", (unsigned long long)(retinfo->sunum)); /* -1 converted to ULLONG_MAX. */
               if ((pinfo = hcon_lookup(map, key)) != NULL)
               {
                  *pinfo = retinfo;
               }
               else
               {
                  fprintf(stderr, "Information returned for an sunum ('%s') that is unknown to DRMS.\n", key);
                  status = 99;
                  break;
               }
            }
         }

         /* Caller's responsibility to clean up the reply since caller is waiting for the reply. */
         if (reply)
         {
            free(reply);
            reply = NULL;
         }

         nReqs = 0;
      }
   } /* loop over original sunums */

   if (status == DRMS_SUCCESS)
   {
      /* Copy all the SUM_info_t returned by SUMS into the info parameter (for return to caller). */
      for (isunum = 0; isunum < nsunums; isunum++)
      {
         snprintf(key, sizeof(key), "%llu", (unsigned long long)(sunums[isunum])); /* -1 converted to ULLONG_MAX. */
         if ((pinfo = hcon_lookup(map, key)) != NULL)
         {
            info[isunum] = (SUM_info_t *)malloc(sizeof(SUM_info_t));
            *(info[isunum]) = **pinfo;
            (info[isunum])->next = NULL;
         }
         else
         {
            fprintf(stderr, "sunum '%s' unknown to SUMS.\n", key);
            status = 99;
            break;
         }
      }
   }

   /* clean up - free up the SUM_info_t structs pointed to by map */
   hcon_destroy(&map);

   /* request is shallow-freed by the SUMS thread, so don't free here. */

   drms_unlock_server(env);
   
   return status;
}
#endif

/* Tell SUMS to save this storage unit. */
/* This su commit function writes the Records.txt file. This is used by SUMS when deleting DRMS records (when the archive flag is -1).
 * drms_su_commitsu() does NOT write the Records.txt file, so it is not possible for SUMS to delete DRMS records of SUs committed 
 * with this function. */
#ifndef DRMS_CLIENT
int drms_commitunit(DRMS_Env_t *env, DRMS_StorageUnit_t *su)
{
  int i;
  DRMS_SumRequest_t *request, *reply;
  FILE *fp;
  char filename[DRMS_MAXPATHLEN];
  int actualarchive = 0;
  int docommit = 0;
  int16_t newSuRet = INT16_MIN;

  docommit = !EmptyDir(su->sudir, 0);
  if (docommit)
  {
     if (env->archive != INT_MIN)
     {
        actualarchive = env->archive;
     }
     else
     {
        actualarchive = su->seriesinfo->archive;
     }

     /* Write text file with record numbers to storage unit directory. */
     if ( su->recnum )
     {
        sprintf(filename,"%s/Records.txt",su->sudir);
        if ((fp = fopen(filename,"w")) == NULL)
        {
           fprintf(stderr,"ERROR in drms_commitunit: Failed to open file '%s'\n",
                   filename);
           return 1;
        }

        /* If archive is set to -1, then write flag text at the top of the file to 
         * tell SUMS to delete the DRMS records within the storage unit. */
        if (actualarchive == -1)
        {
           fprintf(fp, "DELETE_SLOTS_RECORDS\n");
        }

        fprintf(fp,"series=%s\n", su->seriesinfo->seriesname);
        if (su->nfree<su->seriesinfo->unitsize)
        {
           fprintf(fp,"slot\trecord number\n");
           for (i=0; i<su->seriesinfo->unitsize; i++)
             if (su->state[i] != DRMS_SLOT_FREE)
               fprintf(fp,"%d\t%lld\n", i, su->recnum[i]);
        }
        fclose(fp);
     }

     request = malloc(sizeof(DRMS_SumRequest_t));
     XASSERT(request);
     request->opcode = DRMS_SUMPUT;
     request->dontwait = 0;
     request->reqcnt = 1;
     request->dsname = su->seriesinfo->seriesname;
     request->group = su->seriesinfo->tapegroup;
     if (actualarchive == 1) 
       request->mode = ARCH + TOUCH;
     else
       request->mode = TEMP + TOUCH;

     /* If the user doesn't override the retention time on the cmd-line, use the db new-su retention value.
      * Since we are creating the SU for the first time, we should make the sign of the retention time sent 
      * to SUMS positive.
      */
     if (env->newsuretention != INT16_MIN) 
     {
        /* The user set the DRMS_NEWSURETENTION argument. It overrides all other ways of specifying the retention time. */
        request->tdays = env->newsuretention;
     }
     else if ((newSuRet = drms_series_getnewsuretention(su->seriesinfo)) != INT16_MIN)
     {
        /* Look at the lower 15 bits of the lower 16 bits of the series retention time. */
        if (newSuRet == 0)
        {
           /* If the staging retention time is 0, then use the STDRETENTION time. */
           request->tdays = abs(STDRETENTION);
        }
        else
        {
           request->tdays = newSuRet;
        }
     }
     else
     {
        /* The user did not set the DRMS_NEWSURETENTION argument, and we couldn't fetch the value from the database. */
        request->tdays = abs(STDRETENTION);
     }

     request->sunum[0] = su->sunum;
     request->sudir[0] = su->sudir;
     request->comment = NULL;

     // must have sum_thread running already
     XASSERT(env->sum_thread);
     /* Submit request to sums server thread. */
     tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);
     /* Wait for reply. FIXME: add timeout. */
     tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);
     if (reply->opcode != 0) 
     {
         if (reply->opcode == -2)
         {
             fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
             free(reply);
             return DRMS_ERROR_PENDINGTAPEREAD;
         }
         
         fprintf(stderr, "ERROR in drms_commitunit: SUM PUT failed with "
                 "error code %d.\n",reply->opcode);
         free(reply);
         return 1;
     }
     free(reply);
     /* Now the storage unit is owned by SUMS, mark it read-only. */
     su->mode = DRMS_READONLY;

  }

  return 0;
}

static int CommitUnits(DRMS_Env_t *env, 
                       LinkedList_t *ll, 
                       const char *seriesName, 
                       int seriesArch,
                       int seriesUS,
                       int seriesTG,
                       int16_t seriesRet)
{
   ListNode_t *node = NULL;
   DRMS_StorageUnit_t *sunit = NULL;
   DRMS_SumRequest_t *request = NULL;
   DRMS_SumRequest_t *reply = NULL;
   int actualarchive;
   char filename[DRMS_MAXPATHLEN];
   FILE *fp = NULL;
   int nsus;
   int statint;
   int isu;
   int islot;
   DRMS_StorageUnit_t *punits[DRMS_MAX_REQCNT]; /* hold pointers to submitted SUs. */

   actualarchive = 0;

   statint = DRMS_SUCCESS;

   if (ll->nitems > 0)
   {
      /* Use series archive flag, but override with cmd-line flag. */
      if (env->archive != INT_MIN)
      {
         actualarchive = env->archive;
      }
      else
      {
         actualarchive = seriesArch;
      }

      request = malloc(sizeof(DRMS_SumRequest_t));
      XASSERT(request);
      nsus = 0;
      list_llreset(ll);
      while ((node = list_llnext(ll)) != NULL)
      {
         sunit = *((DRMS_StorageUnit_t **)(node->data));

         if (!EmptyDir(sunit->sudir, 0))
         {
            if (nsus == DRMS_MAX_REQCNT)
            {
               /* There was at least one additional SU to process, but there are more SUs
                * to process than this function can handle. */
               statint = DRMS_ERROR_INVALIDDATA;
               break;
            }

            /* Write text file with record numbers to storage unit directory. */
            if (sunit->recnum)
            {
               snprintf(filename, sizeof(filename), "%s/Records.txt", sunit->sudir);
               if ((fp = fopen(filename,"w")) == NULL)
               {
                  fprintf(stderr, 
                          "ERROR in drms_commitunits: Failed to open file '%s'\n",
                          filename);
                  statint = DRMS_ERROR_FILECREATE;
                  break;
               }

               /* If archive is set to -1, then write flag text at the top of the file to 
                * tell SUMS to delete the DRMS records within the storage unit. */
               if (actualarchive == -1)
               {
                  fprintf(fp, "DELETE_SLOTS_RECORDS\n");
               }

               fprintf(fp, "series=%s\n", seriesName);
               if (sunit->nfree < seriesUS)
               {
                  fprintf(fp,"slot\trecord number\n");
                  for (islot = 0; islot < seriesUS; islot++)
                  {
                     if (sunit->state[islot] != DRMS_SLOT_FREE)
                     {
                        fprintf(fp,"%d\t%lld\n", islot, sunit->recnum[islot]);
                     }
                  }
               }
               fclose(fp);
               fp = NULL;
            }

            request->sudir[nsus] = sunit->sudir;
            request->sunum[nsus] = sunit->sunum;

            /* Store a pointer to the storage unit - will need to modify the mode 
             * later in the code. */
            punits[nsus] = sunit;
            nsus++;
         }
      } /* loop over storage units */

      if (nsus == 0 || statint != DRMS_SUCCESS)
      {
         free(request);
         request = NULL;
      }
      else
      {
         request->opcode = DRMS_SUMPUT;
         request->dontwait = 0;
         request->reqcnt = nsus;
         request->dsname = seriesName;
         request->group = seriesTG;
         if (actualarchive == 1) 
         {
            request->mode = ARCH + TOUCH;
         }
         else
         {
            request->mode = TEMP + TOUCH;
         }

         /* The logic to determine the final retention time has already been performed in the calling function. */
         request->tdays = (int)seriesRet;

         request->comment = NULL;

         // must have sum_thread running already
         XASSERT(env->sum_thread);

         /* Submit request to sums server thread. */
         tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

         /* Wait for reply. FIXME: add timeout. */
         tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);

         if (reply->opcode != 0) 
         {
             if (reply->opcode == -2)
             {
                 fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
                 statint = DRMS_ERROR_PENDINGTAPEREAD;
             }
             else
             {    
                 fprintf(stderr, "ERROR in drms_commitunit: SUM PUT failed with "
                         "error code %d.\n",reply->opcode);
                 statint = DRMS_ERROR_SUMPUT;
             }
         }
         else
         {
            /* Now the SUs are owned by SUMS, mark them read-only. */
            for (isu = 0; isu < nsus; isu++)
            {
               sunit = punits[isu];
               sunit->mode = DRMS_READONLY;
            }
         }

         free(reply);
         /* No need to free request - sums thread does that. */
      }
   }

   return statint;
}


#endif

/* Loop though all open storage units and commits the 
   ones open for writing that have slots marked as full
   (i.e. non-temporary) to SUMS. Return the largest retention 
   time of any unit committed. */
#ifndef DRMS_CLIENT
int drms_commit_all_units(DRMS_Env_t *env, int *archive, int *status)
{
    int i;
    HContainer_t *scon;
    HIterator_t hit_outer, hit_inner;
    DRMS_StorageUnit_t *su;
    int statint = 0;
    const char *seriesName = NULL;
    DRMS_Record_t *recTemp = NULL;
    int nsus;
    LinkedList_t *sulist = NULL;
    DRMS_SeriesInfo_t *si = NULL;
    int16_t newSuRetentionRaw = INT16_MIN;
    int16_t newSuRetention = INT16_MIN;
    int16_t maxNewSuRetention = INT16_MIN;
    
    XASSERT(env->session->db_direct==1);
    hiter_new(&hit_outer, &env->storageunit_cache);
    if (archive)
        *archive = 0;
    
    nsus = 0;
    
    while((scon = (HContainer_t *)hiter_extgetnext(&hit_outer, &seriesName)))
    {
        /* If ANY series has its storage units archived, and the caller
         * has not set the archive flag on the cmd-line, set the return
         * archive flag. This will cause the session log to be archived
         * as well. */
        if (archive && *archive == 0 && env->archive == INT_MIN)
        {
            /* Get the archive value from the series info */
            recTemp = drms_template_record(env, seriesName, &statint);
            
            if (statint != DRMS_SUCCESS)
            {
                break;
            }
            
            if (recTemp->seriesinfo->archive)
            {
                *archive = 1;
            }
        }
        
        si = NULL; /* fetch series info on each outer iteration */
        maxNewSuRetention = INT16_MIN; /* reset for every series - take the max retention for all SUs within each series */
        
        /* loops over SUs within a single series */
        hiter_new(&hit_inner, scon);
        while((su = (DRMS_StorageUnit_t *)hiter_getnext(&hit_inner)))
        {
            if (!si)
            {
                si = su->seriesinfo;
                
                if (env->newsuretention != INT16_MIN)
                {
                    /* The user set the DRMS_NEWSURETENTION argument. It overrides all other ways of specifying the retention time. */
                    newSuRetention = env->newsuretention;
                }
                else if ((newSuRetentionRaw = drms_series_getnewsuretention(si)) != INT16_MIN)
                {
                    /* Look at the lower 15 bits of the lower 16 bits of the series retention time. */
                    if (newSuRetentionRaw == 0)
                    {
                        /* If the staging retention time is 0, then use the STDRETENTION time. */
                        newSuRetention = (int16_t)abs(STDRETENTION);
                    }
                    else
                    {
                        newSuRetention = newSuRetentionRaw;
                    }
                }
                else
                {
                    /* The user did not set the DRMS_NEWSURETENTION argument, and we couldn't fetch the value from the database. */
                    newSuRetention = (int16_t)abs(STDRETENTION);
                }
                
                if (newSuRetention > maxNewSuRetention)
                {
                    maxNewSuRetention = newSuRetention;
                }
            }
            
            if ( su->mode == DRMS_READWRITE )
            {
                /* See if this unit has any non-temporary, full slots. */
                for (i=0; i<(su->seriesinfo->unitsize - su->nfree); i++)
                {
                    if (su->state[i] == DRMS_SLOT_FULL)
                    {
                        if (!sulist)
                        {
                            /* Don't deep free SUs - that is done elsewhere. */
                            sulist = list_llcreate(sizeof(DRMS_StorageUnit_t *), NULL);
                        }
                        
                        list_llinserttail(sulist, &su);
                        nsus++;
                        break;
                    }
                }
                
                /* When SUMS batches, it uses keylist.c, which is inefficient. Empirically, 64
                 * is an optimal batch size. */
                if (nsus == MAXSUMREQCNT)
                {
                    statint = CommitUnits(env, sulist, seriesName, si->archive, si->unitsize, si->tapegroup, maxNewSuRetention);
                    list_llfree(&sulist);
                    nsus = 0;
                    
                    if (statint != DRMS_SUCCESS)
                    {
                        break;
                    }
                }
            }
        }
        
        hiter_free(&hit_inner);
        
        /* May be some SUs in sulist not yet committed (because there are fewer than 64). */
        if (nsus > 0)
        {
            statint = CommitUnits(env, sulist, seriesName, si->archive, si->unitsize, si->tapegroup, maxNewSuRetention);
            list_llfree(&sulist);
            nsus = 0;
        }
    } /* loop over series */
    
    hiter_free(&hit_outer);
    
    /* If the caller set the archive flag on the cmd-line, then override what the series' jsds say. */
    if (archive && *archive == 0 && env->archive == 1)
        *archive = 1;
    
    if (status)
    {
        *status = statint;
    }
    
    /* The retention-time value returned here will be used as the retention time for the SU created that 
     * contains the DRMS log (-L flag). 
     */
    return maxNewSuRetention < DRMS_LOG_RETENTION ? DRMS_LOG_RETENTION : maxNewSuRetention;
}
#endif

/* Look up a storage unit and its container in the storage unit cache. */
DRMS_StorageUnit_t *drms_su_lookup(DRMS_Env_t *env, char *series,
				   long long sunum, HContainer_t **scon_out)
{
  HIterator_t hit; 
  HContainer_t *scon;
  DRMS_StorageUnit_t *su;
  char hashkey[DRMS_MAXHASHKEYLEN];

  //  XASSERT(env->session->db_direct==1);
  
  su = NULL;
  scon = NULL;
  if (series==NULL)
  {
    /* Don't know series. Iterate through the containers corresponding to
       all the series we have retrieved so far to see if it is among them. */
    sprintf(hashkey, DRMS_SUNUM_FORMAT, sunum);
    hiter_new(&hit, &env->storageunit_cache);
    while( (scon = (HContainer_t *)hiter_getnext(&hit)) )
    {
      if ( (su = hcon_lookup(scon, hashkey)) )
	break;
    }      
  }
  else
  {
    /* Look up container corresponding to the series. */
    if ( (scon = hcon_lookup(&env->storageunit_cache, series)) )
    {
      sprintf(hashkey, DRMS_SUNUM_FORMAT, sunum);
      su = hcon_lookup(scon, hashkey);
    }
  }
  if (scon_out) 
    *scon_out = scon;
  return su;
}

void drms_su_freeunit(DRMS_StorageUnit_t *su)
{
  if (su->state)
  {
    free(su->state);
    su->state=NULL;
  }
  if (su->recnum)
  {
    free(su->recnum);
    su->recnum=NULL;
  }
}

void drms_freeunit(DRMS_Env_t *env, DRMS_StorageUnit_t *su)
{
  char hashkey[DRMS_MAXHASHKEYLEN];
  HContainer_t *scon;

  if ( (scon = hcon_lookup(&env->storageunit_cache, su->seriesinfo->seriesname)) )
  {
    sprintf(hashkey,DRMS_SUNUM_FORMAT, su->sunum);
    if (su->state)
    {
      free(su->state);
      su->state = NULL;
    }
    if (su->recnum)
    {
      free(su->recnum);
      su->recnum=NULL;
    }
    hcon_remove(scon, hashkey);
  }      
}



/* Mark a storage unit slot as free and remove the associated directory 
   (if any). This is only valid if the storage
   unit is open in mode DRMS_READWRITE. */
int drms_su_freeslot(DRMS_Env_t *env, char *series, long long sunum,
		     int slotnum)
{
  DRMS_StorageUnit_t *su;
  char slotdir[DRMS_MAXPATHLEN];
  int state;
  state = DRMS_SLOT_FREE;
  if ((su = drms_su_markslot(env, series, sunum, slotnum, &state)) == NULL)
    return 1;
  else
  {
    if (state != DRMS_SLOT_FREE)
    {
      /* We just freed a non-empty slot. Remove the associated directory. */
      CHECKSNPRINTF(snprintf(slotdir, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT,
			     su->sudir,slotnum), DRMS_MAXPATHLEN);
     
      return (RemoveDir(slotdir, 64) == 0 ? 0 : 1);
    }
    return 0;
  }
}


/* Change state of a storage unit slot. This is only valid if 
   the storage unit is open in mode DRMS_READWRITE. On entry *state
   must contain the new state. On exit *state will contain the old
   state.
    Possible states are DRMS_SLOT_FREE, DRMS_SLOT_FULL, DRMS_SLOT_TEMP.
*/
DRMS_StorageUnit_t *drms_su_markslot(DRMS_Env_t *env, char *series, 
				     long long sunum, int slotnum, int *state)
{
  int oldstate;
  HContainer_t *scon;
  DRMS_StorageUnit_t *su;

  
  if (state==NULL)
    return NULL;
  if ((su = drms_su_lookup(env, series, sunum, &scon)) == NULL)
    return NULL;
  if ( su->mode!=DRMS_READWRITE )
    return NULL;

  oldstate = su->state[slotnum];
  if (oldstate!=DRMS_SLOT_FREE && *state==DRMS_SLOT_FREE)
    su->nfree++;
  else if (oldstate==DRMS_SLOT_FREE && *state!=DRMS_SLOT_FREE)
    su->nfree--;
  /* Swap old and new state. */
  su->state[slotnum] = (char)(*state);
  *state = oldstate;
  return su;
}

int drms_su_isremotesu(long long sunum)
{
   return !drmssite_sunum_is_local((unsigned long long)sunum);
}

int drms_su_getexportURL(DRMS_Env_t *env, long long sunum, char *url, int size)
{
   int ret = 0;
   DRMSSiteInfo_t *info = NULL;

#ifdef DRMS_CLIENT
   /* drms_server will communicate with this client via the socket connection.
    * drmssite_info_from_sunum() will call db_client_query_txt() three times,
    * and drmssite_server_siteinfo() will respond by calling db_query_txt() three times.
    */
   drms_send_commandcode(env->session->sockfd, DRMS_SITEINFO);
   ret = drmssite_client_info_from_sunum((unsigned long long)sunum, 
                                         env->session->sockfd,
                                         &info);
#else
   ret = drmssite_server_info_from_sunum((unsigned long long)sunum, 
                                         env->session->db_handle,
                                         &info);
#endif
   
   if (!ret && info)
   {
      snprintf(url, size, "%s", info->request_URL);
      drmssite_freeinfo(&info);
   }
   else
   {
      ret = 1;
   }

   return ret;
}

/* Return the name of the export server that can serve 
 * data files that compose the storage unit identified by 
 * sunum. */
int drms_su_getexportserver(DRMS_Env_t *env, 
                            long long sunum, 
                            char *expserver, 
                            int size)
{
   int ret = 0;
   DRMSSiteInfo_t *info = NULL;

#ifdef DRMS_CLIENT
   drms_send_commandcode(env->session->sockfd, DRMS_SITEINFO);
   ret = drmssite_client_info_from_sunum((unsigned long long)sunum,  
                                         env->session->sockfd,
                                         &info);
#else
   ret = drmssite_server_info_from_sunum((unsigned long long)sunum,  
                                         env->session->db_handle,
                                         &info);
#endif

   /* info->SUMS_URL: scp://jsoc_export@j0.stanford.edu/:55000 */

   if (!ret && info)
   {
      snprintf(expserver, size, "%s", info->SUMS_URL);
      drmssite_freeinfo(&info);
   }
   else
   {
      ret = 1;
   }

   return ret;
}

#ifndef DRMS_CLIENT
int drms_su_allocsu(DRMS_Env_t *env, 
                    uint64_t size, 
                    long long sunum, 
                    char **sudir, 
                    int *tapegroup, 
                    int *status)
{
   return drms_su_alloc2(env, size, sunum, sudir, tapegroup, status);
}

/* drms_su_commitsu() does NOT write the Records.txt file, so it is not possible for SUMS to delete DRMS records of SUs committed 
 * with this function. */
int drms_su_commitsu(DRMS_Env_t *env, 
                     const char *seriesname, 
                     long long sunum,
                     const char *sudir)
{
  DRMS_SumRequest_t *request, *reply;
  int actualarchive = 0;
  DRMS_SeriesInfo_t *seriesinfo = NULL;
  DRMS_Record_t *templaterec = NULL;
  int drmsst = DRMS_SUCCESS;
  int docommit = 0;
  int16_t newSuRet = INT16_MIN;

  /* Don't commit a unit if its directory is empty */
  docommit = !EmptyDir(sudir, 0);
  if (docommit)
  {
     templaterec = drms_template_record(env, seriesname, &drmsst);

     if (templaterec)
     {
        seriesinfo = templaterec->seriesinfo;
     }

     if (seriesinfo)
     {
        char *tmp = NULL;

        if (env->archive != INT_MIN)
        {
           actualarchive = env->archive;
        }
        else
        {
           actualarchive = seriesinfo->archive;
        }   

        request = malloc(sizeof(DRMS_SumRequest_t));
        XASSERT(request);
        request->opcode = DRMS_SUMPUT;
        request->dontwait = 0;
        request->reqcnt = 1;
        request->dsname = seriesinfo->seriesname;
        request->group = seriesinfo->tapegroup;

        if (actualarchive == 1) 
        {
           request->mode = ARCH + TOUCH;
        }
        else
        {
           request->mode = TEMP + TOUCH;
        }

        /* If the user doesn't override the retention time on the cmd-line, use the db new-su retention value.
         * Since we are creating the SU for the first time, we should make the sign of the retention time sent
         * to SUMS positive.
         */
        if (env->newsuretention != INT16_MIN)
        {
           /* The user set the DRMS_NEWSURETENTION argument. It overrides all other ways of specifying the retention time. */
           request->tdays = env->newsuretention;
        }
        else if ((newSuRet = drms_series_getnewsuretention(seriesinfo)) != INT16_MIN)
        {
           /* Look at the lower 15 bits of the lower 16 bits of the series retention time. */
           if (newSuRet == 0)
           {
              /* If the staging retention time is 0, then use the STDRETENTION time. */
              request->tdays = abs(STDRETENTION);
           }
           else
           {
              request->tdays = newSuRet;
           }
        }
        else
        {
           /* The user did not set the DRMS_NEWSURETENTION argument, and we couldn't fetch the value from the database. */
           request->tdays = abs(STDRETENTION);
        }

        tmp = strdup(sudir);

        request->sunum[0] = sunum;
        request->sudir[0] = tmp;
        request->comment = NULL;

        // must have sum_thread running already
        XASSERT(env->sum_thread);

        /* Submit request to sums server thread. */
        tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

        /* Wait for reply. FIXME: add timeout. */
        tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);

        if (reply->opcode != 0) 
        {
            if (reply->opcode == -2)
            {
                fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
                drmsst = DRMS_ERROR_PENDINGTAPEREAD;
            }
            else
            {
                fprintf(stderr, "ERROR in drms_commitunit: SUM PUT failed with "
                        "error code %d.\n", reply->opcode);
                drmsst = DRMS_ERROR_SUMPUT;
            }
        }

        if (tmp)
        {
           free(tmp);
        }

        free(reply);
     }
     else
     {
        fprintf(stderr, "Unknown series '%s'.\n", seriesname);
        drmsst = DRMS_ERROR_UNKNOWNSERIES;
     }
  }

  return drmsst;
}

int drms_su_sumexport(DRMS_Env_t *env, SUMEXP_t *sumexpt)
{
   int drmsst = DRMS_SUCCESS;

   DRMS_SumRequest_t *request = NULL;
   DRMS_SumRequest_t *reply = NULL;

   request = malloc(sizeof(DRMS_SumRequest_t));
   XASSERT(request);
   memset(request, 0, sizeof(DRMS_SumRequest_t));

   /* Only 2 fields matter - opcode and comment. The latter is used to hold 
    * a pointer to the SUMEXP_t object. */
   request->opcode = DRMS_SUMEXPORT;
   request->comment = (char *)sumexpt;

   drms_lock_server(env);
   if (!env->sum_thread) 
   {
      if((drmsst = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
                                (void *) env))) 
      {
         fprintf(stderr, "Thread creation failed: %d\n", drmsst);
         drms_unlock_server(env);
         return 1;
      }
   }

   /* Submit request to sums server thread. */
   tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

   drms_unlock_server(env);
   /* Wait for reply. FIXME: add timeout. */
   tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&reply);

   if (reply->opcode)
   {
       if (reply->opcode == -2)
       {
           fprintf(stderr, "Cannot access SUMS in this DRMS session - a tape read is pending.\n");
           drmsst = DRMS_ERROR_PENDINGTAPEREAD;
       }
       else
       {
           fprintf(stderr,"SUM_EXPORT failed with error code %d.\n", reply->opcode);
           drmsst = reply->opcode;
       }
   }
   else
   {
      drmsst = DRMS_SUCCESS;
   }

   if (reply)
   {
      free(reply);
   }

   return drmsst;
}

#if defined(JMD_IS_INSTALLED) && JMD_IS_INSTALLED
//igor ISS VSO HTTP request to JMD START {

int send_POST_request(char * postrequeststr, curl_off_t postsize, struct POSTState *ps) 
{
   CURL *curl;

   curl_global_init(CURL_GLOBAL_ALL);

   curl = curl_easy_init();
   curl_easy_setopt(curl, CURLOPT_URL, JMD_URL);

   /* Now specify we want to POST data */
   curl_easy_setopt(curl, CURLOPT_POST, 1L);

   curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
   curl_easy_setopt(curl, CURLOPT_WRITEDATA, ps);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postrequeststr);
   curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (curl_off_t) postsize);

   curl_easy_perform(curl);

   /* always cleanup */
   curl_easy_cleanup(curl);

   return 0;
}

void populate_with_sunums(DRMS_Env_t *env, HContainer_t *postmap, char * seriesname, int n, long long sulist[]) 
{
   int k = 0;
   for (k = 0; k < n; k++) 
   {
      add_sunum_to_POST(env, postmap, seriesname, sulist[k]);
   }
}

/*
struct PassSunumList
{
  char series[500];
  char **sunumlist;
  long long *sunumarr;
  int  n;
  int  ncount;
  char id[256];
  int   submitted;
  int  sizeadded;
};
*/

//After create_post_msg has created the basic stuff we can start adding sunums to the JMD request
void add_sunum_to_POST(DRMS_Env_t *env, HContainer_t *postmap, char *seriesname, long long sunum) 
{
   int  allocsize = 1000;

   struct PassSunumList *poststruct = NULL;

   //if no postmap struct found for the given seriesname
   // go and create a new one and set it in the hash structure
   if (!hcon_member(postmap,seriesname))
   {
      poststruct = calloc(1,sizeof(struct PassSunumList)); //want to do a memset too
      strcpy(poststruct->series,seriesname);
      poststruct->sunumarr = malloc(sizeof(long long)*allocsize);
      hcon_insert(postmap,seriesname,&poststruct);
   } 
   else
   {
      struct PassSunumList **ptr;
      if ((ptr = hcon_lookup(postmap,seriesname))!= NULL)
      {
         poststruct = *ptr;
      } 
      else 
      {
         //error!!
         fprintf(stderr, "Seriesname [%s], is unknown to hash structure in remote POST handling.\n", seriesname);
         /* ART - cannot call exit(). Send a message to the signal thread to shutdown instead.
            exit(1);
         */
         pthread_kill(env->signal_thread, SIGTERM);
      }

      //note that we increment the size of poststruct in allocsize chunks
      if (((poststruct->n+1) % allocsize) == 0) 
      {
         //realloc allocsize array to sunumarr
         if ((poststruct->sunumarr = realloc((void *) poststruct->sunumarr, sizeof(long long ) * (poststruct->n+1) + sizeof(long long ) * allocsize)) == NULL)
         {
            fprintf(stderr, "Can not allocate new memory for POST sunum array.\n");
            /* xxx NO - cannot call exit()! Send a message to the signal thread to shutdown. 
               exit(1);
            */
            pthread_kill(env->signal_thread, SIGTERM);

         } 
         else 
         {

         }
      }
   }

   poststruct->sunumarr[poststruct->n] = sunum;
   poststruct->n++;
}

//Creates the basic POST msg container
size_t create_post_msg(HContainer_t *postmap, char **ptrr) 
{
   int size_count = 0;   //count of sunums so far in message??
   int i = 0;
   struct PassSunumList **lpptr = NULL;
   struct PassSunumList *list = NULL;

   char *seriesname = NULL;
   long estsize = 0;
   int totalSuCount = 0;
   HIterator_t hit;

   hiter_new(&hit,postmap);
   
   while ((lpptr = (struct PassSunumList **)hiter_getnext(&hit))) 
   {
      list = *lpptr;
      estsize = estsize + 1000 + list->n * (strlen(list->series) + 20);

      if (*ptrr == NULL) 
      {
         //estimated size
         *ptrr = calloc(1,sizeof(char) * estsize);
      } 
      else 
      {
         *ptrr = realloc(*ptrr, sizeof(char) * estsize);
      }

      char *ptr2 = *ptrr;
      ptr2 += size_count;

      if (seriesname == NULL || strcmp(seriesname, list->series)) 
      {
         if (seriesname == NULL) 
         {
            sprintf(ptr2, "type=request&series=%s", list->series);
         } 
         else 
         {
            sprintf(ptr2, "&series=%s", list->series);
         }

         seriesname = list->series;
         size_count += strlen(ptr2);
         ptr2 = *ptrr;
         ptr2 += size_count; //note: ptr2 is pointing to the char after the "="
      }

      // loop through the list of sunums and start building the
      // post query in array form.
      //  i.e. series_name=sunum1&series_name=sunum2 ...
      for(i = 0; i < list->n; i++) 
      {
         //if not the first time. I.e. there are already sunums in the POST array

         char chunk[1000];  //top size of a post sunum slice. e.g. &<series_name>=sunum

         sprintf(chunk, "&%s=%lld", list->series, list->sunumarr[i]);
         int chunklen = strlen(chunk);

         size_count += chunklen;

         if (size_count > estsize) 
         {
            estsize = estsize + (list->n - list->ncount) * (strlen(list->series) + 40);
            *ptrr = realloc(*ptrr, sizeof(char) * estsize);
            ptr2 = *ptrr;
            ptr2 += (size_count - chunklen);
         }

         strcpy(ptr2,chunk);
         //reset ptr2
         ptr2 = *ptrr;
         ptr2 += size_count; //increment pointer to end of POST string
         list->ncount++;   //increment sunum count
         totalSuCount++;   //increment total sunum count. Only for test purposes
      }
   }

   return size_count;
}

//Callback function in charge of populating the POSTState structure
// this callback is called from send_POST_request
size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp) 
{
   /*
     sessionID=4022d6a7-d7e3-4b73-898a-3526efc321a4
     Summary:
     SUCCESFUL submissions:0
   */

   struct POSTState *ps = (struct POSTState *)userp;
   int submissions = -2;
   char id[256] = "NO SessionID";

   //  char *buffer = "sessionID=4022d6a7-d7e3-4b73-898a-3526efc321a4\nSummary:\nSUCCESFUL submissions:0";
   sscanf(buffer, "sessionID=%s\nSummary:\nSUCCESFUL submissions:%d", id, &submissions);
   strcpy(ps->session_id, id);
   ps->no_submitted = submissions;
   return strlen(buffer);
}

//parse routine of JMD output
size_t parse_session_state(void *buffer, size_t size, size_t nmemb, void *userp) 
{
   int *inprogress = (int *)userp;
   int total = -2;
   char rest[1000];

   //char *buffer = "QUERY REQUEST:\nTotal Count=30\nIn Progress=0\nDONE=30\nPNDG=24\n";
   sscanf(buffer, "QUERY REQUEST:\nTotal Count=%d\nIn Progress=%d\n%s", &total, inprogress, rest);
   return strlen(buffer);
}

//for a given request (session) return its status
// return status is the number of sunums waiting to be downloaded
// see parse_session_state
int session_status (char *session) 
{
   CURL *curl;

   curl_global_init(CURL_GLOBAL_ALL);
   curl = curl_easy_init();

   char url[2000];

   //sprintf(url,"http://vso2.tuc.noao.edu:8080/JMD/JMD?type=query&sessionid=%s",session);
   sprintf(url, "%s?type=query&sessionid=%s", JMD_URL, session);
   curl_easy_setopt(curl, CURLOPT_URL, url);

   /* Now specify we want to POST data */
   int inprogress = -1;
   curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, parse_session_state);
   curl_easy_setopt(curl, CURLOPT_WRITEDATA, &inprogress);
   curl_easy_perform(curl);

   /* always cleanup */
   curl_easy_cleanup(curl);

   return inprogress;
}

void free_post_request (HContainer_t *postmap)
{
   HIterator_t *hitmap = hiter_create(postmap);
   if (hitmap) 
   {
      struct PassSunumList **ppoststruct = NULL;
      while ((ppoststruct = hiter_getnext(hitmap)) != NULL)
      {
         struct PassSunumList *poststruct = *ppoststruct;
         if (poststruct->sunumarr != NULL) 
         {
            free(poststruct->sunumarr);
            poststruct->sunumarr=NULL;
         }

         free(poststruct);
         poststruct = NULL;
      }
   }

   hiter_destroy(&hitmap);
   hcon_destroy(&postmap);
}
//igor ISS VSO HTTP request to JMD END }
#endif /* JMD_IS_INSTALLED */

#endif // DRMS_CLIENT
