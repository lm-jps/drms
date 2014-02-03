//#define DEBUG

#include <dirent.h>
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "util.h"
#include "list.h"

#ifdef DRMS_CLIENT
#define DEFS_CLIENT
#endif
#include "drmssite_info.h"

#ifdef DEFS_CLIENT
#undef DEFS_CLIENT
#endif

#define SUMIN(a,b)  ((a) > (b) ? (b) : (a))
#ifdef DRMS_DEFAULT_RETENTION
  #define STDRETENTION DRMS_DEFAULT_RETENTION
#else
  #define STDRETENTION (-3)
#endif

#define kEXTREMOTESUMS "remotesums_master.pl"
#define kSUNUMLISTSIZE 512

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
int drms_su_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retrieve)
{  
  int sustatus = DRMS_SUCCESS;
  DRMS_SumRequest_t *request, *reply;
  int tryagain;
  int natts;

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

     /* If the user doesn't override on the cmd-line, use the standard retention (some small negative value).
      * If the user specifies a negative value on the cmd-line, use that.  If the user specifies
      * a positive value on the cmd-line and the user owns this series, use that value.  But if the
      * user doesn't own this series multiply the positive value by -1.
      */
      
      /* Override STDRETENTION with the jsd retention time if the user has set the DRMS_JSDRETENTION main flag. */
      if (env->jsdsgetret && su->seriesinfo)
      {
          request->tdays = su->seriesinfo->retention;
      }
      else
      {
          request->tdays = STDRETENTION;
      }
      
     if (request->tdays > 0)
     {
        /* Since STDRETENTION can be customized, don't allow the definition of a positive number */
        request->tdays *= -1;
     }

     if (env->retention != INT_MIN) 
     {
         /* if su->seriesinfo->retention_perm == 1, then the user has permission to reduce retention. */
        if (!su->seriesinfo || !su->seriesinfo->retention_perm)
        {
           if (env->retention > 0)
           {
              request->tdays = -1 * env->retention;
           }
           else if (env->retention < 0)
           {
              request->tdays = env->retention;
           }
        }
        else
        {
           request->tdays = env->retention;
        }
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
        else if (retrieve && natts < 2 && drms_su_isremotesu(su->sunum))
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

           int infd[2];  /* child writes to this pipe */
           char rbuf[128]; /* expecting ascii for 0 or 1 */

           pipe(infd);

           if(!fork())
           {
              char url[DRMS_MAXPATHLEN];
              char *sunumlist = NULL;
              size_t listsize;
              char listbuf[128];

              /* child - writes to pipe */
              close(infd[0]); /* close fd to read end of pipe */
              dup2(infd[1], 1);  /* redirect stdout to write end of pipe */
              close(infd[1]); /* close fd to write end of pipe; okay since stdout now points to 
                               * write end of pipe */

              if (!drms_su_getexportURL(env, su->sunum, url, sizeof(url)))
              {
                 /* Call external program; doesn't return - must be in path */
                 char cmd[PATH_MAX];
                 sunumlist = (char *)malloc(kSUNUMLISTSIZE);
                 memset(sunumlist, 0, kSUNUMLISTSIZE);

                 listsize = kSUNUMLISTSIZE;
                 snprintf(listbuf, 
                          sizeof(listbuf), 
                          "%s\\{%lld\\}",
                          su->seriesinfo ? su->seriesinfo->seriesname : "unknown",
                          (long long)su->sunum);
                 sunumlist = base_strcatalloc(sunumlist, url, &listsize);
                 sunumlist = base_strcatalloc(sunumlist, "=", &listsize);
                 sunumlist = base_strcatalloc(sunumlist, listbuf, &listsize);
                 snprintf(cmd, sizeof(cmd), "%s %s", kEXTREMOTESUMS, sunumlist);
                                
                 /* Careful with the $PATH env variable! The following call creates a new 
                  * shell instance. If the path to remotesums_master.pl is not properly 
                  * added to the $PATH variable via .cshrc or some other login script, 
                  * then remotesums_master.pl will never be found. The parent will then 
                  * unblock and fail to read an data from infd[0]. */
                 execl("/bin/tcsh", "tcsh", "-c", cmd, (char *)0);
              }
           }
           else
           {
              /* parent - reads from pipe */
              close(infd[1]); /* close fd to write end of pipe */

              /* Read results from external program - either 0 (don't try again) or 1 (try again) */
              if (read(infd[0], rbuf, sizeof(rbuf)) > 0)
              {
                 sscanf(rbuf, "%d", &tryagain);

                 if (tryagain == 0)
                 {
                    sustatus = DRMS_REMOTESUMS_TRYLATER;
                 }
                 else if (tryagain == -1)
                 {
                    /* error running remotesums_master.pl */
                    sustatus = DRMS_ERROR_REMOTESUMSMASTER;
                    tryagain = 0;
                    fprintf(stderr, "Master remote SUMS script did not run properly.\n");
                 }
              }
              else
              {
                 fprintf(stderr, "Master remote SUMS script did not run properly.\n");
              }

              close(infd[0]);
           }
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

  /* Since this affects more than one storage unit, in order to specify a positive 
   * retention, must be owner of ALL series to which these storage units belong */
  int isowner = 1;
  int isu;
  int iSUMSsunum;
  DRMS_StorageUnit_t *onesu = NULL;

    /* SUMS does not support dontwait == 1, so force dontwait to be 0 (deprecate the dontwait parameter). */
    dontwait = 0;
    
  for (isu = 0; isu < n; isu++)
  {
     onesu = su[isu];
     if (!onesu->seriesinfo || !onesu->seriesinfo->retention_perm)
     {
        isowner = 0;
     }

     /* Set all returned sudirs to empty strings - used as a flag to know what has been processed. */
     *(onesu->sudir) = '\0';
  }

  /* There is a maximum no. of SUs that can be requested from SUMS, MAXSUMREQCNT. So, loop. */
  int start = 0;
  int end = SUMIN(MAXSUMREQCNT, n); /* index of SU one past the last one to be processed */
  int maxret;

  workingsus = su;
  workingn = n;

  tryagain = 1;
  natts = 1;
  while (tryagain && natts < 3)
  {
     tryagain = 0;

     /* Ask SUMS for ALL SUS in workingsus (in chunks of MAXSUMREQCNT) */
     while (start < workingn)
     {
        maxret = -1;
         
        /* create SUMS request (apparently, SUMS frees this request) */
        request = malloc(sizeof(DRMS_SumRequest_t));
        XASSERT(request);

        request->opcode = DRMS_SUMGET;
        request->reqcnt = end - start;
         
         if (env->jsdsgetret)
         {
             for (isu = start, iSUMSsunum = 0; isu < end; isu++, iSUMSsunum++) 
             {
                 request->sunum[iSUMSsunum] = workingsus[isu]->sunum;
                 
                 /* Find largest jsd retention time. We will use this when setting the retention time 
                  * of SUs retrieved from tape. */
                 if (workingsus[isu]->seriesinfo && workingsus[isu]->seriesinfo->retention > maxret)
                 {
                     maxret = workingsus[isu]->seriesinfo->retention;
                 }
             }
         }
         else
         {
             for (isu = start, iSUMSsunum = 0; isu < end; isu++, iSUMSsunum++) 
             {
                 request->sunum[iSUMSsunum] = workingsus[isu]->sunum;
             }
         }
        
        request->mode = NORETRIEVE + TOUCH;
        if (retrieve) 
          request->mode = RETRIEVE + TOUCH;

        request->dontwait = dontwait;

        /* If the user doesn't override on the cmd-line, use the standard retention 
         * (some small negative value).
         * If the user specifies a negative value on the cmd-line, use that.  If the user specifies
         * a positive value on the cmd-line and the user owns this series, use that value.  But if the
         * user doesn't own this series multiply the positive value by -1.
         */
         
         /* Override STDRETENTION with the jsd retention time if the user has set the DRMS_JSDRETENTION main flag. */
         if (env->jsdsgetret && maxret > -1)
         {
             request->tdays = maxret;
         }
         else
         {
             request->tdays = STDRETENTION;
         }
         
        if (request->tdays > 0)
        {
           /* Since STDRETENTION can be customized, don't allow the definition of a positive number */
           request->tdays *= -1;
        }
     
        if (env->retention != INT_MIN) 
        {
           if (!isowner)
           {
              if (env->retention > 0)
              {
                 request->tdays = -1 * env->retention;
              }
              else if (env->retention < 0)
              {
                 request->tdays = env->retention;
              }
           }
           else
           {
              request->tdays = env->retention;
           }
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
                 else if (retrieve && natts < 2 && drms_su_isremotesu(workingsus[isu]->sunum))
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
        int infd[2];  /* child writes to this pipe */
        char rbuf[128]; /* expecting ascii for 0 or 1 */

        pipe(infd);

        if(!fork())
        {
           /* child - writes to pipe */
           close(infd[0]); /* close fd to read end of pipe */
           dup2(infd[1], 1);  /* redirect stdout to write end of pipe */
           close(infd[1]); /* close fd to write end of pipe; okay since stdout now points to 
                            * write end of pipe */

           /* iterate through each sunum */
           ListNode_t *node = NULL;
           DRMS_StorageUnit_t *rsu = NULL;
           HContainer_t *sulists = hcon_create(sizeof(HContainer_t), 128, NULL, NULL, NULL, NULL, 0);
           char listbuf[128];
           char url[DRMS_MAXPATHLEN];
           HContainer_t *acont = NULL;
           SUList_t *alist = NULL;
           SUList_t *newlist = NULL;
           char cmd[PATH_MAX];
           char *sunumlist = NULL;
           HIterator_t *hiturl = NULL;
           HIterator_t *hitsers = NULL;
           
           int firsturl;
           int firstsers;
           size_t listsize;
           list_llreset(retrysus);

           while ((node = list_llnext(retrysus)) != NULL)
           {
              rsu = *((DRMS_StorageUnit_t **)(node->data));
              char *sname = rsu->seriesinfo ? rsu->seriesinfo->seriesname : "unknown";
              if (!drms_su_getexportURL(env, rsu->sunum, url, sizeof(url)))
              {
                 /* Each exportURL contains one or more series, and each series contains
                  * one or more SUNUMs. So sulists is a container of (expURLs, series container), and
                  * series container is a container of (series, sulist) */

                 /* see if a list for rsunum already exists. */
                 if ((acont = hcon_lookup(sulists, url)) != NULL)
                 {
                    /* Got the (url, series cont) container; look for series name. */
                    if ((alist = hcon_lookup(acont, sname)) != NULL)
                    {
                       /* alist is the sname-specific sulist; append this sunum */
                       snprintf(listbuf, sizeof(listbuf), ",%lld", rsu->sunum);
                       alist->str = base_strcatalloc(alist->str, listbuf, &(alist->size)); 
                    }
                    else
                    {
                       newlist = hcon_allocslot(acont, sname);
                       newlist->str = malloc(kSUNUMLISTSIZE);
                       newlist->size = kSUNUMLISTSIZE;
                       memset(newlist->str, 0, kSUNUMLISTSIZE);

                       snprintf(listbuf, sizeof(listbuf), "%s=%s\\{%lld", url, sname, rsu->sunum);
                       newlist->str = base_strcatalloc(newlist->str, listbuf, &(newlist->size));
                    }
                 }
                 else
                 {
                    /* Create a new (series, sulist) container */
                    HContainer_t *newcont = hcon_allocslot(sulists, url);
                    hcon_init(newcont, sizeof(SUList_t), DRMS_MAXSERIESNAMELEN, NULL, NULL);

                    /* create a new list and add rsunum to it */
                    newlist = hcon_allocslot(newcont, sname);

                    newlist->str = malloc(kSUNUMLISTSIZE);
                    newlist->size = kSUNUMLISTSIZE;
                    memset(newlist->str, 0, kSUNUMLISTSIZE);

                    snprintf(listbuf, sizeof(listbuf), "%s=%s\\{%lld", url, sname, rsu->sunum);
                    newlist->str = base_strcatalloc(newlist->str, listbuf, &(newlist->size));
                 }
              }
           }

           /* Call external program; doesn't return - must be in path */
           sunumlist = (char *)malloc(kSUNUMLISTSIZE);
           memset(sunumlist, 0, kSUNUMLISTSIZE);

           /* Make the lists of SUNUMs - one for each export URL */
           listsize = kSUNUMLISTSIZE;
           firsturl = 1;
           firstsers = 1;
           hiturl = hiter_create(sulists);

           if (hiturl)
           {
              while ((acont = hiter_getnext(hiturl)) != NULL)
              {
                 if (!firsturl)
                 {
                    sunumlist = base_strcatalloc(sunumlist, "#", &listsize);
                 }
                 else
                 {
                    firsturl = 0;
                 }

                 hitsers = hiter_create(acont);
                 if (hitsers)
                 {
                    while ((alist = hiter_getnext(hitsers)) != NULL)
                    {
                       /* Must append the final '}' to all lists */
                       alist->str = base_strcatalloc(alist->str, "\\}", &(alist->size));

                       /* create a string that contains all sulists */
                       if (!firstsers)
                       {
                          sunumlist = base_strcatalloc(sunumlist, "&", &listsize);
                       }
                       else
                       {
                          firstsers = 0;
                       }

                       sunumlist = base_strcatalloc(sunumlist, alist->str, &listsize);
                    }

                    hiter_destroy(&hitsers);
                 }
              }

              hiter_destroy(&hiturl);
           }

           snprintf(cmd, sizeof(cmd), "%s %s", kEXTREMOTESUMS, sunumlist);

           // fprintf(stderr, "cmd is %s\n", cmd);

           execl("/bin/tcsh", "tcsh", "-c", cmd, (char *)0);

           /* Execution never gets here. */
        }
        else
        {
           /* parent - reads from pipe */
           close(infd[1]); /* close fd to write end of pipe */

           /* Read results from external program - either 0 (don't try again) or 1 (try again) */
           if (read(infd[0], rbuf, sizeof(rbuf)) > 0)
           {
              sscanf(rbuf, "%d", &tryagain);

              if (tryagain == 0)
              {
                 sustatus = DRMS_REMOTESUMS_TRYLATER;
              }
              else if (tryagain == -1)
              {
                 /* error running remotesums_master.pl */
                 sustatus = DRMS_ERROR_REMOTESUMSMASTER;
                 tryagain = 0;
                 fprintf(stderr, "Master remote SUMS script did not run properly.\n");
              }
              else
              {
                 /* remotesums_master.pl worked - need to call SUM_get() one more time
                  * with SUNUMS that failed originally */
                 nretrySUNUMS = list_llgetnitems(retrysus);

                 start = 0;
                 /* index of SU one past the last one to be processed */
                 end = SUMIN(MAXSUMREQCNT, nretrySUNUMS); 
                 rsumssus = (DRMS_StorageUnit_t **)malloc(sizeof(DRMS_StorageUnit_t *) * nretrySUNUMS);
                 ListNode_t *node = NULL;

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
           }
        } /* end parent*/
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

     /* If the user doesn't override on the cmd-line, start with the jsd retention.  Otherwise, 
      * start with the cmd-line value.  It doesn't matter if the value is positive or negative 
      * since only the series owner can create a record in the first place.
      */
     if (env->retention==INT_MIN) 
     {  
        request->tdays = su->seriesinfo->retention;
     }
     else
     {
        request->tdays = env->retention; 
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
                       int seriesRet)
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

         /* If the user doesn't override on the cmd-line, start with the jsd retention.  Otherwise, 
          * start with the cmd-line value.  It doesn't matter if the value is positive or negative 
          * since only the series owner can create a record in the first place.
          */
         if (env->retention == INT_MIN) 
         {  
            request->tdays = seriesRet;
         }
         else
         {
            request->tdays = env->retention; 
         }

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

     /* loops over SUs within a single series */
    hiter_new(&hit_inner, scon);
    while((su = (DRMS_StorageUnit_t *)hiter_getnext(&hit_inner)))
    {
       if (!si)
       {
          si = su->seriesinfo;
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
           statint = CommitUnits(env, sulist, seriesName, si->archive, si->unitsize, si->tapegroup, si->retention);
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

    /* May be some SUs in sulist not yet committed (because there are fewer than 64. */
    if (nsus > 0)
    {
       statint = CommitUnits(env, sulist, seriesName, si->archive, si->unitsize, si->tapegroup, si->retention);
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

  if (env->retention==INT_MIN)
    return DRMS_LOG_RETENTION;
  else
    return env->retention;
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

        /* If the user doesn't override on the cmd-line, start with the jsd retention.  Otherwise, 
         * start with the cmd-line value.  It doesn't matter if the value is positive or negative 
         * since only the series owner can create a record in the first place.
         */
        if (env->retention == INT_MIN) 
        {  
           request->tdays = seriesinfo->retention;
        }
        else
        {
           request->tdays = env->retention; 
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
#endif // DRMS_CLIENT
