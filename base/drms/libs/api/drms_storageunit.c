//#define DEBUG

#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "util.h"

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

/* Allocate a storage unit of the indicated size from SUMS and return
   its sunum and directory. */
#ifndef DRMS_CLIENT
long long drms_su_alloc(DRMS_Env_t *env, uint64_t size, char **sudir, 
			int *status)
{
  int stat;
  DRMS_SumRequest_t *request, *reply;
  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));
  long long sunum;

  //  printf("************** HERE I AM *******************\n");

  /* No free slot was found, allocate a new storage unit from SUMS. */
  request->opcode = DRMS_SUMALLOC;
  request->dontwait = 0;
  request->reqcnt = 1;
  request->bytes = size;
  if (request->bytes <=0 )
  {
    fprintf(stderr,"Invalid storage unit size %lf\n",request->bytes);
    return 0;
  }

  if (!env->sum_thread) {
    if((stat = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
			      (void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", stat);          
      return 1;
    }
  }
  /* Submit request to sums server thread. */
  tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);
  /* Wait for reply. FIXME: add timeout. */
  tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&reply);
  if (reply->opcode)
  {
    fprintf(stderr,"SUM ALLOC failed with error code %d.\n",reply->opcode);
    stat = reply->opcode;
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
                   int *status)
{
   int stat;
   DRMS_SumRequest_t *request = NULL;
   DRMS_SumRequest_t *reply = NULL;
   XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));

   request->opcode = DRMS_SUMALLOC2;
   request->dontwait = 0;
   request->reqcnt = 1;
   request->bytes = size;
   request->sunum[0] = sunum;
   if (request->bytes <=0 )
   {
      fprintf(stderr,"Invalid storage unit size %lf\n",request->bytes);
      return 0;
   }

   if (!env->sum_thread) 
   {
      if((stat = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
                                (void *) env))) 
      {
         fprintf(stderr,"Thread creation failed: %d\n", stat);          
         return 1;
      }
   }

   /* Submit request to sums server thread. */
   tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);

   /* Wait for reply. FIXME: add timeout. */
   tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&reply);

   if (reply->opcode)
   {
      fprintf(stderr,"SUM ALLOC2 failed with error code %d.\n",reply->opcode);
      stat = reply->opcode;
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
int drms_su_newslots(DRMS_Env_t *env, int n, char *series, 
		     long long *recnum, DRMS_RecLifetime_t lifetime,
		     int *slotnum, DRMS_StorageUnit_t **su,
                     int createslotdirs)
{
  int i, status, slot;
  HContainer_t *scon; 
  HIterator_t hit; 
  long long sunum;
  char slotdir[DRMS_MAXPATHLEN+40], hashkey[DRMS_MAXHASHKEYLEN], *sudir;
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

      sunum = drms_su_alloc(env, drms_su_size(env, series) + 1000000, 
			    &sudir, &status);
      if (status)
      {
	if (sudir)
	  free(sudir);
	goto bail;
      }	  
      sprintf(hashkey,DRMS_SUNUM_FORMAT, sunum);
      /* Insert new entry in hash table. */
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
      XASSERT(su[i]->state = malloc(su[i]->nfree));
      memset(su[i]->state, DRMS_SLOT_FREE, su[i]->nfree);
      XASSERT(su[i]->recnum = malloc(su[i]->nfree*sizeof(long long)));
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
  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));
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

  if (env->retention==INT_MIN) 
  {
     request->tdays = STDRETENTION;
     if (request->tdays > 0)
     {
        /* Since STDRETENTION can be customized, don't allow the definition of a positive number */
        request->tdays *= -1;
     }
  }
  else
  {
     request->tdays = env->retention;
     if (request->tdays > 0 && 
         (!su->seriesinfo || !drms_series_cancreaterecord(env, su->seriesinfo->seriesname)))
     {
        request->tdays *= -1;
     }
  }

  if (!env->sum_thread) {
    int status;
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
				(void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);
      return 1;
    }
  }

  tryagain = 1;
  natts = 1;
  while (tryagain && natts < 3)
  {
     tryagain = 0;

     /* Submit request to sums server thread. */
     tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);
     /* Wait for reply. FIXME: add timeout. */
     tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);

     if (reply->opcode != 0)
     {
        fprintf(stderr, "SUM GET failed with error code %d.\n",reply->opcode);
        free(reply);
        return 1;
     }
     else
     {
        su->sudir[0] = '\0';

        if (strlen(reply->sudir[0]) > 0)
        {
           snprintf(su->sudir, sizeof(su->sudir), "%s", reply->sudir[0]);
           free(reply->sudir[0]);
        }
        else if (retrieve && natts < 2 && drms_su_isremotesu(request->sunum[0]))
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

              if (!drms_su_getexportURL(request->sunum[0], url, sizeof(url)))
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
                          (long long)request->sunum[0]);
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
                 /* Discard whatever else is being written to the pipe */
                 while (read(infd[0], rbuf, sizeof(rbuf)) > 0);

                 sscanf(rbuf, "%d", &tryagain);
                 if (!tryagain)
                 {
                    sustatus = DRMS_REMOTESUMS_TRYLATER;
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
  int workingn;
  int tryagain;
  int natts;

  if (!env->sum_thread) {
    int status;
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
				(void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);
      return 1;
    }
  }

  /* Since this affects more than one storage unit, in order to specify a positive 
   * retention, must be owner of ALL series to which these storage units belong */
  int isowner = 1;
  int isu;
  int iSUMSsunum;
  DRMS_StorageUnit_t *onesu = NULL;

  for (isu = 0; isu < n; isu++)
  {
     onesu = su[isu];
     if (!onesu->seriesinfo || !drms_series_cancreaterecord(env, onesu->seriesinfo->seriesname))
     {
        isowner = 0;
     }

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
  while (tryagain && natts < 3)
  {
     tryagain = 0;

     while (start < workingn)
     {
        /* create SUMS request (apparently, SUMS frees this request) */
        XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));

        request->opcode = DRMS_SUMGET;
        request->reqcnt = end - start;

        for (isu = start, iSUMSsunum = 0; isu < end; isu++, iSUMSsunum++) {
           request->sunum[iSUMSsunum] = workingsus[isu]->sunum;
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
        if (env->retention==INT_MIN) 
        {  
           request->tdays = STDRETENTION;
           if (request->tdays > 0)
           {
              /* Since STDRETENTION can be customized, don't allow the definition of a positive number */
              request->tdays *= -1;
           }
        }
        else
        {
           request->tdays = env->retention;
        
           if (request->tdays > 0 && !isowner)
           {
              request->tdays *= -1;
           }
        }

        /* Submit request to sums server thread. */
        tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) request);
  
        /* Wait for reply. FIXME: add timeout. */
        if (!dontwait) 
        {
           /* If and only if user wants to wait for the reply, then return back 
            * to user all SUDIRs found. */
           tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);

           if (reply->opcode != 0)
           {
              fprintf(stderr, "SUM GET failed with error code %d.\n",reply->opcode);
              free(reply);
              return 1;
           }
           else
           {
              //retrysunums = list_llcreate(sizeof(uint64_t));
              retrysus = list_llcreate(sizeof(DRMS_StorageUnit_t *));

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
                    //list_llinserttail(retrysunums, &(workingsus[isu]->sunum));
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
     //if (natts < 2 && list_llgetnitems(retrysunums) > 0)
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
           //HContainer_t *sulists = hcon_create(sizeof(SUList_t *), 128, NULL, NULL, NULL, NULL, 0);
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
              if (!drms_su_getexportURL(rsu->sunum, url, sizeof(url)))
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

                    snprintf(listbuf, sizeof(listbuf), "%s=%s\\{%lld", url, sname, rsu->sunum);
                    newlist->str = base_strcatalloc(newlist->str, listbuf, &(newlist->size));
                 }
              }
           }

           /* Call external program; doesn't return - must be in path */
           sunumlist = (char *)malloc(kSUNUMLISTSIZE);

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
           execl("/bin/tcsh", "tcsh", "-c", cmd, (char *)0);

           /* Execution never gets here. */
        }
        else
        {
           /* parent - reads from pipe */
           close(infd[1]); /* close fd to write end of pipe */
           dup2(infd[0], 0); /* redirect stdin to read end o fpipe */
           close(infd[0]); /* close fd to read end of pipe; okay since stdin now points to 
                            * read end of pipe */

           /* Read results from external program - either 0 (don't try again) or 1 (try again) */
           if (read(infd[0], rbuf, sizeof(rbuf)) > 0)
           {
              /* Discard whatever else is being written to the pipe */
              while (read(infd[0], rbuf, sizeof(rbuf)) > 0);

              sscanf(rbuf, "%d", &tryagain);
              if (!tryagain)
              {
                 sustatus = DRMS_REMOTESUMS_TRYLATER;
              }
           }
        }
        
        start = 0;
        /* index of SU one past the last one to be processed */
        //end = SUMIN(MAXSUMREQCNT, list_llgetnitems(retrysunums)); 
        end = SUMIN(MAXSUMREQCNT, list_llgetnitems(retrysus)); 

        //rsumssus = (DRMS_StorageUnit_t **)malloc(sizeof(DRMS_StorageUnit_t *) * list_llgetnitems(retrysunums));
        rsumssus = (DRMS_StorageUnit_t **)malloc(sizeof(DRMS_StorageUnit_t *) * list_llgetnitems(retrysus));
        ListNode_t *node = NULL;

        isu = 0;
        //while (node = list_llgethead(retrysunums))
        while (node = list_llgethead(retrysus))
        {
           onesu = (DRMS_StorageUnit_t *)malloc(sizeof(DRMS_StorageUnit_t));
           onesu->sunum = *((uint64_t *)(node->data));
           *(onesu->sudir) = '\0';
           rsumssus[isu] = onesu;
           isu++;
           //list_llremove(retrysunums, node);
           list_llremove(retrysus, node);
           list_llfreenode(&node);
        }

        workingsus = rsumssus;
        //workingn = list_llgetnitems(retrysunums);
        workingn = list_llgetnitems(retrysus);
     }
  } /* while - retry */

  //if (retrieve && list_llgetnitems(retrysunums) > 0)
  if (retrieve && retrysus && list_llgetnitems(retrysus) > 0)
  {
     /* Merge results of remotesums request with original su array */
     for (isu = 0, iSUMSsunum = 0; isu < n; isu++, iSUMSsunum++)
     {
        if (strcmp(su[isu]->sudir, "rs") == 0)
        {
           /* This SUNUM was sent to remotesums. */
           snprintf(su[isu]->sudir, DRMS_MAXPATHLEN, "%s", rsumssus[iSUMSsunum]->sudir);
        }
     }
  }

  if (rsumssus)
  {
     free(rsumssus);
  }

  //list_llfree(&retrysunums);
  if (retrysus)
  {
     list_llfree(&retrysus);
  }

  return sustatus;
}
#endif

/* Tell SUMS to save this storage unit. */
#ifndef DRMS_CLIENT
int drms_commitunit(DRMS_Env_t *env, DRMS_StorageUnit_t *su)
{
  int i;
  DRMS_SumRequest_t *request, *reply;
  FILE *fp;
  char filename[DRMS_MAXPATHLEN];
  int actualarchive = 0;

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
	if (su->state!=DRMS_SLOT_FREE)
	  fprintf(fp,"%d\t%lld\n", i, su->recnum[i]);
    }
    fclose(fp);
  }

  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));
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
    fprintf(stderr, "ERROR in drms_commitunit: SUM PUT failed with "
	    "error code %d.\n",reply->opcode);
    free(reply);
    return 1;
  }
  free(reply);
  /* Now the storage unit is owned by SUMS, mark it read-only. */
  su->mode = DRMS_READONLY;
  return 0;
}
#endif

/* Loop though all open storage units and commits the 
   ones open for writing that have slots marked as full
   (i.e. non-temporary) to SUMS. Return the largest retention 
   time of any unit committed. */
#ifndef DRMS_CLIENT
int drms_commit_all_units(DRMS_Env_t *env, int *archive)
{
  int i, docommit;
  HContainer_t *scon; 
  HIterator_t hit_outer, hit_inner; 
  DRMS_StorageUnit_t *su;
  int max_retention=0;

  XASSERT(env->session->db_direct==1);
  hiter_new(&hit_outer, &env->storageunit_cache);  
  if (archive)
    *archive = 0;
  while( (scon = (HContainer_t *)hiter_getnext(&hit_outer)) )
  {
    hiter_new(&hit_inner, scon);
    while( (su = (DRMS_StorageUnit_t *)hiter_getnext(&hit_inner)) )   
    {
      if ( su->mode == DRMS_READWRITE )	
      {
	/* See if this unit has any non-temporary, full slots. */
	docommit = 0;
	for (i=0; i<(su->seriesinfo->unitsize - su->nfree); i++)
	{
	  if (su->state[i] == DRMS_SLOT_FULL)
	  {
	    docommit = 1;
	    break;
	  }
	}
	if (docommit)
	{
	  drms_commitunit(env, su);
	  if (su->seriesinfo->retention > max_retention)
	    max_retention = su->seriesinfo->retention;
          if (archive && env->archive == INT_MIN)
          {
             if (su->seriesinfo->archive)
               *archive = 1;
          }
	}
      }
    }
  }

  if (archive && *archive == 0 && env->archive == 1) 
    *archive = 1;

  if (env->retention==INT_MIN)
    return DRMS_LOG_RETENTION;
    //    return max_retention;    
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
      unlink(slotdir); /* Ignore return code. */
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
  su->state[slotnum] = *state;
  *state = oldstate;
  return su;
}

int drms_su_isremotesu(long long sunum)
{
   /* XXX - for now, assume that if you're asking this question, then it is remote. */
   return 1;
}

int drms_su_getexportURL(long long sunum, char *url, int size)
{
   /* XXX - for now, assume that the owner of the "remote" sunum is Stanford */
   snprintf(url, size, "http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_fetch");
   return 0;
}

const char *drms_su_getexportserver()
{
   /* XXX - for now, assume j0 */
   return "j0.stanford.edu";
}
