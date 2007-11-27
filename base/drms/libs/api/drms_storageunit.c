/*
 *  drms_storageunit.c						2007.11.26
 *
 *  functions defined:
 *	drms_su_alloc
 *	drms_su_newslots
 *	drms_su_getsudir
 *	drms_commitunit
 *	drms_commit_all_units
 *	drms_su_lookup
 *	drms_su_freeunit
 *	drms_freeunit
 *	drms_su_freeslot
 *	drms_su_markslot
 */
//#define DEBUG

#include "drms.h"  
#include "xmem.h"

/*
 *  Allocate a storage unit of the indicated size from SUMS and return
 *    its sunum and directory
 */
#ifndef DRMS_CLIENT
long long drms_su_alloc (DRMS_Env_t *env, uint64_t size, char **sudir,
    int *status) {
  int stat;
  DRMS_SumRequest_t request, *reply;
  long long sunum;

  //  printf("************** HERE I AM *******************\n");

  /* No free slot was found, allocate a new storage unit from SUMS. */
  request.opcode = DRMS_SUMALLOC;
  request.reqcnt = 1;
  request.bytes = size;
  if (request.bytes <=0 )
  {
    fprintf(stderr,"Invalid storage unit size %lf\n",request.bytes);
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
  tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) &request);
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
/*
 *  Get a new empty slot for a record from "series". If no storage units 
   from this series with empty slots are currently open, allocate a new 
   one from SUMS. When an appropriate storate unit is found, a new
   slot number (slot) is assigned corresponding to the 
   subdirectory printf("slot%04d",slot) which gets created.
   
   Returns storage unit struct (which contains sunum and directory), and 
   slot number.
*/
#ifndef DRMS_CLIENT
int drms_su_newslots (DRMS_Env_t *env, int n, char *series, long long *recnum,
    DRMS_RecLifetime_t lifetime, int *slotnum, DRMS_StorageUnit_t **su) {
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
  status = DRMS_SUCCESS;
 bail:
  return status;
}
#endif
/*
 *  Get the actual storage unit directory from SUMS
 */
#ifndef DRMS_CLIENT
int drms_su_getsudir (DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retrieve) {  
  DRMS_SumRequest_t request, *reply;

  request.opcode = DRMS_SUMGET;
  request.reqcnt = 1;
  request.sunum[0] = su->sunum;
  request.mode = NORETRIEVE;
  if (retrieve) 
    request.mode = RETRIEVE;
  if (env->retention==-1)   
    request.tdays = su->seriesinfo->retention;
  else
    request.tdays = env->retention;

  if (!env->sum_thread) {
    int status;
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread,
				(void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);
      return 1;
    }
  }
  /* Submit request to sums server thread. */
  tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) &request);
  /* Wait for reply. FIXME: add timeout. */
  tqueueDel(env->sum_outbox,  (long) pthread_self(), (char **)&reply);
  if (reply->opcode != 0)
  {
    printf("SUM GET failed with error code %d.\n",reply->opcode);
    free(reply);
    return 1;
  }
  strncpy(su->sudir, reply->sudir[0], sizeof(su->sudir));
  free(reply->sudir[0]);
  free(reply);
  return DRMS_SUCCESS;
}
#endif
/*
 *  Tell SUMS to save this storage unit
 */
#ifndef DRMS_CLIENT
int drms_commitunit (DRMS_Env_t *env, DRMS_StorageUnit_t *su) {
  int i;
  DRMS_SumRequest_t request, *reply;
  FILE *fp;
  char filename[DRMS_MAXPATHLEN];

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

  request.opcode = DRMS_SUMPUT;
  request.reqcnt = 1;
  request.dsname = su->seriesinfo->seriesname;
  request.group = su->seriesinfo->tapegroup;
  if (env->archive) 
    request.mode = ARCH + TOUCH;
  else {
    if (su->seriesinfo->archive)
      request.mode = ARCH + TOUCH;
    else
      request.mode = TEMP + TOUCH;
  }
  if (env->retention==-1)   
    request.tdays = su->seriesinfo->retention;
  else
    request.tdays = env->retention;

  request.sunum[0] = su->sunum;
  request.sudir[0] = su->sudir;
  request.comment = NULL;

  // must have sum_thread running already
  XASSERT(env->sum_thread);
  /* Submit request to sums server thread. */
  tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) &request);
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
/*
 *  Loop though all open storage units and commits the 
   ones open for writing that have slots marked as full
   (i.e. non-temporary) to SUMS. Return the largest retention 
   time of any unit committed.
 */
#ifndef DRMS_CLIENT
int drms_commit_all_units (DRMS_Env_t *env, int *archive) {
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
	  if (archive && su->seriesinfo->archive)
	    *archive = 1;
	}
      }
    }
  }

  if (archive && env->archive) 
    *archive = 1;

  if (env->retention==-1)   
    return max_retention;    
  else
    return env->retention;
}
#endif
/*
 *  Look up a storage unit and its container in the storage unit cache
 */
DRMS_StorageUnit_t *drms_su_lookup (DRMS_Env_t *env, char *series,
    long long sunum, HContainer_t **scon_out) {
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
/*
 *
 */
void drms_su_freeunit (DRMS_StorageUnit_t *su) {
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
/*
 *
 */
void drms_freeunit (DRMS_Env_t *env, DRMS_StorageUnit_t *su) {
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
/*
 *  Mark a storage unit slot as free and remove the associated directory 
   (if any). This is only valid if the storage
   unit is open in mode DRMS_READWRITE.
 */
int drms_su_freeslot (DRMS_Env_t *env, char *series, long long sunum,
    int slotnum) {
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
/*
 *  Change state of a storage unit slot. This is only valid if 
   the storage unit is open in mode DRMS_READWRITE. On entry *state
   must contain the new state. On exit *state will contain the old
   state.
    Possible states are DRMS_SLOT_FREE, DRMS_SLOT_FULL, DRMS_SLOT_TEMP.
*/
DRMS_StorageUnit_t *drms_su_markslot (DRMS_Env_t *env, char *series,
    long long sunum, int slotnum, int *state) {
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
