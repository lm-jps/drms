/*!!!!! THIS IS NOW OBSOLETE !!!!!!!!!!!!!!!!!!*/

/* Called by sum_svc when it gets a SUMRMDO call from sum_rm (typically
 * every 5 minutes). This gets  a list of eligible delete pending
 * storage units. This finds the list and sends it back with a RMRESPDO to
 * SUMRMPROG, SUMRMVERS.
 * An input keylist here looks like:
 * dbname: KEYTYP_STRING   hmidbX
 * num_sets_in_cfg:        KEYTYP_INT      2
 * max_part_free_1:        KEYTYP_DOUBLE              2.202010e+10
 * max_free_1:     KEYTYP_DOUBLE              3.145728e+11
 * max_part_free_0:        KEYTYP_DOUBLE              2.202010e+10
 * max_free_0:     KEYTYP_DOUBLE              3.145728e+11
*/

#include <SUM.h>
#include <sys/time.h>
#include <rpc/rpc.h>
#include <soi_key.h>
#include <soi_error.h>
#include <sum_rpc.h>
#include <sys/statvfs.h>

extern PADATA *pahdr_uid_start[];
extern PADATA *pahdr_uid_end[];
extern PADATA *pahdr_rw;
extern PADATA *pahdr_ro;
extern PADATA *pahdr_dp;
extern PADATA *pahdr_ap;
extern PART ptab[];             /* defined in sum_svc.c */
extern CLIENT *current_client;
extern uint32_t rinfo, procnum;
extern int debugflg;
extern int write_log();
static struct timeval TIMEOUT = { 20, 0 };
void logkey();


KEY *sumrmdo_1(KEY *params)
{
  static KEY *retlist;  /* must be static for svc dispatch rte */
  CLIENT *clresp;
  PART *pptr;
  PADATA *pdptr;
  PADATA *prptr;
  struct statvfs vfs;
  enum clnt_stat clnt_stat;
  double df_avail, bytesdeleted;
  double max_free_set[MAXSUMSETS]; /* bytes to keep free for each SUM set */
  double max_free_part[MAXSUMSETS];/* keep free on a partition in a set */
  double allbytes[MAXSUMSETS];
  double bytesreq[MAXSUMSETS];
  double bytespart[MAXSUMSETS];
  double bytestmp[MAXSUMSETS];
  char *cptr, *call_err, *dbname;
  char line[80];
  char partn_name[MAX_STR], ext[MAX_STR], ext2[MAX_STR];
  char rootdir[MAX_STR], rootcmp[MAX_STR];
  int max_found[MAXSUMSETS], maxpart_found[MAXSUMSETS];
  int dpcnt[MAXSUMSETS], need[MAXSUMSETS];
  int status, num_sets, i, j, sn, set;
  int num_wd = 0;
  uint64_t eff_date, today_date;

  write_time();
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in sumrmdo_1() is:\n");
    keyiterate(logkey, params);
  }
  if(!(clresp = set_client_handle(SUMRMPROG, SUMRMVERS))) {
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  dbname = GETKEY_str(params, "dbname");
  retlist=newkeylist();
  add_keys(params, &retlist);
  rinfo = 0;
  send_ack();
  /* first try to sync our free storage with what 'df' thinks is free */
  for(i=0; i<MAX_PART-1; i++) {
    pptr=(PART *)&ptab[i];
    if(pptr->name == NULL) break;
    if(status = statvfs(pptr->name, &vfs)) {
      write_log("Error %d on statvfs() for %s\n",status,pptr->name);
    }
    else {
      df_avail = (double)vfs.f_bavail;/* #free blks avail to non su */
      df_avail = df_avail * (double)vfs.f_bsize; /* times block size */
      if(SUMLIB_PavailUpdate(pptr->name,df_avail))
       write_log("Err: SUMLIB_PavailUpdate(%s, %e, ...)\n",
                      pptr->name,df_avail);
      else
        write_log("%s Update free bytes to %e\n", pptr->name, df_avail);
    }
  }
  num_sets = getkey_int(params, "num_sets_in_cfg");
  for(i=0; i < num_sets; i++) {
    sprintf(line, "max_free_%d", i);
    max_free_set[i] = getkey_double(params, line);
    sprintf(line, "max_part_free_%d", i);
    max_free_part[i] = getkey_double(params, line);
    allbytes[i] = 0.0;
    bytespart[i] = 0.0;
    max_found[i] = 0;
    maxpart_found[i] = 0;
  }
  for(i=0; i<MAX_PART-1; i++) {
    pptr=(PART *)&ptab[i];
    if(pptr->name == NULL) break;
    sn = pptr->pds_set_num;
    allbytes[sn] += pptr->bytes_left;
    if(pptr->bytes_left >= max_free_part[sn])
      maxpart_found[sn] = 1;
    else {                          /* keep track of least amount need */
      if(max_free_part[sn] > pptr->bytes_left) {	/* !!NEW CK */
        bytestmp[sn] = max_free_part[sn] - pptr->bytes_left;
        /*if(bytestmp[sn] < bytespart[sn]) bytespart[sn] = bytestmp[sn];*/
        if(bytestmp[sn] > bytespart[sn]) bytespart[sn] = bytestmp[sn];
      }
    }
  }
  for(i=0; i < num_sets; i++) {
    if(allbytes[i] >= max_free_set[i]) max_found[i] = 1;
  }
  for(i=0; i < num_sets; i++) {
    if(maxpart_found[i] && max_found[i]) continue;
    break;
  }
  if(i == num_sets) {		/* all done, storage avail */
    write_log("sumrmdo_1() reports storage available\n");
    clnt_destroy(clresp);
    return((KEY *)1);
  }
  for(i=0; i < num_sets; i++) {
    /*if(maxpart_found[i])*/
    if(max_free_set[i] > allbytes[i])
      bytesreq[i] = max_free_set[i] - allbytes[i]; /* #of bytes to free */
    else
      bytesreq[i] = bytespart[i];
    write_log("Attempt to del on SUM set %d %g bytes...\n", i, bytesreq[i]); 
  }
  /* now try to free any storage from the delete pending list */
  /* make a list of dp in ascending uid order if current list empty */
  for(i=0; i < num_sets; i++) { 
    dpcnt[i] = 0;
    need[i] = 0;
    if(pahdr_uid_start[i] == NULL) need[i] = 1;
  }
  today_date = (uint64_t)atol(get_effdate(0));
  for(i=0; i < num_sets; i++) {
    if(need[i]) break;
  }
  if(i != num_sets) {	/* need a new list */
     for(pdptr=(PADATA *)getpanext(pahdr_dp); pdptr != NULL;
       pdptr=(PADATA *)getpanext((PADATA *)-1))/* get all delete pending */
     {
       if(prptr=(PADATA *)getpawd(pahdr_ro, pdptr->wd)) /* still read only */
         continue;                             /* continue the for */
       /* check that effective date to delete is now or past */
       if(pdptr->effective_date)
         eff_date = (uint64_t)atol(pdptr->effective_date);
       else
         eff_date = 0;
       if(eff_date != 0 && eff_date > today_date)
         continue;                             /* continue the for */
       /* For SUMS there is only one sum_set_num (formerly pds_set_num) */
       /* i.e. all storage is treated as local. no host info required */
       set = 0;			/* !!TBD get from pdptr new field if impli. */
       /* put on new list in ascending uid order. this is for del by FIFO */
       if(need[set]) {
         dpcnt[set]++;
         uidpadata(pdptr, &pahdr_uid_start[set], &pahdr_uid_end[set]);
       }
     }
     for(i=0; i < num_sets; i++) {
       if(need[i]) 
         write_log("Total current eligible dp entries SUM set %d = %d\n",
			i, dpcnt[i]);
     }
  }
  for(i=0; i < num_sets; i++) {
    if(need[i] && !dpcnt[i])
      write_log("No SUM set %d del pend storage available\n", i);
  }
  for(i=0; i < num_sets; i++) {
    if(pahdr_uid_start[i] != NULL) break;
  }
  if(i == num_sets) {
    sprintf(line, "/bin/rm -f %s.%s", SUM_STOP_NOT, dbname);
    if(system(line)) {
      write_log("Error on system cmd: %s\n", line);
    }
    clnt_destroy(clresp);
    return((KEY *)1);		/* nothing to be deleted */
  }

  /* now delete the eligible sets del pend in increasing uid order */
  /* must check for ro and eff date eligibility ea time as they can change*/
  for(i=0; i < num_sets; i++) {
    bytesdeleted = 0.0;
    for(pdptr=(PADATA *)getpanext(pahdr_uid_start[i]); pdptr != NULL;
        pdptr=(PADATA *)getpanext((PADATA *)-1))
    {                                           /* for dp in ascending uid */
      if(bytesdeleted >= bytesreq[i])   /* only delete enough,then try again */
        break;                          /* exit for dp in ascend uid*/
      if(prptr=(PADATA *)getpawd(pahdr_ro, pdptr->wd)) { /* now read only */
        /* rem from the elig list so wont potentially stop list from emptying*/
        remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
			pdptr->wd, pdptr->sumid);
        continue;                               /* continue the for */
      }
      /* check that effective date to delete is now or past */
      /* must ck it on the main pahdr_dp list as this is where it changes */
      prptr=(PADATA *)getpadata(pahdr_dp, pdptr->wd, pdptr->sumid);
      if(prptr->effective_date)
        eff_date = (uint64_t)atol(prptr->effective_date);
      else
        eff_date = 0;
      if(eff_date != 0 && eff_date > today_date) {
        /* the eff date doesn't change often so rem from the elig list */
        remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
			pdptr->wd, pdptr->sumid);
        continue;                               /* continue the for */
      }
      /* move not implemented in SUMS */
      /*if(prptr->archsub == DADMVA) {            /* being moved, don't del */
      /* /* rm from the elig list, it will be changed to DADMVC later */
      /* remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
      /*                pdptr->wd, pdptr->sumid);
      /*  continue;                               /* continue the for */
      /*}
      */

      /* now put the storage back in the partn_avail table */
      strcpy(rootdir, pdptr->wd);               /* the online_loc */
      strcpy(rootcmp, pdptr->wd);
      write_log("Del from db: %s\n", rootdir);
      /* find /Dn term after any root storage partition */
      if(!(cptr = strstr(rootcmp+1, "/D"))) {
        write_log("The wd=%s doesn't have a /.../Dxxx term!\n",rootcmp);
        remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
			pdptr->wd, pdptr->sumid);
        continue;                               /* continue the for */
      }
      *cptr = (char)NULL;                       /* rootcmp now /SUMn */

      /* find the partn_name part of rootdir from the partn_avail table */
      for(j=0; j<MAX_PART-1; j++) {
        pptr=(PART *)&ptab[j];
        if(pptr->name == NULL) {
          write_log("Error attempting to take off-line wd=%s\n", rootdir);
          write_log("  Cannot find corresponding entry in partn_avail tbl\n");
          write_log("  You've either removed the entry from the partn_avail table (forbidden)\n");
          write_log("  or this is a bum entry in the del pend table\n");
          write_log("Removing entry from my partial dp list for now\n");
          remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
			pdptr->wd, pdptr->sumid)
;
          break;                                /* exit for j<MAX_PARTN-1 */
        }
        if(!strcmp(rootcmp, pptr->name)) {      /* found the corresp. partn */
          cptr = rootdir+strlen(pptr->name)+1;  /* pos past "partn_name/" */
          if(cptr = index(cptr, '/'))           /* pos past Duid dir */
            *cptr = (char)NULL;                 /* terminate rootdir */
          break;                                /* exit for j<MAX_PARTN-1 */
        }
      }
      if(pptr->name == NULL) continue;          /* contin for dp in ascend */
      strcpy(partn_name, pptr->name);

      /* tell database that wd is off-line */
      /* Don't call StatOffline if already moved */
      if(prptr->archsub != DADMVC) {            /* note: must use prptr */
        if(SUM_StatOffline(pdptr->ds_index)) {
          write_log("Err: SUM_StatOffline(%l, ...)\n", pdptr->ds_index);
        }
      }
      /* commit remove del pend entry from sum_partn_alloc tbl */
      if(NC_PaUpdate
          (pdptr->wd, pdptr->sumid,pdptr->bytes,DADP,0,0,0,0,0,0,1)) 
      {
        write_log("Err: NC_PaUpdate(%s,%l,%e,DADP,0,0,0, ...)to rm from dp list\n", pdptr->wd,pdptr->sumid,pdptr->bytes);
        write_log("  ??This is how we got the info in the first place!\n");
      }
      /* now add the storage back to its partition in db and memory */
      /* Obsolete in SUMS */
      /*if(NC_PavailPut (partn_name, pdptr->bytes, history, errlog)) {
      /*  write_log("Err: NC_PavailPut(%s, %e) to add back storage, 
      /*	proceeding...\n", partn_name, pdptr->bytes);
      /*}
      /*DS_Commit(history, errlog);
      */
      /* now remove the entry from sum_main if this was a del from a
       * temporary type archive, i.e. archsub = DAAEDDP
      */
      if(pdptr->archsub == DAAEDDP) {   /* a temporary dataset */
        /* 31Aug99 The DS_CatDelete() will now be done by dsds_rm.
         * dsds_rm will know to do this by the keyword tmp_wd_%d instead
         * of wd_%d for each wd to be deleted.
        */
        /*(*history)("Removing dsds_main for %s,%l\n",pdptr->wd,pdptr->sumid);*/
        /*if(DS_CatDelete(pdptr->ds_index, history, errlog)) {
        /*  (*history)("Err: DS_CatDelete(%s, %l)\n", pdptr->wd, pdptr->sumid);
        /*}
        */
        sprintf(ext, "tmp_wd_%d", num_wd);
        sprintf(ext2, "tmp_ds_index_%d", num_wd);
        setkey_uint64(&retlist, ext2, pdptr->ds_index);
      }
      else {
        sprintf(ext, "wd_%d", num_wd);
      }
      setkey_str(&retlist, ext, pdptr->wd);
      sprintf(ext, "rootdir_%d", num_wd);
      setkey_str(&retlist, ext, rootdir);
      setkey_int(&retlist, "num_wd", ++num_wd);
      pptr->bytes_left += pdptr->bytes;
      bytesdeleted += pdptr->bytes;     /* add what we deleted so far */
      rempadata(&pahdr_dp, pdptr->wd, pdptr->sumid);/* remove from dp list */
      /* remove the entry from the uid in ascend order list & update ptrs */
      remuidpadata(&pahdr_uid_start[i], &pahdr_uid_end[i],
			pdptr->wd, pdptr->sumid);
    }                                   /* end for dp in ascend uid */
  }
  clnt_stat = clnt_call(clresp, RMRESPDO, (xdrproc_t)xdr_Rkey, (char *)retlist,
                            (xdrproc_t)xdr_void, 0, TIMEOUT);
  if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);             /* outputs to stderr */
            write_log("***Error on clnt_call() back to RMRESPDO procedure\n");
            call_err = clnt_sperror(current_client, "Err");
            write_log("%s\n", call_err);
   }
   clnt_destroy(clresp);
   freekeylist(&retlist);
   return((KEY *)1);
}

