/* tape_svc_proc.c: Server side procedures (called from tape_svc.c)
 *
 * There are two types of calls here. One with a new read or write request
 * (readdo_1() or writedo_1()) which puts a new entry on the queue and tries
 * to kick it off. The other type of call is for an entry already kicked 
 * off and it is now responding (taperespwritedo_1(), taperespreaddo_1() or
 * taperesprobotdo_1()). When these complete they check the queue for another
 * entry that can be kicked off.
*/
#include <SUM.h>
#include <sys/time.h>
#include <rpc/pmap_clnt.h>
#include <soi_error.h>
#include <sum_rpc.h>
#include <tape.h>
#include <printk.h>

extern int write_log();
extern int tapeindrive();
extern int tapeinslot();
extern void rd_q_print();
extern void tq_entry_rd_dump();
extern void insert_tq_entry_rd_sort();
extern void insert_tq_entry_rd_need();
extern void insert_tq_entry_wrt_need();
extern int find_empty_impexp_slot();
extern TQ *delete_q_rd_need_front();
extern TQ *delete_q_wrt_need_front();
extern CLIENT *current_client, *clntsum, *clntdrv0, *clntdrv1;
extern CLIENT *clntdrv2, *clntdrv3;
extern CLIENT *clntdrv[];
extern CLIENT *clntrobot0;
extern SVCXPRT *glb_transp;
extern int drive_order[];
extern uint64_t rinfo;
extern uint32_t procnum;
extern int debugflg;
extern int current_client_destroy;
extern int sim;
extern int tapeoffline;
extern int robotoffline;
extern int driveonoffstatus;
extern char libdevname[];
extern char hostn[];
extern SLOT slots[];
extern DRIVE drives[];
extern TQ *poff;

TQ *q_rd_front = NULL;		/* front of tape read Q */
TQ *q_rd_rear = NULL;		/* rear of tape read Q */
TQ *q_wrt_front = NULL;		/* front of tape write Q */
TQ *q_wrt_rear = NULL;		/* rear of tape write Q */
TQ *q_rd_need_front = NULL;	/* front of tape need read Q */
TQ *q_rd_need_rear = NULL;	/* rear of tape need read Q */
TQ *q_wrt_need_front = NULL;	/* front of tape need write Q */
TQ *q_wrt_need_rear = NULL;	/* rear of tape need write Q */
SUMOFFCNT *offcnt_hdr = NULL;/* linked list of offline counts for a uid*/

int nxtscanrd = 0;	//where to start next scan of drive_order[]
int nxtscanwt = 0;	//where to start next scan of drive_order[]
int robotbusy = 0;
int robotcmdseq = 0;
int eeactive = 0;	/* only allow Qs to rd/wrt tape already in a drive */
int full_impexp_slotnum_internal[NUM_IMP_EXP_SLOTS];
int empty_slotnum_internal[NUM_IMP_EXP_SLOTS];
char dstring[32];
void write_time();
int send_mail(char *fmt, ...);
void logkey ();
char *find_tape_from_group();
KEY *taperesprobotdo_1_rd(KEY *params);
KEY *taperesprobotdo_1_wt(KEY *params);
static struct timeval TIMEOUT = { 30, 0 };

static struct timeval first[5], second[5];
float ftmp;

/*********************************************************/
void StartTimer(int n)
{
  gettimeofday (&first[n], NULL);
}

float StopTimer(int n)
{
  gettimeofday (&second[n], NULL);
  if (first[n].tv_usec > second[n].tv_usec) {
    second[n].tv_usec += 1000000;
    second[n].tv_sec--;
  }
  return (float) (second[n].tv_sec-first[n].tv_sec) +
    (float) (second[n].tv_usec-first[n].tv_usec)/1000000.0;
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[].
*/
static char *datestring()
{
  char datestr[32];
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  datestr[19] = (char)NULL;
  return(&datestr[4]);          /* isolate the mmm dd hh:mm:ss */
}

/* Get client handle for return of result and store in glb vrbl current_client.
*/
CLIENT *set_client_handle(uint32_t prognum, uint32_t versnum)
{
  static CLIENT *client;
  struct sockaddr_in *sock_in;
  int sock = RPC_ANYSOCK;

    /* set up a client handle for eventual ret of the result with a call
     * to the requestor's local daemon. But
     * first must translate into caller host info to call the cliens back.
    */
    sock_in = svc_getcaller(glb_transp);/* get caller socket info */
    sock_in->sin_port = 0;      /* makes clnttcp_create consult yp */
    client = clnttcp_create(sock_in,prognum,versnum,&sock,0,0);
    if(!client) {
      clnt_pcreateerror("Can't do a clnttcp_create to send a response");
      write_time();
      write_log("**** Can't do a clnttcp_create to send a response ****\n");
      write_log("**** prognum=0x%x  versnum=%d ****\n", prognum, versnum);
      write_log("**** Did someone remove us from the portmapper? ****\n");
      return(0);                /* error ret */
    }
    /* set glb vrbl for poss use */
    current_client = client;
    return(client);
}

/* Send ack to original caller. Uses global vrbls glb_transp and
 * rinfo which are set up before this call.
 * I'm not quite sure what to do on an error here?? I've never seen it and
 * will ignore it for now.
*/
void send_ack()
{
  /* send ack back with the rinfo value */
  if (!svc_sendreply(glb_transp, (xdrproc_t)xdr_uint32_t, (char *)&rinfo)) {
    write_log("***Error on immed ack back to client. FATAL???\n");
    svcerr_systemerr(glb_transp);
  }
}

/************************************************************************/

/* Trys to start an entry on the rd queue.
 * Starts with the top entry of the read queue and works down to see if 
 * it can start one.
 *
 *  0 = can't process entry now. remains on the q
 *  1 = an entry has been successfully started and removed from q 
 *  2 = entry was removed from the q and an error occured. 
 *      an error list needs to be sent to the original caller
 * poff is set if the entry has been removed from the queue but not freed yet.
*/
int kick_next_entry_rd() {
  TQ *p, *ptmp;
  uint32_t driveback, robotback;
  int d, e, snum, sback;
  char cmd[80];
  char *call_err, *tapeid;
  enum clnt_stat status;

  poff = NULL;
  sback = 0;
  p = q_rd_front;
  /* scan the rd Q to try to find a tape in a non-busy drive */
  while(p) {
    if((d=tapeindrive(p->tapeid)) != -1) {  /* tape in drive# d */
      if(drives[d].busy || drives[d].offline) {
        p = p->next;
        continue;
      }
      else {
        if(poff) {	  /* free any preceeding p. last freed elsewhere */
          free(poff->tapeid);
          free(poff->username);
          freekeylist((KEY **)&poff->list);
          free(poff);
        }
        setkey_int(&p->list, "dnum", d);   /* tape is in drive d */
        poff = delete_q_rd(p);    /* remove from q */
        write_log("*Tp:RdQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
        drives[d].busy = 1;       /* set drive busy */
        write_log("*Tp:DrBusy: drv=%d\n", d);
        drives[d].tapemode = TAPE_RD_CONT;
        if(drives[d].filenum == -1)  /* must indicate rewind to drive_svc*/
          setkey_int(&poff->list, "tapemode", TAPE_RD_INIT);
        else 
          setkey_int(&poff->list, "tapemode", TAPE_RD_CONT);
        setkey_int(&poff->list, "filenum", drives[d].filenum);
        /* call drive[0,1]_svc and tell our caller to wait for completion */
        /*write_log("Calling: clnt_call(clntdrv%d, READDRVDO, ...) \n", d);*/
        status = clnt_call(clntdrv[d], READDRVDO, (xdrproc_t)xdr_Rkey, 
	(char *)poff->list, (xdrproc_t)xdr_uint32_t,(char *)&driveback,TIMEOUT);
        if(status != RPC_SUCCESS) {
          if(status != RPC_TIMEDOUT) {  /* allow timeout? */
            call_err = clnt_sperror(clntdrv[d], "Err clnt_call for READDRVDO");
            drives[d].busy = 0;   /* free drive */
            write_log("*Tp:DrNotBusy: drv=%d\n", d);
            write_log("%s %s\n", datestring(), call_err);
            return(2);
          } else {
            write_log("%s timeout occured for READDRVDO drv#%d in kick_next_entry_rd()\n", datestring(), d);
          }
        }
        if(driveback == 1) {
          drives[d].busy = 0;     /* free drive */
          write_log("*Tp:DrNotBusy: drv=%d\n", d);
          write_log("**Error in kick_next_entry_rd() in tape_svc_proc.c\n");
          return(2);
        }
        sback = 1;
        p = poff->next;
        continue;
      }
    }
    p = p->next;
  }

  /* now try for a tape not in any drive */
  p = q_rd_front;
  while(p) {
    if(eeactive) {              /* import/export active, don't do this now */
      sback = 0;
      break;			/* break while(p) */
    }
    if((d=tapeindrive(p->tapeid)) != -1) {  /* tape in drive# d */
      if(drives[d].busy || drives[d].offline) {	/* skip if busy drive */
        p = p->next;
        continue;
      }
    }
    /* ck if tape in any slot or drive */
    if(((snum=tapeinslot(p->tapeid)) == -1) && (d == -1)) {
#ifdef SUMDC
      /* the datacapture t50 is write oriented. needs to know this is for rd */
      write_log("*Tp:Need:Rd tapeid=%s is not in live slots\n", p->tapeid);
#else
      write_log("*Tp:Need: tapeid=%s is not in live slots\n", p->tapeid);
#endif
      send_mail("tapeid=%s is not in live slot\n", p->tapeid);
      ptmp = delete_q_rd(p);      /* remove from q */
      p = p->next;
      ptmp->next = NULL;
      write_log("*Tp:RdQdel: dsix=%lu drv=**WRN:tape_not_in_robot\n", 
			ptmp->ds_index);
      insert_tq_entry_rd_need(ptmp); /* put at end of need rd q */
      write_log("NEED RD Q:\n");
      rd_q_print(q_rd_need_front);	/* !!!TEMP */
      sback = 0;
      continue;			/* see if there's more */
    }
    /* try to find a free drive, first w/no tape and then not busy */
    for(e=0; e < MAX_DRIVES; e++) {
      d = drive_order[e];
      if((!drives[d].tapeid) && (!drives[d].offline)) break;
    }
    if(e == MAX_DRIVES) {         /* all drives have a tape or offline */
      for(e=0; e < MAX_DRIVES; e++) {
        d = drive_order[nxtscanrd++];
        if(nxtscanrd >= MAX_DRIVES) nxtscanrd = 0;
        if((!drives[d].busy) && (!drives[d].offline)) break;
      }
    }
    if(e == MAX_DRIVES) {         /* all drives are busy */
      sback = 0;
      break;			/* break while(p) */
    }
    /* d = drive# of a free drive */
    if(robotbusy) {             /* robot eventually calls taperesprobotdo_1 */
      sback = 0;
      break;			/* break while(p) */
    }
    robotbusy = 1;
    drives[d].busy = 1;
    write_log("*Tp:DrBusy: drv=%d\n", d);
    setkey_int(&p->list, "dnum", d);
    setkey_int(&p->list, "snum", snum);
    if(tapeid = drives[d].tapeid) {	/* must unload current tape */
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, (drives[d].slotnum)+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd1", cmd);
      sprintf(cmd,"mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, snum+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd2", cmd);
    }
    else {
      sprintf(cmd,"mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, snum+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd1", cmd);
    }
    /* !!TBD eventually do switch(robotnum) */
    /*write_log("Calling: clnt_call(clntrobot0, ROBOTDO, ...) \n");*/
    StartTimer(3); //!!TEMP
    status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)p->list,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    ftmp = StopTimer(3);
    write_log("Time 3 for ROBOTDO in tape_svc = %f sec\n", ftmp);
    if(status != RPC_SUCCESS) {
      if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
        call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
        write_log("%s %s\n", datestring(), call_err);
        robotbusy = 0;
        drives[d].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", d);
        poff = delete_q_rd(p);      /* remove from q */
        write_log("*Tp:RdQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
        sback = 2;
        break;			/* break while(p) */
      } else {
        write_log("%s timeout occured for ROBOTDO in kick_next_entry_rd() \n", 
		datestring());
      }
    }
    if(robotback == 1) {
      write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
      robotbusy = 0;
      drives[d].busy = 0;
      write_log("*Tp:DrNotBusy: drv=%d\n", d);
      poff = delete_q_rd(p);      /* remove from q */
      write_log("*Tp:RdQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
      sback = 2;
      break;			/* break while(p) */
    }
    poff = delete_q_rd(p);        /* remove from q */
    write_log("*Tp:RdQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
    sback = 1;
    break;			/* break while(p) */
  }
  return(sback);
}
/**************************************************************************/
/* Starts with the top entry of the write queue and works down to see if
 * it can start one.
 *
 *  0 = can't process entry now. remains on the q
 *  1 = an entry has been successfully started and removed from q 
 *  2 = entry was removed from the q and started but an error occured. 
 *      an error list needs to be sent to the original caller
 * poff is set if the entry has been removed from the queue.
*/
int kick_next_entry_wt() {
  TQ *p, *ptmp;
  TAPE tapeinfo;
  uint64_t driveback, robotback;
  int d, e, snum, sback, tape_closed, group_id, nxtwrtfn;
  double total_bytes;
  char cmd[80];
  char *call_err, *tapeid;
  enum clnt_stat status;

  poff = NULL;
  sback = 0;
  p = q_wrt_front;
  /* scan the wrt Q to try to find a tape in a non-busy drive */
  while(p) {
    if((d=tapeindrive(p->tapeid)) != -1) {  /* tape in drive# d */
      if(drives[d].busy || drives[d].offline) {
        p = p->next;
        continue;
      }
      else {              /* write the tape in the idle drive */
        if(poff) {	  /* free any preceeding p. last freed elsewhere */
          free(poff->tapeid);
          free(poff->username);
          freekeylist((KEY **)&poff->list);
          free(poff);
          poff = NULL;
        }
        setkey_int(&p->list, "dnum", d);   /* tape is in drive d */
        tape_closed = SUMLIB_TapeState(p->tapeid);/* get anew*/
        if(tape_closed == TAPECLOSED) { /* assign group to another tape */
          group_id = getkey_int(p->list, "group_id_0");
          total_bytes = getkey_double(p->list, "total_bytes");
          if(SUMLIB_TapeFindGroup(group_id, total_bytes, &tapeinfo)) {
            write_log("Error: no tape in group %d\n", group_id);
            send_mail("Error: no tape in group %d\n", group_id);
            write_log("*Tp:Need: tapeid=%s\n", "NOGROUP");
            poff = delete_q_wrt(p);    /* remove from q */
            write_log("*Tp:WtQdel: dsix=%lu drv=**WRN:tape_not_in_robot\n", 
			poff->ds_index);
            return(2);
          }

          p->tapeid = tapeinfo.tapeid;
          setkey_str(&p->list, "tapeid", tapeinfo.tapeid);
          setkey_uint64(&p->list, "availblocks", tapeinfo.availblocks);
          continue;		/* process this Q entry again */
        }
        poff = delete_q_wrt(p);    /* remove from q */
        write_log("*Tp:WtQdel: dsix=%lu drv=%d\n", p->ds_index, d);
        setkey_int(&poff->list, "tape_closed", tape_closed);
        /* get the next file# to write on this tape. Send it to driven_svc */
        if((nxtwrtfn = SUMLIB_TapeFilenumber(poff->tapeid)) == 0) {
          write_log("Error: can't get next file# to write for tape=%s\n",
				poff->tapeid);
          return(2);
        }
        setkey_int(&poff->list, "nxtwrtfn", nxtwrtfn);
        drives[d].busy = 1;       /* set drive busy */
        write_log("*Tp:DrBusy: drv=%d\n", d);
        /* call drive[0,1]_svc and tell our caller to wait for completion */
        /*write_log("Calling: clnt_call(clntdrv%d, WRITEDRVDO, ...) \n", d);*/
    StartTimer(1); //!!TEMP
        status = clnt_call(clntdrv[d], WRITEDRVDO, (xdrproc_t)xdr_Rkey, 
	(char *)poff->list, (xdrproc_t)xdr_uint32_t,(char *)&driveback,TIMEOUT);
    ftmp = StopTimer(1);
    write_log("Time 1 for WRITEDRVDO in tape_svc = %f sec\n", ftmp);

        if(status != RPC_SUCCESS) {
          if(status != RPC_TIMEDOUT) {  /* allow timeout? */
            call_err = clnt_sperror(clntdrv[d], "Err clnt_call for WRITEDRVDO");
            drives[d].busy = 0;   /* free drive */
            write_log("*Tp:DrNotBusy: drv=%d\n", d);
            write_log("%s %s\n", datestring(), call_err);
            return(2);
          }
        }
        if(driveback == 1) {
          drives[d].busy = 0;     /* free drive */
          write_log("*Tp:DrNotBusy: drv=%d\n", d);
          write_log("**Error in kick_next_entry_wt() in tape_svc_proc.c\n");
          return(2);
        }
        sback = 1;
        p = poff->next;
        continue;
      }
    }
    p = p->next;
  }  
 
  /* now try for a tape not in any drive */
  p = q_wrt_front;
  while(p) {
    if(eeactive) {              /* import/export active, don't do this now */
      sback = 0;
      break;			/* break while(p) */
    }
    tape_closed = SUMLIB_TapeState(p->tapeid);/* get anew*/
    if(tape_closed == TAPECLOSED) { /* assign group to another tape */
      group_id = getkey_int(p->list, "group_id_0");
      total_bytes = getkey_double(p->list, "total_bytes");
      if(SUMLIB_TapeFindGroup(group_id, total_bytes, &tapeinfo)) {
        write_log("Error: no tape in group %d\n", group_id);
        send_mail("Error: no tape in group %d\n", group_id);
        write_log("*Tp:Need: tapeid=%s\n", "NOGROUP");
        poff = delete_q_wrt(p);      /* remove from q */
        write_log("*Tp:WtQdel: dsix=%lu drv=**WRN:tape_not_in_robot\n", 
			poff->ds_index);
        return(2);
      }
      p->tapeid = tapeinfo.tapeid;
      setkey_str(&p->list, "tapeid", tapeinfo.tapeid);
      setkey_uint64(&p->list, "availblocks", tapeinfo.availblocks);
    }

    if((d=tapeindrive(p->tapeid)) != -1) {  /* tape in drive# d */
      if(drives[d].busy || drives[d].offline) {	/* skip if busy drive */
        p = p->next;
        continue;
      }
    }
    /* ck if tape in any slot or drive */
    if(((snum=tapeinslot(p->tapeid)) == -1) && (d == -1)) {
      write_log("*Tp:Need: tapeid=%s is not in live slots\n", p->tapeid);
      send_mail("tapeid=%s is not in live slot\n", p->tapeid);
#ifdef SUMDC
      /* give error back to caller for datacapture machine */
      poff = delete_q_wrt(p);      /* remove from q */
      write_log("*Tp:WtQdel: dsix=%lu drv=**WRN:tape_not_in_robot\n", 
			poff->ds_index);
      return(2);
#endif
      ptmp = delete_q_wrt(p);     /* remove from q */
      p = p->next;
      ptmp->next = NULL;
      write_log("*Tp:WtQdel: dsix=%lu drv=**WRN:tape_not_in_robot\n", 
			ptmp->ds_index);
      insert_tq_entry_wrt_need(ptmp); /* put at end of need wrt q */
      sback = 0;
      continue;			/* see if there's more */
    }
    /* try to find a free drive, first w/no tape and then not busy */
    for(e=0; e < MAX_DRIVES; e++) {
      d = drive_order[e];
      if((!drives[d].tapeid) && (!drives[d].offline)) break;
    }
    if(e == MAX_DRIVES) {         /* all drives have a tape or offline */
      for(e=0; e < MAX_DRIVES; e++) {
        d = drive_order[nxtscanwt++];
        if(nxtscanwt >= MAX_DRIVES) nxtscanwt = 0;
        if((!drives[d].busy) && (!drives[d].offline)) break;
      }
    }
    if(e == MAX_DRIVES) {         /* all drives are busy */
      sback = 0;
      break;                    /* break while(p) */
    }
    /* d = drive# of a free drive */
    if(robotbusy) {             /* robot eventually calls taperesprobotdo_1 */
      sback = 0;
      break;                    /* break while(p) */
    }
    robotbusy = 1;
    drives[d].busy = 1;
    write_log("*Tp:DrBusy: drv=%d\n", d);
    setkey_int(&p->list, "dnum", d);
    setkey_int(&p->list, "snum", snum);
    if(tapeid = drives[d].tapeid) {	/* must unload current tape */
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, (drives[d].slotnum)+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd1", cmd);
      sprintf(cmd,"mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, snum+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd2", cmd);
    }
    else {
      sprintf(cmd,"mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
  		libdevname, snum+1, d, robotcmdseq++);
      setkey_str(&p->list, "cmd1", cmd);
    }
    /* !!TBD eventually do switch(robotnum) */
    /*write_log("Calling: clnt_call(clntrobot0, ROBOTDO, ...) \n");*/
    StartTimer(4); //!!TEMP
    status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)p->list,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    ftmp = StopTimer(4);
    //write_log("Time 4 for ROBOTDO in tape_svc = %f sec\n", ftmp);
    if(status != RPC_SUCCESS) {
      if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
        call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
        write_log("%s %s\n", datestring(), call_err);
        robotbusy = 0;
        drives[d].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", d);
        poff = delete_q_wrt(p);      /* remove from q */
        write_log("*Tp:WtQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
        sback = 2;
        break;                  /* break while(p) */
      }
    }
    if(robotback == 1) {
      write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
      robotbusy = 0;
      drives[d].busy = 0;
      write_log("*Tp:DrNotBusy: drv=%d\n", d);
      poff = delete_q_wrt(p);      /* remove from q */
      write_log("*Tp:WtQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
      sback = 2;
      break;                    /* break while(p) */
    }
    poff = delete_q_wrt(p);        /* remove from q */
    write_log("*Tp:WtQdel: dsix=%lu drv=%d\n", poff->ds_index, d);
    sback = 1;
    break;                      /* break while(p) */
  }				/* end while(p) */
  return(sback);
}
/**************************************************************************/


/* Called by sum_svc from getdo_1() as a result of a user calling the
 * SUM API call SUM_get() to get the wd of a storage unit back online.
 * Call tape_svc to read the wd back online.
 * Sample keylist looks like:
 * offcnt: KEYTYP_INT      2
 * partn_name:     KEYTYP_STRING   /SUM3/D679
 * ds_index:       KEYTYP_UINT64    679
 * offline:        KEYTYP_INT      1
 * tapefilenum_1:  KEYTYP_INT      2
 * tapeid_1:       KEYTYP_STRING   000014S1
 * wd_1:   KEYTYP_STRING   /SUM3/D679
 * ds_index_1:     KEYTYP_UINT64    285
 * create_sumid_1: KEYTYP_UINT64    347
 * bytes_1:        KEYTYP_DOUBLE              1.200000e+08
 * archive_status_1:       KEYTYP_STRING   Y
 * online_status_1:        KEYTYP_STRING   N
 * tapefilenum_0:  KEYTYP_INT      1
 * tapeid_0:       KEYTYP_STRING   000013S1
 * wd_0:   KEYTYP_STRING   /SUM2/D678
 * ds_index_0:     KEYTYP_UINT64    282
 * create_sumid_0: KEYTYP_UINT64    344
 * bytes_0:        KEYTYP_DOUBLE              1.200000e+08
 * archive_status_0:       KEYTYP_STRING   Y
 * online_status_0:        KEYTYP_STRING   N
 * (void)				(this is fileptr current_client)
 * uid:    KEYTYP_UINT64    574
 * mode:   KEYTYP_INT      16
 * tdays:  KEYTYP_INT      5
 * reqcnt: KEYTYP_INT      2
 * DEBUGFLG:       KEYTYP_INT      1
 * REQCODE:        KEYTYP_INT      4
 * dsix_0: KEYTYP_UINT64    282
 * dsix_1: KEYTYP_UINT64    285
 * rootwd_0:       KEYTYP_STRING   /SUM0/D48
 * rootwd_1:       KEYTYP_STRING   /SUM0/D49
 * OP:		   KEYTYP_STRING   rd	(NOTE: added here)
*/
KEY *readdo_1(KEY *params) {
  TQ *p;
  SUMOFFCNT *offptr;
  uint64_t uid, dsix;
  int reqcnt, i, j, k, tapefilenum, state;
  char *tapeid, *cptr, *user;
  char tmpname[80];

  setkey_str(&params, "OP", "rd");
  setkey_int(&params, "sim", sim);
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("TEMP in readdo_1() call in tape_svc. keylist is:\n");
      keyiterate(logkey, params);
    }
  }
  user = getkey_str(params, "username");
  uid = getkey_uint64(params, "uid");
  write_log("!!TEMP setsumoffcnt uid=%lu\n", uid); //!!TEMP
  if(!(offptr = setsumoffcnt(&offcnt_hdr, uid, 0))) {
    write_log("**Err: setsumoffcnt() in readdo_1() has malloc error for uid=%lu\n", uid);
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    free(user);
    return((KEY *)1);  /* error. nothing to be sent */
  }
  /* get tapeid for the offline storage units (su) */
  /* Only queue one request for any duplicate tapeid/tapefilenum */
  reqcnt = getkey_int(params, "reqcnt"); /* #of su in this request */
  //NOTE: the reqcnt was verified at the SUM_get() call
  for(i=0; i < reqcnt; i++) {
    sprintf(tmpname, "online_status_%d", i);
    cptr = GETKEY_str(params, tmpname);
    //proceed if this su is offline and archived
    if(strcmp(cptr, "N") == 0) { 
      sprintf(tmpname, "archive_status_%d", i);
      cptr = GETKEY_str(params, tmpname);
      if(strcmp(cptr, "Y") == 0) {
        /*setkey_int(&params, "reqofflinenum", i); /* tell others which one */
        sprintf(tmpname, "tapeid_%d", i);
        tapeid = GETKEY_str(params, tmpname);
        sprintf(tmpname, "tapefilenum_%d", i);
        tapefilenum = getkey_int(params, tmpname);
        sprintf(tmpname, "ds_index_%d", i);
        dsix = getkey_uint64(params, tmpname);
        j = offptr->uniqcnt;
        if(j == 0) {
          offptr->tapeids[0] = (char *)strdup(tapeid);
          offptr->tapefns[0] = tapefilenum;
          offptr->reqofflinenum[0] = i;
          offptr->dsix[0] = dsix;
          offptr->uniqcnt = 1;
        }
        else {			/* see if any dups */
          for(k=0; k < j; k++) {
            if(!strcmp(offptr->tapeids[k], tapeid)) {
              if(offptr->tapefns[k] == tapefilenum) {
                break;		/* it's a dup */
              }
            }
          }
          if(k == j) {
            offptr->tapeids[j] = (char *)strdup(tapeid);
            offptr->tapefns[j] = tapefilenum;
            offptr->reqofflinenum[j] = i;
            offptr->dsix[j] = dsix;
            offptr->uniqcnt++;
          }
        }
      }
    }
  }
  write_log("!!TEMP offptr->uniqcnt = %d\n", offptr->uniqcnt); /* !!TEMP */
  for(j=0; j < offptr->uniqcnt; j++) {	/* now q all unique entries */
    /* put in the keylist for this entry which entry was offline */
    setkey_int(&params, "reqofflinenum", offptr->reqofflinenum[j]);
    if((p=q_entry_make(params, uid, offptr->tapeids[j], 
	offptr->tapefns[j], user, offptr->dsix[j]))==NULL) {
      write_log("**Err: can't malloc a new rd Q entry\n");
      rinfo = 1;  /* give err status back to original caller */
      send_ack();
      free(user);
      return((KEY *)1);  /* error. nothing to be sent */
    }
    rinfo = RESULT_PEND;    /* tell caller to wait later for results */
    send_ack();		    /* ack to caller */
    insert_tq_entry_rd_sort(p);	/* put in file# order in rd q */
    /*insert_tq_entry_rd(p);	/* put at end of rd q */
    /*write_log("RD Q:\n");*/
    /*rd_q_print(q_rd_front);		/* !!!TEMP */
  }
  write_log("*Tp:RdQsort:\n");	/* tell tui new RdQ sort */
  tq_entry_rd_dump(user);
  state = kick_next_entry_rd();	/* see if can start an entry */
    				/* poff set if entry removed from q */
  free(user);
  write_time(); 
  /*write_log("kick_next_entry_rd() ret w/status=%d for tapeid=%s\n", */
  /*  		state, state ?  poff->tapeid : "NA"); */
  switch(state) {
  case 0:		    /* can't process now, remains on q */
    return((KEY *)1);
    break;
  case 1:		    /* entry started and removed from q */
    return((KEY *)1);
    break;
  case 2:		    /* removed from q, error occured */
    setkey_int(&poff->list, "STATUS", 1); /* give error back to caller */
    setkey_fileptr(&poff->list, "current_client", (FILE *)getkey_fileptr(params, "current_client"));
    current_client = clntsum;
    procnum = SUMRESPDO;
    return(poff->list);
    break;
  }
  return((KEY *)1);  /* just get rid of compiler warning */
}

/* Called by tapearc doing: clnt_call(clnttape, WRITEDO, ...) 
 * when it has a SUM storage unit(s) to write to tape.
 * Any call is storage units for the same group_id.
 * The input keylist looks like:
 * reqcnt:	KEYTYP_INT	3
 * total_bytes: KEYTYP_DOUBLE   	   4.800000e+08
 * username_2:	KEYTYP_STRING	jim
 * ds_index_2:	KEYTYP_UINT64	376
 * safe_id_2:	KEYTYP_INT	0
 * group_id_2:	KEYTYP_INT	99
 * archsub_2:	KEYTYP_INT	128
 * status_2:	KEYTYP_INT	4
 * bytes_2:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_2:	KEYTYP_UINT64	408
 * effective_date_2:	KEYTYP_STRING	200504281119
 * wd_2:	KEYTYP_STRING	/SUM3/D376
 * username_1:	KEYTYP_STRING	jim
 * ds_index_1:	KEYTYP_UINT64	378
 * safe_id_1:	KEYTYP_INT	0
 * group_id_1:	KEYTYP_INT	99
 * archsub_1:	KEYTYP_INT	128
 * status_1:	KEYTYP_INT	4
 * bytes_1:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_1:	KEYTYP_UINT64	409
 * effective_date_1:	KEYTYP_STRING	200504281119
 * wd_1:	KEYTYP_STRING	/SUM5/D378
 * DEBUGFLG:	KEYTYP_INT	1
 * username_0:	KEYTYP_STRING	jim
 * ds_index_0:	KEYTYP_UINT64	379
 * safe_id_0:	KEYTYP_INT	0
 * group_id_0:	KEYTYP_INT	99
 * archsub_0:	KEYTYP_INT	128
 * status_0:	KEYTYP_INT	4
 * bytes_0:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_0:	KEYTYP_UINT64	410
 * effective_date_0:	KEYTYP_STRING	200504281119
 * wd_0:	KEYTYP_STRING	/SUM1/D379
 * OP:		KEYTYP_STRING   wt	 (NOTE: added here)
 * group_id:    KEYTYP_INT      99       (NOTE: added here)
 * availblocks: KEYTYP_UINT64   48828000 (NOTE: added here) 
 * tape_closed: KEYTYP_INT      -1       (NOTE: added here)
 * tapeid:      KEYTYP_STRING   000014S  (NOTE: added here)
*/
KEY *writedo_1(KEY *params) {
  static CLIENT *clresp;
  TAPE tapeinfo;
  TQ *p;
  int group_id, state, snum, dnum;
  int tapearcvers;
  char *tapeid, *user;
  uint64_t sumid, dsix;
  double total_bytes;

  StartTimer(5); //!!TEMP do time till send ack to caller
  setkey_str(&params, "OP", "wt");
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("writedo_1() call in tape_svc. keylist is:\n");
      keyiterate(logkey, params);
    }
  }
  user = getkey_str(params, "username_0");
  sumid = getkey_uint64(params, "sumid_0");
  dsix = getkey_uint64(params, "ds_index_0");
  group_id = getkey_int(params, "group_id_0");
  total_bytes = getkey_double(params, "total_bytes");
  tapearcvers = getkey_int(params, "tapearcvers");
  if(!(clresp = set_client_handle(TAPEARCPROG, tapearcvers))) { 
    rinfo = NO_CLNTTCP_CREATE;  /* give err status back to original caller */
    send_ack();
    ftmp = StopTimer(5);
    free(user);
    return((KEY *)1);  /* error. nothing to be sent */
  }
  setkey_fileptr(&params, "current_client", (FILE *)clresp);
  current_client = clresp;	/* set for call to tapearc */
  procnum = TAPEARCDO;		/* for this proc number */
  setkey_uint32( &params, "procnum", procnum);

  /* get a tapeid for this group with enough storage to write to */
  /* will get the same tapeid until the tape fills up and is closed */
  if(SUMLIB_TapeFindGroup(group_id, total_bytes, &tapeinfo)) {
    rinfo = NO_TAPE_IN_GROUP;  /* give err status back to original caller */
    send_ack();
    ftmp = StopTimer(5);
    free(user);
    return((KEY *)1);  /* error. nothing to be sent */
  }
#ifdef SUMDC
  if((snum=tapeinslot(tapeinfo.tapeid)) == -1) {    /* tape not in any slot */
    if((dnum=tapeindrive(tapeinfo.tapeid)) == -1) { /* tape not in any drive */
      rinfo = NO_TAPE_IN_GROUP;  /* give err status back to original caller */
      send_ack();
      ftmp = StopTimer(5);
      write_log("Tape %s not in T50", tapeinfo.tapeid);
      free(user);
      return((KEY *)1);  /* error. nothing to be sent */
    }
  }
#endif
  setkey_int(&params, "sim", sim);
  setkey_int(&params, "group_id", tapeinfo.group_id);
  setkey_int(&params, "tape_closed", tapeinfo.closed);
  setkey_uint64(&params, "availblocks", tapeinfo.availblocks);
  tapeid = tapeinfo.tapeid;
  setkey_str(&params, "tapeid", tapeid);
  //write_log("In writedo_1() tapeid = %s\n", tapeid); /* !!TEMP */
  if((p=q_entry_make(params, sumid, tapeid, 0, user, dsix))==NULL) {
      write_log("**Err: can't malloc a new wrt Q entry\n");
      rinfo = 1;  /* give err status back to original caller */
      send_ack();
      ftmp = StopTimer(5);
      free(user);
      return((KEY *)1);  /* error. nothing to be sent */
  }
  write_log("*Tp:WtQadd: uid=%lu tapeid=%s filenum=0 user=%s dsix=%lu\n",
                sumid, tapeid, user, dsix);
  free(user);
  insert_tq_entry_wrt(p);    /* put at end of wrt q */

  state = kick_next_entry_wt(); /* see if can start an entry */
                                /* poff set if entry removed from q */
  write_time();
  /*write_log("kick_next_entry_wt() ret w/status=%d for tapeid=%s\n", */
  /*              state, state ? poff->tapeid : "NA"); */
  switch(state) {
  case 0:		/* can't process now, remains on q */
    rinfo = RESULT_PEND;    /* tell caller to wait later for results */
    send_ack();
    ftmp = StopTimer(5);
    //write_log("Time 5 for send_ack() in tape_svc back to tapearc = %f sec\n", ftmp);
    return((KEY *)1);
    break;
  case 1:		/* entry started and removed from q */
    rinfo = RESULT_PEND;    /* tell caller to wait later for results */
    send_ack();
    ftmp = StopTimer(5);
    //write_log("Time 5 for send_ack() in tape_svc back to tapearc = %f sec\n", ftmp);
    return((KEY *)1);
    break;
  case 2:		/* removed from q, error occured */
    rinfo = 0;
    send_ack();
    ftmp = StopTimer(5);
    //write_log("Time 5 for send_ack() in tape_svc back to tapearc = %f sec\n", ftmp);
    setkey_int(&poff->list, "STATUS", 1); /* give error back to caller */
    return(poff->list);
    break;
  default:
    rinfo = 0;
    send_ack();
    ftmp = StopTimer(5);
    //write_log("Time 5 for send_ack() in tape_svc back to tapearc = %f sec\n", ftmp);
    write_log("Error: kick_next_entry_wt() ret = %d invalid\n", state);
    return((KEY *)1);
    break;
  }
}

/* This is called from jmtx doing a JMTXTAPEDO call to tape_svc.
 * It has the params to do an mtx call under the auspices of SUMS
 * so that SUMS knows what tapes were moved.
 * The keylist looks like:
 * mode:         KEYTYP_STRING   status | load | unload
 * slotnum:      KEYTYP_INT      234
 * drivenum:     KEYTYP_INT      0
*/
KEY *jmtxtapedo_1(KEY *params) {
  static CLIENT *clresp;
  int slotnum, drivenum, status;
  uint64_t robotback;
  char cmd[80], errstr[80];
  char *call_err, *mode;
  KEY *xlist;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("jmtxtapedo_1() call in tape_svc. keylist is:\n");
      keyiterate(logkey, params);
    }
  }
  mode = getkey_str(params, "mode");
  slotnum = getkey_int(params, "slotnum");
  drivenum = getkey_int(params, "drivenum");
  xlist = newkeylist();
  if(!(clresp = set_client_handle(JMTXPROG, JMTXVERS))) { 
    rinfo = NO_CLNTTCP_CREATE;  /* give err status back to original caller */
    send_ack();
    free(mode);
    return((KEY *)1);  /* error. nothing to be sent */
  }
  setkey_fileptr(&params, "current_client", (FILE *)clresp);
  current_client = clresp;   /* set for call to jmtx */
  procnum = JMTXDO;          /* for this proc number */
  setkey_uint32( &params, "procnum", procnum);
  if(!strcmp(mode, "status")) {
    current_client_destroy = 1;
    add_keys(params, &xlist);
    sprintf(cmd, "mtx -f %s status 1> /tmp/mtx/mtx_robot_%d.log 2>&1", 
			libdevname, robotcmdseq);
    if(system(cmd)) {
      write_log("Error on: %s\n", cmd);
      setkey_int(&xlist, "STATUS", 1);
      sprintf(errstr, "Error on: %s", cmd);
      setkey_str(&xlist, "ERRSTR", errstr);
      setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
      ++robotcmdseq;
      free(mode);
      return(xlist);
    }
    write_log("jmtx status in: %s:/tmp/mtx/mtx_robot_%d.log\n", 
		hostn, robotcmdseq);
    sprintf(errstr, "jmtx Status in: %s:/tmp/mtx/mtx_robot_%d.log\n",
		hostn, robotcmdseq++);
    setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
    setkey_str(&xlist, "MSG", errstr);
    setkey_int(&xlist, "STATUS", 0);
    free(mode);
    return(xlist);
  }
  if(drives[drivenum].busy) {
    current_client_destroy = 1;
    sprintf(errstr, "Error: drive# %d is currently busy\n", drivenum);
    add_keys(params, &xlist);
    setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
    setkey_str(&xlist, "ERRSTR", errstr);
    setkey_int(&xlist, "STATUS", 1);
    free(mode);
    return(xlist);
  }

  //call robot_svc to do load/unload of a drive
  robotbusy = 1;
  drives[drivenum].busy = 1;
  rinfo = 0;
  write_log("*Tp:DrBusy: drv=%d\n", drivenum);
  add_keys(params, &xlist);
  setkey_str(&xlist, "OP", "jmtx");
  setkey_int(&xlist, "dnum", drivenum);
  setkey_int(&xlist, "snum", slotnum);
  sprintf(cmd, "mtx -f %s %s %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, mode, slotnum, drivenum, robotcmdseq++);
  setkey_str(&xlist, "cmd1", cmd);
  setkey_int(&xlist, "DEBUGFLG", 0);
  setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
  //when the robot completes it calls taperesprobotdo_1()
  status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {  /* allow timeout */
      current_client_destroy = 1;
      call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
      write_log("%s %s\n", datestring(), call_err);
      robotbusy = 0;
      drives[drivenum].busy = 0;
      rinfo = 1;
      write_log("*Tp:DrNotBusy: drv=%d\n", drivenum);
    }
  }
  if(robotback == 1) {
    current_client_destroy = 1;
    write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
    robotbusy = 0;
    drives[drivenum].busy = 0;
    rinfo = 1;
    write_log("*Tp:DrNotBusy: drv=%d\n", drivenum);
  }
  if(!rinfo) {			//no err, tell caller RESULT_PEND
    rinfo = RESULT_PEND;	//tell caller to wait later for results 
  }
  send_ack();
  free(mode);
  return((KEY *)1);
}


/* This is called from drive[0,1]_svc when a tape write completes.
 * The keylist looks like:
 * tape_closed:    KEYTYP_INT      -1
 * tapeid:         KEYTYP_STRING   000014S
 * md5cksum:	KEYTYP_STRING   5ddf4230d9566ca38a7927512244c4ae
 * gtarblock:   KEYTYP_INT      256
 * availblocks:	KEYTYP_UINT64	48828000
 * tellblock:	KEYTYP_UINT64	28000
 * STATUS:	KEYTYP_INT	0
 * group_id:	KEYTYP_INT	99
 * procnum:	KEYTYP_UINT32	1
 * current_client:	KEYTYP_FILEP	6917529027641678096
 * OP:	KEYTYP_STRING	wt
 * reqcnt:	KEYTYP_INT	3
 * username_2:	KEYTYP_STRING	jim
 * ds_index_2:	KEYTYP_UINT64	460
 * safe_id_2:	KEYTYP_INT	0
 * group_id_2:	KEYTYP_INT	99
 * archsub_2:	KEYTYP_INT	128
 * status_2:	KEYTYP_INT	4
 * bytes_2:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_2:	KEYTYP_UINT64	458
 * effective_date_2:	KEYTYP_STRING	200504281119
 * wd_2:	KEYTYP_STRING	/SUM1/D460
 * username_1:	KEYTYP_STRING	jim
 * ds_index_1:	KEYTYP_UINT64	464
 * safe_id_1:	KEYTYP_INT	0
 * group_id_1:	KEYTYP_INT	99
 * archsub_1:	KEYTYP_INT	128
 * status_1:	KEYTYP_INT	4
 * bytes_1:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_1:	KEYTYP_UINT64	460
 * effective_date_1:	KEYTYP_STRING	200504281119
 * wd_1:	KEYTYP_STRING	/SUM1/D464
 * DEBUGFLG:	KEYTYP_INT	1
 * username_0:	KEYTYP_STRING	jim
 * ds_index_0:	KEYTYP_UINT64	1523
 * safe_id_0:	KEYTYP_INT	0
 * group_id_0:	KEYTYP_INT	99
 * archsub_0:	KEYTYP_INT	128
 * status_0:	KEYTYP_INT	4
 * bytes_0:	KEYTYP_DOUBLE	           1.200000e+08
 * sumid_0:	KEYTYP_UINT64	840
 * effective_date_0:	KEYTYP_STRING	200504281119
 * wd_0:	KEYTYP_STRING	/SUM5/D1523
 * dnum:	KEYTYP_INT	0
 * snum:	KEYTYP_INT	14
 * TAPENXTFN:	KEYTYP_INT	12
 * cmd1:  KEYTYP_STRING	mtx -f /dev/sg7 load 15 0 1> /tmp/mtx_robot.log 2>&1
*/
KEY *taperespwritedo_1(KEY *params) {
  static KEY *retlist, *xlist;
  uint64_t tellblock, robotback;
  int i, dnum, status, tape_closed, filenum_written, reqcnt, slotnum;
  int tapenxtfn;
  int tclosed = 0;
  double totalbytes = 0.0;
  char *tapeid, *md5cksum, *call_err, *errstr;
  char tmpname[80], cmd[80];

  /*write_log("You've called taperespwritedo_1() in tape_svc\n"); /* !!TEMP*/
  send_ack();
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in taperespwritedo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  dnum = getkey_int(params, "dnum");	/* get drive # that wrote */
  tapenxtfn = getkey_int(params, "TAPENXTFN"); /* get next file # on tape */
  slotnum = drives[dnum].slotnum;
  drives[dnum].busy = 0;		/* drive is free now */
  write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
  drives[dnum].filenum = -1; /* reset for any read that may be next*/
  tapeid = drives[dnum].tapeid;
  if(SUMLIB_EffDateUpdate(tapeid, 1)) { /* update sum_group effective_date */
    write_log("**Error on update of sum_group effective_date for tapeid %s\n",
		tapeid);
    send_mail("Error on SUMLIB_EffDateUpdate(%s, 1)", tapeid);
    /* continue anyway */
  }
  retlist = newkeylist();
  add_keys(params, &retlist);
  /* s/b tapearc */
  current_client = (CLIENT *)getkey_fileptr(params, "current_client");
  current_client_destroy = 1;
  procnum = getkey_uint32(params, "procnum"); /* s/b TAPEARCDO */
  tape_closed = getkey_int(params, "tape_closed");
  tellblock = getkey_uint64(params, "tellblock"); /* set by sender */

  status = getkey_int(params, "STATUS");
  if(status) {			/* the write operation had an error */
    write_log("***Error: write of tape %s gave an error return\n", tapeid);
    errstr = GETKEY_str(params, "ERRSTR");
    write_log("***%s\n", errstr);
    if(status != TAPECLOSEDREJECT) {	/* tape not already closed */
      write_log("***Tape %s is being closed\n", tapeid);
      send_mail("Error on write of tapeid %s", tapeid);
      if(SUMLIB_TapeClose(tapeid)) {
        send_mail("Error: Can't close tapeid %s", tapeid);
        write_log("***Error: Can't close tape %s. Tape may be corrupted!\n",
			tapeid);
      }
#ifdef SUMDC
      /* for datacapture system - make .md5 file */
      if(SUMLIB_MD5info(tapeid)) {
        send_mail("Error: Can't make MD5 file for tape %s", tapeid);
        write_log("***Error: Can't make MD5 file for tape %s\n", tapeid);
        /* proceed. we will have to make an MD5 file offline (?) */
      }
      /* put closed tape back in its slot now */
      xlist = newkeylist();
      robotbusy = 1;
      drives[dnum].busy = 1;
      write_log("*Tp:DrBusy: drv=%d\n", dnum);
      setkey_str(&xlist, "OP", "mv");
      setkey_int(&xlist, "dnum", dnum);
      setkey_int(&xlist, "snum", slotnum);
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, slotnum+1, dnum, robotcmdseq++);
      setkey_str(&xlist, "cmd1", cmd);
      setkey_int(&xlist, "DEBUGFLG", 0);
      setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);

    StartTimer(1); //!!TEMP
      status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    ftmp = StopTimer(1);
    write_log("Time 1 for ROBOTDO in tape_svc = %f sec\n", ftmp);
      if(status != RPC_SUCCESS) {
        if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
          call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
          write_log("%s %s\n", datestring(), call_err);
          robotbusy = 0;
          drives[dnum].busy = 0;
          write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
        }
      }
      if(robotback == 1) {
        write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
        robotbusy = 0;
        drives[dnum].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
      }
#endif
    }
  }
  else {			/* write ok, update DB */
    if(tape_closed == TAPEUNINIT) { /* now make tape active */
      if(SUMLIB_TapeActive(tapeid)) {
        send_mail("Error: Can't make active tapeid %s", tapeid);
        write_log("***Error: Can't make active tape %s. Tape is unusable.\n",
			tapeid);
        setkey_int(&retlist, "STATUS", 1);
        return(retlist);
      }
    }
    md5cksum = GETKEY_str(retlist, "md5cksum");
    write_log("Tape write md5cksum = %s\n", md5cksum);
/*#ifdef SUMDC*/
/* NOTE: the tellblock wasn't accurate. So always use totalbytes */
    /* NOTE: There is no mt tell cmd in the data capture system. 
     * tellblock is always 0 here. We will instead send the SUMLIB_TapeUpdate()
     * function a byte count for the current tape file. It will then update
     * the sum_tape db table accordingly.
    */
    reqcnt = getkey_int(params, "reqcnt");
    for(i=0; i < reqcnt; i++) {
      sprintf(tmpname, "bytes_%d", i);
      totalbytes += getkey_double(params, tmpname);
    }
    tellblock = 0;		/* force use of totalbytes */
/*#endif*/
    if((filenum_written=SUMLIB_TapeUpdate(tapeid,tapenxtfn,tellblock,totalbytes)) == 0) {
      send_mail("Error: Can't update sum_tape in DB for tape %s\n",tapeid);
      write_log("***Error: Can't update sum_tape in DB for tape %s\n",tapeid);
      setkey_int(&retlist, "STATUS", 1);
      return(retlist);
    }
    if(filenum_written < 0) {		/* neg means tape was closed */ 
      filenum_written = -filenum_written;
      tclosed = 1;
    }
    setkey_int(&retlist, "nxtwrtfn", filenum_written);
    /* update sum_main and sum_partn_alloc and sum_file db tables */
    if(SUMLIB_MainTapeUpdate(retlist)) {
      send_mail("Error: sum_main update failed after write to %s\n", tapeid);
      write_log("***Error: sum_main update failed after write to %s\n",tapeid);
      setkey_int(&retlist, "STATUS", 1);
    }
#ifdef SUMDC
    if(tclosed) {			/* tape was closed */
      /* for datacapture system - make .md5 file */
      if(SUMLIB_MD5info(tapeid)) {
        send_mail("Error: Can't make MD5 file for tape %s", tapeid);
        write_log("***Error: Can't make MD5 file for tape %s\n", tapeid);
        /* proceed. we will have to make an MD5 file offline (?) */
      }
      /* for datacapture, put closed tape back in its slot now */
      xlist = newkeylist();
      robotbusy = 1;
      drives[dnum].busy = 1;
      write_log("*Tp:DrBusy: drv=%d\n", dnum);
      setkey_str(&xlist, "OP", "mv");
      setkey_int(&xlist, "dnum", dnum);
      setkey_int(&xlist, "snum", slotnum);
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, slotnum+1, dnum, robotcmdseq++);
      setkey_str(&xlist, "cmd1", cmd);
      setkey_int(&xlist, "DEBUGFLG", 0);
      setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);

    StartTimer(2); //!!TEMP
      status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    ftmp = StopTimer(2);
    write_log("Time 2 for ROBOTDO in tape_svc = %f sec\n", ftmp);
      if(status != RPC_SUCCESS) {
        if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
          call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
          write_log("%s %s\n", datestring(), call_err);
          robotbusy = 0;
          drives[dnum].busy = 0;
          write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
        }
      }
      if(robotback == 1) {
        write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
        robotbusy = 0;
        drives[dnum].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
      }
      freekeylist(&xlist);
    }
#endif
  }
  return(retlist);
}

/* This is called from drive[0,1]_svc when a tape read completes 
 * successfully. A typical keylist is:
 * STATUS:	KEYTYP_INT	0
 * reqofflinenum:	KEYTYP_INT	2
 * sim:	KEYTYP_INT	0
 * OP:	KEYTYP_STRING	rd
 * offcnt:	KEYTYP_INT	1
 * rootwd_2:	KEYTYP_STRING	/SUM0/D7853
 * partn_name:	KEYTYP_STRING	/SUM0/D7853
 * ds_index:	KEYTYP_UINT64	7853
 * offline:	KEYTYP_INT	1
 * tapefilenum_2:	KEYTYP_INT	1
 * tapeid_2:	KEYTYP_STRING	000003S1
 * wd_2:	KEYTYP_STRING	/SUM0/D7853/D2233
 * ds_index_2:	KEYTYP_UINT64	2233
 * create_sumid_2:	KEYTYP_UINT64	623
 * bytes_2:	KEYTYP_DOUBLE	           1.200000e+08
 * archive_status_2:	KEYTYP_STRING	Y
 * online_status_2:	KEYTYP_STRING	N
 * tapefilenum_1:	KEYTYP_INT	1
 * tapeid_1:	KEYTYP_STRING	000002S1
 * wd_1:	KEYTYP_STRING	/SUM0/D2918/D2232
 * ds_index_1:	KEYTYP_UINT64	2232
 * create_sumid_1:	KEYTYP_UINT64	623
 * bytes_1:	KEYTYP_DOUBLE	           1.200000e+08
 * archive_status_1:	KEYTYP_STRING	Y
 * online_status_1:	KEYTYP_STRING	Y
 * tapefilenum_0:	KEYTYP_INT	1
 * tapeid_0:	KEYTYP_STRING	000001S1
 * wd_0:	KEYTYP_STRING	/SUM0/D2917/D2231
 * ds_index_0:	KEYTYP_UINT64	2231
 * create_sumid_0:	KEYTYP_UINT64	623
 * bytes_0:	KEYTYP_DOUBLE	           1.200000e+08
 * archive_status_0:	KEYTYP_STRING	Y
 * online_status_0:	KEYTYP_STRING	Y
 * current_client:	KEYTYP_FILEP	6346176
 * uid:	KEYTYP_UINT64	43055
 * mode:	KEYTYP_INT	16
 * tdays:	KEYTYP_INT	5
 * reqcnt:	KEYTYP_INT	3
 * DEBUGFLG:	KEYTYP_INT	1
 * REQCODE:	KEYTYP_INT	4
 * username:	KEYTYP_STRING	production
 * dsix_0:	KEYTYP_UINT64	2231
 * dsix_1:	KEYTYP_UINT64	2232
 * dsix_2:	KEYTYP_UINT64	2233
 * dnum:	KEYTYP_INT	0
 * tapemode:	KEYTYP_INT	1
 * filenum:	KEYTYP_INT	1
*/
KEY *taperespreaddo_1(KEY *params) {
  CLIENT *client;
  SUMOFFCNT *offptr;
  SUMID_t uid;
  static KEY *retlist;
  int dnum, reqofflinenum, tapefilenum, status;
  char tmpname[80];
  char *rwd, *tapeid;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in taperespreaddo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  dnum = getkey_int(params, "dnum");
  status = getkey_int(params, "STATUS");
  drives[dnum].busy = 0;		/* drive is free now */
  write_log("*Tp:DrNotBusy: drv=%d\n", dnum);
  reqofflinenum = getkey_int(params, "reqofflinenum");
  sprintf(tmpname, "tapefilenum_%d", reqofflinenum);
  tapefilenum = getkey_int(params, tmpname);
  drives[dnum].filenum = tapefilenum;  /* file# just read */
  if(status) drives[dnum].filenum = -1; /* error, next read will rewind */
  sprintf(tmpname, "rootwd_%d", reqofflinenum);
  rwd = GETKEY_str(params, tmpname);
  //sprintf(tmpname, "sudo chmod -R go-w %s; sudo chown -Rf production %s", 
  //			rwd, rwd);
  sprintf(tmpname, "%s/sum_chmown %s", SUMBIN_BASEDIR, rwd);
  if(system(tmpname)) {
      write_log("**Warning: Error on: %s\n", tmpname);
  }
  sprintf(tmpname, "tapeid_%d", reqofflinenum);
  tapeid = GETKEY_str(params, tmpname);
  if(SUMLIB_EffDateUpdate(tapeid, 0)) { /* update sum_group effective_date */
    write_log("**Error on update of sum_group effective_date for tapeid %s\n",
		tapeid);
    send_mail("Error on SUMLIB_EffDateUpdate(%s, 0)", tapeid);
    /* continue anyway */
  }
  retlist = newkeylist();
  add_keys(params, &retlist);
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  current_client = clntsum;
  procnum = SUMRESPDO;
  send_ack();
  uid = getkey_uint64(params, "uid");
  offptr = getsumoffcnt(offcnt_hdr, uid);
  offptr->offcnt++;
  if(offptr->uniqcnt == offptr->offcnt) { /* now can send completion msg back */
    write_log("!!TEMP remsumoffcnt uid=%lu\n", uid); //!!TEMP
    remsumoffcnt(&offcnt_hdr, uid);
    if(DS_DataRequest_WD(retlist, &retlist)) { /* get all wd's */
      write_log("***Err: DS_DataRequest_WD() returned error.\n");
      setkey_int(&retlist, "STATUS", 1);
    }
    return(retlist);
  }
  return((KEY *)1);	/* our caller will try to kick off another read */
}

/* This is called from robot[0,1]_svc when a robot operation completes 
 * successfully. The keylist looks like:
 * availblocks:    KEYTYP_UINT64   48828000
 * STATUS: KEYTYP_INT      0
 * group_id:       KEYTYP_INT      99
 * procnum:        KEYTYP_UINT32    1
 * current_client: KEYTYP_FILEP    6917529027641678096
 * OP:     KEYTYP_STRING   wt
 * reqcnt: KEYTYP_INT      3
 * username_2:     KEYTYP_STRING   jim
 * ds_index_2:     KEYTYP_UINT64    460
 * safe_id_2:      KEYTYP_INT      0
 * group_id_2:     KEYTYP_INT      99
 * archsub_2:      KEYTYP_INT      128
 * status_2:       KEYTYP_INT      4
 * bytes_2:        KEYTYP_DOUBLE              1.200000e+08
 * sumid_2:        KEYTYP_UINT64    458
 * effective_date_2:       KEYTYP_STRING   200504281119
 * wd_2:   KEYTYP_STRING   /SUM1/D460
 * username_1:     KEYTYP_STRING   jim
 * ds_index_1:     KEYTYP_UINT64    464
 * safe_id_1:      KEYTYP_INT      0
 * group_id_1:     KEYTYP_INT      99
 * archsub_1:      KEYTYP_INT      128
 * status_1:       KEYTYP_INT      4
 * bytes_1:        KEYTYP_DOUBLE              1.200000e+08
 * sumid_1:        KEYTYP_UINT64    460
 * effective_date_1:       KEYTYP_STRING   200504281119
 * wd_1:   KEYTYP_STRING   /SUM1/D464
 * DEBUGFLG:       KEYTYP_INT      1
 * username_0:     KEYTYP_STRING   jim
 * ds_index_0:     KEYTYP_UINT64    1523
 * safe_id_0:      KEYTYP_INT      0
 * group_id_0:     KEYTYP_INT      99
 * archsub_0:      KEYTYP_INT      128
 * status_0:       KEYTYP_INT      4
 * bytes_0:        KEYTYP_DOUBLE              1.200000e+08
 * sumid_0:        KEYTYP_UINT64    840
 * effective_date_0:       KEYTYP_STRING   200504281119
 * wd_0:   KEYTYP_STRING   /SUM5/D1523
 * dnum:   KEYTYP_INT      0
 * snum:   KEYTYP_INT      14
 * cmd1:   KEYTYP_STRING   mtx -f /dev/sg7 unload 14 0
 * cmd2:   KEYTYP_STRING   mtx -f /dev/sg7 load 15 0
*/
KEY *taperesprobotdo_1(KEY *params) {
  static KEY *retlist;
  CLIENT *client;
  int s, d, status;
  char scr[80];
  char *cptr, *cmd;

  robotbusy = 0;
  send_ack();
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in taperesprobotdo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  cptr = GETKEY_str(params, "OP");
  if(!strcmp(cptr, "rd")) {
    /* This robot completion is for a read operation */
    retlist = taperesprobotdo_1_rd(params);
  }
  else if(!strcmp(cptr, "wt")) {
    /* This robot completion is for a write operation */
    retlist = taperesprobotdo_1_wt(params);
  }
  else if(!strcmp(cptr, "mv")) {
    /* nothing to do after a tape mv from drive back to its slot */
    d = getkey_int(params, "dnum");
    s = getkey_int(params, "snum");
    slots[s].tapeid = drives[d].tapeid;
    drives[d].tapeid = NULL;
    drives[d].slotnum = -1;
    drives[d].tapemode = TAPE_NOT_LOADED;
    drives[d].filenum = 0;
    drives[d].busy = 0;
    write_log("*Tp:DrNotBusy: drv=%d\n", d);
    retlist = (KEY *)1;
  }
  else if(!strcmp(cptr, "jmtx")) {
    status = getkey_int(params, "STATUS");
    if(findkey(params, "cmd1")) {
      cmd = GETKEY_str(params, "cmd1");
      if(strstr(cmd, " load ")) {  /* cmd like: mtx -f /dev/sg7 load 14 1 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       if(status == 0) {
         //write_log("jmtx load cmd1 s=%d d=%d\n", s, d);
         drives[d].tapeid = slots[s].tapeid;
         drives[d].slotnum = s;
         slots[s].tapeid = NULL; 
       }
      }
      if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       if(status == 0) {
         //write_log("jmtx unload cmd1 s=%d d=%d\n", s, d);
         slots[s].tapeid = drives[d].tapeid; 
         drives[d].tapeid = NULL;
         drives[d].slotnum = -1;
       }
      }
      drives[d].busy = 0;
      retlist = newkeylist();
      add_keys(params, &retlist);           /* NOTE:does not do fileptr */
      current_client = (CLIENT *)getkey_fileptr(params, "current_client");
      current_client_destroy = 1;
      /* final destination */
      setkey_fileptr(&retlist,  "current_client", (FILE *)current_client);
      procnum = getkey_uint32(params, "procnum");
      return(retlist);
    }
  }
  else if(!strcmp(cptr, "clean")) {
    if(findkey(params, "cmd1")) {
      cmd = GETKEY_str(params, "cmd1");
      if(strstr(cmd, " load ")) {  /* cmd like: mtx -f /dev/sg7 load 14 1 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       /*write_log("!!!TEMP load cmd1 s=%d d=%d\n", s, d);*/
       drives[d].tapeid = slots[s].tapeid;
       drives[d].slotnum = s;
       slots[s].tapeid = NULL; 
      }
      if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       /*write_log("!!!TEMP unload cmd1 s=%d d=%d\n", s, d);*/
       slots[s].tapeid = drives[d].tapeid; 
       drives[d].tapeid = NULL;
       drives[d].slotnum = -1;
      }
    }
    if(findkey(params, "cmd2")) {
      cmd = GETKEY_str(params, "cmd2");
      if(strstr(cmd, " load ")) {  /* cmd like: mtx -f /dev/sg7 load 14 1 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       /*write_log("!!!TEMP load cmd2 s=%d d=%d\n", s, d);*/
       drives[d].tapeid = slots[s].tapeid;
       drives[d].slotnum = s;
       slots[s].tapeid = NULL; 
      }
      if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       /*write_log("!!!TEMP unload cmd2 s=%d d=%d\n", s, d);*/
       slots[s].tapeid = drives[d].tapeid; 
       drives[d].tapeid = NULL;
       drives[d].slotnum = -1;
      }
    }
    write_log("*Tp:CleanInProgress: drv=%d\n", d);
    // next thing is a call to IMPEXPDO by impexp for clean_stop
    retlist = (KEY *)1;
  }
  else if(!strcmp(cptr, "clean_robot_unload")) {
    if(findkey(params, "cmd1")) {
      cmd = GETKEY_str(params, "cmd1");
      if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
       sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
       s--;			/* must use internal slot # */
       /*write_log("!!!TEMP unload cmd1 s=%d d=%d\n", s, d);*/
       slots[s].tapeid = drives[d].tapeid; 
       drives[d].tapeid = NULL;
       drives[d].slotnum = -1;
      }
    }
    write_log("*Tp:CleaningDone: drv=%d slot=%d\n", d, s+1);
    drives[d].busy = 0;
    write_log("*Tp:DrNotBusy: drv=%d\n", d);
    retlist = (KEY *)1;
  }
  return(retlist);
}

/* Called from taperesprobotdo_1() to start a read operation after a 
 * robot operation completes. See taperesprobotdo_1() for sample keylist.
*/
KEY *taperesprobotdo_1_rd(KEY *params) {
  CLIENT *client;
  SUMID_t driveback;
  static struct timeval TIMEOUT = { 120, 0 };
  static KEY *retlist;
  char *call_err, *cmd;
  char scr[80];
  enum clnt_stat status;
  int dnum, snum, stat, d, s;

  dnum = getkey_int(params, "dnum");
  snum = getkey_int(params, "snum");
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  if(stat=getkey_int(params, "STATUS")) {	/* error in robot */
    write_log("**Error return from robot_svc for read op.\n");
    current_client = clntsum;
    procnum = SUMRESPDO;
    return(retlist);
  }
  if(findkey(params, "cmd1")) {
    cmd = GETKEY_str(params, "cmd1");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg7 load 14 1 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP load cmd1 s=%d d=%d\n", s, d);*/
     drives[d].tapeid = slots[s].tapeid;
     drives[d].slotnum = s;
     drives[d].tapemode = TAPE_RD_INIT;
     drives[d].filenum = 0;
     slots[s].tapeid = NULL; 
    }
    if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP unload cmd1 s=%d d=%d\n", s, d);*/
     slots[s].tapeid = drives[d].tapeid; 
     drives[d].tapeid = NULL;
     drives[d].slotnum = -1;
     drives[d].tapemode = TAPE_NOT_LOADED;
     drives[d].filenum = 0;
    }
  }
  if(findkey(params, "cmd2")) {
    cmd = GETKEY_str(params, "cmd2");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg7 load 14 1 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP load cmd2 s=%d d=%d\n", s, d);*/
     drives[d].tapeid = slots[s].tapeid;
     drives[d].slotnum = s;
     drives[d].tapemode = TAPE_RD_INIT;
     drives[d].filenum = 0;
     slots[s].tapeid = NULL; 
    }
    if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP unload cmd2 s=%d d=%d\n", s, d);*/
     slots[s].tapeid = drives[d].tapeid; 
     drives[d].tapeid = NULL;
     drives[d].slotnum = -1;
     drives[d].tapemode = TAPE_NOT_LOADED;
     drives[d].filenum = 0;
    }
  }
  setkey_int(&retlist, "tapemode", drives[d].tapemode);
  setkey_int(&retlist, "filenum", drives[d].filenum);

  /* call drive[0,1]_svc and tell our caller to wait for completion */
  /*write_log("Calling: clnt_call(clntdrv%d, READDRVDO, ...) \n", dnum);*/
  status = clnt_call(clntdrv[dnum], READDRVDO, (xdrproc_t)xdr_Rkey,
         (char *)retlist, (xdrproc_t)xdr_uint32_t, (char *)&driveback, TIMEOUT);
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
      call_err = clnt_sperror(clntdrv[dnum], "Err clnt_call for READDRVDO");
      write_log("%s %s\n", datestring(), call_err);
      setkey_int(&retlist, "STATUS", 1);	/* give error back to caller */
      current_client = clntsum;
      current_client_destroy = 1;
      procnum = SUMRESPDO;
      return(retlist);
    } else {
      write_log("%s timeout occured for READDRVDO in taperesprobotdo_1_rd()\n", datestring());
    }
  }
  if(driveback == 1) {
    write_log("**Error in readdo_1() in tape_svc_proc.c\n");
    setkey_int(&retlist, "STATUS", 1);	/* give error back to caller */
    current_client = clntsum;
    current_client_destroy = 1;
    procnum = SUMRESPDO;
    return(retlist);
  }
  return((KEY *)1);		/* driven_svc will send answer later */
}

/* Called from taperesprobotdo_1() to start a write operation after a 
 * robot operation completes. See taperesprobotdo_1() for sample keylist.
*/
KEY *taperesprobotdo_1_wt(KEY *params) {
  CLIENT *client;
  SUMID_t driveback;
  static struct timeval TIMEOUT = { 120, 0 };
  static KEY *retlist;
  char *call_err, *cmd;
  char scr[80];
  enum clnt_stat status;
  int dnum, snum, stat, tape_closed, d, s, nxtwrtfn;

  dnum = getkey_int(params, "dnum");
  snum = getkey_int(params, "snum");
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  if(stat=getkey_int(params, "STATUS")) {	/* error in robot */
    write_log("**Error return from robot_svc for write op\n");
    current_client = client;
    current_client_destroy = 1;
    procnum = getkey_uint32(params, "procnum");
    return(retlist);
  }
  if(findkey(params, "cmd1")) {
    cmd = GETKEY_str(params, "cmd1");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg7 load 14 1 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP load cmd1 s=%d d=%d\n", s, d);*/
     drives[d].tapeid = slots[s].tapeid;
     drives[d].slotnum = s;
     slots[s].tapeid = NULL; 
    }
    if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP unload cmd1 s=%d d=%d\n", s, d);*/
     slots[s].tapeid = drives[d].tapeid; 
     drives[d].tapeid = NULL;
     drives[d].slotnum = -1;
    }
  }
  if(findkey(params, "cmd2")) {
    cmd = GETKEY_str(params, "cmd2");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg7 load 14 1 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP load cmd2 s=%d d=%d\n", s, d);*/
     drives[d].tapeid = slots[s].tapeid;
     drives[d].slotnum = s;
     slots[s].tapeid = NULL; 
    }
    if(strstr(cmd, " unload ")) { /* cmd like: mtx -f /dev/sg7 unload 15 0 */
     sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d); 
     s--;			/* must use internal slot # */
     /*write_log("!!!TEMP unload cmd2 s=%d d=%d\n", s, d);*/
     slots[s].tapeid = drives[d].tapeid; 
     drives[d].tapeid = NULL;
     drives[d].slotnum = -1;
    }
  }

  tape_closed = SUMLIB_TapeState(drives[dnum].tapeid); /* get anew */
  setkey_int(&retlist, "tape_closed", tape_closed);
  /* get the next file# to write on this tape. Send it to driven_svc */
  if((nxtwrtfn = SUMLIB_TapeFilenumber(drives[dnum].tapeid)) == 0) {
    write_log("Error: can't get next file# to write for tape=%s\n",
		drives[dnum].tapeid);
    setkey_int(&retlist, "STATUS", 1);	/* give error back to caller */
    current_client = client;
    current_client_destroy = 1;
    procnum = getkey_uint32(params, "procnum");
    return(retlist);
  }
  setkey_int(&retlist, "nxtwrtfn", nxtwrtfn);

  /* call drive[0,1]_svc and tell our caller to wait for completion */
  /*write_log("Calling: clnt_call(clntdrv%d, WRITEDRVDO, ...) \n", dnum);*/
  StartTimer(2); //!!TEMP
  status = clnt_call(clntdrv[dnum], WRITEDRVDO, (xdrproc_t)xdr_Rkey,
        (char *)retlist, (xdrproc_t)xdr_uint32_t, (char *)&driveback, TIMEOUT);
  ftmp = StopTimer(2);
  write_log("Time 2 for WRITEDRVDO in tape_svc = %f sec\n", ftmp);
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
      call_err = clnt_sperror(clntdrv[dnum], "Err clnt_call for WRITEDRVDO");
      write_log("%s %s\n", datestring(), call_err);
      setkey_int(&retlist, "STATUS", 1);	/* give error back to caller */
      current_client = client;
      current_client_destroy = 1;
      procnum = getkey_uint32(params, "procnum");
      return(retlist);
    } else {
      write_log("%s timeout occured for WRITEDRVDO in taperesprobotdo_1_wt()\n", datestring());
    }
  }
  if(driveback == 1) {
    write_log("**Error in writedo_1() in tape_svc_proc.c\n");
    setkey_int(&retlist, "STATUS", 1);	/* give error back to caller */
    current_client = client;
    current_client_destroy = 1;
    procnum = getkey_uint32(params, "procnum");
    return(retlist);
  }
  return((KEY *)1);		/* driven_svc will send answer later */
}

/* This is called from robot[0,1]_svc when a robot transfer to the imp/exp
 * door completes. The keylist looks like:
 * tapeid_1:       KEYTYP_STRING   013401S1
 * STATUS: KEYTYP_INT      0
 * tapeid_0:       KEYTYP_STRING   013389S1
 * reqcnt: KEYTYP_INT      2
 * cmd_0:  KEYTYP_STRING   mtx -f /dev/sg12 transfer 1 123 1> 
 * 				/tmp/mtx_robot_0.log 2>&1
 * cmd_1:  KEYTYP_STRING   mtx -f /dev/sg12 transfer 2 124 1> 
 * 				/tmp/mtx_robot_1.log 2>&1
 * OP:     KEYTYP_STRING   impexp
*/
KEY *taperesprobotdoordo_1(KEY *params) {
  int reqcnt, i, s, d, retry, istat;
  char *cmd, *cptr;
  char ext[80], scr[80];

  robotbusy = 0;
  rinfo = 0;
  send_ack();
  /*debugflg = getkey_int(params, "DEBUGFLG");*/
  if(debugflg) {
    write_log("!!Keylist in taperesprobotdoordo_1() is:\n");
    keyiterate(logkey, params);
  }
  reqcnt = getkey_int(params, "reqcnt");
  for(i=0; i < reqcnt; i++) {
    sprintf(ext, "cmd_%d", i);
    cmd = GETKEY_str(params, ext); /*cmd like: mtx -f /dev/sg12 transfer 2 124*/
    sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d);
    /*write_log("!!TEMP transfer cmd s=%d d=%d\n", s, d);*/
    s--;                       /* must use internal slot # */
    d--;                       /* must use internal slot # */
    slots[d].tapeid = slots[s].tapeid;
    slots[s].tapeid = NULL;
  }

#ifdef SUMDC
  cptr = GETKEY_str(params, "OP");
  if(!strcmp(cptr, "door")) {	 /* tapes were moved from door to slots */
    /* now get new inventory */
    retry = 6;
    while(retry) {
      if((istat = tape_inventory(sim, 0)) == 0) { /* no catalog here */
        write_log("***Error: Can't do tape inventory. Will retry...\n");
        --retry;
        if(retry == 0) {
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
        continue;
      }
      if(istat == -1) {   /* didn't get full slot count. retry */
        --retry;
        if(retry == 0) {
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
      }
      else { retry = 0; }
    }
  }
#endif
  return((KEY *)1);
}

/* Called when tui schedules the impexp program and it does a clnt_call to
 * TAPEPROG, TAPEVERS for IMPEXPDO. This is done when tui gets a request 
 * to unload tapes into the imp/exp door. Calls the robot_svc to unload 
 * the given tapeids (start option). This is also called again when the 
 * operator has indicated that the Bulk Load is done (stop option).
 * Also called for a drive cleaning operation (clean option) where
 * keywords cleanslot = slot # with cleaning tape, cleandrive = drive #
 * to clean.
 * The keylist looks like:
 * tapeid_1:       KEYTYP_STRING   013401S1
 * tapeid_0:       KEYTYP_STRING   013389S1
 * OP:             KEYTYP_STRING   start (or stop)
 * reqcnt: KEYTYP_INT      2
 *
 * or for clean:
 * cleanslot:      KEYTYP_INT   25
 * cleandrive:     KEYTYP_INT   0
 * OP:             KEYTYP_STRING   clean
*/
KEY *impexpdo_1(KEY *params)
{
  TQ *p;
  static KEY *retlist, *xlist;
  enum clnt_stat status;
  char *tid, *call_err, *op, *tapeid;
  char ext[64], cmd[128];
  uint32_t robotback;
  int reqcnt, i, j, snum, dnum, eeslot, retry, istat;
  int count_impexp_full, count_slots_empty, cleanslot, cleandrive;
  int slotnum;

  /*write_log("!!Keylist in impexpdo_1() is:\n");*/
  /*keyiterate(logkey, params);*/
  poff = NULL;
  rinfo = 0;
  op = getkey_str(params, "OP");
  if(!strcmp(op, "clean_start")) {	/* do drive cleaning */
    cleanslot = atoi(GETKEY_str(params, "cleanslot"));
    cleandrive = atoi(GETKEY_str(params, "cleandrive"));
    write_log("cleanslot=%d cleandrive=%d\n", cleanslot, cleandrive);
    xlist = newkeylist();
    robotbusy = 1;
    drives[cleandrive].busy = 1;
    write_log("*Tp:DrBusy: drv=%d\n", cleandrive);
    setkey_str(&xlist, "OP", "clean");
    setkey_int(&xlist, "dnum", cleandrive);
    setkey_int(&xlist, "snum", slotnum);
    if(tapeid = drives[cleandrive].tapeid) { /* unload the current tape */
      slotnum = drives[cleandrive].slotnum;
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, slotnum+1, cleandrive, robotcmdseq++);
      setkey_str(&xlist, "cmd1", cmd);
      sprintf(cmd, "mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
		libdevname, cleanslot, cleandrive, robotcmdseq++);
      setkey_str(&xlist, "cmd2", cmd);
    }
    else {			/* just load the cleaning tape */
      sprintf(cmd, "mtx -f %s load %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, cleanslot, cleandrive, robotcmdseq++);
      setkey_str(&xlist, "cmd1", cmd);
    }
    setkey_int(&xlist, "DEBUGFLG", 0);
    setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
    status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
        call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
        write_log("%s %s\n", datestring(), call_err);
        robotbusy = 0;
        drives[cleandrive].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", cleandrive);
      }
    }
    if(robotback == 1) {
      write_log("**Error in ROBOTDO for impexp clean call in tape_svc_proc.c\n");
      robotbusy = 0;
      drives[cleandrive].busy = 0;
      write_log("*Tp:DrNotBusy: drv=%d\n", cleandrive);
    }
    freekeylist(&xlist);
    free(op);
    send_ack();			/* ack original impexp caller */
    return((KEY *)1);		/* nothing will be sent later */
  }
  if(!strcmp(op, "clean_stop")) {	/* drive cleaning done */
    cleandrive = atoi(GETKEY_str(params, "cleandrive"));
    cleanslot = atoi(GETKEY_str(params, "cleanslot"));
    xlist = newkeylist();		/* now unload the cleaning tape*/
    robotbusy = 1;
    drives[cleandrive].busy = 1;
    write_log("*Tp:DrBusy: drv=%d\n", cleandrive);
    setkey_str(&xlist, "OP", "clean_robot_unload");
    setkey_int(&xlist, "dnum", cleandrive);
    setkey_int(&xlist, "snum", cleanslot);
    sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, cleanslot, cleandrive, robotcmdseq++);
    setkey_str(&xlist, "cmd1", cmd);
    setkey_int(&xlist, "DEBUGFLG", 0);
    setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);
    status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
        call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
        write_log("%s %s\n", datestring(), call_err);
        robotbusy = 0;
        drives[cleandrive].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", cleandrive);
      }
    }
    if(robotback == 1) {
      write_log("**Error in ROBOTDO for impexp clean call in tape_svc_proc.c\n");
      robotbusy = 0;
      drives[cleandrive].busy = 0;
      write_log("*Tp:DrNotBusy: drv=%d\n", cleandrive);
    }
    freekeylist(&xlist);
    free(op);
    send_ack();			/* ack original impexp caller */
    return((KEY *)1);		/* nothing will be sent later */
  }
  if(!strcmp(op, "stop")) {	/* end of imp/exp cycle */
    eeactive = 0;	/* enable the Q code again */
    send_ack();
    write_log("*Tp:BulkLoadDone\n");
    retry = 6;
    while(retry) {
#ifdef SUMDC
    istat = tape_inventory(sim, 1); /* and catalog too */
#else
    istat = tape_inventory(sim, 0); /* eventually '1' here when t120/t50 are*/
				    /* the same tape types */
#endif
      if(istat == 0) { 
        write_log("***Error: Can't do tape inventory. Will retry...\n");
        --retry;
        if(retry == 0) {
          write_log("***Inv: failure\n\n");
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
        continue;
      }
      if(istat == -1) {   /* didn't get full slot count. retry */
        --retry;
        if(retry == 0) {
          write_log("***Inv: failure\n\n");
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
      }
      else { retry = 0; }
    }

/*****************************************************************
/*!!!!TBD partial t50 changes:
*/
#ifdef SUMDC
    /* NEW for t50: now move the new tapes in door into empty slots */
    write_log("*Tp:t50BulkLoadDone\n");	 /* tell tui we need next window */
    count_impexp_full = 0; count_slots_empty = 0;
    xlist = newkeylist();
    robotbusy = 1;
    setkey_str(&xlist, "OP", "door");
    for(j=0, i=(MAX_SLOTS - NUM_IMP_EXP_SLOTS); i < MAX_SLOTS; j++, i++) {
      if(slots[i].tapeid != NULL) {
        full_impexp_slotnum_internal[j] = i;
        count_impexp_full++;
      }
      else full_impexp_slotnum_internal[j] = -1;
    }
    for(i=0, j=0; i < MAX_SLOTS_LIVE; i++) {
      if(!slots[i].tapeid) {
        count_slots_empty++;
        if(count_slots_empty > count_impexp_full) {
          /*write_log("WARN: More empty slots than new tapes to import\n");*/
        }
        empty_slotnum_internal[j++] = i;
      }
    }
    for(i=0, j=0; i < NUM_IMP_EXP_SLOTS; i++) {
      if(full_impexp_slotnum_internal[i] == -1) continue;
      eeslot = full_impexp_slotnum_internal[i];
      snum = empty_slotnum_internal[j];
      sprintf(cmd, "mtx -f %s transfer %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, eeslot+1, snum+1, robotcmdseq++);
      sprintf(ext, "cmd_%d", j++);
      setkey_str(&xlist, ext, cmd);
        /* !!!TEMP for now do this move directly */
        /*slot_to_slot(0, eeslot, snum);*/
    }
    setkey_int(&xlist, "reqcnt", j);
    write_log("*Rb:t50startdoor:\n");
    status = clnt_call(clntrobot0, ROBOTDOORDO, (xdrproc_t)xdr_Rkey,
         (char *)xlist, (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDOORDO");
      write_log("%s %s\n", datestring(), call_err);
      rinfo = 1;          /* give err status back to caller */
    }
    if(robotback == 1) {
      write_log("**Error in ROBOTDOORDO call in tape_svc_proc.c\n");
      rinfo = 1;          /* give err status back to caller */
    }
#endif
    /* now put the need Q back onto the normal rd or wrt Q */
    while(p = delete_q_rd_need_front()) {
      p->next = NULL;
      write_log("*Tp:RdQadd: uid=%lu tapeid=%s filenum=%d user=%s dsix=%lu\n",
		p->uid, p->tapeid, p->filenum, p->username, p->ds_index);
      insert_tq_entry_rd(p);
      write_log("RD Q:\n");
      rd_q_print(q_rd_front);		/* !!!TEMP */
    }
    while((p = delete_q_wrt_need_front())) {
      p->next = NULL;
      write_log("*Tp:WtQadd: uid=%lu tapeid=%s filenum=0 user=%s dsix=%lu\n",
                p->uid, p->tapeid, p->username, p->ds_index);
      insert_tq_entry_wrt(p);
    }
    free(op);
    return((KEY *)1);
  }

  if(strcmp(op, "start")) {	/* this must be start */
      write_log("!!ERR impexpdo_1(): Illegal option=%s\n", op);
      rinfo = 1;
      send_ack();
      free(op);
      return((KEY *)1);
  }
  free(op);
  //this is the start option
  eeactive = 1;		/* tell Q code that imp/exp is underway */
  retlist = newkeylist();
  add_keys(params, &retlist);
  reqcnt = getkey_int(params, "reqcnt");
  for(i=0; i < reqcnt; i++) {
    sprintf(ext, "tapeid_%d", i);
    tid = GETKEY_str(params, ext);
    if((snum=tapeinslot(tid)) == -1) {	  /* tape not in slot */
      if((dnum=tapeindrive(tid)) == -1) { /* tape not in any drive */
        write_log("!!!ERR impexpdo_1(): tape %s must be in a slot\n", tid);
        rinfo = 1;
        send_ack();
        return((KEY *)1);
      }
    }
    if((eeslot=find_empty_impexp_slot()) == -1) {
      write_log("!!!ERR impexpdo_1(): There must be an empty imp/exp slot\n", tid);
      rinfo = 1;
      send_ack();
      return((KEY *)1);
    }
    slots[eeslot].tapeid = tid;	
    if(snum == -1) {			//tape is in a drive
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, eeslot+1, dnum, robotcmdseq++);
    }
    else {
      sprintf(cmd, "mtx -f %s transfer %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, snum+1, eeslot+1, robotcmdseq++);
    }
    sprintf(ext, "cmd_%d", i);
    setkey_str(&retlist, ext, cmd);
  }
  setkey_str(&retlist, "OP", "impexp");

  write_log("In impexpdo_1(): clnt_call(clntrobot0, ROBOTDOORDO, ...) \n");
  /*keyiterate(logkey, retlist);		/* !!!TEMP */
  status = clnt_call(clntrobot0, ROBOTDOORDO, (xdrproc_t)xdr_Rkey, 
         (char *)retlist, (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDOORDO");
    write_log("%s %s\n", datestring(), call_err);
    rinfo = 1;		/* give err status back to caller */
  }
  if(robotback == 1) {
    write_log("**Error in ROBOTDOORDO call in tape_svc_proc.c\n");
    rinfo = 1;		/* give err status back to caller */
  }
  send_ack();		/* ack original impexp caller */
  return((KEY *)1);		/* nothing will be sent later */
}

/* Called when a program does a clnt_call to
 * TAPEPROG, TAPEVERS for TAPETESTDO. This code is for testing various
 * interactions with tape_svc. Its function varies during development.
*/
KEY *tapetestdo_1(KEY *params) {
  static KEY *retlist;
  static CLIENT *clresp;
  int tapearcvers;

  write_log("You've called tapetestdo_1() in tape_svc\n");
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in taperespwritedo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  tapearcvers = getkey_int(params, "tapearcvers");
  retlist = newkeylist();
  add_keys(params, &retlist);
  if(!(clresp = set_client_handle(TAPEARCPROG, tapearcvers))) {
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  setkey_fileptr(&retlist, "current_client", (FILE *)clresp);
  current_client = clresp;      /* set for call to tapearc */
  current_client_destroy = 1;
  procnum = TAPEARCDO;          /* for this proc number */
  rinfo = RESULT_PEND;
  send_ack();
  setkey_int(&retlist, "STATUS", 0);
  setkey_int(&retlist, "group_id", 666);
  /*sleep(2);*/
  return(retlist);
}

/* Called from the tapeonoff utility program to turn tape_svc on/off.
*/
KEY *onoffdo_1(KEY *params)
{
  char *action;
  int retry, istat;

  action = getkey_str(params, "action");
  write_log("%s: tapeonoff = %s\n", datestring(), action);
  if(!strcmp(action, "on")) tapeoffline = 0;
  else if(!strcmp(action, "off")) {
    if(!robotbusy) tapeoffline = 1;
    else {
      tapeoffline = 4;
      write_log("Can't take offline while robot is busy\n");
    }
  }
  else if(!strcmp(action, "inv")) {
    if(!tapeoffline) {
      write_log("Can't inventory tapes while tape_svc is still online\n");
      tapeoffline = 2; 
    }
    else {
      retry = 6;
      while(retry) {
#ifdef SUMDC
        if((istat = tape_inventory(sim, 1)) == 0) {
#else
        if((istat = tape_inventory(sim, 0)) == 0) { /* no catalog here */
#endif
          write_log("***Error: Can't do tape inventory. Will retry...\n");
          --retry;
          if(retry == 0) {
            write_log("***Fatal error: Can't do tape inventory\n");
            (void) pmap_unset(TAPEPROG, TAPEVERS);
            exit(1);
          }
          continue;
        }
        if(istat == -1) { /* didn't get full slot count. retry */
          --retry;
          if(retry == 0) {
            write_log("***Fatal error: Can't do tape inventory\n");
            (void) pmap_unset(TAPEPROG, TAPEVERS);
            exit(1);
          }
        }
        else { retry = 0; }
      }
      tapeoffline = 3; 
    }
  }
  rinfo = tapeoffline;
  send_ack();
  free(action);
  return((KEY *)1);
}

/* Called from the robotonoff utility program to turn the robot on/off.
*/
//!!!NOTE: This is preliminary and is not fully implemented yet.
KEY *robotonoffdo_1(KEY *params)
{
  char *action;

  action = getkey_str(params, "action");
  write_log("%s: robotonoff = %s\n", datestring(), action);
  if(!strcmp(action, "on")) robotoffline = 0;
  else if(!strcmp(action, "off")) robotoffline = 1;
  rinfo = robotoffline;
  send_ack();
  free(action);
  return((KEY *)1);
}

/* Called from the driveonoff utility program to turn a tape drive on/off.
*/
KEY *dronoffdo_1(KEY *params)
{
  static KEY *xlist;
  uint64_t robotback;
  char *action, *tapeid, *call_err;
  int drivenum, slotnum;
  char cmd[80];
  enum clnt_stat status;

  action = getkey_str(params, "action");
  drivenum = getkey_int(params, "drivenum");
  write_log("%s: driveonoff = %s %d\n", datestring(), action, drivenum);
  if(drivenum >= MAX_DRIVES) {
    write_log("Error driveonoff drivenum >= MAX_DRIVES (%d)\n", MAX_DRIVES);
    driveonoffstatus = 2;
  }
  else if(drives[drivenum].busy) {
    write_log("Drive# %d currently busy. Try again later.\n", drivenum);
    driveonoffstatus = 3;
  }
  else if(!strcmp(action, "on")) {
    driveonoffstatus = 0;
    drives[drivenum].offline = 0;
  }
  else if(!strcmp(action, "off")) {
    driveonoffstatus = 1;
    drives[drivenum].offline = 1;
    if(tapeid = drives[drivenum].tapeid) { /* unload the current tape */
      xlist = newkeylist();
      robotbusy = 1;
      drives[drivenum].busy = 1;
      write_log("*Tp:DrBusy: drv=%d\n", drivenum);
      slotnum = drives[drivenum].slotnum;
      setkey_str(&xlist, "OP", "mv");
      setkey_int(&xlist, "dnum", drivenum);
      setkey_int(&xlist, "snum", slotnum);
      sprintf(cmd, "mtx -f %s unload %d %d 1> /tmp/mtx/mtx_robot_%d.log 2>&1",
                libdevname, slotnum+1, drivenum, robotcmdseq++);
      setkey_str(&xlist, "cmd1", cmd);
      setkey_int(&xlist, "DEBUGFLG", 0);
      setkey_fileptr(&xlist,  "current_client", (FILE *)current_client);

      status = clnt_call(clntrobot0, ROBOTDO, (xdrproc_t)xdr_Rkey,(char *)xlist,
                        (xdrproc_t)xdr_uint32_t, (char *)&robotback, TIMEOUT);
      if(status != RPC_SUCCESS) {
        if(status != RPC_TIMEDOUT) {  /* allow timeout?? */
          call_err = clnt_sperror(clntrobot0, "Err clnt_call for ROBOTDO");
          write_log("%s %s\n", datestring(), call_err);
          robotbusy = 0;
          drives[drivenum].busy = 0;
          write_log("*Tp:DrNotBusy: drv=%d\n", drivenum);
        }
      }
      if(robotback == 1) {
        write_log("**Error in ROBOTDO call in tape_svc_proc.c\n");
        robotbusy = 0;
        drives[drivenum].busy = 0;
        write_log("*Tp:DrNotBusy: drv=%d\n", drivenum);
      }
      freekeylist(&xlist);
    }
  }
  else if(!strcmp(action, "status")) {
    driveonoffstatus = drives[drivenum].offline;
  }
  rinfo = driveonoffstatus;
  send_ack();
  free(action);
  return((KEY *)1);
}

int send_mail(char *fmt, ...)
{
  va_list args;
  char string[1024], cmd[1024];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  /* !!TBD send to admin alias instead of jim */
  sprintf(cmd, "echo \"%s\" | Mail -s \"test mail\" jim@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}

void write_time()
{
  struct timeval tvalr;
  struct tm *t_ptr;

  if(gettimeofday(&tvalr, NULL) == -1) {
    write_log("Error on gettimeofday() in write_time()\n");
    return;
  }
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(dstring, "%s", asctime(t_ptr));
  write_log("%s", dstring);
}
