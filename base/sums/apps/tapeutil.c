/* tapeutil.c
 * Utilities to support tape_svc for staging tapes and reading and writing.
 * Assumes write_log() function is available to write to log file.
*/

#include <SUM.h>
#include <tape.h>
#include <sum_rpc.h>

extern int write_log(const char *fmt, ...);
extern SLOT slots[];
extern DRIVE drives[];
extern TQ *q_rd_front;          /* front of tape read Q */
extern TQ *q_rd_rear;           /* rear of tape read Q */
extern TQ *q_wrt_front;         /* front of tape write Q */
extern TQ *q_wrt_rear;          /* rear of tape write Q */
extern TQ *q_rd_need_front;     /* front of tape read need Q */
extern TQ *q_rd_need_rear;      /* rear of tape read need Q */
extern TQ *q_wrt_need_front;     /* front of tape write need Q */
extern TQ *q_wrt_need_rear;      /* rear of tape write need Q */


/* !!NOTE: These routines use internal slot#, i.e. 0 to MAX_SLOTS_LIVE-1 */

/* Move the tape from the given slot# to the given drive#.
 * Note: called with internal slot# (so must add 1 for mtx cmd).
 * Returns the slot number on success, else -1.
*/
int slot_to_drive(int sim, int slot, int drive) 
{
  char cmd[128];

  sprintf(cmd, "mtx -f %s load %d %d", LIBDEV, slot+1, drive);
  write_log("Executing: %s\n", cmd);
  if(sim) {			/* simulation mode only */
    sleep(2);
  }
  else {
    if(system(cmd)) {
      write_log("  cmd failure\n");
      return(-1);
    }
  }
  write_log("  cmd success\n");
  drives[drive].tapeid = slots[slot].tapeid;
  drives[drive].sumid = 0;
  drives[drive].busy = 0;
  drives[drive].tapemode = 0;
  drives[drive].filenum = 0;
  drives[drive].blocknum = 0;
  drives[drive].slotnum = slot;
  slots[slot].tapeid = NULL;
  return(slot);
}


/* Move the tape from the given drive# to the given slot#.
 * Returns the drive number on success, else -1.
*/
int drive_to_slot(int sim, int drive, int slot) 
{
  char cmd[128];

  sprintf(cmd, "mtx -f %s unload %d %d", LIBDEV, slot, drive);
  write_log("Executing: %s\n", cmd);
  if(sim) {			/* simulation mode only */
    sleep(2);
  }
  else {
    if(system(cmd)) {
      write_log("  cmd failure\n");
      return(-1);
    }
  }
  write_log("  cmd success\n");
  slots[slot].tapeid = drives[drive].tapeid;
  drives[drive].tapeid = NULL;
  drives[drive].sumid = 0;
  drives[drive].busy = 0;
  drives[drive].tapemode = 0;
  drives[drive].filenum = 0;
  drives[drive].blocknum = 0;
  drives[drive].slotnum = -1;
  return(drive);
}

/* Move the tape from the given slot# A to the given slot# B.
 * Returns the slot A number on success, else -1.
*/
int slot_to_slot(int sim, int slota, int slotb) 
{
  char cmd[128];

  sprintf(cmd, "mtx -f %s transfer %d %d", LIBDEV, slota+1, slotb+1);
  write_log("Executing: %s\n", cmd);
  if(sim) {			/* simulation mode only */
    sleep(2);
  }
  else {
    if(system(cmd)) {
      write_log("  cmd failure\n");
      return(-1);
    }
  }
  write_log("  cmd success\n");
  slots[slotb].tapeid = slots[slota].tapeid;
  slots[slota].tapeid = NULL;
  return(slota);
}

/* Find an empty import/export slot. Return with internal slot# else -1.
*/
int find_empty_impexp_slot()
{
  int i;

  for(i=(MAX_SLOTS - NUM_IMP_EXP_SLOTS); i < MAX_SLOTS; i++) {
    if(slots[i].tapeid == NULL) {
      return(i);
    }
  }
  return(-1);
}

/* Determine if the given tape is in a drive. 
 * Return drive# else -1.
*/
int tapeindrive(char *tape) 
{
  int i;

  for(i=0; i < MAX_DRIVES; i++) {
    if(!drives[i].tapeid) continue;
    if(!strcmp(tape, drives[i].tapeid)) {
      return(i);
    }
  }
  return(-1);
}

/* Determine if the given tape is in a slot. 
 * Return slot# else -1.
*/
int tapeinslot(char *tape) 
{
  int i;

  for(i=0; i < MAX_SLOTS_LIVE; i++) {
    if(!slots[i].tapeid) continue;
    if(!strcmp(tape, slots[i].tapeid)) {
      return(i);
    }
  }
  return(-1);
}

/* Find if a drive is free. Return the drive# else -1.
*/
int findfreedrive()
{
  int i;

  for(i=0; i < MAX_DRIVES; i++) {
    if(drives[i].tapeid == NULL) {
      return(i);
    }
  }
  return(-1);
}

/* Position the tape in the given drive to the given file#.
 * Return file# or -1 or error.
*/
/**********************************************************************
int position_tape(int dnum, int filenum)
{
  char cmd[128], dname[80];

  sprintf(dname, "%s%d", SUMDR, dnum);
  sprintf(cmd, "mt -f %s asf %d 1> %s 2>&1", dname, filenum, POSITIONDUMP);
  write_log("Now executing: %s\n", cmd);
  if(system(cmd)) {
    write_log("  cmd error\n");
    return(-1);
  }
  write_log("  cmd success\n");
}
***********************************************************************/

/* Return any tapes in drives to free slots. Assumes tape_inventory()
 * already called and our internal tables are set up.
 * Return 0 on error.
*/
int tape_free_drives()
{
  int d, s;
  int nobar = 0;
  char cmd[128];

  for(d=0; d < MAX_DRIVES; d++) {
    //if(d == 1) continue;		//!!!TEMP dr1 is broken!!
    if(!drives[d].tapeid) continue;
#ifdef SUMDC
    //for data capture ck if cleaning tape (NoBar) and put in last slot
    if(strstr(drives[d].tapeid, "NoBar")) { nobar = 1; }
    //can also be a cleaning tape with a label
    if(strstr(drives[d].tapeid, "CLN")) { nobar = 1; }
#endif
    /* now find first free slot to put the tape in drive #d into */
    for(s=0; s < MAX_SLOTS_LIVE; s++) {
      if(nobar) {
        s = MAX_SLOTS_LIVE - 1;
      }
      if(!slots[s].tapeid) { /* this is a free slot */
        sprintf(cmd, "%s %d %d 1> %s 2>&1", UNLOADCMD, s+1, d, UNLOADDUMP);
        write_log("*Rb:cmd: %s\n", cmd);   /* need for t120view to work */
        if(system(cmd)) {
          write_log("Err Retry: %s\n", cmd);
          if(system(cmd)) {               /* try it again */
            write_log("***Rb:failure\n\n");
            return(0);
          }
        }
        write_log("***Rb:success\n\n");
        slots[s].tapeid = drives[d].tapeid;
        slots[s].slotnum = s;
        drives[d].tapeid = NULL;
        drives[d].sumid = 0;
        drives[d].busy = 0;
        drives[d].tapemode = 0;
        drives[d].filenum = 0;
        drives[d].blocknum = 0;
        drives[d].slotnum = -1;
        break;
      }
    }
    if(s == MAX_SLOTS_LIVE) {
      if(nobar) 
       write_log("Must have slot# %d free for cleaning tape\n", MAX_SLOTS_LIVE);
      else
       write_log("No free slots to unload drive\n");
      return(0);
    }
  }
  return(1);
}

/**************************************************************************/
/* Read and write queue functions */
int empty_q_rd(void) {
  return q_rd_rear == NULL;
}

int empty_q_wrt(void) {
  return q_wrt_rear == NULL;
}

int empty_q_rd_need(void) {
  return q_rd_need_rear == NULL;
}

int empty_q_wrt_need(void) {
  return q_wrt_need_rear == NULL;
}

/* Make an TQ *entry that can be put on a tape queue */
TQ *q_entry_make(KEY *list, SUMID_t uid, char *tapeid, int filenum, 
		char *user, uint64_t dsix) {
  CLIENT *client;
  TQ *p = (TQ *)malloc(sizeof(TQ));
  if(p != NULL) {
    //write_log("%lu malloc in q_entry_make\n", p); //!!TEMP
    p->next = NULL;
    p->uid = uid;
    p->ds_index = dsix;
    p->tapeid = strdup(tapeid);
    p->username = strdup(user);
    p->filenum = filenum;
    p->list = newkeylist();
    add_keys(list, &p->list);	/* NOTE:does not do fileptr */
    /* must explicitly put in this fileptr if present */
    if(client = (CLIENT *)getkey_fileptr(list, "current_client")) {
      setkey_fileptr(&p->list,  "current_client", (FILE *)client);
    }
  }
  return p;
}

/* Write to log file the current read Q entries for the tui to display.
 * This should be called whenever a new rd Q entry is made as the order
 * of the Q can change and tui needs to know.
 * NOTE: 27Oct2011 This rte was found to be N.G. as the user arg is
 * not always the user of the Q entry. 
 * Switch to using tq_entry_rd_dump2() below.
*/
void tq_entry_rd_dump(char *user) {
  TQ *q = q_rd_front;
  while(q) {
    write_log("*Tp:RdQadd: uid=%lu tapeid=%s filenum=%d user=%s dsix=%lu\n",
                q->uid, q->tapeid, q->filenum, user, q->ds_index);
    q = q->next;
  }
}
void tq_entry_rd_dump2() {	//See NOTE: above
  TQ *q = q_rd_front;
  while(q) {
    write_log("*Tp:RdQadd: uid=%lu tapeid=%s filenum=%d user=%s dsix=%lu\n",
                q->uid, q->tapeid, q->filenum, q->username, q->ds_index);
    q = q->next;
  }
}

/* Put in rd Q in ascending file number order. Note: the tapeid
 * order doesn't matter as once a tape gets in a drive, the entire
 * rd Q is scanned for any other files from that tape. 
*/
void insert_tq_entry_rd_sort(TQ *p) {
  TQ *qprev = q_rd_front;
  TQ *q = q_rd_front;
  if(q == NULL) {
    insert_tq_entry_rd(p);
    return;
  }
  while(q) {
    if(q->filenum > p->filenum) {  /* insert it here */
      if(q_rd_front == q) {
        q_rd_front = p;
        p->next = q;
      }
      else {
        qprev->next = p;
        p->next = q;
      }
      return;
    }
    else {
      if(q->next == NULL) {  /* end of Q, put it in here */
        insert_tq_entry_rd(p);
        return;
      }
    }
    qprev = q;
    q = q->next;
  }
}

/* Insert entry at end of rd Q */
void insert_tq_entry_rd(TQ *p) {
  if(empty_q_rd()) q_rd_front = p;
  else q_rd_rear->next = p;
  q_rd_rear = p;
}

void insert_tq_entry_wrt(TQ *p) {
  if(empty_q_wrt()) q_wrt_front = p;
  else q_wrt_rear->next = p;
  q_wrt_rear = p;
}

void insert_tq_entry_rd_need(TQ *p) {
  TQ *px;

  if(empty_q_rd_need()) {
    q_rd_need_front = p;
    q_rd_need_rear = p;
    return;
  }
  px = q_rd_need_front;
  while(px) {
    if((px->filenum == p->filenum) && (!strcmp(px->tapeid, p->tapeid))) {
      return; //no dups
    }
    px = px->next;
  }
  q_rd_need_rear->next = p;
  q_rd_need_rear = p;
}

void insert_tq_entry_wrt_need(TQ *p) {
  if(empty_q_wrt_need()) q_wrt_need_front = p;
  else q_wrt_need_rear->next = p;
  q_wrt_need_rear = p;
}

void rd_q_print(TQ *p) {
  while(p) {
    //write_log("next=%ld, dsix=%lu, filenum=%d, tapeid=%s, user=%s\n",
    //	p->next, p->ds_index, p->filenum, p->tapeid, p->username);
    write_log("uid=%lu, dsix=%lu, filenum=%d, tapeid=%s, user=%s\n",
	p->uid, p->ds_index, p->filenum, p->tapeid, p->username);
    p = p->next;
  }
}

/* delete the entry from the front of the read q */
TQ *delete_q_rd_front(void) {
  TQ *p = q_rd_front;
  if(q_rd_front == q_rd_rear) q_rd_rear = q_rd_front = NULL;
  else q_rd_front = p->next;
  return(p);
}

/* delete the entry from the front of the write q */
TQ *delete_q_wrt_front(void) {
  TQ *p = q_wrt_front;
  if(q_wrt_front == q_wrt_rear) q_wrt_rear = q_wrt_front = NULL;
  else q_wrt_front = p->next;
  return(p);
}

/* delete the entry from the front of the need rd q */
TQ *delete_q_rd_need_front(void) {
  TQ *p = q_rd_need_front;
  if(q_rd_need_front == q_rd_need_rear) q_rd_need_rear = q_rd_need_front = NULL;
  else q_rd_need_front = p->next;
  return(p);
}

/* delete the entry from the front of the need wrt q */
TQ *delete_q_wrt_need_front(void) {
  TQ *p = q_wrt_need_front;
  if(q_wrt_need_front == q_wrt_need_rear) q_wrt_need_rear = q_wrt_need_front = NULL;
  else q_wrt_need_front = p->next;
  return(p);
}

/* delete the given entry from the read q.
 * Returns the pointer to the deleted entry or null if not found.
*/
TQ *delete_q_rd(TQ *p) {
  TQ *e = NULL;
  TQ *f = NULL; 
  if(p == q_rd_front) {
    q_rd_front = p->next;
    if(q_rd_front == NULL) q_rd_rear = NULL;
    return(p);
  }
  e = q_rd_front->next;
  f = q_rd_front;
  while(e != NULL) {
    if(e == p) {
      f->next = e->next;
      if(p == q_rd_rear) q_rd_rear = f;
      return(p);
    }
    f = e;
    e = e->next;
  }
  return(e);
}

/* delete the given entry from the write q.
 * Returns the pointer to the deleted entry or null if not found.
*/
TQ *delete_q_wrt(TQ *p) {
  TQ *e, *f; 
  if(p == q_wrt_front) {
    q_wrt_front = p->next;
    if(q_wrt_front == NULL) q_wrt_rear = NULL;
    return(p);
  }
  e = q_wrt_front->next;
  f = q_wrt_front;
  while(e != NULL) {
    if(e == p) {
      f->next = e->next;
      if(p == q_wrt_rear) q_wrt_rear = f;
      return(p);
    }
    f = e;
    e = e->next;
  }
  return(e);
}

/* delete the given entry from the need rd q.
 * Returns the pointer to the deleted entry or null if not found.
*/
TQ *delete_q_rd_need(TQ *p) {
  TQ *e = NULL;
  TQ *f = NULL; 
  if(p == q_rd_need_front) {
    q_rd_need_front = p->next;
    if(q_rd_need_front == NULL) q_rd_need_rear = NULL;
    return(p);
  }
  e = q_rd_need_front->next;
  f = q_rd_need_front;
  while(e != NULL) {
    if(e == p) {
      f->next = e->next;
      if(p == q_rd_need_rear) q_rd_need_rear = f;
      return(p);
    }
    f = e;
    e = e->next;
  }
  return(e);
}

/**************************************************************************/


/*
 * SUMOFFCNT is a linked list storage structure for all the SUM_get()
 * calls to keep track of all the offline storage units that need to be
 * read from tape for this SUM_get() call.
 * Only when all the storage units have been read is a return msg sent
 * to the original sum_svc caller.
 *
 * The following functions are defined for use with SUMOFFCNT lists:
 * SUMOFFCNT *setsumoffcnt (SUMOFFCNT **list, SUMID_t uid, int offcnt)
 *	Add an entry with the given values. return ptr to entry or null.
 * SUMOFFCNT *getsumoffcnt (SUMOPENED *list, SUMID_t uid)
 *	return pointer to entry containing the uid. (can only be one)
 * void remsumoffcnt (SUMOFFCNT **list, SUMID_t uid)
 *	remove the entry containing the uid. (can only be one)
*/

SUMOFFCNT *setsumoffcnt(SUMOFFCNT **list, SUMID_t uid, int offcnt)
{
  SUMOFFCNT *newone;

  newone = (SUMOFFCNT *)malloc(sizeof(SUMOFFCNT));
  if(newone == NULL) return(newone);	/* err can't malloc */
  newone->next = *list;
  newone->uid = uid;
  newone->offcnt = offcnt;
  newone->uniqcnt = 0;
  *list = newone;
  return(newone);
}

SUMOFFCNT *getsumoffcnt(SUMOFFCNT *list, SUMID_t uid)
{
  SUMOFFCNT *walk = list;

  while(walk) {
    if(walk->uid != uid)
      walk = walk->next;
    else
      return walk;
  }
  return walk;
}

void remsumoffcnt(SUMOFFCNT **list, SUMID_t uid)
{
  SUMOFFCNT *walk = *list;
  SUMOFFCNT *trail = NULL;
  int i;

  while(walk) {
    if(walk->uid != uid) {
      trail = walk;
      walk = walk->next;
    }
    else {
      if(trail) 
        trail->next = walk->next;
      else 
        *list = walk->next;
      for(i = 0; i < walk->uniqcnt; i++) {
        free(walk->tapeids[i]);
      }
      free(walk);
      walk = NULL;
    }
  }
}
