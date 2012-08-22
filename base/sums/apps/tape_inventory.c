/* tape_inventory.c
 *
 * This is called by tape_svc.c during its setup. It will send a status
 * cmd to the Spectra Logic T120 to get an inventory of the slots and
 * drives and what tapeid each contains. Here's what an inventory looks like:
 *
 *   Storage Changer /dev/sg7:2 Drives, 38 Slots ( 8 Import/Export )
 * Data Transfer Element 0:Empty
 * Data Transfer Element 1:Empty
 *       Storage Element 1:Full :VolumeTag=000000S1                            
 *       Storage Element 2:Full :VolumeTag=000001S1                            
 *       Storage Element 3:Full :VolumeTag=000002S1                            
 *       Storage Element 4:Full :VolumeTag=000003S1                            
 *       Storage Element 5:Full :VolumeTag=000004S1                            
 *       Storage Element 6:Full :VolumeTag=000005S1                            
 *       Storage Element 7:Full :VolumeTag=000006S1                            
 *       Storage Element 8:Full :VolumeTag=000007S1                            
 *       Storage Element 9:Full :VolumeTag=000008S1                            
 *       Storage Element 10:Full :VolumeTag=000009S1                            
 *       Storage Element 11:Full :VolumeTag=000010S1                            
 *       Storage Element 12:Full :VolumeTag=000011S1                            
 *       Storage Element 13:Full :VolumeTag=000012S1                            
 *       Storage Element 14:Full :VolumeTag=000013S1                            
 *       Storage Element 15:Full :VolumeTag=000014S1                            
 *       Storage Element 16:Full :VolumeTag=000015S1                            
 *       Storage Element 17:Full :VolumeTag=000016S1                            
 *       Storage Element 18:Full :VolumeTag=000017S1                            
 *       Storage Element 19:Full :VolumeTag=000018S1                            
 *       Storage Element 20:Full :VolumeTag=000019S1                            
 *       Storage Element 21:Full :VolumeTag=000020S1                            
 *       Storage Element 22:Full :VolumeTag=000021S1                            
 *       Storage Element 23:Full :VolumeTag=000022S1                            
 *       Storage Element 24:Full :VolumeTag=000023S1                            
 *       Storage Element 25:Empty
 *       Storage Element 26:Empty
 *       Storage Element 27:Empty
 *       Storage Element 28:Empty
 *       Storage Element 29:Empty
 *       Storage Element 30:Full 
 *       Storage Element 31 IMPORT/EXPORT:Empty
 *       Storage Element 32 IMPORT/EXPORT:Empty
 *       Storage Element 33 IMPORT/EXPORT:Empty
 *       Storage Element 34 IMPORT/EXPORT:Empty
 *       Storage Element 35 IMPORT/EXPORT:Empty
 *       Storage Element 36 IMPORT/EXPORT:Empty
 *       Storage Element 37 IMPORT/EXPORT:Empty
 *       Storage Element 38 IMPORT/EXPORT:Empty
 *
 * tape_inventory() then reads this status and sets up its internal tables
 * for what was found.
*/

#include <SUM.h>
#include <sum_rpc.h>
#include <tape.h>
#include <string.h>
#include <sys/errno.h>
extern int errno;

#ifdef SUMDC
  #define STATUSDUMP "/tmp/t50_status_dump.out"
  #define STATUSDUMPSIM "/usr/local/logs/SUM/t50_status_dump.out.sim"
#elif SUMT120
  #define STATUSDUMP "/tmp/t120_status_dump.out"
  #define STATUSDUMPSIM "/usr/local/logs/SUM/t120_status_dump.out.sim"
#elif SUMT950
  #define STATUSDUMP "/tmp/t950_status_dump.out"
  #define STATUSDUMPSIM "/usr/local/logs/SUM/t950_status_dump.out.sim"
  #define STATUSDUMPSIMREINV "/tmp/t950_status_dump.jmtx"
#endif
#define VDUMP "/usr/local/logs/SUM/t950_status.verify"

extern int write_log();

extern SLOT slots[];
extern DRIVE drives[];
extern int Empty_Slot_Cnt;

/* Get the tape inventory and set up our internal tables. If catalog is
 * set, also make a sum_tape table entry if necessary.
 * Return 0 on error. Return -1 if don't see the full slot count.
 * (remember that externally slot#s start at 1)
 * Counts the number of empty slots found in the glb vrbl Empty_Slot_Cnt.
*/
int tape_inventory(int sim, int catalog)
{
  FILE *finv;
  char cmd[MAXSTR], row[MAXSTR], smsg[MAXSTR];
  char *token, *cptr;
  char *tapeidclosed[MAX_SLOTS+MAX_DRIVES];
  int drivenum, slotnum, i, j, k, tstate;
  int drive_full[MAX_DRIVES];
  int cx = 0;

  write_log("tape_inventory() called with sim = %d\n", sim);
/*  sprintf(cmd, "/bin/rm -f %s", STATUSDUMP);
/*  system(cmd);
*/
  sprintf(cmd, "/usr/sbin/mtx -f %s status 1> %s 2>&1", LIBDEV, STATUSDUMP);
  write_log("*Inv: %s\n", cmd);
  if(sim) {			/* simulation mode only */
    sleep(5);
    sprintf(cmd, "cp %s %s", STATUSDUMPSIM, STATUSDUMP);
    system(cmd);
  }
  else {
    if(system(cmd)) {
       /* write_log("***Inv: failure\n\n");/* Can't do this here. caller does*/
      return(0);
    }
  }
  write_log("***Inv: success\n\n");
  if (!(finv = fopen(STATUSDUMP, "r"))) {
    write_log("**Fatal error: can't open %s\n", STATUSDUMP);
    return(0);
  }
  drivenum = slotnum = 0;  /* remember that externally slot#s start at 1 */
  Empty_Slot_Cnt = 0;
  for(i=0; i < MAX_DRIVES; i++) { drive_full[i] = -1; }
  i = j = 0;
  while (fgets (row,MAXSTR,finv)) {
    if(strstr(row, "Data Transfer Element")) {
      if(drivenum == MAX_DRIVES) {
        write_log("**Warning: there are more drives then MAX_DRIVES (%d)\n",
			MAX_DRIVES);
        /*return(0);*/
      }
      if(strstr(row, ":Full")) {	/* Drive has tape */
        token = (char *)strtok(row, "=");
        token = (char *)strtok(NULL, "=");
        if(!token) {			/* no bar code */
          token = "NoBar  ";
        } else {
          token = token+1;		/* skip leading space */
          cptr = index(token, ' ');	/* find trailing space */
          *cptr = (char)NULL;		/* elim trailing stuff */
        }
        drive_full[i++] = drivenum;	/* this drive needs a free slot */
        drives[drivenum].tapeid = (char *)strdup(token);
        drives[drivenum].sumid = 0;
        drives[drivenum].busy = 0;
        drives[drivenum].tapemode = 0;
        drives[drivenum].filenum = 0;
        drives[drivenum].blocknum = 0;
        drives[drivenum].slotnum = 0; /* filled in w/free slot# below */
        write_log("tapeid in drive %d = %s\n", 
			drivenum, drives[drivenum].tapeid);
        if(catalog) { 
	  if((tstate=SUMLIB_TapeCatalog(drives[drivenum].tapeid)) == 0) {
            write_log("***ERROR: Can't catalog new tapeid = %s\n", 
			drives[drivenum].tapeid);
          }
          else {
            if(tstate == TAPECLOSED) {	/* let tui know already closed */
              tapeidclosed[cx++] = drives[drivenum].tapeid;
            }
          }
        }
      }
      else {				/* EMPTY */
        drives[drivenum].tapeid = NULL;
        drives[drivenum].sumid = 0;
        drives[drivenum].busy = 0;
        drives[drivenum].tapemode = 0;
        drives[drivenum].filenum = 0;
        drives[drivenum].blocknum = 0;
        drives[drivenum].slotnum = -1;
        write_log("tapeid in drive %d = %s\n", 
			drivenum, drives[drivenum].tapeid);
      }
      drivenum++;
    }
    else if(strstr(row, "Storage Element")) {
      if(slotnum == MAX_SLOTS) {
        write_log("**Fatal error: there are more slots then MAX_SLOTS=%d\n",
			MAX_SLOTS);
        return(0);
      }
      sprintf(smsg, " ");
      if(strstr(row, ":Full")) {	/* slot has tape */
        token = (char *)strtok(row, "=");
        token = (char *)strtok(NULL, "=");
        if(!token) {			/* no bar code */
          token = "NoBar  ";
        } else {
          cptr = index(token, ' ');	/* find trailing space */
          *cptr = (char)NULL;		/* elim trailing stuff */
        }
        slots[slotnum].slotnum = slotnum;
        slots[slotnum].tapeid = (char *)strdup(token);
        if(catalog) { 
          if((tstate=SUMLIB_TapeCatalog(slots[slotnum].tapeid)) == 0) {
            write_log("***ERROR: Can't catalog new tapeid = %s\n", 
			slots[slotnum].tapeid);
          }
          else {
            if(tstate == TAPECLOSED) {	/* let tui know already closed */
              tapeidclosed[cx++] = slots[slotnum].tapeid;
            }
          }
        }
      }
      else {				/* EMPTY */
        if(j < MAX_DRIVES) {		/* more empty slots then drives */
          if((k = drive_full[j++]) != -1) { /* drive need this empty slot# */
            drives[k].slotnum = slotnum;
            sprintf(smsg, "Slot assigned to drive #%d", k);
          }
        }
        slots[slotnum].slotnum = slotnum;
        slots[slotnum].tapeid = NULL;
        Empty_Slot_Cnt++;
      }
      write_log("tapeid in slot# %d = %s %s\n", 
		slotnum+1, slots[slotnum].tapeid, smsg);
      slotnum++;
    }
  }
  write_log("***ENDInv: slots=%d\n", slotnum);
  fclose(finv);
  if(slotnum != MAX_SLOTS) {
    write_log("Inv returned wrong # of slots. Retry.\n");
    return(-1);
  }
  /* now can output the ReClose tapeids. Must be done after ***ENDInv
   * msg as tui gobbles up outout in the log file during an inventory
   * until it finds this msg.
  */
  write_log("***ENDInvPhase:\n");       /* for tui to see */
  for(i=0; i < cx; i++) {
      write_log("*Tp:ReClose: tapeid=%s\n", tapeidclosed[i]);
  }
  return(1);
}

/* Called from tape_svc_proc.c  to re-inventory all the drives and slots
 * to find out what's really where.
 * Return 0 on error. Return -1 if don't see the full slot count.
 *
 * Also called from tape_svc_proc.c onoffdo_1() when the user calls the
 * 'tapeonoff inv' utility.
*/
int tape_reinventory(int sim, int catalog)
{
  FILE *finv;
  char cmd[MAXSTR], row[MAXSTR], smsg[MAXSTR];
  char *token, *cptr;
  char *tapeidclosed[MAX_SLOTS+MAX_DRIVES];
  int drivenum, slotnum, i, j, k, tstate;
  int drive_full[MAX_DRIVES];
  int cx = 0;

  write_log("tape_reinventory() called with catalog = %d\n", catalog);
/*  sprintf(cmd, "/bin/rm -f %s", STATUSDUMP);
/*  system(cmd);
*/
  sprintf(cmd, "/usr/sbin/mtx -f %s status 1> %s 2>&1", LIBDEV, STATUSDUMP);
  if(sim) {			/* simulation mode only */
    sleep(5);
    sprintf(cmd, "cp %s %s", STATUSDUMPSIMREINV, STATUSDUMP);
    write_log("*Inv: %s\n", cmd);
    system(cmd);
  }
  else {
    write_log("*Inv: %s\n", cmd);
    if(system(cmd)) {
      write_log("ReInv Fail on: %s\n", cmd);
      return(0);
    }
  }
  if (!(finv = fopen(STATUSDUMP, "r"))) {
    write_log("**Fatal error: can't open %s\n", STATUSDUMP);
    return(0);
  }
  write_log("***Inv: success\n\n");
  drivenum = slotnum = 0;  /* remember that externally slot#s start at 1 */
  Empty_Slot_Cnt = 0;
  for(i=0; i < MAX_DRIVES; i++) { drive_full[i] = -1; }
  i = j = 0;
  while (fgets (row,MAXSTR,finv)) {
    if(strstr(row, "Data Transfer Element")) {
      if(drivenum == MAX_DRIVES) {
        write_log("**Warning: there are more drives then MAX_DRIVES (%d)\n",
			MAX_DRIVES);
        return(0);
      }
      if(strstr(row, ":Full")) {	/* Drive has tape */
        token = (char *)strtok(row, "=");
        token = (char *)strtok(NULL, "=");
        if(!token) {			/* no bar code */
          token = "NoBar  ";
        } else {
          token = token+1;		/* skip leading space */
          cptr = index(token, ' ');	/* find trailing space */
          *cptr = (char)NULL;		/* elim trailing stuff */
        }
        drive_full[i++] = drivenum;	/* this drive needs a free slot */
        drives[drivenum].tapeid = (char *)strdup(token);
        //drives[drivenum].sumid = 0;
        //drives[drivenum].busy = 0;
        //drives[drivenum].tapemode = 0;
        drives[drivenum].filenum = 0;	//force reposition
        //drives[drivenum].blocknum = 0;
        //drives[drivenum].slotnum = 0; /* filled in w/free slot# below */
        write_log("tapeid in drive %d = %s\n", 
			drivenum, drives[drivenum].tapeid);
        if(catalog) { 
	  if((tstate=SUMLIB_TapeCatalog(drives[drivenum].tapeid)) == 0) {
            write_log("***ERROR: Can't catalog new tapeid = %s\n", 
			drives[drivenum].tapeid);
          }
          else {
            if(tstate == TAPECLOSED) {	/* let tui know already closed */
              tapeidclosed[cx++] = drives[drivenum].tapeid;
            }
          }
        }
      }
      else {				/* EMPTY */
          drives[drivenum].tapeid = NULL;
          drives[drivenum].sumid = 0;
          drives[drivenum].busy = 0;
          drives[drivenum].tapemode = 0;
          drives[drivenum].filenum = 0;
          drives[drivenum].blocknum = 0;
          drives[drivenum].slotnum = -1;
          write_log("tapeid in drive %d = %s\n", 
			drivenum, drives[drivenum].tapeid);
      }
      drivenum++;
    }
    else if(strstr(row, "Storage Element")) {
      if(slotnum == MAX_SLOTS) {
        write_log("**Fatal error: there are more slots then MAX_SLOTS=%d\n",
			MAX_SLOTS);
        return(0);
      }
      sprintf(smsg, " ");
      if(strstr(row, ":Full")) {	/* slot has tape */
        token = (char *)strtok(row, "=");
        token = (char *)strtok(NULL, "=");
        if(!token) {			/* no bar code */
          token = "NoBar  ";
        } else {
          cptr = index(token, ' ');	/* find trailing space */
          *cptr = (char)NULL;		/* elim trailing stuff */
        }
        slots[slotnum].slotnum = slotnum;
        slots[slotnum].tapeid = (char *)strdup(token);
        if(catalog) { 
          if((tstate=SUMLIB_TapeCatalog(slots[slotnum].tapeid)) == 0) {
            write_log("***ERROR: Can't catalog new tapeid = %s\n", 
			slots[slotnum].tapeid);
          }
          else {
            if(tstate == TAPECLOSED) {	/* let tui know already closed */
              tapeidclosed[cx++] = slots[slotnum].tapeid;
            }
          }
        }
      }
      else {				/* EMPTY */
        if(j < MAX_DRIVES) {		/* more empty slots then drives */
          if((k = drive_full[j++]) != -1) { /* drive need this empty slot# */
            drives[k].slotnum = slotnum;
            sprintf(smsg, "Slot assigned to drive #%d", k);
          }
        }
        slots[slotnum].slotnum = slotnum;
        slots[slotnum].tapeid = NULL;
        Empty_Slot_Cnt++;
      }
      write_log("tapeid in slot# %d = %s %s\n", 
		slotnum+1, slots[slotnum].tapeid, smsg);
      slotnum++;
    }
  }
  write_log("***ENDInv: slots=%d\n", slotnum);
  fclose(finv);
  if(slotnum != MAX_SLOTS) {
    write_log("Inv returned wrong # of slots. Retry.\n");
    return(-1);
  }
  /* now can output the ReClose tapeids. Must be done after ***ENDInv
   * msg as tui gobbles up outout in the log file during an inventory
   * until it finds this msg.
  */
  write_log("***ENDInvPhase:\n");       /* for tui to see */
  for(i=0; i < cx; i++) {
      write_log("*Tp:ReClose: tapeid=%s\n", tapeidclosed[i]);
  }
  return(1);
}

/* Verify that the given operation succesfully completed.
 * For the given action, checks if the slot and slotordrive
 * are full or empty accordingly. For example for the command,
 * mtx -f /dev/t950 load 13 1
 * will check that slot 13 is empty and slot 1 contains a tape.
 * If the check is true, returns 1, if not true returns 0.
 * If can't verify, returns -1.
 * 
 * The actions are load, unload, transfer. For load and unload the
 * slotordrive is a drive#. Foe transfer, the slotordrive is a slot#.
 *
 *NOTE:!!This starts out as the same code in rb_verify() found in robotn.c
*/
int robot_verify(char *action, int slot, int slotordrive)
{
  FILE *finv;
  int s, sord, retry;
  int drivenum, slotnum, i, j, k, tstate;
  char *drive_tapes[MAX_DRIVES], *slot_tapes[MAX_SLOTS];
  char *token, *cptr;
  char cmd[MAXSTR], row[MAXSTR];

  retry = 6;
  while(retry) {
    sprintf(cmd, "/usr/sbin/mtx -f %s status 1> %s 2>&1", LIBDEV, VDUMP);
    if(system(cmd)) {
      write_log("***Verify: failure. errno=%d\n", errno);
      return(-1);
    }
    if (!(finv = fopen(VDUMP, "r"))) {
      write_log("**Fatal error: can't open %s\n", VDUMP);
      return(-1);
    }
    drivenum = slotnum = 0;  /* remember that externally slot#s start at 1 */
    for(i=0; i < MAX_DRIVES; i++) { drive_tapes[i] = "NoTape"; }
    while (fgets (row,MAXSTR,finv)) {
      if(strstr(row, "Data Transfer Element")) {
        if(strstr(row, ":Full")) {	/* Drive has tape */
          token = (char *)strtok(row, "=");
          token = (char *)strtok(NULL, "=");
          if(!token) {			/* no bar code */
            token = "NoTape";		/* treat as NoTape
          } else {
            token = token+1;		/* skip leading space */
            cptr = index(token, ' ');	/* find trailing space */
            *cptr = (char)NULL;		/* elim trailing stuff */
          }
          drive_tapes[drivenum] = (char *)strdup(token);
        }
        write_log("tapeid in drive %d = %s\n", 
  			drivenum, drive_tapes[drivenum]);
        drivenum++;
      }
      else if(strstr(row, "Storage Element")) {
        if(strstr(row, ":Full")) {	/* slot has tape */
          token = (char *)strtok(row, "=");
          token = (char *)strtok(NULL, "=");
          if(!token) {			/* no bar code */
            token = "NoTape";		/* treat as no tape */
          } else {
            cptr = index(token, ' ');	/* find trailing space */
            *cptr = (char)NULL;		/* elim trailing stuff */
          }
          slot_tapes[slotnum] = (char *)strdup(token);
        }
        else {				/* slot EMPTY */
          slot_tapes[slotnum] = "NoTape";
        }
        write_log("tapeid in slot# %d = %s\n", 
  		slotnum+1, slot_tapes[slotnum]);
        slotnum++;
      }
    }
    fclose(finv);
    if(slotnum != MAX_SLOTS) {
      write_log("Inv returned wrong # of slots. Retry.\n");
      --retry;
      if(retry == 0) {
        write_log("***Fatal error: Can't do tape inventory\n");
        return(-1);
      }
    }
    else { retry = 0; }
  }
  //Now check if the given slot and slotordrive args are ok
  if(!strcmp(action, "unload")) {	//drive emtpy and slot full
    if(strcmp(drive_tapes[slotordrive], "NoTape")) { //drive has tape. NG
      return(0);
    }
    if(!strcmp(slot_tapes[slot], "NoTape")) {	//slot has no tape. NG
      return(0);
    }
    return(1);		//verifies ok
  }
  else if(!strcmp(action, "load")) {	//slot empty and drive full
    if(!strcmp(drive_tapes[slotordrive], "NoTape")) { //drive has no tape. NG
      return(0);
    }
    if(strcmp(slot_tapes[slot], "NoTape")) {   //slot has tape. NG
      return(0);
    }
    return(1);          //verifies ok
  }
  else {			//it's a transfer. 1st slot empty, 2nd full
    if(strcmp(slot_tapes[slot], "NoTape")) {   //slot has tape. NG
      return(0);
    }
    if(!strcmp(slot_tapes[slotordrive], "NoTape")) { //slot has no tape. NG
      return(0);
    }
    return(1);
  }
}
