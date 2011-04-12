/* This is just play stuff.
 * It was put into the cvs tree because it was used to 
 * test some cvs stuff too.
*/
#include <SUM.h>
#include <soi_key.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <sum_rpc.h>
#include <sum_info.h>
#include <soi_error.h>
/*************************/
#include <stropts.h>
#include <sys/mtio.h>
/*************************/
#include <scsi/sg.h>
#include <unistd.h>

extern int errno;
/*static void respd();*/
/*static char datestr[32];*/
/*char *datestring();*/

void logkey();

KEY *rlist;             /* The global keylist returned by unpackmsg() */
KEY *list = NULL;
FILE *logfp;
SUM_t *sum;
SUMID_t uid;
/*static int errcnt = 0;*/
int soi_errno = NO_ERROR;
int bytes, msgtag, petid, req_num, status, cnt, i, j, inum;
char **cptr;
float ftmp;
uint64_t *dsixpt;
uint64_t alloc_index;
char alloc_wd[64];
char cmd[128];
char mod_name[] = "sum_rpc";
char dsname[] = "hmi_lev1_fd_V";	/* !!TEMP name */
char hcomment[] = "this is a dummy history comment that is greater than 80 chars long to check out the code";

static struct timeval first[8], second[8];

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

int write_log(const char *fmt, ...)
{
  va_list args;
  char string[4096];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  if(logfp) {
    fprintf(logfp, string);
    fflush(logfp);
  }
  else
    fprintf(stderr, string);
  va_end(args);
  return(0);
}

void str_compress (char *s) {
   char *source, *dest;
   source = dest = s;
   while (*source) {
      if (!isspace (*source)) {
         *dest = *source;
         dest++;
      }
      source++;
   }
   *dest = 0;
}

/* Before running this you must have the sum_svc running on d00 like so:
 * sum_svc hmidb &
 * The log file will be at /usr/local/logs/SUM/sum_svc_PID.log
*/
int main(int argc, char *argv[])
{
  /*char touched[128];*/
/*******************************TEMP stuff******************
  int fd;
  int ret;
  int waitcnt = 0;
  struct mtget mt_stat;

  fd = open("/dev/sg12", O_RDONLY | O_NONBLOCK);
  while(1) {
    ioctl(fd, MTIOCGET, &mt_stat);
    if(mt_stat.mt_gstat == 0) {  
      printf("/dev/nst0 NOT READY\n");
      if(++waitcnt == 10) {
        ret = -1;
        break;
      }
      sleep(1);
    }
    else {
      ret = mt_stat.mt_fileno;
      break;
    }
  }
  close(fd);
  printf("/dev/nst0  ret= %d\n", ret);
  exit(0);
*******************************END TEMP stuff******************/

//!!TEMP for tesing
//int rvstat;
//  rvstat = robot_verify("transfer", 701, 703);
//  printf("rvstat=%d\n", rvstat);
//  exit(0);


/*  unsigned int Xstatus;
/*  sg_io_hdr_t io_hdr;
/*  unsigned char CDB[6];
/*  unsigned char DataBuffer[6];
/*  unsigned char RequestSense[18];
/*  int fd;
/*
/*  CDB[0] = 0x00;          /* TEST_UNIT_READY */
/*  CDB[1] = 0;
/*  CDB[2] = 0;
/*  CDB[3] = 0;                     /* 1-5 all unused. */
/*  CDB[4] = 0;
/*  CDB[5] = 0;
/*
/*  fd = open("/dev/sg12", O_RDONLY | O_NONBLOCK);
/*  memset(&io_hdr, 0, sizeof(sg_io_hdr_t));
/*        /* Fill in the common stuff... */
/*        io_hdr.interface_id = 'S';
/*        io_hdr.cmd_len = 6;
/*        io_hdr.mx_sb_len = 18;
/*        io_hdr.dxfer_len = 6;
/*        io_hdr.cmdp = (unsigned char *) CDB;
/*        io_hdr.sbp = (unsigned char *) RequestSense;
/*        io_hdr.dxferp = DataBuffer;
/*        io_hdr.timeout = 60;
/*        io_hdr.dxfer_direction=SG_DXFER_FROM_DEV;
/*        if ((Xstatus = ioctl(fd, SG_IO , &io_hdr)) || io_hdr.masked_status)
/*        {
/*        }
/*        printf("Xstatus = %d\n", Xstatus);
/*  close(fd);
*/
  FILE *logfp, *drfp;
  int jmode, order0;
  char *j_env, *deldate;
  char sqlcmd[256], pcmd[256], line[256], logget[80];
  char logname[256], rwchars[32];
  int drive_order_rd[MAX_DRIVES];
  int drive_order_wt[MAX_DRIVES];
  int nxtscanrd;
  int nxtscanwt;
  int max_drives_rd = 0;
  int max_drives_wt = 0;


/************************************************************************
  sprintf(logname, "/usr/local/logs/SUM/drive_order_rwX.txt");
  if((drfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file file %s\n", logname);
    exit(1);
  }
  else {
    i = 0;
    //NOTE: All the rd must be assigned before any wt
    while(fgets(line, 256, drfp)) {  //Must be exactly 12 (MAX_DRIVES) entries
      if(!strncmp(line, "#", 1)) {   //ignore line starting with #
        continue;
      }
      sscanf(line, "%d %s", &order0, rwchars);
      write_log("rw = %s  drive# = %d\n", rwchars, order0);
      if(!strcmp(rwchars, "rd")) {
        if(i == 0) nxtscanrd = order0;  //all the 'rd' must be before the 'wt'
        drive_order_rd[i] = order0;
        max_drives_rd++;
      }
      else if(!strcmp(rwchars, "wt")) {
        drive_order_wt[i] = order0;
        max_drives_wt++;
      }
      else {
        printf("%s is incorrect format\n", logname);
        exit(1);
      }
      i++;
    }
    nxtscanwt = nxtscanrd + max_drives_rd;
    if(nxtscanwt >= MAX_DRIVES) nxtscanwt = 0;
    write_log("max_drives_rd=%d, max_drives_wt=%d, nxtscanrd=%d, nxtscanwt=%d\n",
                max_drives_rd, max_drives_wt, nxtscanrd, nxtscanwt);
  }
  fclose(drfp);
  exit(0);
*********************************************************************/

/*********************************************************************
  deldate = get_effdate(2);
  printf("deldate = %s\n", deldate);

  gid_t gid = getgid();
  printf("gid = %u\n", gid);

  if(j_env = (char *)getenv("JSOC_MODE")) jmode = atoi(j_env);
  else jmode = 0;
  printf("JSOC_MODE = %d\n", jmode);

    sprintf(logget, "/tmp/get_ds_%d.log", getpid());
    sprintf(sqlcmd, "select conform,ext_desc,ser_typ,log_tbl,mml_ds,descr from sum_ds_naming where prog='mdi' and levl='raw' and series='sci5k'");
    sprintf(pcmd, "echo \"%s\" | psql -h hmidb -p 5434 jsoc_sums 1> %s 2>&1", sqlcmd, logget);
    if(system(pcmd)) {
      printf("Failed: %s\n", pcmd);
      exit(1);
    }
    if(!(logfp=fopen(logget, "r"))) {
      printf("Can't open the log file %s\n", logget);
      exit(1);
    }
    fgets(line, 256, logfp);            //skip 1st 2 lines
    fgets(line, 256, logfp);
    fgets(line, 256, logfp);            //info line
    char *token = (char *)strtok(line, "|");
    str_compress(token);
    setkey_str(&list, "CONFORM_0", token);
    token = (char *)strtok(NULL, "|");
    str_compress(token);
    setkey_str(&list, "EXT_DESC_0", token);
    token = (char *)strtok(NULL, "|");
    str_compress(token);
    setkey_str(&list, "SER_TYP_0", token);
    token = (char *)strtok(NULL, "|");
    str_compress(token);
    setkey_str(&list, "LOG_TBL_0", token);
    token = (char *)strtok(NULL, "|");
    str_compress(token);
    setkey_str(&list, "MML_DS_0", token);
    token = (char *)strtok(NULL, "|");
    str_compress(token);
    setkey_str(&list, "DESCR_0", token);
    keyiterate(logkey, list);           // !!TEMP 

      sprintf(logget, "/tmp/get_epoch_%d.log", getpid());
      sprintf(sqlcmd, "select epoch from sum_epoch_table where ser_typ='%s'", 
                getkey_str(list, "SER_TYP_0"));
      sprintf(pcmd, "echo \"%s\" | psql -h hmidb -p 5434 jsoc_sums 1> %s 2>&1", sqlcmd, logget);
      printf("%s\n", pcmd);
      if(system(pcmd)) {
        printf("Error on sql = %s\n", pcmd);
        exit(1);
      }
      if(!(logfp=fopen(logget, "r"))) {
        printf("Can't open the log file %s\n", logget);
        exit(1);
      }
      fgets(line, 256, logfp);          //skip 1st 2 lines
      fgets(line, 256, logfp);
      fgets(line, 256, logfp);          //info line
      token = (char *)strtok(line, "|"); //epoch
      str_compress(token);
      list = newkeylist();
      setkey_str(&list, "EPOCH_0", token);
      keyiterate(logkey, list);           // !!TEMP

*********************************************************************/

  //if((sum = SUM_open("n02", NULL, printf)) == 0) {
  if((sum = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open()\n");
    exit(1);
  }
goto XXX;
  uid = sum->uid;
  //sum->debugflg = 1;			/* use debug mode for future calls */
  /*sum->debugflg = 0;*/
  sum->username = "production";		// !!TEMP 
  printf("Opened with sumid = %d\n", uid);

  sum->bytes = (double)120000000;	/* 120MB */
  sum->reqcnt = 1;
  sum->storeset = 1;			//!!TEMP test if get /SUM100
  if(status = SUM_alloc(sum, printf)) {	/* allocate a data segment */
   printf("SUM_alloc() failed to alloc %g bytes. Error code = %d\n", 
			sum->bytes, status);
   SUM_close(sum, printf);
   exit(1);
  }
  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  alloc_index = *dsixpt;
  strcpy(alloc_wd, *cptr);
  printf("Allocated %g bytes at %s with dsindex=%ld\n", 
			sum->bytes, *cptr, alloc_index);
/*  /* put something in the alloc wd for this test */
/*  sprintf(cmd, "cp -rp /home/jim/cvs/PROTO/src/SUM %s", *cptr);
/*  printf("cmd is: %s\n", cmd);
/*  system(cmd);
/*  sprintf(cmd, "touch %s/%s%d", *cptr, "touch", uid);
/*  printf("cmd is: %s\n", cmd);
/*  system(cmd);
*/

/*************************************************************************/
XXX:
  sum->mode = RETRIEVE + TOUCH;
  //sum->mode = NORETRIEVE + TOUCH;
  sum->tdays = 30;
  //sum->reqcnt = 3;
  sum->reqcnt = 1;
  dsixpt = sum->dsix_ptr;
  *dsixpt++ = 102079934;
//  *dsixpt++ = 102080317;
//  *dsixpt++ = 19328277;
//  *dsixpt++ = 116105590;
//  *dsixpt++ = 123082013;
//  *dsixpt++ = 94850450;
//  *dsixpt++ = 4294969685;
//  *dsixpt++ = 4294969686;
//  *dsixpt++ = 4294969687;
//  *dsixpt++ = 669;
//  *dsixpt++ = 4294967380;
//  *dsixpt++ = 1433434;
//  *dsixpt++ = 1159350;
//  *dsixpt++ = 17499;
//  *dsixpt++ = 14802;
//  *dsixpt++ = 14539;
//  *dsixpt++ = 14686;
//  *dsixpt++ = 634591;
//  *dsixpt++ = 634592;
  //sleep(10);
  status = SUM_get(sum, printf); 
  printf("status from SUM_get() = %d\n", status);
//exit(0); //!!!TEMP TEST
  switch(status) {
  case 0:			// success. data in sum
      cnt = sum->reqcnt;
      cptr = sum->wd;
      dsixpt = sum->dsix_ptr;
      //printf("The wd's found from the SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("ds_index = %u\n", *dsixpt++);
        printf("wd = %s\n", *cptr++);
      }
    break;
  case 1:			// error
    cptr = sum->wd;
    dsixpt = sum->dsix_ptr;
    printf("Failed on SUM_get() for ix=%lu\n", *dsixpt++);
    break;
  case RESULT_PEND:		// result will be sent later 
    printf("SUM_get() call RESULT_PEND...\n");
while(1) {
  status = SUM_poll(sum);
  printf("SUM_poll() returns %d\n", status);
  if(status == 0) break;   //successful answer from the SUM_get()
  sleep(8);		   //must be <10sec for sum_svc t.o. to caller
  status = SUM_nop(sum, printf);
  printf("SUM_nop() returns %d\n", status);
  if(status < 4) continue;   //sums alive. continue to wait
  //status 4 means sum_svc is dead. 5 means tape_svc is dead
  /* OK, sums or tape_svc is dead. DRMS now has the ball on what to do*/
}

/**********************************************************************
    status = SUM_wait();
    printf("status from SUM_wait() = %d\n", status);
**********************************************************************/


      if(sum->status) {
        printf("***Error on SUM_get() call. tape_svc may have died or\n");
        printf("check /usr/local/logs/SUM/ logs for possible tape errs\n\n");
        break;
      }
      cnt = sum->reqcnt;
      cptr = sum->wd;
      printf("The wd's found from the SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }
    break;
  default:
    printf("Error: unknown status from SUM_get()\n");
    break;
  }

goto GOEND;

ftmp = StopTimer(0);
//printf("\nTime sec for %d SUM_get() in one call = %f\n\n", MAXSUMREQCNT, ftmp);

/*************************************************************************/

SUM_info_t *sinfo;
int i;
uint64_t ix;

//ix = 4294967357;
//ix = 4294967357;
//ix = 824880;
//ix = 2102677;
//ix = 15035009;
//ix = 6379855;
//ix = 4294968790;

//ix = 40954590;
//ix = 4294969691;
ix = 669;
/***********************************************************/

goto ABC;	//just do the SUM_infoEx()

StartTimer(1);
for(i=0; i < 64; i++) {
  if(SUM_info(sum, ix, printf)) {
    printf("Fail on SUM_info() in main3 ix=%u\n", ix);
  }
  else {
    sinfo = sum->sinfo;
/******
    printf("\nsum_info username = %s\n", sinfo->username);
    printf("sum_info online_loc = %s\n", sinfo->online_loc);
    printf("sum_info online_status = %s\n", sinfo->online_status);
    printf("sum_info archive_status = %s\n", sinfo->archive_status);
    printf("sum_info owning_series = %s\n", sinfo->owning_series);
    printf("sum_info creat_date = %s\n", sinfo->creat_date);
    printf("sum_info arch_tape = %s\n", sinfo->arch_tape);
    printf("sum_info arch_tape_fn = %d\n", sinfo->arch_tape_fn);
    printf("sum_info arch_tape_date = %s\n", sinfo->arch_tape_date);
    printf("sum_info pa_status = %d\n", sinfo->pa_status);
    printf("sum_info pa_substatus = %d\n", sinfo->pa_substatus);
    printf("sum_info effective_date = %s\n", sinfo->effective_date);
******/
  }
  ix++;
}
ftmp = StopTimer ( 1 ) ;
printf( "\nTime sec for 64 SUM_info() = %f\n\n", ftmp );

/**************************************************************/
ABC:

int cy;
ix = 40350694;
StartTimer(1);			//time all 8 cycles
for(cy=0; cy < 8; cy++) {

//sum->reqcnt = 512;
sum->reqcnt = 64;
sum->sinfo = NULL;		//allow auto malloc
dsixpt = sum->dsix_ptr;
for(i=0; i < 64; i++) {
  *dsixpt++ = ix++;
}
/**************************************
*dsixpt++ = 6379855;
*dsixpt++ = 40954592;
//*dsixpt++ = 6379855;
*dsixpt++ = 40350695;
//*dsixpt++ = 6379855;
****************************************/
StartTimer(0);
if(SUM_infoEx(sum, printf)) {
  printf("\nFail on SUM_infoEx()\n");
}
else {
  printf("\nOk, on SUM_infoEx()\n");
  sinfo = sum->sinfo;
  i = 1;
  while(sinfo) {
    printf("\ncount of sinfo = %d\n", i++);
    printf("sum_info sunum = %u\n", sinfo->sunum);
/******
    printf("sum_info username = %s\n", sinfo->username);
    printf("sum_info online_loc = %s\n", sinfo->online_loc);
    printf("sum_info online_status = %s\n", sinfo->online_status);
    printf("sum_info archive_status = %s\n", sinfo->archive_status);
    printf("sum_info owning_series = %s\n", sinfo->owning_series);
    printf("sum_info creat_date = %s\n", sinfo->creat_date);
    printf("sum_info arch_tape = %s\n", sinfo->arch_tape);
    printf("sum_info arch_tape_fn = %d\n", sinfo->arch_tape_fn);
    printf("sum_info arch_tape_date = %s\n", sinfo->arch_tape_date);
    printf("sum_info pa_status = %d\n", sinfo->pa_status);
    printf("sum_info pa_substatus = %d\n", sinfo->pa_substatus);
    printf("sum_info effective_date = %s\n", sinfo->effective_date);
*******/
    sinfo = sinfo->next;
  }
  SUM_infoEx_free(sum);
}
ftmp = StopTimer ( 0 ) ;
printf( "\nTime sec for SUM_infoEX() = %f\n\n", ftmp );
}
ftmp = StopTimer ( 1 ) ;
printf( "\nTime sec for 8 cycles of 64 SUM_infoEX() = %f\n\n", ftmp );

//now do 1 cycle of 128 or 512
sum->reqcnt = 128;
sum->sinfo = NULL;		//allow auto malloc
dsixpt = sum->dsix_ptr;
for(i=0; i < 256; i++) {
  *dsixpt++ = ix++;
}
StartTimer(0);
if(SUM_infoEx(sum, printf)) {
  printf("\nFail on SUM_infoEx()\n");
}
else {
  printf("\nOk, on SUM_infoEx()\n");
  sinfo = sum->sinfo;
  i = 1;
  while(sinfo) {
    printf("\ncount of sinfo = %d\n", i++);
    printf("sum_info sunum = %u\n", sinfo->sunum);
    sinfo = sinfo->next;
  }
  SUM_infoEx_free(sum);
}
ftmp = StopTimer ( 0 ) ;
printf( "\nTime sec for 128 SUM_infoEX() = %f\n\n", ftmp );
GOEND:
  SUM_close(sum, printf);
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
*/

#define VDUMP "/usr/local/logs/SUM/t950_status.verify"

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
