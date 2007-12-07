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
#include <soi_error.h>
/*************************/
#include <stropts.h>
#include <sys/mtio.h>
/*************************/
#include <scsi/sg.h>

extern int errno;
/*static void respd();*/
/*static char datestr[32];*/
/*char *datestring();*/


KEY *rlist;             /* The global keylist returned by unpackmsg() */
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

  if((sum = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open()\n");
    exit(1);
  }
  uid = sum->uid;
  /*sum->debugflg = 1;			/* use debug mode for future calls */
  /*sum->debugflg = 0;*/
  sum->username = "production";		/* !!TEMP */
  printf("Opened with sumid = %d\n", uid);

/*  sum->bytes = (double)120000000;	/* 120MB */
/*  sum->reqcnt = 1;
/*  if(status = SUM_alloc(sum, printf)) {	/* allocate a data segment */
/*   printf("SUM_alloc() failed to alloc %g bytes. Error code = %d\n", 
/*			sum->bytes, status);
/*   SUM_close(sum, printf);
/*   exit(1);
/*  }
/*  cptr = sum->wd;
/*  dsixpt = sum->dsix_ptr;
/*  alloc_index = *dsixpt;
/*  strcpy(alloc_wd, *cptr);
/*  printf("Allocated %g bytes at %s with dsindex=%ld\n", 
/*			sum->bytes, *cptr, alloc_index);
/*  /* put something in the alloc wd for this test */
/*  sprintf(cmd, "cp -rp /home/jim/cvs/PROTO/src/SUM %s", *cptr);
/*  printf("cmd is: %s\n", cmd);
/*  system(cmd);
/*  sprintf(cmd, "touch %s/%s%d", *cptr, "touch", uid);
/*  printf("cmd is: %s\n", cmd);
/*  system(cmd);
*/

  /*sum->mode = RETRIEVE + TOUCH;*/
  sum->mode = NORETRIEVE;
  sum->tdays = 5;
  sum->reqcnt = 3;
  dsixpt = sum->dsix_ptr;
  *dsixpt++ = 131047;
  *dsixpt++ = 99;
  *dsixpt++ = 131049;
/*  *dsixpt++ = 1159350; */
/*  *dsixpt++ = 17499; */
/*  *dsixpt++ = 14802;   */
/*  *dsixpt++ = 14539;   */
/*  *dsixpt++ = 14686;   */
/*  *dsixpt++ = 634591;  */
/*  *dsixpt++ = 634592;  */
  status = SUM_get(sum, printf); 
  switch(status) {
  case 0:			/* success. data in sum */
      cnt = sum->reqcnt;
      cptr = sum->wd;
      /*printf("The wd's found from the SUM_get() call are:\n");*/
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }
    break;
  case 1:			/* error */
    printf("Failed on SUM_get()\n");
    break;
  case RESULT_PEND:		/* result will be sent later */
    printf("SUM_get() call RESULT_PEND...\n");
    /* NOTE: the following is the same as doing a SUM_wait() */
    while(1) {
      if(!SUM_poll(sum)) break;
    }

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
ftmp = StopTimer(0);
/*printf("\nTime sec for %d SUM_get() in one call = %f\n\n", MAXSUMREQCNT, ftmp);*/

  SUM_close(sum, printf);
exit(0);
}
