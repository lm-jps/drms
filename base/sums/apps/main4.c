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
int bytes, msgtag, petid, req_num, status, cnt, i, j, k, inum;
char **cptr;
float ftmp;
uint64_t *dsixpt;
uint64_t alloc_index;
char alloc_wd[64];
char cmd[128];
char mod_name[] = "sum_rpc";
//char dsname[] = "hmi_lev1_fd_V";	/* !!TEMP name */
char dsname[] = "main4";	/* !!TEMP name */
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

  if((sum = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open()\n");
    exit(1);
  }
  uid = sum->uid;
  //sum->debugflg = 1;			/* use debug mode for future calls */
  /*sum->debugflg = 0;*/
  sum->username = "production";		/* !!TEMP */
  //sum->username = "jim";		/* !!TEMP */
  printf("Opened with sumid = %d\n", uid);

//for(k = 1; k < 14; k++) {
  sum->bytes = (double)120000000;	/* 120MB */
  sum->reqcnt = 1;
  //if(status = SUM_alloc(sum, printf)) {	
  if(status = SUM_alloc2(sum, 0x100000000, printf)) {
   printf("SUM_alloc() failed to alloc %g bytes. Error code = %d\n", 
			sum->bytes, status);
   SUM_close(sum, printf);
   exit(1);
  }
  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  alloc_index = *dsixpt;
  strcpy(alloc_wd, *cptr);
  printf("Allocated %g bytes at %s with dsindex=%lu\n", 
			sum->bytes, *cptr, alloc_index);
  // put something in the alloc wd for this test
  sprintf(cmd, "cp -rp /home/jim/junk %s", *cptr);
  printf("cmd is: %s\n", cmd);
  system(cmd);
  sprintf(cmd, "touch %s/%s%d", *cptr, "touch", uid);
  printf("cmd is: %s\n", cmd);
  system(cmd);

  //sum->mode = NORETRIEVE + TOUCH;
  sum->mode = NORETRIEVE;
  //sum->mode = RETRIEVE + TOUCH;
  sum->tdays = 10;
  sum->reqcnt = 3;
  dsixpt = sum->dsix_ptr;
  *dsixpt++ = 4294968731;
  *dsixpt++ = 4294968746;
  *dsixpt++ = 4294968722;
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

  sum->mode = TEMP;
  //sum->mode = ARCH;
  //sum->dsname = "testname";
  //sum->group = 100;
  //sum->group = 1;
  sum->group = k;
  /*sum->group = 65;*/
  /*sum->group = 101;*/
  sum->reqcnt = 1;
  dsixpt = sum->dsix_ptr;
  *dsixpt = alloc_index;        /* ds_index of alloced data segment */
//*dsixpt = 669;        /* !!!TEMP */
  //sum->debugflg = 1;		/* !!TEMP use debug mode for future calls */
  cptr = sum->wd;
  *cptr = (char *)malloc(64);
  strcpy(*cptr, alloc_wd);
  sum->dsname = dsname;
  sum->history_comment = hcomment;
  /*sum->group = 99;*/
  sum->storeset = 0;
  sum->bytes = 120000000.0;
  StartTimer(0);
  if(SUM_put(sum, printf)) {    /* save the data segment for archiving */
    printf("Error: on SUM_put()\n");
  }
  else {
    printf("The put wd = %s\n", *sum->wd);
    printf("Marked for archive data unit ds_index=%lu\n", *dsixpt);
  }
  ftmp = StopTimer(0);
  printf("Time for SUM_put() = %f sec\n", ftmp);
//}

  SUM_close(sum, printf);


}
