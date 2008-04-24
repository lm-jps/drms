/* This is just play stuff.
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

if(argc != 2) { 
  printf("Usage: sumget ds_index\n"); 
  return;
}
//printf("argv[1] = %u\n", atoi(argv[1]));

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

  //sum->mode = NORETRIEVE + TOUCH;
  //sum->mode = NORETRIEVE;
  sum->mode = RETRIEVE + TOUCH;
  sum->tdays = 10;
  sum->reqcnt = 1;
  dsixpt = sum->dsix_ptr;
  *dsixpt = atol(argv[1]);
  //*dsixpt = 60331;
    StartTimer(1);
  status = SUM_get(sum, printf); 
    ftmp = StopTimer(1);
    printf("Time SUM_get = %fsec\n", ftmp);
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

      if(status = sum->status) {
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

  SUM_close(sum, printf);
  if(status) printf("Error exit\n");
  else printf("Normal exit\n");
}
