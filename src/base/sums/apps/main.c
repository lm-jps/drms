#include <SUM.h>
#include <soi_key.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <sum_rpc.h>
#include <soi_error.h>

extern int errno;
#define NUMTIMERS 10            /* number of seperate timers avail */

KEY *rlist;             /* The global keylist returned by unpackmsg() */
FILE *logfp;
SUM_t *sum;
SUM_t *sum1, *sum2, *sum3;
SUMID_t uid;
int soi_errno = NO_ERROR;
int waitcnt = 0;
int bytes, msgtag, petid, req_num, status, cnt, i, j;
char **cptr;
uint64_t *dsixpt;
uint64_t alloc_index;
char alloc_wd[64];
char cmd[128];
char mod_name[] = "sum_rpc";
char dbname[] = "jsoc";
char dsname[] = "hmi_lev1_fd_V";	/* !!TEMP name */
char hcomment[] = "this is a dummy history comment that is greater than 80 chars long to check out the code";
static struct timeval first[NUMTIMERS], second[NUMTIMERS];

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

int SUM_delete_series_test(SUMID_t *ids)
{
  SUMID_t sunum;
  int i;

  for(i=0; ; i++) {
    if((sunum = ids[i]) == 0) {
      break;
    }
  }
  return(i);
}


/* Before running this you must have the sum_svc running on d00 like so:
 * sum_svc hmidb &
 * The log file will be at /usr/local/logs/SUM/sum_svc_PID.log
*/
int main(int argc, char *argv[])
{
  double bytesdel;
  float ftmp;

  printk_set(printf, printf);
/**************************************
  DS_ConnectDB(dbname);
  DS_RmDo(&bytesdel);
  printf("bytes del from DS_RmDo = %e\n", bytesdel);
  exit(1);
***************************************/

  StartTimer(0);
  if((sum1 = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open() for sum1\n");
    exit(1);
  }
  ftmp = StopTimer(0);
  /*printf("Time sec for SUM_open() = %f\n", ftmp);*/
  uid = sum1->uid;
  printf("Opened sum1 with sumid = %d\n", uid);
  /*sum1->mode = RETRIEVE + TOUCH;*/
  sum1->mode = RETRIEVE;
  sum1->tdays = 10;
  sum1->reqcnt = 1;
  sum1->status = -1;
  dsixpt = sum1->dsix_ptr;
  *dsixpt++ = 861;
  /* *dsixpt++ = 50; */
  StartTimer(1);
  status = SUM_get(sum1, printf); 
  ftmp = StopTimer(1);
  /*printf("Time sec for SUM_get() w/o TOUCH = %f\n", ftmp);*/
  /*printf("Time sec for SUM_get() = %f\n", ftmp);*/
  switch(status) {
  case 0:			/* success. data in sum1 */
    break;
  case 1:			/* error */
    printf("Failed on SUM_get() for sum1\n");
    break;
  case RESULT_PEND:		/* result will be sent later */
    printf("SUM_get() sum1 call RESULT_PEND...\n");
    waitcnt++;
    break;
  default:
    printf("Error: unknown status from SUM_get()\n");
    break;
  }

  StartTimer(4);
  if((sum2 = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open() for sum2\n");
    exit(1);
  }
  ftmp = StopTimer(4);
  /*printf("\nTime sec for SUM_open() = %f\n", ftmp);*/
  uid = sum2->uid;
  printf("Opened sum2 with sumid = %d\n", uid);
  /*sum2->mode = RETRIEVE + TOUCH;*/
  sum2->mode = RETRIEVE;
  sum2->tdays = 10;
  sum2->reqcnt = 5;
  sum2->status = -1;
  dsixpt = sum2->dsix_ptr;
  *dsixpt++ = 894;
  *dsixpt++ = 871;
  *dsixpt++ = 864;
  *dsixpt++ = 909;
  *dsixpt++ = 877;
  /* *dsixpt++ = 51; */
  StartTimer(2);
  status = SUM_get(sum2, printf); 
  ftmp = StopTimer(2);
  /*printf("Time sec for SUM_get() = %f\n\n", ftmp);*/
  switch(status) {
  case 0:			/* success. data in sum2 */
    break;
  case 1:			/* error */
    printf("Failed on SUM_get() for sum2\n");
    break;
  case RESULT_PEND:		/* result will be sent later */
    printf("SUM_get() sum2 call RESULT_PEND...\n");
    waitcnt++;
    break;
  default:
    printf("Error: unknown status from SUM_get()\n");
    break;
  }

  if((sum3 = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open() for sum3\n");
    exit(1);
  }
  uid = sum3->uid;
  printf("Opened sum3 with sumid = %d\n", uid);
  sum3->mode = RETRIEVE;
  sum3->tdays = 10;
  sum3->reqcnt = 1;
  sum3->status = -1;
  dsixpt = sum3->dsix_ptr;
  *dsixpt++ = 885;
  /* *dsixpt++ = 51; */
  status = SUM_get(sum3, printf); 
  switch(status) {
  case 0:			/* success. data in sum3 */
    break;
  case 1:			/* error */
    printf("Failed on SUM_get() for sum3\n");
    break;
  case RESULT_PEND:		/* result will be sent later */
    printf("SUM_get() sum3 call RESULT_PEND...\n");
    waitcnt++;
    break;
  default:
    printf("Error: unknown status from SUM_get()\n");
    break;
  }
  if(waitcnt) printf("Will now poll for completion msgs...\n");
  while(waitcnt) {
    if(!SUM_poll(sum1)) {	/* something has completed */
      if(sum1->status != -1) {
        printf("sum1 complete w/status=%d\n", sum1->status);
      }
      if(sum2->status != -1) {
        printf("sum2 complete w/status=%d\n", sum2->status);
      }
      if(sum3->status != -1) {
        printf("sum3 complete w/status=%d\n", sum3->status);
      }
      waitcnt--;
    }
  }
      cnt = sum1->reqcnt;
      cptr = sum1->wd;
      printf("The wd's found from the sum1 SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }
      cnt = sum2->reqcnt;
      cptr = sum2->wd;
      printf("The wd's found from the sum2 SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }
      cnt = sum3->reqcnt;
      cptr = sum3->wd;
      printf("The wd's found from the sum3 SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }

  SUM_close(sum1, printf);
  SUM_close(sum2, printf);
  SUM_close(sum3, printf);

  /* test out some new, independent stuff */
  SUMID_t ids[10];

  ids[0] = 100;
  ids[1] = 101;
  ids[2] = 102;
  ids[3] = 103;
  ids[4] = 0;
  /*status = SUM_delete_series_test(ids);*/
  /*status = SUM_delete_series(ids, printf);*/
  /*printf("status = %d\n", status);*/

}

