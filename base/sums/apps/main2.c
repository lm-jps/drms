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

#define MAXSUMREQ_CNT 3


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
  int status;
  char **srcptr;
  char **destptr;
  SUMEXP_t sumexp;

  if((sum = SUM_open(NULL, NULL, printf)) == 0) {
    printf("Failed on SUM_open()\n");
    exit(1);
  }
  //uid = sum->uid;
  sumexp.uid = sum->uid;
  sumexp.host = "n02";
  sumexp.cmd = "scp";
  sumexp.src = (char **)calloc(SUMARRAYSZ, sizeof(char *));
  sumexp.dest = (char **)calloc(SUMARRAYSZ, sizeof(char *));
  srcptr = sumexp.src;
  destptr = sumexp.dest;
  *srcptr++ = "/home/jim/mark.alias";
  *destptr++ = "/tmp/jim";
  *srcptr++ = "/home/jim/.aliases";
  *destptr++ = "/tmp/jim";
  sumexp.reqcnt = 2;
  sumexp.port = 0;	//0= don't use -P in scp

  printf("Calling: SUM_export()\n");
  status = SUM_export(&sumexp, printf);

  printf("status = %d\n", status);
  free(sumexp.src); 
  free(sumexp.dest);

  SUM_close(sum, printf);
}

/*!!! THIS IS IN sumsapi/sum_open.c */
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[].
*/
/*char *datestring()
/*{
/*  struct timeval tvalr;
/*  struct tm *t_ptr;
/*  int tvalr_int;
/*                                                                              
/*  gettimeofday(&tvalr, NULL);
/*  tvalr_int = (int)tvalr.tv_sec; /* need int vrbl for this to work on sgi4*/
/*  t_ptr = localtime((const time_t *)&tvalr_int);
/*  sprintf(datestr, "%s", asctime(t_ptr));
/*  datestr[19] = NULL;
/*  return(&datestr[4]);          /* isolate the mmm dd hh:mm:ss */
/*}
*/

