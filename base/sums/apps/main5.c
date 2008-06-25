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
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>


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

void fsize(char *);
void dirwalk(char *, void (*fcn)(char *));

static double total;		/* # of bytes */

/* Original call with a directory name */
double du_dir(char *dirname)
{
  total = 0.0;
  fsize(dirname);
  return(total);		/* ret # of bytes */
}


/* Get size of given file. If a dir then call dirwalk which will call us. */
void fsize(char *name)
{
  struct stat stbuf;

  if(lstat(name, &stbuf) == -1) {
    fprintf(stderr, "du_dir can't access %s\n", name); 
    return;
  }
  if((stbuf.st_mode & S_IFMT) == S_IFDIR)
    dirwalk(name, fsize);
  total = total + (double)stbuf.st_size;
}

/* Apply fcn to all files in the given dir. */
void dirwalk(char *dir, void (*fcn)(char *))
{
  char name[196];
  struct dirent *dp;
  DIR *dfd;

  if((dfd=opendir(dir)) == NULL) {
    fprintf(stderr, "du_dir can't open dir %s\n", dir);
    return;
  }
  while((dp=readdir(dfd)) != NULL) {
    if(strcmp(dp->d_name, ".") == 0
    || strcmp(dp->d_name, "..") == 0)
      continue;				/* skip self and parent */
    sprintf(name, "%s/%s", dir, dp->d_name);
    (*fcn)(name);
  }
  closedir(dfd);
}

/* Before running this you must have the sum_svc running on d00 like so:
 * sum_svc hmidb &
 * The log file will be at /usr/local/logs/SUM/sum_svc_PID.log
*/
int main(int argc, char *argv[])
{
  double size;
  char indir[96] = "/SUM6/D2640053";
  int status;
  SUM_t *sum;
  SUMID_t uid;

  //size = du_dir(indir);
  //printf("size of %s = %g\n", indir, size);
  //if((sum = SUM_open(NULL, NULL, printf)) == 0) {
  //  printf("Failed on SUM_open()\n");
  //  exit(1);
 // }
 // uid = sum->uid;
 // printf("Opened with sumid = %d\n", uid);
  DS_ConnectDB("jim");
  
  status = SUM_drop_series("jimtestdrop");
  printf("status from SUM_drop_series() = %d\n", status);
  DS_DisConnectDB();
  //SUM_close(sum, printf);
}
