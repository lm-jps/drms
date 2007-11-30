/* sum_test.c */
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sum_rpc.h>

extern int errno;
static struct timeval first[8], second[8];
SUM_t *sum;
float ftmp;


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

int main(int argc, char *argv[])
{
  int bytes, msgtag, petid, req_num, status, cnt, i, j;
  uint64_t *dsixpt;
  char **cptr;

  StartTimer(1);
  for(j =0; j< 10; j++) {
    StartTimer(2);
    if((sum = SUM_open(NULL, NULL, printf)) == 0) {
      printf("Failed on SUM_open()\n");
      exit(1);
    }
    ftmp = StopTimer(2);
    printf("Time SUM_Open = %fsec\n", ftmp);
    StartTimer(2);
    sum->mode = NORETRIEVE;
    /*sum->mode = RETRIEVE;*/
    sum->tdays = 10;
    sum->reqcnt = 2;
    dsixpt = sum->dsix_ptr;
    *dsixpt++ = 131107;
    *dsixpt++ = 131103;
/*    *dsixpt++ = 65907; */
    status = SUM_get(sum, printf);
    ftmp = StopTimer(2);
    printf("Time SUM_get = %fsec\n", ftmp);
    switch(status) {
    case 0:                       /* success. data in sum */
      cnt = sum->reqcnt;
      cptr = sum->wd;
      printf("The wd's found from the SUM_get() call are:\n");
      for(i = 0; i < cnt; i++) {
        printf("wd = %s\n", *cptr++);
      }
    break;
    case 1:                       /* error */
      printf("Failed on SUM_get()\n");
      break;
    case RESULT_PEND:             /* result will be sent later */
      break;
    default:
      printf("Error: unknown status from SUM_get()\n");
      break;
    }
    StartTimer(2);
    SUM_close(sum, printf);
    ftmp = StopTimer(2);
    printf("Time SUM_Close = %fsec\n", ftmp);
  }
  ftmp = StopTimer(1);
  printf("Time Total = %fsec\n", ftmp);

}

