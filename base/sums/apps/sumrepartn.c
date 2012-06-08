/* Send a SUMREPARTN message to all sum processes.
 * This causes each sum process to call DS_PavailRequest2()
 * which will set internal tables to a new read of the
 * sum_partn_avail table in the sums DB.
 * When a change has been made to the sum_partn_avail DB table,
 * then this sumrepartn utility is called to make
 * the change affective. Formerly, the sums would need to be
 * restarted for the sum_partn_avail table change to take effect.
 * 
 * 
*/
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <sum_rpc.h>

void usage() 
{
 printf("Send a SUMREPARTN message to all sum processes.\n");
 printf("This causes each sum process to call DS_PavailRequest2()\n");
 printf("which will set internal tables to a new read of the\n");
 printf("sum_partn_avail table in the sums DB.\n");
 printf("Formerly, the sums would need to be restarted for a table change to take effect.\n\n");
 exit(0);
}

int main(int argc, char *argv[])
{
  SUM_t *sum;
  char line[32];
  int status, c;

  while((--argc > 0) && ((*++argv)[0] == '-')) {
    while((c = *++argv[0])) {
      switch(c) {
      case 'h':
        usage();
        break;
      }
    }
  }
  printf("Do you want SUMS to read a new sum_partn_avail table (yes/no) [no] = ");
  if(gets(line) == NULL) { exit(0); }
  if(!strcmp(line, "yes")) {
    if((sum = SUM_open(NULL, NULL, printf)) == 0) {
      printf("Failed on SUM_open()\n");
      exit(1);
    }
    printf("Opened with SUMS uid=%lu\n", sum->uid);
    if(status = SUM_repartn(sum, printf)) {
      printf("ERROR on call to SUM_repartn() API\n");
    }
    SUM_close(sum, printf);
    printf("Complete\n");
  }
}
