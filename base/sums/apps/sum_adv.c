/* sum_adv.c
 * Queries the sum_svc.
 * This returns an exit(1) if it is now safe to shutdown the sum_svc
 * via a sum_stop.
 * Prints out 0 if a user is still opened in SUMS and you
 * should wait for it to close before you shut down SUMS.
 * Else prints 1.
 * Normal call will turn off new SUM_open() calls in sum_svc.
 * Call with -q to not turn off new SUM_open() or to reenable if 
 * already turned off.
*/
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <sum_rpc.h>

int main(int argc, char *argv[])
{
  int shutmode, c;
  int queryonly = 0;

  while((--argc > 0) && ((*++argv)[0] == '-')) {
    while((c = *++argv[0])) {
      switch(c) {
      case 'q':
        queryonly=1;	//don't turn off/reenable new SUM_open() in sum_svc
        break;
      default:
        break;
      }
    }
  }
  if((shutmode = SUM_shutdown(queryonly, printf)) == 0) {
    printf("0\n");
    if(queryonly) 
      printf("Don't shutdown. A SUM_open() is still active. New opens still allowed\n");
    else
      printf("Don't shutdown. A SUM_open() is still active. New opens not allowed\n");
  }
  else {
    printf("1\n");
    if(queryonly)
      printf("No active opens in SUMS, New opens still allowed\n");
    else
      printf("Ok to shutdown SUMS, New opens in sum_svc not allowed\n");
  }
}
