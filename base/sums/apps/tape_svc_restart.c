/* tape_svc_restart.c
 *
 * This is for restarting a tape_svc on d02 and sending sum_svc a
 * message to connect to the new tape_svc.
 *
 * Usage: tape_svc_restart
*/

#include <SUM.h>
#include <sys/socket.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>

struct timeval TIMEOUT = { 30, 0 };

CLIENT *current_client, *clntsum, *clntget, *clntget1, *clntget2;
char hostn[MAX_STR];
char datestr[32];
char *username;

//here are all the rpc prog names and vesions to create client handle for
//!!!NO. Didn't use. Do explicitly for these few.
char *sname[] = {"SUMPROG", "SUMGET", "SUMGET1", "SUMGET2", "EOD"};
char *vname[] = {"SUMVERS", "SUMGETV", "SUMGETV", "SUMGETV", "EOD"};

/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[]. */
static char *datestring()
{
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  datestr[19] = (char)NULL;
  return(&datestr[4]);          /* isolate the mmm dd hh:mm:ss */
}


/*********************************************************/
void get_cmd(int argc, char *argv[])
{
  char *cptr;

  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    printf("!!NOTE: You must be user production to run tape_svc_restart!\n");
    exit(1);
  }
  gethostname(hostn, MAX_STR);
  cptr = index(hostn, '.');     // must be short form
  if(cptr) *cptr = (char)NULL;
  if(strcmp(hostn, "d02")) {
    printf("!!NOTE: tape_svc_restart must be run on d02!\n");
    exit(1);
  }
}

int send_mail(char *fmt, ...)
{
  va_list args;
  char string[1024], cmd[1024];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  sprintf(cmd, "echo \"%s\" | Mail -s \"test mail\" jeneen@sun.stanford.edu,jim@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}


/*********************************************************/
int main(int argc, char *argv[])
{
  char *call_err;
  char line[32];
  uint32_t restartback;
  enum clnt_stat status;
  KEY *list = newkeylist();
  int i;
  char **p = sname;

//  for(i=0; i < numprogs; i++) {
//    if(!strcmp(*p, "EOD")) break;
//    ++p;
//  }

  get_cmd(argc, argv);
  printf("\nYou must first take these steps to stop and start a tape_svc on d02\n\n");
  printf("Do you want sum_svc to disconnect from tape_svc (yes/no) [no] = ");
  if(gets(line) == NULL) { return; }
  if(!strcmp(line, "yes")) { 
    /* Create client handle used for calling the sum_svc on j1 */
    clntsum = clnt_create(SUMSVCHOST, SUMPROG, SUMVERS, "tcp");
    if(!clntsum) {       /* server not there */
      clnt_pcreateerror("Can't get client handle to sum_svc in tape_svc_restart");
      printf("***tape_svc_restart can't get sum_svc on %s\n", hostn);
      printf("Please consider what this means. You might want to restart all of SUMS\n");
      printf("Going to skip the call to sums.\n");
      goto SKIPSUMSCALL;
      //exit(1);
    }
    setkey_str(&list, "HOST", hostn);
    setkey_str(&list, "USER", username);
    setkey_str(&list, "ACTION", "close");
    status =clnt_call(clntsum, TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntsum, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        printf("Going to skip the call to sums.\n");
        printf("Please consider what this means. You might want to restart all of SUMS\n");
        goto SKIPSUMSCALL;
    }
    switch(restartback) {
    case 0:
      printf("Success sum_svc has closed to tape_svc\n");
      break;
    case 1:
      printf("Failure sum_svc has not closed to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to sum_svc\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to sum_svc\n");
      exit(1);
      break;
    }
    clntget = clnt_create(SUMSVCHOST, SUMGET, SUMGETV, "tcp");
    if(!clntget) {       /* server not there */
      clnt_pcreateerror("Can't get client handle to Sget in tape_svc_restart");
      printf("***tape_svc_restart can't get Sget on %s\n", hostn);
      printf("Please consider what this means. You might want to restart all of SUMS\n");
      printf("Going to skip the call to sums.\n");
      goto SKIPSUMSCALL;
    }
    status =clnt_call(clntget, TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        printf("Going to skip the call to sums.\n");
        printf("Please consider what this means. You might want to restart all of SUMS\n");
        goto SKIPSUMSCALL;
    }
    switch(restartback) {
    case 0:
      printf("Success Sget has closed to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget has not closed to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    }
    clntget1 = clnt_create(SUMSVCHOST, SUMGET1, SUMGETV, "tcp");
    if(!clntget1) {       /* server not there */
      clnt_pcreateerror("Can't get client handle to Sget1 in tape_svc_restart");
      printf("***tape_svc_restart can't get Sget1 on %s\n", hostn);
      printf("Please consider what this means. You might want to restart all of SUMS\n");
      printf("Going to skip the call to sums.\n");
      goto SKIPSUMSCALL;
    }
    status =clnt_call(clntget1,TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget1, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        printf("Going to skip the call to sums.\n");
        printf("Please consider what this means. You might want to restart all of SUMS\n");
        goto SKIPSUMSCALL;
    }
    switch(restartback) {
    case 0:
      printf("Success Sget1 has closed to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget1 has not closed to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget1\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    }
    clntget2 = clnt_create(SUMSVCHOST, SUMGET2, SUMGETV, "tcp");
    if(!clntget2) {       /* server not there */
      clnt_pcreateerror("Can't get client handle to Sget2 in tape_svc_restart");
      printf("***tape_svc_restart can't get Sget2 on %s\n", hostn);
      printf("Please consider what this means. You might want to restart all of SUMS\n");
      printf("Going to skip the call to sums.\n");
      goto SKIPSUMSCALL;
    }
    status =clnt_call(clntget2,TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget2, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        printf("Going to skip the call to sums.\n");
        printf("Please consider what this means. You might want to restart all of SUMS\n");
        goto SKIPSUMSCALL;
    }
    switch(restartback) {
    case 0:
      printf("Success Sget2 has closed to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget2 has not closed to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget2\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    }
  }
  else return;

SKIPSUMSCALL:

  printf("Do as production on d02:\n\n");
  printf("> /home/production/cvs/JSOC/base/sums/scripts/sum_stop_d02_tape\n");
  printf("> sum_forker jsoc_sums 2010.12.14.115800\n\n");

  printf("Have you successfully started a new tape_svc and now want sum_svc\n");
  printf("to connect to it (yes/no) [no] = ");
  if(gets(line) == NULL) { return; }
  if(!strcmp(line, "yes")) { 
    send_mail("tape_svc has been manually restarted\n");
    setkey_str(&list, "HOST", hostn);
    setkey_str(&list, "USER", username);
    setkey_str(&list, "ACTION", "reconnect");
    status =clnt_call(clntsum, TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntsum, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        exit(1);
    }
    switch(restartback) {
    case 0:
      printf("Success sum_svc has reconnected to tape_svc\n");
      break;
    case 1:
      printf("Failure sum_svc has not reconnected to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to sum_svc\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to sum_svc\n");
      exit(1);
      break;
    }
    status =clnt_call(clntget, TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        exit(1);
    }
    switch(restartback) {
    case 0:
      printf("Success Sget has reconnected to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget has not reconnected to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget\n");
      exit(1);
      break;
    }
    status =clnt_call(clntget1,TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget1, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        exit(1);
    }
    switch(restartback) {
    case 0:
      printf("Success Sget1 has reconnected to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget1 has not reconnected to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget1\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget1\n");
      exit(1);
      break;
    }
    status =clnt_call(clntget2,TAPERECONNECTDO,(xdrproc_t)xdr_Rkey,(char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&restartback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clntget2, "Err clnt_call for TAPERECONNECTDO");
        printf("%s %s\n", datestring(), call_err);
        exit(1);
    }
    switch(restartback) {
    case 0:
      printf("Success Sget2 has reconnected to tape_svc\n");
      break;
    case 1:
      printf("Failure Sget2 has not reconnected to tape_svc\n");
      break;
    case -1:
      printf("**Error in TAPERECONNECTDO call to Sget2\n");
      exit(1);
      break;
    default:
      printf("**Error in TAPERECONNECTDO call to Sget2\n");
      exit(1);
      break;
    }
  }
}

