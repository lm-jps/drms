#include "drms.h"
#include <printk.h>
#include <stdio.h>
#include <time.h>
#include <strings.h>

#ifdef __linux__
#include <sched.h>
#endif

/*
 * vso_sum_alloc_put.c
 *
 * This program is run from the Java code in the JSOC Mirroring Daemon (JMD).
 * The program opens an RPC connection to SUMS, does the SUMS allocation,
 * scp's the data into place, then does the SUMS put, and returns the
 * elapsed time in milliseconds that the scp command took to the JMD (so
 * that the JMD can keep track of transfer speeds).
 *
 * This program is intended to replace vso_sum_alloc.c and vso_sum_put.c, which
 * did the same thing but did so by opening and closing the socket connection
 * to SUMS. Doing things this way means the socket is opened once only,
 * which is the way SUMS is conventionally used.
 *
 * You may need to add "-lrt" to the compile command to get the library with
 * the clock functions linked in.
 *
 * Niles Oien oien@nso.edu April 2015
 */

/* Global variables for defining and parsing command line args. */
ModuleArgs_t module_args[] =
{
   {ARG_INT,    "sunum",         NULL,    "SU number",                              ""},
   {ARG_INT,    "size",          NULL,    "Size in bytes of the SU",                ""},
   {ARG_STRING, "seriesname",    NULL,    "SU series name",                         ""},
   {ARG_INT,    "retention",     "-3",     "retention time (days)",                  ""},
   {ARG_STRING, "scpcommand",    NULL,    "Command to scp with, like /usr/bin/scp", ""},
   {ARG_STRING, "scphost",       NULL,    "Host to copy from",                      ""},
   {ARG_STRING, "scpuser",       NULL,    "User to copy as",                        ""},
   {ARG_INT,    "scpport",       "55000", "Port to scp over, like 55000",           ""},
   {ARG_STRING, "scpremotepath", NULL,    "Remote path to copy from",               ""},
   {ARG_INT,    "debug",         "0",     "How noisy to be, default silent",        ""},
   {ARG_STRING, "logfile",       "NONE",  "Log file for ERRORS ONLY. NONE means do not log.", ""},
   {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;
CmdParams_t cmdparams;

/* Small function to barf out a dying gasp message in a log file if something went bad */
void error_message_to_logfile(char *logfile, char *message, long sunum, long size, int status){

 FILE *fp;
 time_t now;
 struct tm *runTime;
 char runTimeStr[64];

 /* If the logfile is set to NONE, we are not logging. */
 if (strcmp(logfile, "NONE") == 0){
  return;
 }

 /* If we got here, we are logging. Get the current time, open the log file append and
    put our message in it. */

 fp = fopen(logfile, "a");
 if (fp == NULL) return;

 now = time(NULL);
 runTime = localtime( &now );
 sprintf(runTimeStr, "%d/%02d/%02d %02d:%02d:%02d",
         runTime->tm_year + 1900, runTime->tm_mon + 1, runTime->tm_mday,
         runTime->tm_hour, runTime->tm_min, runTime->tm_sec);


 fprintf(fp, "%s : %s : sunum=%ld, size=%ld status=%d\n", runTimeStr, message, sunum, size, status);

 fclose(fp);

 return;

}

/* Small function to convert a timespec structure to seconds as a double */
double timespecToDouble(struct timespec t){
 return (double)t.tv_sec + (double)t.tv_nsec / 1000000000.0;
}

/* 
 * Small function to print the message we need to print prior to exiting so that 
 * the Java code can parse it and know how we did. The return value, which is also
 * the value the program exits with, is one of :
 * 
 *  0 : Successful termination
 * -1 : Failed to parse command line
 * -2 : SUM_open() failed
 * -3 : SUM_alloc2() failed
 * -4 : Failed to get current time
 * -5 : scp command returned non-zero status
 * -6 : SUM_put() call failed
 * -7 : SUM_close() call failed
 *
 * If the return value is non-zero the elapsed time is set to be equal to it and
 * should be ignored. This is the only thing sent to stdout, everything else
 * (excluding debugging, if any) goes to stderr.
 *
 */
void printFinal(double elapsedMillisec, int returnVal, char *localDir){
  if (localDir == NULL){	
   fprintf(stdout, "elapsedMillisec:%d;exitStatus:%d;localpath:NONE\n", (int)elapsedMillisec, returnVal);
  } else {
   fprintf(stdout, "elapsedMillisec:%d;exitStatus:%d;localpath:%s\n", (int)elapsedMillisec, returnVal, localDir);
  }
  fflush(stdout);
  return;
}


/*----------------- Main program starts. ----------------*/
int main(int argc, char *argv[]) {

  /* Stuff for the alloc part (the first part) */
  long long    sunum     =   0;
  uint64_t     size      =   0;
  SUM_t       *sum       =   (SUM_t *) NULL;

  /* Stuff for the put part (the second part) */
  int       retention    =   60;
  char     *seriesname   =   (char *) NULL;

  /* Stuff for the scp (which now takes place between the first and second parts) */
  char     *scpcommand;
  char     *scphost;
  char     *scpuser;
  int       scpport;
  char     *scpremotepath;
  char     *scplocalpath;
  char     *logfile;

  struct timespec t1,t2;
  double elapsedTimeMilli;
  
  char      command[1024];
  int       status       =   0;

  int debug;
  int retVal;

  double bytesPerSec;
  int numSpeedFactors=4;
  int speedFactorIndex;
  double speedScaleFactor[] = { 1.0, 1024.0, 1048576.0, 1073741824.0 }; /* Increasing powers of 1024 */
  char *speedDesc[] = { "bytes/sec", "kilobytes/sec", "megabytes/sec", "gigabytes/sec" };
  int i;
 
  FILE *goFile;
  FILE *debugFile;
  char goFileName[512];
  char debugFileName[512];
  time_t now;
  struct tm *dbgTime;
  char dbgTimeStr[64];
 
  /* Parse the command line */
  if ((status = cmdparams_parse(&cmdparams, argc, argv)) < CMDPARAMS_SUCCESS){
    fprintf(stderr,"Error: Command line parsing failed. Aborting. [%d]\n\n", status);
    fprintf(stderr, "Usage : %s sunum=X size=X seriesname=X scpcommand=X scphost=X  \\\n", argv[0]);
    fprintf(stderr, "           scpuser=X scpremotepath=X [scpport=X retention=X debug=X logfile=X]\n\n");
    fprintf(stderr, "Items in square brackets are optional, by default scpport=55000 retention=3 debug=0\n\n");

    fprintf(stderr, "EXAMPLE : vso_sum_alloc_put sunum=665937791 size=565107497 seriesname=hmi.s_720s \\\n");
    fprintf(stderr, "          scpcommand=/opt/bin/scp scphost=jsocport.stanford.edu scpuser=jsocexp \\\n");
    fprintf(stderr, "          scpremotepath=/SUM60/D665937791 debug=5 logfile=/home/production/vso_sum_alloc_put.log\n\n");

    fprintf(stderr, "NOTE : No logging is done if logfile=NONE, which is the default. Also, only errors\n");
    fprintf(stderr, "       are appended to this file - in normal operation, this log file will not even\n");
    fprintf(stderr, "       be created.\n\n");

    fprintf(stderr, "FEATURE : If the file $HOME/vso_sum_alloc_put.debug exists (in the home directory\n");
    fprintf(stderr, "          of the user running the JMD, which in turn runs this) then a brief message\n");
    fprintf(stderr, "          will be written for successfully downloaded sunums to the file\n");
    fprintf(stderr, "          $HOME/vso_sum_alloc_put.debugLog. Use this sparingly.\n\n");

    fprintf(stderr, "Niles Oien oien@nso.edu April 2015\n\n");
    printFinal(-1.0, -1, NULL);
    return -1;
  }

  sunum         = cmdparams_get_int64(&cmdparams, "sunum", NULL);
  size          = cmdparams_get_int64(&cmdparams, "size", NULL);
  seriesname    = strdup(cmdparams_get_str(&cmdparams, "seriesname", NULL));
  retention     = cmdparams_get_int(&cmdparams, "retention", NULL);

  scpcommand    = strdup(cmdparams_get_str(&cmdparams, "scpcommand",    NULL));
  scphost       = strdup(cmdparams_get_str(&cmdparams, "scphost",       NULL));
  scpuser       = strdup(cmdparams_get_str(&cmdparams, "scpuser",       NULL));
  scpport       = cmdparams_get_int64(&cmdparams, "scpport", NULL);
  scpremotepath = strdup(cmdparams_get_str(&cmdparams, "scpremotepath", NULL));
  debug         = cmdparams_get_int64(&cmdparams, "debug", NULL);
  logfile       = strdup(cmdparams_get_str(&cmdparams, "logfile",    NULL));

  /* Open a socket so we can talk to SUMS */
  if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL){
    fprintf(stderr,"ERROR: drms_open: Failed to connect to SUMS.\n");
    printFinal(-2.0, -2, NULL);
    error_message_to_logfile(logfile, "SUM_open() returned NULL", (long)sunum, (long)size, 0); 
    return -2;
  }

  if (sum->status){
    fprintf(stderr,"ERROR: drms_open: Returned status %d\n", sum->status);
    error_message_to_logfile(logfile, "SUM_open() returned bad sum->status", (long)sunum, (long)size, sum->status);
    printFinal(-2.0, -2, NULL);
    return -2;
  }

  if (debug > 1){
    fprintf(stdout, "SUM_open() successful\n");
  }
  
  /* Get ready to do the alloc */
  sum->reqcnt = 1;
  sum->bytes = size;

  /* Make RPC call to the SUM server for alloc */
  if ((status = SUM_alloc2(sum, sunum, printf))){
    fprintf(stderr,"ERROR: SUM_alloc2 RPC call failed with error code %d\n", status);
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum, printf);
#endif
    error_message_to_logfile(logfile, "SUM_alloc2() returned bad status", (long)sunum, (long)size, status);
    printFinal(-3.0, -3, NULL);
    return -3;
  }

  if (sum->status){
    fprintf(stderr,"ERROR: SUM_alloc2 RPC call returned status %d\n", sum->status);
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "SUM_alloc2() returned bad sum->status", (long)sunum, (long)size, sum->status);
    printFinal(-3.0, -3, NULL);
    return -3;
  }

  scplocalpath = strdup(sum->wd[0]);

  if (debug > 1){
    fprintf(stdout, "SUM_alloc2() successful for %d bytes, have local directory %s\n",
	    size, scplocalpath);
  }
  
  /* Now do the scp, and time it. */
  /* To do : Maybe execute this in a fork? Although maximum scp timeout is configurable... */
  if (debug){ /* Take -q out of command in debug mode */
   sprintf(command,"%s -pr -P%d %s@%s:%s/* %s",
    scpcommand, scpport, scpuser, scphost, scpremotepath, scplocalpath);
  } else {
   sprintf(command,"%s -qpr -P%d %s@%s:%s/* %s",
    scpcommand, scpport, scpuser, scphost, scpremotepath, scplocalpath);
  }

  if (debug > 0){
    fprintf(stdout, "Preparing to execute command : %s\n", command);
  }
  
  if (clock_gettime(CLOCK_REALTIME, &t1)){
    fprintf(stderr, "Failed to get start time\n");
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "clock_gettime() failed", (long)sunum, (long)size, 0);
    printFinal(-4.0, -4, scplocalpath);
    return -4;
  }

  retVal=system(command);
  
  if (clock_gettime(CLOCK_REALTIME, &t2)){
    fprintf(stderr, "Failed to get end time\n");
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "clock_gettime() Failed", (long)sunum, (long)size, 0);
    printFinal(-4.0, -4, scplocalpath);
    return -4;
  }

  elapsedTimeMilli=1000.0*(timespecToDouble(t2)-timespecToDouble(t1));

  if (debug > 0){
    bytesPerSec=(double)size/(elapsedTimeMilli/1000.0);
    speedFactorIndex = 0;
    for (i=0; i < numSpeedFactors-1; i++){
     if (bytesPerSec >= speedScaleFactor[i+1]){
      speedFactorIndex++;
     } 
    }
    fprintf(stdout, "Command took %lf ms (%lf min) [%lf %s] and returned %d\n",
             elapsedTimeMilli, elapsedTimeMilli/60000.0, bytesPerSec/speedScaleFactor[speedFactorIndex],
             speedDesc[speedFactorIndex],  retVal);
  }


  if (retVal){
    fprintf(stderr, "scp command failed with return %d : %s\n", retVal, command);
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "scp returned non-zero", (long)sunum, (long)size, retVal);
    printFinal(-5.0, -5, scplocalpath);
    return -5;
  }
  
  sum->dsname = seriesname;
  sum->group =  0;
  sum->mode =   TEMP + TOUCH;
  sum->tdays =  retention;
  sum->reqcnt = 1;
  sum->history_comment = "";

  sum->dsix_ptr[0] = sunum;
  sum->wd[0] = scplocalpath;

  /* Make RPC call to the SUM server to do the put */
  if ((status = SUM_put(sum, printf))) {
    fprintf(stderr,"ERROR: SUM_put RPC call failed with error code %d\n", status);
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    SUM_rollback(sum, printf);
#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "SUM_put() returned non-zero status", (long)sunum, (long)size, status);
    printFinal(-6.0, -6, scplocalpath);
    return -6;
  }

  if (sum->status){
    fprintf(stderr,"ERROR: SUM_put RPC returned %d\n", sum->status);
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION

#else
    SUM_close(sum,printf);
#endif
    error_message_to_logfile(logfile, "SUM_put() returned non-zero sum->status", (long)sunum, (long)size, sum->status);
    printFinal(-6.0, -6, scplocalpath);
    return -6;
  }


  if (debug > 1){
    fprintf(stdout, "SUM_put() succeeded\n");
  }

  status=SUM_close(sum,printf);
  if (status){
    fprintf(stderr,"ERROR: SUM_close RPC call failed with error code %d\n", status);
    error_message_to_logfile(logfile, "SUM_close() returned non-zero status", (long)sunum, (long)size, status);
    printFinal(-7.0, -7, scplocalpath);
    return -7;
  }

  if (sum->status){
    fprintf(stderr,"ERROR: SUM_close RPC call failed with sum->status %d\n", sum->status);
    error_message_to_logfile(logfile, "SUM_close() created non-zero sum->status", (long)sunum, (long)size, sum->status);
    printFinal(-7.0, -7, scplocalpath);
    return -7;
  }

  if (debug > 1){
   fprintf(stdout, "SUM_close() complete\n");
  }

  /* Print the scp time for Java to parse so that the JMD has the scp speed information. */
  printFinal(elapsedTimeMilli, 0, scplocalpath);

  /* A little back door to allow debugging of the system. If the file 
     $HOME/vso_sum_alloc_put.debug exists and is readable, then write
     very brief information about completed download to the file
     $HOME/vso_sum_alloc_put.debugLog */
  sprintf(goFileName, "%s/vso_sum_alloc_put.debug", getenv("HOME"));
  goFile=fopen(goFileName, "r");
  if (goFile != NULL){
   fclose(goFile);
   sprintf(debugFileName, "%s/vso_sum_alloc_put.debugLog", getenv("HOME"));
   debugFile=fopen(debugFileName, "a");
   if (debugFile != NULL){

    now = time(NULL);
    dbgTime = localtime( &now );
    sprintf(dbgTimeStr, "%d/%02d/%02d %02d:%02d:%02d",
         dbgTime->tm_year + 1900, dbgTime->tm_mon + 1, dbgTime->tm_mday,
         dbgTime->tm_hour, dbgTime->tm_min, dbgTime->tm_sec);

    fprintf(debugFile, "%s : completed sunum=%ld size=%ld elapsedMilliSec=%d path=%s\n", dbgTimeStr, (long)sunum, (long)size,
                        (int)elapsedTimeMilli, scplocalpath);
    fclose(debugFile);
   } 
  }

  /* Free up stuff we've strdup'd - probably a bit over the top to set to NULL, but why not */
 
  if (seriesname != NULL) {
    free(seriesname); seriesname=NULL;
  }
  
  if (scpcommand != NULL) {
    free(scpcommand); scpcommand=NULL;
  }
  
  if (scphost != NULL) {
    free(scphost); scphost=NULL;
  }
  
  if (scpuser != NULL) {
    free(scpuser); scpuser=NULL;
  }
  
  if (scpremotepath != NULL) {
    free(scpremotepath); scpremotepath=NULL;
  }
  
  if (scplocalpath != NULL) {
    free(scplocalpath); scplocalpath=NULL;
  }

  if (logfile != NULL) {
    free(logfile); logfile=NULL;
  }
  
  return 0;

}

