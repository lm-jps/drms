/**
For Full SUMS API see:\n
http://sun.stanford.edu/web.hmi/development/SU_Development_Plan/SUM_API.html

   @addtogroup sum_api
   @{
*/

/**
   @fn SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))

	A DRMS instance opens a session with SUMS. It gives the  server
	name to connect to, defaults to SUMSERVER env else SUMSERVER define.
	The db name has been depricated and has no effect. The db will be
	the one that sum_svc was started with, e.g. sum_svc hmidb.
	The history is a printf type logging function.
	Returns a pointer to a SUM handle that is
	used to identify this user for this session. 
	Returns NULL on failure.
	Currently the dsix_ptr[] and wd[] arrays are malloc'd to size
	SUMARRAYSZ (64).
*/

/**
  @}
*/


/* sum_open.c */
/* Here are the API functions for SUMS.
 * This is linked in with each program that is going to call SUMS.
*/
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sum_rpc.h>
#include "serverdefs.h"

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <Python.h>

struct Pickler_struct
{
    PyObject *globalDict;
    PyObject *pickleModule; /* We need a handle to this to free it. */
    PyObject *pickleDict;
};

typedef struct Pickler_struct Pickler_t;
#endif

extern int errno;

/* Static prototypes. */
SUMID_t sumrpcopen_1(KEY *argp, CLIENT *clnt, int (*history)(const char *fmt, ...));
static void respd(struct svc_req *rqstp, SVCXPRT *transp);
static KEY *respdoarray_1(KEY *params);

int getanymsg(int block);
static int getmsgimmed(void);
static char *datestring(void);

/* External prototypes */
extern void printkey (KEY *key);

//static struct timeval TIMEOUT = { 240, 0 };
static struct timeval TIMEOUT = { 3600, 0 };
static int RESPDO_called;
static SUMOPENED *sumopened_hdr = NULL;/* linked list of opens for this client*/
static CLIENT *cl, *clalloc, *clget, *clput, *clinfo, *cldelser;
static CLIENT *clopen,*clopen1,*clopen2,*clopen3,*clopen4,*clopen5,*clopen6,*clopen7;
static CLIENT *clprev, *clclose;
static SVCXPRT *transp[MAXSUMOPEN];
static SUMID_t transpid[MAXSUMOPEN];
static int numopened = 0;
static int numSUM = 0;
static int taperdon_cleared = 0;
//KEY *infoparams;

int rr_random(int min, int max)
{
  return rand() % (max - min + 1) + min;
}


/* Returns 1 if ok to shutdown sums.
 * Return 0 is someone still has an active SUM_open().
 * Once called, will prevent any user from doing a new SUM_open()
 * unless query arg = 1.
*/
int SUM_shutdown(int query, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *server_name, *cptr, *username;
  char *call_err;
  int response;
  enum clnt_stat status;

  if (!(server_name = getenv("SUMSERVER")))
  {
    server_name = alloca(sizeof(SUMSERVER)+1);
    strcpy(server_name, SUMSERVER);
  }
  cptr = index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = '\0';
  /* Create client handle used for calling the server */
  cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
  if(!cl) {              //no SUMPROG in portmap or timeout (default 25sec?)
    clnt_pcreateerror("Can't get client handle to sum_svc");
    (*history)("sum_svc timeout or not there on %s\n", server_name);
    (*history)("Going to retry in 1 sec\n");
    sleep(1);
    cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
    if(!cl) { 
      clnt_pcreateerror("Can't get client handle to sum_svc");
      (*history)("sum_svc timeout or not there on %s\n", server_name);
      return(1);	//say ok to shutdown
    }
  }
  clprev = cl;
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  klist = newkeylist();
  setkey_str(&klist, "USER", username);
  setkey_int(&klist, "QUERY", query);
  status = clnt_call(cl, SHUTDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&response, TIMEOUT);
  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(cl, "Err clnt_call for SHUTDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    return (1);
  }
  return(response);
}

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
static MSUMSCLIENT_t ConnectToMultiSums(Pickler_t **pickler, int (*history)(const char *fmt, ...))
{
    MSUMSCLIENT_t msums = -1;
    int sockfd = -1;
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    char service[16];
    struct sockaddr_in server;

    PyObject *mainModule = NULL;
    PyObject *globalDict = NULL;
    PyObject *pickleModule = NULL;
    PyObject *pickleDict = NULL;
            
    if (pickler)
    {
        *pickler = calloc(1, sizeof(Pickler_t));
        
        if (!pickler)
        {
            (*history)("No memory with which to pickle.\n");
        }
        else
        {
            memset(&hints, 0, sizeof(struct addrinfo));
            hints.ai_flags = 0; /* A field to make this as complicated as possible. */
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = 0; /* Another field to make this as complicated as possible. */
    
            snprintf(service, sizeof(service), "%d", SUMSD_LISTENPORT);
            // if (getaddrinfo(SUMSERVER, service, &hints, &result) != 0)
            if (getaddrinfo("k1.stanford.edu", service, &hints, &result) != 0)
            {
                (*history)("Unable to get SUMS server address.\n");
            }
            else
            {
                if ((sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol)) == -1)
                {
                    (*history)("");
                }
                else
                {
                    /* connect the socket to the server's address */
                    if (connect(sockfd, result->ai_addr, result->ai_addrlen) == -1)
                    {
                        (*history)("Unable to connect to SUMS server (errno = %d).\n", errno);
                    }
                    else
                    {
                        msums = (MSUMSCLIENT_t)sockfd;
                    }
            
                    freeaddrinfo(result);
                }
            }
    
            /* We also have to initialize the Python engine that we use to pickle and unpickle messages
             * that we send over the socket. This requires that an environment variable be set. */
            setenv("PYTHONHOME", PYTHONHOME, 1);
            Py_Initialize();
    
            /* Load pickle functions and constant. */
            pickleModule = PyImport_ImportModule("pickle");
        
            if (pickleModule)
            {
                /* This is a new reference. It must be dereferenced when we shut down the SUMS connection. */
                (*pickler)->pickleModule = pickleModule;
                (*pickler)->pickleDict = PyModule_GetDict(pickleModule);
            }
            else
            {
                (*history)("Unable to load pickle package.\n");
                msums = -1;
            }
    
            if (msums >= 0)
            {
                mainModule = PyImport_AddModule("__main__");
                if (mainModule)
                {   /* mainModule is a borrowed reference. It does not need to be dereferenced. */
                    (*pickler)->globalDict = PyModule_GetDict(mainModule);        
                }
                else
                {
                    (*history)("Unable to main module.\n");
                    msums = -1;
                }
            }
        }
    }
    else
    {
        (*history)("Invalid argument - pickler must not be NULL.\n");
    }
    
    return msums;
}

static void DisconnectFromMultiSums(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    if (sums && sums->mSumsClient != -1)
    {
        int sockfd = (int)sums->mSumsClient;
        shutdown(sockfd, SHUT_RDWR);
        close(sockfd);
        
        /* Mark the socket variable as not being used. */
        sums->mSumsClient = -1;

        /* Remove the refcount on the pickle modle. */
        if (sums->pickler)
        {
            if (sums->pickler->globalDict)
            {
                /* We were only borrowing the global dictionary reference - no need to decrement pointer. */
                sums->pickler->globalDict = NULL;
            }

            if (sums->pickler->pickleModule)
            {
                /* We own the reference - decrement it. */
                Py_DECREF(sums->pickler->pickleModule);
                sums->pickler->pickleModule = NULL;
            }
            
            if (sums->pickler->pickleDict)
            {
                /* We were only borrowing the global dictionary reference - no need to decrement pointer. */
                sums->pickler->pickleDict = NULL;
            }
            
            free(sums->pickler);
        }

        /* Shut-down the Python engine we used to pickle and unpickle messages. */
        Py_Finalize();
    }
}
#endif

/* This must be the first thing called by DRMS to open with the SUMS.
 * Any one client may open up to MAXSUMOPEN times (TBD check) 
 * (although it's most likely that a client only needs one open session 
 * with SUMS, but it's built this way in case a use arises.).
 * Returns 0 on error else a pointer to a SUM structure.
 * **NEW 7Oct2005 the db arg has been depricated and has no effect. The db
 * you will get depends on how sum_svc was started, e.g. "sum_svc jsoc".
*/

/* THE PARAMETER db IS NOT USED. IT IS PASSED TO sum_svc, but sum_svc
 * DOES NOT USE IT. IF db IS NOT DEFINED, THEN THE ENVIRONMENT VARIABLE
 * "SUMDB" IS USED (AND THEN IGNORED BY sum_svc). IT USED TO BE THE CASE THAT THE INTERNAL
 * SUMDB MACRO WAS PASSED TO sum_svc (AND THEN IGNORED) IF db == NULL AND
 * THE "SUMDB" ENVIRONMENT VARIABLE WAS NOT DEFINED, BUT I CHANGED THAT TO PASS IN THE 
 * OFFICIAL LOCALIZED DEFINE SUMS_DB_HOST (WHICH IS THEN IGNORED BY sum_svc).
 *   ART 
 */
 
/* If numSUM is 1, then all DRMS-SUMS API functions are handled by a single RPC server: sum_svc. 
 * The handle for the RPC connection
 * to sum_svc is cl. The fields representing the API function calls in sumptr all point to this cl. 
 * If instead
 * numSUM > 1, then cl (sum_svc) handles a single API function - the delete-series function. 
 * cl is assigned to 
 * sumptr->cldelser. Then each API function is handled by one or more new RPC clients. 
 *
 * If the configuration parameter SUMS_USEMTSUMS is set to 1, then the SopenX servers are all disabled
 * (and they are not launched by sum_start). Instead, a single server daemon is launched to handle
 * the Sum_info() calls - sumsd.py. 
 */
SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))
{
  CLIENT *clopx;
  KEY *klist;
  SUM_t *sumptr;
  SUMID_t configback;
  SUMID_t sumid;
  enum clnt_stat status;
  struct timeval tval;
  unsigned int stime;
  char *server_name, *cptr, *username;
  char *call_err;
  int i, j, rr;

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
    MSUMSCLIENT_t msums = -1;
    Pickler_t *pickler = NULL;
#endif

  if(numopened >= MAXSUMOPEN) {
    (*history)("Exceeded max=%d SUM_open() for a client\n", MAXSUMOPEN);
    return(NULL);
  }

  if (server)
  {
      server_name = alloca(strlen(server)+1);
      strcpy(server_name, server);
  }
  else
  {
    if (!(server_name = getenv("SUMSERVER")))
    {
      server_name = alloca(sizeof(SUMSERVER)+1);
      strcpy(server_name, SUMSERVER);
    }
  }
  cptr = index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = '\0';
  gettimeofday(&tval, NULL);
  stime = (unsigned int)tval.tv_usec;
  srand(stime);				//seed rr_random()
  /* Create client handle used for calling the server */
  cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
  if(!cl) {              //no SUMPROG in portmap or timeout (default 25sec?)
    clnt_pcreateerror("Can't get client handle to sum_svc");
    (*history)("sum_svc timeout or not there on %s\n", server_name);
    (*history)("Going to retry in 1 sec\n");
    sleep(1);
    cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
    if(!cl) { 
      clnt_pcreateerror("Can't get client handle to sum_svc");
      (*history)("sum_svc timeout or not there on %s\n", server_name);
      return(NULL);
    }
  }
  clprev = cl;
  
  /* Connect to sumsd.py (if configuration specifies it should be used). */
#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
    msums = ConnectToMultiSums(&pickler, history);
    if (msums == -1)
    {
        (*history)("DRMS is not able to connect to SUMS (unable to connect to sumsd.py).");
        return NULL;
    }
#endif
  
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  klist = newkeylist();
  setkey_str(&klist, "db_name", db);
  setkey_str(&klist, "USER", username);
  status = clnt_call(cl, CONFIGDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&configback, TIMEOUT);
  configback = (SUMID_t)configback;

  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(cl, "Err clnt_call for CONFIGDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    freekeylist(&klist);
    return (0);
  }
  freekeylist(&klist);
  numSUM = (int)configback;
  if(numSUM == 0) {
    (*history)("numSUM = 0 on call to CONFIGDO in SUM_open(). Can't config\n");
    (*history)("(sum_svc may have been manually shutdown. No new open allowed)\n");
    return(0);
  }
  if(numSUM > SUM_MAXNUMSUM) {
    (*history)("**ERROR: #of sum_svc > SUM_MAXNUMSUM (%d)\n", SUM_MAXNUMSUM);
    (*history)("This is a fatal sum_svc configuration error\n");
    return(0);
  }

for(i=0; i < numSUM; i++) {
  switch(i) {
  case 0:	//this is numSUM=1. just sum_svc is running
    clopen = cl;		//already opened to sum_svc
    break;
  case 1:	//numSUM=2. sum_svc, Sopen, and Sopen1 are running
    clopen = clnt_create(server_name, SUMOPEN, SUMOPENV, "tcp");
    if(!clopen) {              //no SUMOPEN in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN to sum_svc");
      (*history)("sum_svc OPEN timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen = clnt_create(server_name, SUMOPEN, SUMOPENV, "tcp");
      if(!clopen) { 
        clnt_pcreateerror("Can't get client handle for OPEN to sum_svc");
        (*history)("sum_svc OPEN1 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    clopen1 = clnt_create(server_name, SUMOPEN1, SUMOPENV, "tcp");
    if(!clopen1) {              //no SUMOPEN1 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN1 to sum_svc");
      (*history)("sum_svc OPEN1 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen1 = clnt_create(server_name, SUMOPEN1, SUMOPENV, "tcp");
      if(!clopen1) { 
        clnt_pcreateerror("Can't get client handle for OPEN1 to sum_svc");
        (*history)("sum_svc OPEN1 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 2:
    clopen2 = clnt_create(server_name, SUMOPEN2, SUMOPENV, "tcp");
    if(!clopen2) {              //no SUMOPEN2 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN2 to sum_svc");
      (*history)("sum_svc OPEN2 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen2 = clnt_create(server_name, SUMOPEN2, SUMOPENV, "tcp");
      if(!clopen2) { 
        clnt_pcreateerror("Can't get client handle for OPEN2 to sum_svc");
        (*history)("sum_svc OPEN2 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 3:
    clopen3 = clnt_create(server_name, SUMOPEN3, SUMOPENV, "tcp");
    if(!clopen3) {              //no SUMOPEN3 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN3 to sum_svc");
      (*history)("sum_svc OPEN3 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen3 = clnt_create(server_name, SUMOPEN3, SUMOPENV, "tcp");
      if(!clopen3) { 
        clnt_pcreateerror("Can't get client handle for OPEN3 to sum_svc");
        (*history)("sum_svc OPEN3 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 4:
    clopen4 = clnt_create(server_name, SUMOPEN4, SUMOPENV, "tcp");
    if(!clopen4) {              //no SUMOPEN4 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN4 to sum_svc");
      (*history)("sum_svc OPEN4 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen4 = clnt_create(server_name, SUMOPEN4, SUMOPENV, "tcp");
      if(!clopen4) { 
        clnt_pcreateerror("Can't get client handle for OPEN4 to sum_svc");
        (*history)("sum_svc OPEN4 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 5:
    clopen5 = clnt_create(server_name, SUMOPEN5, SUMOPENV, "tcp");
    if(!clopen5) {              //no SUMOPEN5 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN5 to sum_svc");
      (*history)("sum_svc OPEN5 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen5 = clnt_create(server_name, SUMOPEN5, SUMOPENV, "tcp");
      if(!clopen5) { 
        clnt_pcreateerror("Can't get client handle for OPEN5 to sum_svc");
        (*history)("sum_svc OPEN5 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 6:
    clopen6 = clnt_create(server_name, SUMOPEN6, SUMOPENV, "tcp");
    if(!clopen6) {              //no SUMOPEN6 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN6 to sum_svc");
      (*history)("sum_svc OPEN6 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen6 = clnt_create(server_name, SUMOPEN6, SUMOPENV, "tcp");
      if(!clopen6) { 
        clnt_pcreateerror("Can't get client handle for OPEN6 to sum_svc");
        (*history)("sum_svc OPEN6 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  case 7:
    clopen7 = clnt_create(server_name, SUMOPEN7, SUMOPENV, "tcp");
    if(!clopen7) {              //no SUMOPEN7 in portmap or timeout (default 25sec?)
      clnt_pcreateerror("Can't get client handle for OPEN7 to sum_svc");
      (*history)("sum_svc OPEN7 timeout or not there on %s\n", server_name);
      (*history)("Going to retry in 1 sec\n");
      sleep(1);
      clopen7 = clnt_create(server_name, SUMOPEN7, SUMOPENV, "tcp");
      if(!clopen7) { 
        clnt_pcreateerror("Can't get client handle for OPEN7 to sum_svc");
        (*history)("sum_svc OPEN7 timeout or not there on %s\n", server_name);
        return(NULL);
      }
    }
    break;
  }
}

  if (!db)
  {
    if (!(db = getenv("SUMDB")))
    {
      db = alloca(sizeof(SUMS_DB_HOST)+1);
      strcpy(db, SUMS_DB_HOST);
    }
  }
  klist = newkeylist();
  setkey_str(&klist, "db_name", db);
  setkey_str(&klist, "USER", username);
  /* get a unique id from sum_svc for this open */
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0:
    clopx = clopen;
    break;
  case 1:
    clopx = clopen1;
    break;
  case 2:
    clopx = clopen2;
    break;
  case 3:
    clopx = clopen3;
    break;
  case 4:
    clopx = clopen4;
    break;
  case 5:
    clopx = clopen5;
    break;
  case 6:
    clopx = clopen6;
    break;
  case 7:
    clopx = clopen7;
    break;
  }
  if((sumid = sumrpcopen_1(klist, clopx, history)) == 0) {
    (*history)("Failed to get SUMID from sum_svc\n");
    clnt_destroy(cl);
    freekeylist(&klist);
    return(NULL);
  }
  numopened++;
  //sumptr = (SUM_t *)malloc(sizeof(SUM_t));
  sumptr = (SUM_t *)calloc(1, sizeof(SUM_t)); //NULL filled
  
#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
    sumptr->mSumsClient = msums;
    sumptr->pickler = pickler; /* steal */
#endif

  sumptr->sinfo = NULL;
  sumptr->cl = cl;
for(j=0; j < numSUM; j++) {
  switch(j) {
  case 0:
    sumptr->clopen = cl;
    sumptr->clalloc = cl;
    sumptr->clget = cl;
    sumptr->clput = cl;
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    sumptr->clinfo = cl;
#endif
    break;
  case 1:	//this is the case w/e.g. Salloc and Salloc1
    sumptr->clopen = clopen;
    sumptr->clopen1 = clopen1;
    clalloc = clnt_create(server_name, SUMALLOC, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC");
        (*history)("sum_svc error on handle to SUMALLOC on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc = clalloc;
    clalloc = clnt_create(server_name, SUMALLOC1, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC1. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC1, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC1");
        (*history)("sum_svc error on handle to SUMALLOC1 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc1 = clalloc;
    clget = clnt_create(server_name, SUMGET, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET");
      (*history)("sum_svc error on handle to SUMGET on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET");
        (*history)("sum_svc error on handle to SUMGET on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget = clget;
    clget = clnt_create(server_name, SUMGET1, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET1");
      (*history)("sum_svc error on handle to SUMGET1 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET1, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET1");
        (*history)("sum_svc error on handle to SUMGET1 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget1 = clget;
    clput = clnt_create(server_name, SUMPUT, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT");
      (*history)("sum_svc error on handle to SUMPUT on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT");
        (*history)("sum_svc error on handle to SUMPUT on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput = clput;
    clput = clnt_create(server_name, SUMPUT1, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT1");
      (*history)("sum_svc error on handle to SUMPUT1 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT1, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT1");
        (*history)("sum_svc error on handle to SUMPUT1 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput1 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO");
      (*history)("sum_svc error on handle to SUMINFO on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO");
        (*history)("sum_svc error on handle to SUMINFO on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo = clinfo;

    clinfo = clnt_create(server_name, SUMINFO1, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO1");
      (*history)("sum_svc error on handle to SUMINFO1 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO1, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO1");
        (*history)("sum_svc error on handle to SUMINFO1 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo1 = clinfo;
#endif

    break;
  case 2:
    sumptr->clopen2 = clopen2;
    clalloc = clnt_create(server_name, SUMALLOC2, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC2. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC2, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC2");
        (*history)("sum_svc error on handle to SUMALLOC2 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc2 = clalloc;
    clget = clnt_create(server_name, SUMGET2, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET2");
      (*history)("sum_svc error on handle to SUMGET2 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET2, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET2");
        (*history)("sum_svc error on handle to SUMGET2 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget2 = clget;
    clput = clnt_create(server_name, SUMPUT2, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT2");
      (*history)("sum_svc error on handle to SUMPUT2 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT2, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT2");
        (*history)("sum_svc error on handle to SUMPUT2 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput2 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO2, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO2");
      (*history)("sum_svc error on handle to SUMINFO2 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO2, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO2");
        (*history)("sum_svc error on handle to SUMINFO2 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo2 = clinfo;
#endif
    break;
  case 3:
    sumptr->clopen3 = clopen3;
    clalloc = clnt_create(server_name, SUMALLOC3, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC3. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC3, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC3");
        (*history)("sum_svc error on handle to SUMALLOC3 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc3 = clalloc;
    clget = clnt_create(server_name, SUMGET3, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET3");
      (*history)("sum_svc error on handle to SUMGET3 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET3, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET3");
        (*history)("sum_svc error on handle to SUMGET3 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget3 = clget;
    clput = clnt_create(server_name, SUMPUT3, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT3");
      (*history)("sum_svc error on handle to SUMPUT3 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT3, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT3");
        (*history)("sum_svc error on handle to SUMPUT3 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput3 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO3, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO3");
      (*history)("sum_svc error on handle to SUMINFO3 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO3, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO3");
        (*history)("sum_svc error on handle to SUMINFO3 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo3 = clinfo;
#endif
    break;
  case 4:
    sumptr->clopen4 = clopen4;
    clalloc = clnt_create(server_name, SUMALLOC4, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC4. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC4, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC4");
        (*history)("sum_svc error on handle to SUMALLOC4 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc4 = clalloc;
    clget = clnt_create(server_name, SUMGET4, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET4");
      (*history)("sum_svc error on handle to SUMGET4 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET4, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET4");
        (*history)("sum_svc error on handle to SUMGET4 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget4 = clget;
    clput = clnt_create(server_name, SUMPUT4, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT4");
      (*history)("sum_svc error on handle to SUMPUT4 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT4, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT4");
        (*history)("sum_svc error on handle to SUMPUT4 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput4 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO4, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO4");
      (*history)("sum_svc error on handle to SUMINFO4 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO4, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO4");
        (*history)("sum_svc error on handle to SUMINFO4 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo4 = clinfo;
#endif
    break;
  case 5:
    sumptr->clopen5 = clopen5;
    clalloc = clnt_create(server_name, SUMALLOC5, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC5. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC5, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC5");
        (*history)("sum_svc error on handle to SUMALLOC5 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc5 = clalloc;
    clget = clnt_create(server_name, SUMGET5, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET5");
      (*history)("sum_svc error on handle to SUMGET5 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET5, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET5");
        (*history)("sum_svc error on handle to SUMGET5 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget5 = clget;
    clput = clnt_create(server_name, SUMPUT5, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT5");
      (*history)("sum_svc error on handle to SUMPUT5 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT5, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT5");
        (*history)("sum_svc error on handle to SUMPUT5 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput5 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO5, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO5");
      (*history)("sum_svc error on handle to SUMINFO5 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO5, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO5");
        (*history)("sum_svc error on handle to SUMINFO5 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo5 = clinfo;
#endif
    break;
  case 6:
    sumptr->clopen6 = clopen6;
    clalloc = clnt_create(server_name, SUMALLOC6, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC6. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC6, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC6");
        (*history)("sum_svc error on handle to SUMALLOC6 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc6 = clalloc;
    clget = clnt_create(server_name, SUMGET6, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET6");
      (*history)("sum_svc error on handle to SUMGET6 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET6, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET6");
        (*history)("sum_svc error on handle to SUMGET6 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget6 = clget;
    clput = clnt_create(server_name, SUMPUT6, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT6");
      (*history)("sum_svc error on handle to SUMPUT6 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT6, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT6");
        (*history)("sum_svc error on handle to SUMPUT6 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput6 = clput;
    
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO6, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO6");
      (*history)("sum_svc error on handle to SUMINFO6 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO6, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO6");
        (*history)("sum_svc error on handle to SUMINFO6 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo6 = clinfo;
#endif
    break;
  case 7:
    sumptr->clopen7 = clopen7;
    clalloc = clnt_create(server_name, SUMALLOC7, SUMALLOCV, "tcp");
    if(!clalloc) {
      for(i=0; i < 4; i++) {		//keep on trying
        clnt_pcreateerror("Can't get client handle to sum_svc SUMALLOC7. Retry..");
        (*history)("Going to retry in 1 sec. i=%d\n", i);
        sleep(1);
        clalloc = clnt_create(server_name, SUMALLOC7, SUMALLOCV, "tcp");
        if(clalloc) { break; }
      }
      if(!clalloc) {
        clnt_pcreateerror("Can't get retry client handle to sum_svc SUMALLOC7");
        (*history)("sum_svc error on handle to SUMALLOC7 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clalloc7 = clalloc;
    clget = clnt_create(server_name, SUMGET7, SUMGETV, "tcp");
    if(!clget) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMGET7");
      (*history)("sum_svc error on handle to SUMGET7 on %s\n", server_name);
      sleep(1);
      clget = clnt_create(server_name, SUMGET7, SUMGETV, "tcp");
      if(!clget) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMGET7");
        (*history)("sum_svc error on handle to SUMGET7 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clget7 = clget;
    clput = clnt_create(server_name, SUMPUT7, SUMPUTV, "tcp");
    if(!clput) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT7");
      (*history)("sum_svc error on handle to SUMPUT7 on %s\n", server_name);
      sleep(1);
      clput = clnt_create(server_name, SUMPUT7, SUMPUTV, "tcp");
      if(!clput) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMPUT7");
        (*history)("sum_svc error on handle to SUMPUT7 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clput7 = clput;
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    clinfo = clnt_create(server_name, SUMINFO7, SUMINFOV, "tcp");
    if(!clinfo) {
      clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO7");
      (*history)("sum_svc error on handle to SUMINFO7 on %s\n", server_name);
      sleep(1);
      clinfo = clnt_create(server_name, SUMINFO7, SUMINFOV, "tcp");
      if(!clinfo) { 
        clnt_pcreateerror("Can't get client handle to sum_svc SUMINFO7");
        (*history)("sum_svc error on handle to SUMINFO7 on %s\n", server_name);
        return(NULL);
      }
    }
    sumptr->clinfo7 = clinfo;
#endif
    break;
  }
}
if(numSUM == 1) {
  cldelser = cl;
}
else {
  cldelser = clnt_create(server_name, SUMDELSER, SUMDELSERV, "tcp");
  if(!cldelser) {
    clnt_pcreateerror("Can't get client handle to sum_svc SUMDELSER");
    (*history)("sum_svc error on handle to SUMDELSER on %s\n", server_name);
    sleep(1);
    cldelser = clnt_create(server_name, SUMDELSER, SUMDELSERV, "tcp");
    if(!cldelser) { 
      clnt_pcreateerror("Can't get client handle to sum_svc SUMDELSER");
      (*history)("sum_svc error on handle to SUMDELSER on %s\n", server_name);
      return(NULL);
    }
  }
}
  sumptr->cldelser = cldelser;
  sumptr->uid = sumid;
  sumptr->username = username;
  sumptr->tdays = 0;
  sumptr->debugflg = 0;		/* default debug off */
  sumptr->storeset = JSOC;	/* default storage set */
  sumptr->numSUM = numSUM;	/* # of sum servers available */
  sumptr->dsname = "<none>";
  sumptr->history_comment = NULL;
  sumptr->dsix_ptr = (uint64_t *)malloc(sizeof(uint64_t) * SUMARRAYSZ);
  sumptr->wd = (char **)calloc(SUMARRAYSZ, sizeof(char *));
  setsumopened(&sumopened_hdr, sumid, sumptr, username); //put in open list
  freekeylist(&klist);
  return(sumptr);
}

/* Open with sum_svc. Return 0 on error.
*/
SUMID_t sumrpcopen_1(KEY *argp, CLIENT *clnt, int (*history)(const char *fmt, ...))
{
  char *call_err;
  enum clnt_stat status;
  SUMID_t suidback;
  SVCXPRT *xtp;
                                                                        
  clprev = clnt;
  clclose = clnt;	//use the same process for the CLOSEDO
  status = clnt_call(clnt, OPENDO, (xdrproc_t)xdr_Rkey, (char *)argp, 
			(xdrproc_t)xdr_uint32_t, (char *)&suidback, TIMEOUT);
  suidback = (SUMID_t)suidback;

  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(clnt, "Err clnt_call for OPENDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    return (0);
  }
  /* (*history)("suidback = %d\n", suidback); /* !!TEMP */

  /* register for future calls to receive the sum_svc completion msg */
  /* Use our suidback as the version number. */
  if(suidback) {
    (void)pmap_unset(RESPPROG, suidback); /* first unreg any left over */
    xtp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
    if (xtp == NULL) {
      (*history)("cannot create tcp service in sumrpcopen_1() for responses\n");
      return(0);
    }
    if (!svc_register(xtp, RESPPROG, suidback, respd, IPPROTO_TCP)) {
      (*history)("unable to register RESPPROG in sumrpcopen_1()\n");
      return(0);
    }
    transp[numopened] = xtp;
    transpid[numopened] = suidback;
  }
  return (suidback);
}

/* Allocate the storage given in sum->bytes.
 * Return non-0 on error, else return wd of allocated storage in *sum->wd.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_alloc(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  int rr;
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  if(sum->reqcnt != 1) {
    (*history)("Invalid reqcnt = %d for SUM_alloc(). Can only alloc 1.\n",
		sum->reqcnt);
    return(1);
  }
  klist = newkeylist();
  setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "storeset", sum->storeset);
  setkey_int(&klist, "group", sum->group);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", ALLOCDO);
  setkey_str(&klist, "USER", sum->username);
  if(sum->debugflg) {
    (*history)("In SUM_alloc() the keylist is:\n");
    keyiterate(printkey, klist);
  }
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0:
    clalloc = sum->clalloc;
    break;
  case 1:
    clalloc = sum->clalloc1;
    break;
  case 2:
    clalloc = sum->clalloc2;
    break;
  case 3:
    clalloc = sum->clalloc3;
    break;
  case 4:
    clalloc = sum->clalloc4;
    break;
  case 5:
    clalloc = sum->clalloc5;
    break;
  case 6:
    clalloc = sum->clalloc6;
    break;
  case 7:
    clalloc = sum->clalloc7;
    break;
  }
  clprev = clalloc;
  status = clnt_call(clalloc, ALLOCDO, (xdrproc_t)xdr_Rkey, (char *)klist,
                        (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(clalloc, "Err clnt_call for ALLOCDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  if(retstat) {			/* error on ALLOCDO call */
    (*history)("Error in SUM_alloc()\n");
    freekeylist(&klist);
    return(retstat);
  }
  else {
    msgstat = getanymsg(1);	/* get answer to ALLOCDO call */
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    return(sum->status);
  }
}

/* Allocate the storage given in sum->bytes for the given sunum.
 * Return non-0 on error, else return wd of allocated storage in *sum->wd.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_alloc2(SUM_t *sum, uint64_t sunum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  //!!TEMP until learn how to validate the given sunum
  //(*history)("!TEMP reject of SUM_alloc2() call until we can validate sunum\n");
  //return(1);

  if(sum->reqcnt != 1) {
    (*history)("Invalid reqcnt = %d for SUM_alloc2(). Can only alloc 1.\n",
		sum->reqcnt);
    return(1);
  }
  klist = newkeylist();
  setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "storeset", sum->storeset);
  setkey_int(&klist, "group", sum->group);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_uint64(&klist, "SUNUM", sunum); //unique to the SUM_alloc2() call
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", ALLOCDO);
  setkey_str(&klist, "USER", sum->username);
  if(sum->debugflg) {
    (*history)("In SUM_alloc2() the keylist is:\n");
    keyiterate(printkey, klist);
  }
  clprev = sum->clalloc;
  //This is seldom called, so only use the first Salloc process
  status = clnt_call(sum->clalloc, ALLOCDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->clalloc, "Err clnt_call for ALLOCDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  if(retstat) {			/* error on ALLOCDO call */
    (*history)("Error in SUM_alloc2()\n");
    return(retstat);
  }
  else {
    msgstat = getanymsg(1);	/* get answer to ALLOCDO call */
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    return(sum->status);
  }
}

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS

#define MSGLEN_NUMBYTES 8
#define MAX_MSG_BUFSIZE 4096
#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) ((a) < (b) ? (a) : (b))

/* There could be NULL chars in the message to send, so the caller must provide the message length in msgLen. */
static int sendRequest(SUM_t *sums, const char *msg, uint32_t msgLen, int (*history)(const char *fmt, ...))
{
    size_t bytesSentTotal;
    size_t bytesSent;
    char numBytesMesssage[MSGLEN_NUMBYTES + 1];
    const char *pBuf = NULL;
    int sockfd = -1;
    int err = 0;
    
    if (msg && *msg)
    {
        sockfd = (int)sums->mSumsClient;
        
        /* First send the length of the message, msgLen. */
        bytesSentTotal = 0;
        snprintf(numBytesMesssage, sizeof(numBytesMesssage), "%08x", msgLen);
        
        while (bytesSentTotal < MSGLEN_NUMBYTES)
        {
            pBuf = numBytesMesssage + bytesSentTotal;
            bytesSent = send(sockfd, pBuf, strlen(pBuf), 0);
            
            if (!bytesSent)
            {
                (*history)("Socket to sumsd.py broken.");
                err = 1;
                break;
            }
            
            bytesSentTotal += bytesSent;
        }
        
        if (!err)
        {
            /* Then send the message. */
            bytesSentTotal = 0;
            
            while (bytesSentTotal < msgLen)
            {
                pBuf = msg + bytesSentTotal;
                bytesSent = send(sockfd, pBuf, msgLen - bytesSentTotal, 0);

                if (!bytesSent)
                {
                    (*history)("Socket to sumsd.py broken.");
                    err = 1;
                    break;
                }
            
                bytesSentTotal += bytesSent;
            }
        }       
    }
    
    return err;
}

static int receiveResponse(SUM_t *sums, char **out, size_t *outLen, int (*history)(const char *fmt, ...))
{
    size_t bytesReceivedTotal;
    ssize_t sizeTextRecvd;
    char recvBuffer[MAX_MSG_BUFSIZE];
    char numBytesMessage[MSGLEN_NUMBYTES + 1];
    unsigned int sizeMessage;
    char *allTextReceived = NULL;
    int sockfd = -1;
    int err = 0;
    
    if (!out)
    {
        err = 1;
        (*history)("'out' must not be NULL in receiveResponse().\n");
    }
    
    if (!err)
    {
        if (!sums || sums->mSumsClient == -1)
        {
            err = 1;
            (*history)("Not connected to sumsd.py.\n");
        }
    }

    if (!err)
    {
        sockfd = (int)sums->mSumsClient;
            
        /* First, receive length of message. */
        bytesReceivedTotal = 0;
        
        /* The double guard ensures that MSGLEN_NUMBYTES <= MAX_MSG_BUFSIZE. The header is accumulated directly
         * in recvBuffer. */
        while (bytesReceivedTotal < MSGLEN_NUMBYTES && bytesReceivedTotal < MAX_MSG_BUFSIZE)
        {
            sizeTextRecvd = recv(sockfd, recvBuffer + bytesReceivedTotal, MIN(MSGLEN_NUMBYTES - bytesReceivedTotal, MAX_MSG_BUFSIZE - bytesReceivedTotal), 0);
        
            if (sizeTextRecvd <= 0)
            {
                (*history)("Socket to sumsd.py broken.\n");
                err = 1;
                break;
            }
        
            bytesReceivedTotal += sizeTextRecvd;
        }
    }

    if (!err)
    {
        /* Convert hex string to number. */
        snprintf(numBytesMessage, sizeof(numBytesMessage), "%s", recvBuffer);
        sscanf(numBytesMessage, "%08x", &sizeMessage);
        
        allTextReceived = calloc(1, sizeMessage + 1);
        
        if (!allTextReceived)
        {
            (*history)("Out of memory in receiveResponse().\n");
            err = 1;
        }
        else
        {
            /* Receive the message. */
            bytesReceivedTotal = 0;
        
            while (bytesReceivedTotal < sizeMessage)
            {
                sizeTextRecvd = recv(sockfd, recvBuffer, MIN(sizeMessage - bytesReceivedTotal, MAX_MSG_BUFSIZE), 0);
        
                if (sizeTextRecvd <= 0)
                {
                    (*history)("Socket to sumsd.py broken.\n");
                    err = 1;
                    break;
                }

                /* Can't use string functions, like strncat(), since these functions 
                 * assume the input is a null-terminated string. The input will be 
                 * truncated at the first null character.*/
                memcpy(allTextReceived + bytesReceivedTotal, recvBuffer, sizeTextRecvd);
                bytesReceivedTotal += sizeTextRecvd;
            }
        }
    }
    
    if (err)
    {
        if (allTextReceived)
        {
            free(allTextReceived);
            allTextReceived = NULL;
        }
    }
    else
    {
        *out = allTextReceived;
        *outLen = bytesReceivedTotal;
    }
    
    return err;
}

static int pickleRequest(SUM_t *sums, uint64_t *sunums, size_t nSus, char **pickled, size_t *pickleLen, int (*history)(const char *fmt, ...))
{
    PyObject *list = NULL;
    PyObject *protocol = NULL;
    PyObject *argTuple = NULL;
    PyObject *dumpsFxn = NULL;
    PyObject *result = NULL;
    const char *pickledStr = NULL;
    Py_ssize_t byteLen;
    int isu;
    uint64_t sunum;
    int err;
    
    err = 0;
    
    if (!sums || !sunums || nSus == 0 || !pickled || !pickleLen)
    {
        (*history)("Invalid argument(s) to 'pickleRequest'.\n");
        err = 1;
    }
    
    if (!err)
    {
        list = PyList_New(nSus);
        
        if (!list)
        {
            (*history)("Unable to create new Py list.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        for (isu = 0; isu < nSus; isu++)
        {
            sunum = sunums[isu];
        
            /* PyList_SET_ITEM steals the reference to the Py object provided in its third 
             * argument. To free the memory allocated in the Py environment for the 
             * list reference and all the references in the list, you simply have to
             * decrement the reference on the list (and not the items in the list). */
            PyList_SET_ITEM(list, isu, PyLong_FromUnsignedLongLong(sunum));
        }
        
        protocol = PyDict_GetItemString(sums->pickler->pickleDict, "HIGHEST_PROTOCOL");
    }
    
    if (!protocol)
    {
        (*history)("Unable to load pickle protocol.\n");
        err = 1;
    }
    
    if (!err)
    {
        dumpsFxn = PyDict_GetItemString(sums->pickler->pickleDict, "dumps");

        if (!dumpsFxn)
        {
            (*history)("Unable to load 'dumps' function.\n");
            err = 1;
        }
    }

    if (!err)
    {  
        if (!PyCallable_Check(dumpsFxn)) 
        {
            (*history)("'dumps' function is not callable.\n");
            err = 1;
        }
    }

    if (!err)
    {            
        result = PyObject_CallFunctionObjArgs(dumpsFxn, list, protocol, NULL);

        if (!result)
        {
            (*history)("Unable to call 'dumps' function.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        /* Since this function returns a bytes object in the Py environment, I'm assuming 
         * it returns a PyObject that is a bytes. */
         if (!PyBytes_Check(result))
         {
            (*history)("Unexpected data type for object returned from 'dumps' function.\n");
            err = 1;
         }
    }
    
    if (!err)
    {
        if (PyBytes_AsStringAndSize(result, (char **)&pickledStr, &byteLen) == 0)
        {
            /* PyLong_FromSsize_t() should not fail because PyBytes_AsStringAndSize() should fail
             * if result would result in a byteLen that overflows. */
            *pickleLen = (size_t)PyLong_AsLong(PyLong_FromSsize_t(byteLen));
            *pickled = calloc(1, *pickleLen);
            memcpy(*pickled, pickledStr, *pickleLen);
        }
        else
        {
            (*history)("Unable to extract string from 'dumps' response.\n");
            err = 1;
        }
    }
    
    /* Free memory being held by the Py engine. */
    if (list)
    {
        Py_DECREF(list);
    }
    
    if (result)
    {
        Py_DECREF(result);
    }

    return err;
}

static PyObject *getAttr(PyObject *item, const char *name)
{
    PyObject *attr = NULL;
    
    if (item && name)
    {
        attr = PyObject_GetAttrString(item, name);
    }
    
    return attr;
}

/* Thanks to Rick, we have to deal with the fact that strings from the DB are Latin1. */
static int sprintUnicodeAsLatin1(PyObject *unicode, const char *attrName, char *str, size_t len, int (*history)(const char *fmt, ...))
{
    PyObject *result = NULL;
    int err;
    
    err = 0;
    
    if (!PyUnicode_Check(unicode))
    {
        (*history)("Data type for %s is unexpected.\n", attrName);
        err = 1;
    }
    else
    {
        result = PyUnicode_AsLatin1String(unicode);
        
        if (!result)
        {
            (*history)("Unable to convert unicode to Latin-1 string.\n");
            err = 1;
        }
    }

    if (!err)
    {    
        snprintf(str, len, "%s", PyBytes_AsString(result));
    }

    return err;
}

static int unpickleResponse(SUM_t *sums, const char *msg, size_t msgLen, int (*history)(const char *fmt, ...))
{
    int err;
    PyObject *loadsFxn = NULL;
    PyObject *bytes = NULL;
    PyObject *byteLen = NULL;
    PyObject *result = NULL;
    
    err = 0;
    
    if (!err)
    {
        loadsFxn = PyDict_GetItemString(sums->pickler->pickleDict, "loads");

        if (!loadsFxn)
        {
            (*history)("Unable to load 'loads' function.\n");
            err = 1;
        }  
    }

    if (!err)
    {
        if (!PyCallable_Check(loadsFxn)) 
        {
            (*history)("'loads' function is not callable.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        /* Make a PyObject out of the message. */
        byteLen = PyLong_FromSize_t(msgLen);
        if (!byteLen)
        {
            (*history)("Unable to create byte-length object.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        /* PyLong_AsSsize_t() will raise an exception if byteLen is out of range. */
        bytes = PyBytes_FromStringAndSize(msg, PyLong_AsSsize_t(byteLen));
    }
    
    if (!err)
    {
        /* The response from sumsd.py contains instances of the Info class defined
         * in sumsd.py. If we were to attempt to loads the bytes object, it would
         * fail since class Info is not defined in our Python environment yet. To
         * prevent this from happening, we must defined class Info. */
        err = (PyRun_SimpleString("class Info(object):\n    pass") != 0);
        if (err)
        {
            (*history)("Unable to define Info class in __main__ module.\n");
        }
    }

    if (!err)
    {
        result = PyObject_CallFunctionObjArgs(loadsFxn, bytes, NULL);

        if (!result)
        {
            (*history)("Unable to call 'loads' function.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        /* The loads call should return a Python list. */
        if (!PyList_Check(result))
        {
            (*history)("Unexpected data type for object returned from 'loads' function.\n");
            err = 1;
        }
    }
    
    if (!err)
    {
        /* Extract elements from the list. Stick them into sums->sinfo as a linked-list. */
        Py_ssize_t listLen;
        int isu;
        Py_ssize_t index;
        PyObject *item = NULL;
        PyObject *attr = NULL;
        
        listLen = PyList_Size(result);
        sums->sinfo = (SUM_info_t *)calloc(PyLong_AsLong(PyLong_FromSsize_t(listLen)), sizeof(SUM_info_t));
        
        for (isu = 0; isu < listLen; isu++)
        {
            index = PyLong_AsSsize_t(PyLong_FromSize_t(isu));
            item = PyList_GetItem(result, index); /* This is a SUM_info_t essentially. */
            
            attr = getAttr(item, "sunum");
            if (!attr)
            {
                (*history)("Unable to unpickle sunum.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for sunum is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].sunum = PyLong_AsLong(attr);
            
            attr = getAttr(item, "onlineLoc");
            if (!attr)
            {
                (*history)("Unable to unpickle onlineLoc.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "onlineLoc", sums->sinfo[isu].online_loc, sizeof(sums->sinfo[isu].online_loc), history) != 0);
            if (err)
            {
                break;
            }
                        
            attr = getAttr(item, "onlineStatus");
            if (!attr)
            {
                (*history)("Unable to unpickle onlineStatus.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "onlineStatus", sums->sinfo[isu].online_status, sizeof(sums->sinfo[isu].online_status), history) != 0);
            if (err)
            {
                break;
            }
               
            attr = getAttr(item, "archiveStatus");
            if (!attr)
            {
                (*history)("Unable to unpickle archiveStatus.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "archiveStatus", sums->sinfo[isu].archive_status, sizeof(sums->sinfo[isu].archive_status), history) != 0);
            if (err)
            {
                break;
            }            
            
            attr = getAttr(item, "offsiteAck");
            if (!attr)
            {
                (*history)("Unable to unpickle offsiteAck.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "offsiteAck", sums->sinfo[isu].offsite_ack, sizeof(sums->sinfo[isu].offsite_ack), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "historyComment");
            if (!attr)
            {
                (*history)("Unable to unpickle historyComment.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "historyComment", sums->sinfo[isu].history_comment, sizeof(sums->sinfo[isu].history_comment), history) != 0);
            if (err)
            {
                break;
            }

            attr = getAttr(item, "owningSeries");
            if (!attr)
            {
                (*history)("Unable to unpickle owningSeries.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "owningSeries", sums->sinfo[isu].owning_series, sizeof(sums->sinfo[isu].owning_series), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "storageGroup");
            if (!attr)
            {
                (*history)("Unable to unpickle storageGroup.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for storageGroup is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].storage_group = PyLong_AsLong(attr);
            
            /* bytes is a double in SUM_info_t, but it is an integer in sum_main (and is reported as an integer by sumsd.py). */
            attr = getAttr(item, "bytes");
            if (!attr)
            {
                (*history)("Unable to unpickle bytes.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for bytes is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].bytes = PyLong_AsDouble(attr);
            
            /* Skip createSumid. */
            
            attr = getAttr(item, "creatDate");
            if (!attr)
            {
                (*history)("Unable to unpickle creatDate.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "creatDate", sums->sinfo[isu].creat_date, sizeof(sums->sinfo[isu].creat_date), history) != 0);
            if (err)
            {
                break;
            }            
            
            attr = getAttr(item, "username");
            if (!attr)
            {
                (*history)("Unable to unpickle username.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "username", sums->sinfo[isu].username, sizeof(sums->sinfo[isu].username), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "archTape");
            if (!attr)
            {
                (*history)("Unable to unpickle archTape.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "archTape", sums->sinfo[isu].arch_tape, sizeof(sums->sinfo[isu].arch_tape), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "archTapeFn");
            if (!attr)
            {
                (*history)("Unable to unpickle archTapeFn.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for archTapeFn is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].arch_tape_fn = PyLong_AsLong(attr);
            
            attr = getAttr(item, "archTapeDate");
            if (!attr)
            {
                (*history)("Unable to unpickle archTapeDate.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "archTapeDate", sums->sinfo[isu].arch_tape_date, sizeof(sums->sinfo[isu].arch_tape_date), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "safeTape");
            if (!attr)
            {
                (*history)("Unable to unpickle safeTape.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "safeTape", sums->sinfo[isu].safe_tape, sizeof(sums->sinfo[isu].safe_tape), history) != 0);
            if (err)
            {
                break;
            }

            attr = getAttr(item, "safeTapeFn");
            if (!attr)
            {
                (*history)("Unable to unpickle safeTapeFn.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for safeTapeFn is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].safe_tape_fn = PyLong_AsLong(attr);
            
            attr = getAttr(item, "safeTapeDate");
            if (!attr)
            {
                (*history)("Unable to unpickle safeTapeDate.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "safeTapeDate", sums->sinfo[isu].safe_tape_date, sizeof(sums->sinfo[isu].safe_tape_date), history) != 0);
            if (err)
            {
                break;
            }
            
            attr = getAttr(item, "effectiveDate");
            if (!attr)
            {
                (*history)("Unable to unpickle effectiveDate.\n");
                err = 1;
                break;
            }
            err = (sprintUnicodeAsLatin1(attr, "effectiveDate", sums->sinfo[isu].effective_date, sizeof(sums->sinfo[isu].effective_date), history) != 0);
            if (err)
            {
                break;
            }            
            
            attr = getAttr(item, "paStatus");
            if (!attr)
            {
                (*history)("Unable to unpickle paStatus.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for paStatus is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].pa_status = PyLong_AsLong(attr);
            
            attr = getAttr(item, "paSubstatus");
            if (!attr)
            {
                (*history)("Unable to unpickle paSubtatus.\n");
                err = 1;
                break;
            }
            if (!PyLong_Check(attr))
            {
                (*history)("Data type for paSubstatus is unexpected.\n");
                err = 1;
                break;
            }
            sums->sinfo[isu].pa_substatus = PyLong_AsLong(attr);
        }
    }
    
    return err;
}


/* SUM_t::sinfo is a linear array of malloc'ed SUM_info_ts.
 */
void SUM_infoArray_free(SUM_t *sums)
{
    if(sums->sinfo) 
    {
        free(sums->sinfo);    
        sums->sinfo = NULL;            //must do so no double free in SUM_close()
    }
}

int SUM_infoArray(SUM_t *sums, uint64_t *sunums, int reqcnt, int (*history)(const char *fmt, ...))
{
    char *pickled = NULL;
    size_t pickleLen;
    char *response = NULL;
    size_t rspLen;
    int err;
    
    err = 0;
    
    /* pickle request */
    err = pickleRequest(sums, sunums, reqcnt, &pickled, &pickleLen, history);
    
    if (!err)
    {
        /* send request */
        err = sendRequest(sums, pickled, pickleLen, history);
    }

    if (!err)
    {    
        /* receive response */
        err = receiveResponse(sums, &response, &rspLen, history);
    }
    
    /* unpickle response */
    if (!err)
    {
        err = unpickleResponse(sums, response, rspLen, history);
    }
    
    if (!err)
    {
        free(response);
        free(pickled);
        pickled = NULL;
    }
    
    return err;
}


#else
/* Return information from sum_main for the given sunum (ds_index).
 * Return non-0 on error, else sum->sinfo has the SUM_info_t pointer.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_info(SUM_t *sum, uint64_t sunum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  klist = newkeylist();
  setkey_uint64(&klist, "SUNUM", sunum); 
  setkey_str(&klist, "username", sum->username);
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", INFODO);
  clprev = sum->clinfo;
  //This is seldom called, so only use the first Sinfo process. 
  //Superceeded by SUM_infoEx()
  status = clnt_call(sum->clinfo, INFODO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->clinfo, "Err clnt_call for INFODO");
      if(history) 
        (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  if(retstat) {			/* error on INFODO call */
    if(retstat != SUM_SUNUM_NOT_LOCAL)
      if(history) 
        (*history)("Error in SUM_info()\n"); //be quite for show_info sake
    return(retstat);
  }
  else {
    if(sum->sinfo == NULL) 
      sum->sinfo = (SUM_info_t *)malloc(sizeof(SUM_info_t));
    msgstat = getanymsg(1);	//put answer to INFODO call in sum->sainfo/
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    //printf("\nIn SUM_info() the keylist is:\n"); //!!TEMP
    //keyiterate(printkey, infoparams);
    return(0);
  }
}

/* Free the automatic malloc of the sinfo linked list done from a 
 * SUM_infoEx() call.
*/
void SUM_infoEx_free(SUM_t *sum)
{
  SUM_info_t *sinfowalk, *next;

  sinfowalk = sum->sinfo;
  sum->sinfo = NULL;		//must do so no double free in SUM_close()
  while(sinfowalk) {
    next = sinfowalk->next;
    free(sinfowalk);
    sinfowalk = next;
  }
}

/* Return information from sum_main for the given sunums in
 * the array pointed to by sum->dsix_ptr. There can be up to 
 * MAXSUMREQCNT entries given by sum->reqcnt.
 * Return non-0 on error, else sum->sinfo has the SUM_info_t pointer
 * to linked list of SUM_info_t sturctures (sum->reqcnt).
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_infoEx(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  int rr;
  KEY *klist;
  SUM_info_t *sinfowalk;
  char *call_err;
  char dsix_name[64];
  uint32_t retstat;
  uint64_t *dxlong;
  int i,msgstat;
  enum clnt_stat status;

  if(sum->reqcnt > MAXSUMREQCNT) {
    (*history)("Requent count of %d > max of %d\n", sum->reqcnt, MAXSUMREQCNT);
    return(1);
  }
  klist = newkeylist();
  setkey_str(&klist, "username", sum->username);
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", INFODOX);
  dxlong = sum->dsix_ptr;
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dxlong++);
  }
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0: 
    clinfo = sum->clinfo;
    break;
  case 1:
    clinfo = sum->clinfo1;
    break;
  case 2:
    clinfo = sum->clinfo2;
    break;
  case 3:
    clinfo = sum->clinfo3;
    break;
  case 4:
    clinfo = sum->clinfo4;
    break;
  case 5:
    clinfo = sum->clinfo5;
    break;
  case 6:
    clinfo = sum->clinfo6;
    break;
  case 7:
    clinfo = sum->clinfo7;
    break;
  }
  clprev = clinfo;
  status = clnt_call(clinfo, INFODOX, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  // NOTE: These rtes seem to return after the reply has been received despite
  // the timeout value. If it did take longer than the timeout then the timeout
  // error status is set but it should be ignored.
  // 
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(clinfo, "Err clnt_call for INFODOX");
      if(history) 
        (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  if(retstat) {			// error on INFODOX call
    if(retstat != SUM_SUNUM_NOT_LOCAL)
      if(history) 
        (*history)("Error in SUM_infoEx()\n"); //be quiet for show_info sake
    return(retstat);
  }
  else {
    if(sum->sinfo == NULL) {	//must malloc all sinfo structures
      sum->sinfo = (SUM_info_t *)malloc(sizeof(SUM_info_t));
      sinfowalk = sum->sinfo;
      sinfowalk->next = NULL;
      for(i = 1; i < sum->reqcnt; i++) {
        sinfowalk->next = (SUM_info_t *)malloc(sizeof(SUM_info_t));
        sinfowalk = sinfowalk->next;
        sinfowalk->next = NULL;
      }
    }
    else {
      (*history)("\nAssumes user has malloc'd linked list of (SUM_info_t *)\n");
      (*history)("Else set sum->sinfo = NULL before SUM_infoEx() call\n");
    }
    msgstat = getanymsg(1);	// get answer to INFODOX call
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    //printf("\nIn SUM_info() the keylist is:\n"); //!!TEMP
    //keyiterate(printkey, infoparams);
    return(0);
  }
}

/* Free the automatic malloc of the sinfo linked list done from a 
 * SUM_infoArray() call.
*/
void SUM_infoArray_free(SUM_t *sum)
{
  if(sum->sinfo) free(sum->sinfo);
  sum->sinfo = NULL;            //must do so no double free in SUM_close()
}

/* Return information from sum_main for the given sunums in
 * the input uint64_t dxarray. There can be up to 
 * MAXSUNUMARRAY entries given by reqcnt. The uid and username are picked up
 * from the *sum. 
 * The sum->sinfo will malloc (and free at close) 
 * the memory needed for the reqcnt answers returned by sum_svc.
 * The user can optionally free the memory by calling SUM_infoArray_free().
 * Return non-0 on error, else sum->sinfo has the SUM_info_t pointer
 * to linked list of SUM_info_t sturctures for the reqcnt.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_infoArray(SUM_t *sum, uint64_t *dxarray, int reqcnt, int (*history)(const char *fmt, ...))
{
  int rr;
  Sunumarray suarray;
  SUM_info_t *sinfowalk;
  char *call_err, *jsoc_machine;
  uint32_t retstat;
  int i,msgstat;
  enum clnt_stat status;

  if(reqcnt > MAXSUNUMARRAY) {
    (*history)("Requent count of %d > max of %d\n", reqcnt, MAXSUNUMARRAY);
    return(1);
  }
  suarray.reqcnt = reqcnt;
  suarray.mode = sum->mode;
  suarray.tdays = sum->tdays;
  suarray.reqcode = INFODOARRAY;
  suarray.uid = sum->uid;
  suarray.username = sum->username;
  if(!(jsoc_machine = (char *)getenv("JSOC_MACHINE"))) {
    //(*history)("No JSOC_MACHINE in SUM_infoArray(). Not a JSOC environment\n");
    //return(1);			//error. not a JSOC environment
    jsoc_machine = "linux_x86_64";	//assume this
  }
  suarray.machinetype = jsoc_machine;
  suarray.sunums = dxarray;

  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0: 
    clinfo = sum->clinfo;
    break;
  case 1:
    clinfo = sum->clinfo1;
    break;
  case 2:
    clinfo = sum->clinfo2;
    break;
  case 3:
    clinfo = sum->clinfo3;
    break;
  case 4:
    clinfo = sum->clinfo4;
    break;
  case 5:
    clinfo = sum->clinfo5;
    break;
  case 6:
    clinfo = sum->clinfo6;
    break;
  case 7:
    clinfo = sum->clinfo7;
    break;
  }
  clprev = clinfo;
  status = clnt_call(clinfo, INFODOARRAY, (xdrproc_t)xdr_Sunumarray, (char *)&suarray, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  // NOTE: These rtes seem to return after the reply has been received despite
  // the timeout value. If it did take longer than the timeout then the timeout
  // error status is set but it should be ignored.
  // 
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(clinfo, "Err clnt_call for INFODOX");
      if(history) 
        (*history)("%s %d %s\n", datestring(), status, call_err);
      //freekeylist(&klist);
      return (4);
    }
  }
  if(retstat) {			// error on INFODOARRAY call
    if(retstat != SUM_SUNUM_NOT_LOCAL)
      if(history) 
        (*history)("Error in SUM_infoArray()\n"); //be quiet for show_info sake
    return(retstat);
  }
  else {
    //must contiguous malloc all sinfo structures 0 filled
    //The answer sent back from sum_svc will be read into this mem
    //!!Memory now allocated in respdoarray_1() when the sum_svc answers
    //sum->sinfo = (SUM_info_t *)calloc(reqcnt, sizeof(SUM_info_t));
    //The links will be made when the binary file is read
    //sinfowalk = sum->sinfo;
    //sinfowalk->next = NULL;
    //for(i = 1; i < reqcnt; i++) {
    //  sinfowalk->next = sinfowalk + sizeof(SUM_info_t);
    //  sinfowalk = sinfowalk->next;
    //  sinfowalk->next = NULL;
    //}
    msgstat = getanymsg(1);	// get answer to SUM_infoArray call
    //freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    //printf("\nIn SUM_info() the keylist is:\n"); //!!TEMP
    //keyiterate(printkey, infoparams);
    return(0);
  }
}
#endif

/* Close this session with the SUMS. Return non 0 on error.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_close(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  static char res;
  enum clnt_stat status;
  int i, stat;
  int errflg = 0;

  if(sum->debugflg) {
    (*history)("SUM_close() call: uid = %lu\n", sum->uid);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", CLOSEDO);
  setkey_str(&klist, "USER", sum->username);
  //printf("!!!TEMP In SUM_close() sum->mode=%x %d\n", sum->mode, sum->mode);
  if(sum->mode & TAPERDON) {
    if(!taperdon_cleared) {		//tape rd is still active. Notify sum_svc
      setkey_int(&klist, "TAPERDACTIVE", 1);
    }
  }
  bzero((char *)&res, sizeof(res));
  //Use the same process that we opened with
  clprev = clclose;
  status = clnt_call(clclose, CLOSEDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_void, &res, TIMEOUT);

/* NOTE: These rtes seem to return after the reply has been received despite
 * the timeout value. If it did take longer than the timeout then the timeout
 * error status is set but it should be ignored.
*/
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->clopen, "Err clnt_call for CLOSEDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      errflg = 1;
    }
  }

  stat = getmsgimmed();		//clean up pending response

  (void)pmap_unset(RESPPROG, sum->uid); /* unreg response server */
  remsumopened(&sumopened_hdr, sum->uid); /* rem from linked list */
  if(numSUM == 1) {
    clnt_destroy(sum->cl);	//don't close the same connec more than once
  }
  else {
  clnt_destroy(sum->cl);	/* destroy handle to sum_svc */
  if(sum->clopen) clnt_destroy(sum->clopen);
  if(sum->clopen1) clnt_destroy(sum->clopen1);
  if(sum->clopen2) clnt_destroy(sum->clopen2);
  if(sum->clopen3) clnt_destroy(sum->clopen3);
  if(sum->clopen4) clnt_destroy(sum->clopen4);
  if(sum->clopen5) clnt_destroy(sum->clopen5);
  if(sum->clopen6) clnt_destroy(sum->clopen6);
  if(sum->clopen7) clnt_destroy(sum->clopen7);
  if(sum->clalloc) clnt_destroy(sum->clalloc);
  if(sum->clalloc1) clnt_destroy(sum->clalloc1);
  if(sum->clalloc2) clnt_destroy(sum->clalloc2);
  if(sum->clalloc3) clnt_destroy(sum->clalloc3);
  if(sum->clalloc4) clnt_destroy(sum->clalloc4);
  if(sum->clalloc5) clnt_destroy(sum->clalloc5);
  if(sum->clalloc6) clnt_destroy(sum->clalloc6);
  if(sum->clalloc7) clnt_destroy(sum->clalloc7);
  if(sum->clget) clnt_destroy(sum->clget);
  if(sum->clget1) clnt_destroy(sum->clget1);
  if(sum->clget2) clnt_destroy(sum->clget2);
  if(sum->clget3) clnt_destroy(sum->clget3);
  if(sum->clget4) clnt_destroy(sum->clget4);
  if(sum->clget5) clnt_destroy(sum->clget5);
  if(sum->clget6) clnt_destroy(sum->clget6);
  if(sum->clget7) clnt_destroy(sum->clget7);
  if(sum->clput) clnt_destroy(sum->clput);
  if(sum->clput1) clnt_destroy(sum->clput1);
  if(sum->clput2) clnt_destroy(sum->clput2);
  if(sum->clput3) clnt_destroy(sum->clput3);
  if(sum->clput4) clnt_destroy(sum->clput4);
  if(sum->clput5) clnt_destroy(sum->clput5);
  if(sum->clput6) clnt_destroy(sum->clput6);
  if(sum->clput7) clnt_destroy(sum->clput7);
#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
  if(sum->clinfo) clnt_destroy(sum->clinfo);
  if(sum->clinfo1) clnt_destroy(sum->clinfo1);
  if(sum->clinfo2) clnt_destroy(sum->clinfo2);
  if(sum->clinfo3) clnt_destroy(sum->clinfo3);
  if(sum->clinfo4) clnt_destroy(sum->clinfo4);
  if(sum->clinfo5) clnt_destroy(sum->clinfo5);
  if(sum->clinfo6) clnt_destroy(sum->clinfo6);
  if(sum->clinfo7) clnt_destroy(sum->clinfo7);
#endif
  if(sum->cldelser) clnt_destroy(sum->cldelser);
  }
  
#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
  /* Close connection to sumsd.py. */
  if (sum->mSumsClient != -1)
  {
    DisconnectFromMultiSums(sum, history);
  }
#endif
  
  for(i=0; i < MAXSUMOPEN; i++) {
    if(transpid[i] == sum->uid) {
      svc_destroy(transp[i]);
      --numopened;
      break;
    }
  }
  free(sum->dsix_ptr);
  free(sum->wd);
  if(sum->sinfo) free(sum->sinfo);
  free(sum);
  freekeylist(&klist);
  if(errflg) return(4);
  return(0);
}

/* See if sum_svc is still alive. Return 0 if ok, 1 on timeout,
 * 4 on error (like unable to connect, i.e. the sum_svc is gone),
 * 5 tape_svc is gone (new 03Mar2011).
 * Calls the sums process of the last api call that was made.
*/
int SUM_nop(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  //struct timeval NOPTIMEOUT = { 5, 0 };
  struct timeval NOPTIMEOUT = { 10, 0 };
  KEY *klist;
  char *call_err;
  int ans;
  enum clnt_stat status;
  int i, stat;
  int errflg = 0;

  if(sum->debugflg) {
    (*history)("SUM_nop() call: uid = %lu\n", sum->uid);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", CLOSEDO);
  setkey_str(&klist, "USER", sum->username);
  status = clnt_call(clprev, NOPDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_void, (char *)&ans, NOPTIMEOUT);
  ans = (int)ans;
  if(ans == 5) { //tape_svc is gone
    return(ans);
  }

  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(clprev, "Err clnt_call for NOPDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    freekeylist(&klist);
    if(status != RPC_TIMEDOUT) return (4);
    else return (1);
  }

  stat = getmsgimmed();		//clean up pending response
  freekeylist(&klist);
  return(ans);
}

/* Get the wd of the storage units given in dsix_ptr of the given sum.
 * Return 0 on success w/data available, 1 on error, 4 on connection reset
 * by peer (sum_svc probably gone) or RESULT_PEND (32)
 * when data will be sent later and caller must do a sum_wait() or sum_poll() 
 * when he is ready for it. You can't make another SUM_get() with the
 * samd SUM_t handle while one is still pending.
*/
int SUM_get(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  int rr;
  KEY *klist;
  char *call_err, *dptr;
  char **cptr;
  int i, cnt, msgstat, stat, xmode, pid;
  char dsix_name[64], thishost[80];
  uint64_t *dxlong;
  uint32_t retstat;
  enum clnt_stat status;

  if(sum->debugflg) {
    (*history)("SUM_get() call:\n");
  }
  if(sum->reqcnt > MAXSUMREQCNT) {
    (*history)("Requent count of %d > max of %d\n", sum->reqcnt, MAXSUMREQCNT);
    return(1);
  }
  gethostname(thishost, 80);
  dptr = index(thishost, '.');     // must be short form
  if(dptr) *dptr = '\0';
  pid = getpid();
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", GETDO);
  setkey_str(&klist, "username", sum->username);
  setkey_int(&klist, "newflg", GET_FIX_VER);
  setkey_int(&klist, "pidcaller", pid);
  setkey_str(&klist, "hostcaller", thishost);
  //setkey_int(&klist, "DEBUGFLG", 1);   //!!TEMP put in debug flag
  dxlong = sum->dsix_ptr;
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dxlong++);
  }
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0:
    clget = sum->clget;
    break;
  case 1:
    clget = sum->clget1;
    break;
  case 2:
    clget = sum->clget2;
    break;
  case 3:
    clget = sum->clget3;
    break;
  case 4:
    clget = sum->clget4;
    break;
  case 5:
    clget = sum->clget5;
    break;
  case 6:
    clget = sum->clget6;
    break;
  case 7:
    clget = sum->clget7;
    break;
  }
  sum->mode = sum->mode + TAPERDON; //set in case get ^C during call
  stat = TAPERDON;
  xmode = -stat-1;		//mask to clear bit
  clprev = clget;
  status = clnt_call(clget, GETDO, (xdrproc_t)xdr_Rkey, (char *)klist,
                      (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      sum->mode = sum->mode & xmode;  //clear TAPERDON bit
      call_err = clnt_sperror(clget, "Err clnt_call for GETDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    } else {
      (*history)("%s Ignore timeout in SUM_get()\n", datestring());
    }
  }
  freekeylist(&klist);
  if(sum->debugflg) {
    (*history)("retstat in SUM_get = %ld\n", retstat);
  }
  if(retstat == 1) {
    sum->mode = sum->mode & xmode;  //clear TAPERDON bit
    return(1);			 /* error occured */
  }
  if(retstat == RESULT_PEND) {
    //sum->mode = sum->mode + TAPERDON;	//already set now
    return((int)retstat); /* caller to check later */
  }
  sum->mode = sum->mode & xmode;  //clear TAPERDON bit
  msgstat = getanymsg(1);	/* answer avail now */
  if(msgstat == ERRMESS) return(ERRMESS);
  if(sum->debugflg) {
    /* print out wd's found */
    (*history)("In SUM_get() the wd's found are:\n");
    cnt = sum->reqcnt;
    cptr = sum->wd;
    for(i = 0; i < cnt; i++) {
      printf("wd = %s\n", *cptr++);
    }
  }
  return(sum->status);
}


/* Puts storage units from allocated storage to the DB catalog.
 * Caller gives disposition of a previously allocated data segments. 
 * Allows for a request count to put multiple segments.
 * Returns 0 on success.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_put(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  int rr;
  KEY *klist;
  char dsix_name[64];
  char *call_err;
  char **cptr;
  uint64_t *dsixpt;
  int i, cnt, msgstat;
  uint32_t retstat;
  enum clnt_stat status;

  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  if(sum->debugflg) {
    (*history)("Going to PUT reqcnt=%d with 1st wd=%s, ix=%lu\n", 
			sum->reqcnt, *cptr, *dsixpt);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_str(&klist, "dsname", sum->dsname);
  setkey_str(&klist, "history_comment", sum->history_comment);
  setkey_str(&klist, "username", sum->username);
  setkey_int(&klist, "group", sum->group);
  setkey_int(&klist, "storage_set", sum->storeset);
  //setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", PUTDO);
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dsixpt++);
    sprintf(dsix_name, "wd_%d", i);
    setkey_str(&klist, dsix_name, *cptr++);
  }
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0: 
    clput = sum->clput;
    break;
  case 1:
    clput = sum->clput1;
    break;
  case 2:
    clput = sum->clput2;
    break;
  case 3:
    clput = sum->clput3;
    break;
  case 4:
    clput = sum->clput4;
    break;
  case 5:
    clput = sum->clput5;
    break;
  case 6:
    clput = sum->clput6;
    break;
  case 7:
    clput = sum->clput7;
    break;
  }
  clprev = clput;
  status = clnt_call(clput, PUTDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(clput, "Err clnt_call for PUTDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  freekeylist(&klist);
  if(retstat == 1) return(1);           /* error occured */
  /* NOTE: RESULT_PEND cannot happen for SUM_put() call */
  /*if(retstat == RESULT_PEND) return((int)retstat); /* caller to check later */
  msgstat = getanymsg(1);		/* answer avail now */
  if(msgstat == ERRMESS) return(ERRMESS);
  if(sum->debugflg) {
    (*history)("In SUM_put() print out wd's \n");
    cnt = sum->reqcnt;
    cptr = sum->wd;
    for(i = 0; i < cnt; i++) {
      printf("wd = %s\n", *cptr++);
    }
  }
  return(sum->status);
}

/* Take an array of sunums and mark them for archiving.
 * Any sunum that is not already online, cannot be marked for archiving.
 * This sunum is ignored and does not cause an error.
 * An error return indicates some failure along the way. One or more sunum
 * may not have gotten marked archive pending.
 * If you need detailed knowledge, call w/one sunum at a time.
 * A touch option can be used in the mode and tday fields of the sum_t. 
 * This option wll apply to all the sunums.
 * This function can take up to MAXSUMREQCNT (512) sunums in a call.
*/
int SUM_archSU(SUM_t *sum, int (*history)(const char *fmt, ...))
{
//!!!TBD. NOTE: May develop a seperate utility script to do this 
//instead of a new API. See sum_arch_recset.pl and sum_arch_su.pl
/*****************************************************************
  int rr;
  KEY *klist;
  char dsix_name[64];
  char *call_err;
  uint64_t *dsixpt;
  int i, cnt, msgstat;
  uint32_t retstat;
  enum clnt_stat status;

  dsixpt = sum->dsix_ptr;
  if(sum->debugflg) {
    (*history)("Going to archSU reqcnt=%d with 1st ix=%lu\n", 
			sum->reqcnt, *dsixpt);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  //setkey_str(&klist, "dsname", sum->dsname);
  //setkey_str(&klist, "username", sum->username);
  //setkey_int(&klist, "group", sum->group);
  //setkey_int(&klist, "storage_set", sum->storeset);
  //setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", ARCHSUDO);
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dsixpt++);
    sprintf(dsix_name, "wd_%d", i);
    setkey_str(&klist, dsix_name, *cptr++);
  }
  rr = rr_random(0, numSUM-1);
  switch(rr) {
  case 0: 
    clput = sum->clput;
    break;
  case 1:
    clput = sum->clput1;
    break;
  case 2:
    clput = sum->clput2;
    break;
  case 3:
    clput = sum->clput3;
    break;
  case 4:
    clput = sum->clput4;
    break;
  case 5:
    clput = sum->clput5;
    break;
  case 6:
    clput = sum->clput6;
    break;
  case 7:
    clput = sum->clput7;
    break;
  }
  clprev = clput;
  status = clnt_call(clput, PUTDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  // NOTE: These rtes seem to return after the reply has been received despite
  // the timeout value. If it did take longer than the timeout then the timeout
  // error status is set but it should be ignored.
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(clput, "Err clnt_call for PUTDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  freekeylist(&klist);
  if(retstat == 1) return(1);           // error occured
  // NOTE: RESULT_PEND cannot happen for SUM_put() call
  //if(retstat == RESULT_PEND) return((int)retstat); // caller to check later
  msgstat = getanymsg(1);		// answer avail now
  if(msgstat == ERRMESS) return(ERRMESS);
  if(sum->debugflg) {
    (*history)("In SUM_put() print out wd's \n");
    cnt = sum->reqcnt;
    cptr = sum->wd;
    for(i = 0; i < cnt; i++) {
      printf("wd = %s\n", *cptr++);
    }
  }
  return(sum->status);
*****************************************************************/
    return 0;
}

/* Called by the delete_series program before it deletes the series table.
 * Called with a pointer to a full path name that contains the sunums
 * that are associated with the series about to be deleted.
 * Returns 1 on error, else 0.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
int SUM_delete_series(char *filename, char *seriesname, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  CLIENT *cl;
  char *call_err;
  char *cptr, *server_name, *username;
  uint32_t retstat;
  enum clnt_stat status;

  klist = newkeylist();
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  setkey_str(&klist, "USER", username);
  setkey_int(&klist, "DEBUGFLG", 1);		/* !!!TEMP */
  setkey_str(&klist, "FILE", filename);
  setkey_str(&klist, "SERIESNAME", seriesname);
  if (!(server_name = getenv("SUMSERVER"))) {
    server_name = (char *)alloca(sizeof(SUMSERVER)+1);
    strcpy(server_name, SUMSERVER);
  }
  cptr = (char *)index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = '\0';
  //handle created in SUM_open()
  clprev = cldelser;
  status = clnt_call(cldelser, DELSERIESDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(cldelser, "Err clnt_call for DELSERIESDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (4);
    }
  }
  freekeylist(&klist);
  //clnt_destroy(cldelser);		/* destroy handle to sum_svc !!TBD check */
  if(retstat == 1) return(1);           /* error occured */
  return(0);
}

/* Check if the response for a  previous request is complete.
 * Return 0 = msg complete, the sum has been updated
 * TIMEOUTMSG = msg still pending, try again later
 * ERRMESS = fatal error (!!TBD find out what you can do if this happens)
 * NOTE: Upon msg complete return, sum->status != 0 if error anywhere in the 
 * path of the request that initially returned the RESULT_PEND status.
*/
int SUM_poll(SUM_t *sum)
{
  int stat, xmode;

  stat = getanymsg(0);
  if(stat == RPCMSG) {
    //stat = TAPERDON;
    //xmode = -stat-1;
    //sum->mode = sum->mode & xmode;  //clear TAPERDON bit. !!NO
    taperdon_cleared = 1;
    return(0);		/* all done ok */
  }
  else return(stat);
}

/* Wait until the expected response is complete.
 * Return 0 = msg complete, the sum has been updated
 * ERRMESS = fatal error (!!TBD find out what you can do if this happens)
 * NOTE: Upon msg complete return, sum->status != 0 if error anywhere in the 
 * path of the request that initially returned the RESULT_PEND status.
*/
int SUM_wait(SUM_t *sum)
{
  int stat, xmode;

  while(1) {
    stat = getanymsg(0);
    if(stat == TIMEOUTMSG) {
      usleep(1000);
      continue;
    }
    else break;
  }
  if(stat == RPCMSG) {
    //stat = TAPERDON;
    //xmode = -stat-1;
    //sum->mode = sum->mode & xmode;  //clear TAPERDON bit !!NO
    taperdon_cleared = 1;
    return(0);		/* all done ok */
  }
  else return(stat);
}

/* Check the SUM_repartn() client calls. Returns 1 on error.
*/
int SUM_repartn_ck_error(int stat, uint32_t rstat, CLIENT *rcl, int (*history)(const char *fmt, ...))
{
  char *call_err;

    if(stat != RPC_SUCCESS) {
      if(stat != RPC_TIMEDOUT) {
        call_err = clnt_sperror(rcl, "Err clnt_call for SUMREPARTN");
        (*history)("%s %d %s\n", datestring(), stat, call_err);
        return (1);
      }
    }
    if(rstat) {                 /* error on SUMREPARTN call */
      return(1);
    }
    return(0);
}

/* Send a SUMREPARTN message to all the relevant sum processes to tell them
 * to reread the sum_partn_avail DB table. This is normally called by
 * the sumrepartn utility program after a sum_partn_avail table has 
 * been changed. Returns non-0 on error.
 * The relevant processes are sum_svc, Salloc*, Sget*, Sinfo*.
*/
int SUM_repartn(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  enum clnt_stat status;
  int stat;
  int failflg = 0;

  if(sum->debugflg) {
    (*history)("SUM_repartn() call: uid = %lu\n", sum->uid);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_str(&klist, "USER", sum->username);
  if(numSUM == 1) {
    status = clnt_call(sum->cl, SUMREPARTN, (xdrproc_t)xdr_Rkey, (char *)klist,
                        (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
    if(status = SUM_repartn_ck_error(status, retstat, sum->cl, history)) {
      (*history)("SUM_repartn() failed on call to sum_svc\n");
      freekeylist(&klist);
      return (1);
    }
  }
  else {		//call everyone of interest
    status = clnt_call(sum->cl, SUMREPARTN, (xdrproc_t)xdr_Rkey, (char *)klist,
                        (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
    if(status = SUM_repartn_ck_error(status, retstat, sum->cl, history)) {
      (*history)("SUM_repartn() failed on call to sum_svc\n");
      failflg = 1;
    }
    if(sum->clalloc) {
      status = clnt_call(sum->clalloc, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc,history)) {
        (*history)("SUM_repartn() failed on call to Salloc\n");
        failflg = 1;
      }
    }
    if(sum->clalloc1) {
      status = clnt_call(sum->clalloc1, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc1,history)) {
        (*history)("SUM_repartn() failed on call to Salloc1\n");
        failflg = 1;
      }
    }
    if(sum->clalloc2) {
      status = clnt_call(sum->clalloc2, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc2,history)) {
        (*history)("SUM_repartn() failed on call to Salloc2\n");
        failflg = 1;
      }
    }
    if(sum->clalloc3) {
      status = clnt_call(sum->clalloc3, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc3,history)) {
        (*history)("SUM_repartn() failed on call to Salloc3\n");
        failflg = 1;
      }
    }
    if(sum->clalloc4) {
      status = clnt_call(sum->clalloc4, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc4,history)) {
        (*history)("SUM_repartn() failed on call to Salloc4\n");
        failflg = 1;
      }
    }
    if(sum->clalloc5) {
      status = clnt_call(sum->clalloc5, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc5,history)) {
        (*history)("SUM_repartn() failed on call to Salloc5\n");
        failflg = 1;
      }
    }
    if(sum->clalloc6) {
      status = clnt_call(sum->clalloc6, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc6,history)) {
        (*history)("SUM_repartn() failed on call to Salloc6\n");
        failflg = 1;
      }
    }
    if(sum->clalloc7) {
      status = clnt_call(sum->clalloc7, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clalloc7,history)) {
        (*history)("SUM_repartn() failed on call to Salloc7\n");
        failflg = 1;
      }
    }
    if(sum->clget) {
      status = clnt_call(sum->clget, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget,history)) {
        (*history)("SUM_repartn() failed on call to Sget\n");
        failflg = 1;
      }
    }
    if(sum->clget1) {
      status = clnt_call(sum->clget1, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget1,history)) {
        (*history)("SUM_repartn() failed on call to Sget1\n");
        failflg = 1;
      }
    }
    if(sum->clget2) {
      status = clnt_call(sum->clget2, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget2,history)) {
        (*history)("SUM_repartn() failed on call to Sget2\n");
        failflg = 1;
      }
    }
    if(sum->clget3) {
      status = clnt_call(sum->clget3, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget3,history)) {
        (*history)("SUM_repartn() failed on call to Sget3\n");
        failflg = 1;
      }
    }
    if(sum->clget4) {
      status = clnt_call(sum->clget4, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget4,history)) {
        (*history)("SUM_repartn() failed on call to Sget4\n");
        failflg = 1;
      }
    }
    if(sum->clget5) {
      status = clnt_call(sum->clget5, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget5,history)) {
        (*history)("SUM_repartn() failed on call to Sget5\n");
        failflg = 1;
      }
    }
    if(sum->clget6) {
      status = clnt_call(sum->clget6, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget6,history)) {
        (*history)("SUM_repartn() failed on call to Sget6\n");
        failflg = 1;
      }
    }
    if(sum->clget7) {
      status = clnt_call(sum->clget7, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clget7,history)) {
        (*history)("SUM_repartn() failed on call to Sget7\n");
        failflg = 1;
      }
    }

#if !defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS
    if(sum->clinfo) {
      status = clnt_call(sum->clinfo, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo\n");
        failflg = 1;
      }
    }
    if(sum->clinfo1) {
      status = clnt_call(sum->clinfo1, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo1,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo1\n");
        failflg = 1;
      }
    }
    if(sum->clinfo2) {
      status = clnt_call(sum->clinfo2, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo2,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo2\n");
        failflg = 1;
      }
    }
    if(sum->clinfo3) {
      status = clnt_call(sum->clinfo3, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo3,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo3\n");
        failflg = 1;
      }
    }
    if(sum->clinfo4) {
      status = clnt_call(sum->clinfo4, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo4,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo4\n");
        failflg = 1;
      }
    }
    if(sum->clinfo5) {
      status = clnt_call(sum->clinfo5, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo5,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo5\n");
        failflg = 1;
      }
    }
    if(sum->clinfo6) {
      status = clnt_call(sum->clinfo6, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo6,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo6\n");
        failflg = 1;
      }
    }
    if(sum->clinfo7) {
      status = clnt_call(sum->clinfo7, SUMREPARTN, (xdrproc_t)xdr_Rkey, 
  	(char *)klist, (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
      if(status = SUM_repartn_ck_error(status,retstat,sum->clinfo7,history)) {
        (*history)("SUM_repartn() failed on call to Sinfo7\n");
        failflg = 1;
      }
    }
#endif
  }
  return(0);
}
/**************************************************************************/

/* Attempt to get any sum_svc completion msg.
 * If block = 0 will timeout after 0.5 sec, else will wait until a msg is
 * received.
 * Returns the type of msg or timeout status.
*/
int getanymsg(int block)
{
  fd_set readfds;
  struct timeval timeout;
  int wait, retcode = ERRMESS, srdy, info;
  static int ts=0;   /* file descriptor table size */

  wait = 1;
  //if(!ts) ts = getdtablesize();
  //cluster nodes getdtablesize() is 16384, but select can only handle FD_SETSIZE
  if(!ts) ts = FD_SETSIZE;

  while(wait) {
    readfds=svc_fdset;
    timeout.tv_sec=0;
    timeout.tv_usec=500000;
    srdy=select(ts,&readfds,(fd_set *)0,(fd_set *)0,&timeout); /* # ready */
    switch(srdy) {
    case -1:
      if(errno==EINTR) {
        continue;
      }
      fprintf(stderr, "%s\n", datestring());
      perror("getanymsg: select failed");
      retcode = ERRMESS;
      wait = 0;
      break;
    case 0:			  /* timeout */
      if(block) continue;
      retcode = TIMEOUTMSG;
      wait = 0;
      break;
    default:
      /* can be called w/o dispatch to respd(), but will happen on next call */
      RESPDO_called = 0;	  /* set by respd() */
      svc_getreqset(&readfds);    /* calls respd() */
      retcode = RPCMSG;
      if(RESPDO_called) wait = 0;
      break;
    }
  }
  return(retcode);
}

// Like getanymsg() above but w/no timeout for immediate SUM_close() use.
int getmsgimmed()
{
  fd_set readfds;
  struct timeval timeout;
  int wait, retcode = ERRMESS, srdy, info;
  static int ts=0;   /* file descriptor table size */

  wait = 1;
  timeout.tv_sec=0;
  timeout.tv_usec=0;
  //if(!ts) ts = getdtablesize();
  if(!ts) ts = FD_SETSIZE;    /* cluster nodes have 16384 fd instead of 1024 */
  while(wait) {
    readfds=svc_fdset;
    srdy=select(ts,&readfds,(fd_set *)0,(fd_set *)0,&timeout); /* # ready */
    switch(srdy) {
    case -1:
      if(errno==EINTR) {
        continue;
      }
      fprintf(stderr, "%s\n", datestring());
      perror("getanymsg: select failed");
      retcode = ERRMESS;
      wait = 0;
      break;
    case 0:			  /* timeout */
      retcode = TIMEOUTMSG;
      wait = 0;
      break;
    default:
      /* can be called w/o dispatch to respd(), but will happen on next call */
      RESPDO_called = 0;	  /* set by respd() */
      svc_getreqset(&readfds);    /* calls respd() */
      retcode = RPCMSG;
      if(RESPDO_called) wait = 0;
      break;
    }
  }
  return(retcode);
}

/* Function called on receipt of a sum_svc response message for
 * a RESPDOARRAY.  Called from respd().
*/

/* This is the f*cking function where the SUM_t::sinfo list is created. It looks like 
 * Jim started using xdr_array to create this list, but then couldn't figure out how to do 
 * it, so he used this function instead. However, he left all the xdr crap laying about
 * to cause confusion.
 */
KEY *respdoarray_1(KEY *params)
{
  SUM_t *sum;
  SUM_info_t *sinfod, *sinf;
  SUMOPENED *sumopened;
  FILE *rfp;
  int reqcnt, i, filemode;
  char *file, *cptr;
  char name[128], str1[128], str2[128], line[128];

  sumopened = getsumopened(sumopened_hdr, getkey_uint64(params, "uid"));
  sum = (SUM_t *)sumopened->sum;
  if(sum == NULL) {
    printf("**Response from sum_svc does not have an opened SUM_t *sum\n");
    printf("**Don't know what this will do to the caller, but this is a logic bug\n");
    return((KEY *)NULL);
  }
  reqcnt = getkey_int(params, "reqcnt");
  file = getkey_str(params, "FILE");
  filemode = getkey_int(params, "filemode");
  //printf("mode=%d file=%s\n", filemode, file); //!!TEMP
  if((rfp=fopen(file, "r")) == NULL) { 
    printf("**Can't open %s from sum_svc ret from SUM_infoArray() call\n", file);
    free(file);
    return((KEY *)NULL);
  }
  free(file);
  
  /* ************
   *
   * HERE IT IS!! THE PLACE WHERE THE sinfo LIST IS ALLOCATED.
   *
   * ************ */
  sum->sinfo = (SUM_info_t *)calloc(reqcnt, sizeof(SUM_info_t));
  sinfod = sum->sinfo;
  if(filemode == 0)
    fread(sinfod, sizeof(SUM_info_t), reqcnt, rfp);
  sinfod->next = NULL;
  sinf = sinfod;
  //must now make the links for the current memory
  for(i = 1; i < reqcnt; i++) {
    sinf++;
    sinfod->next = sinf;
    sinfod = sinf;
    sinfod->next = NULL;
  }
  if(filemode == 1) {
  sinfod = sum->sinfo;
  for(i = 0; i < reqcnt; i++) {  //do linked list in sinfo
    fgets(line, 128, rfp);
    if(!strcmp(line, "\n")) { 
      fgets(line, 128, rfp);
    }
    sscanf(line, "%s %lu", name, &sinfod->sunum);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->online_loc);
    
    /* If an invalid SUNUM was encountered, then the next record inserted into the
     * results file begins with "pa_status" instead of the normal "online_loc".  In that
     * case, we add online_loc == '\0' and send that back to DRMS.
     */
    if(!strcmp(name, "pa_status=")) {	//the sunum was not found in the db
      strcpy(sinfod->online_loc, "");
      goto SKIPENTRY;
    }
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->online_status);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->archive_status);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->offsite_ack);
    fgets(line, 128, rfp);
    sscanf(line, "%s %80[^;]", name, sinfod->history_comment); //allow sp in line
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->owning_series);
    fgets(line, 128, rfp);
    sscanf(line, "%s %d", name, &sinfod->storage_group);
    fgets(line, 128, rfp);
    sscanf(line, "%s %lf", name, &sinfod->bytes);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s %s", name, str1, str2); //date strin always the same
    sprintf(sinfod->creat_date, "%s %s", str1, str2);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->username);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->arch_tape);
    fgets(line, 128, rfp);
    sscanf(line, "%s %d", name, &sinfod->arch_tape_fn);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s %s", name, str1, str2);
    sprintf(sinfod->arch_tape_date, "%s %s", str1, str2);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->safe_tape);
    fgets(line, 128, rfp);
    sscanf(line, "%s %d", name, &sinfod->safe_tape_fn);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s %s", name, str1, str2);
    sprintf(sinfod->safe_tape_date, "%s %s", str1, str2);
    fgets(line, 128, rfp);
    sscanf(line, "%s %d", name, &sinfod->pa_status);
  SKIPENTRY:
    fgets(line, 128, rfp);
    sscanf(line, "%s %d", name, &sinfod->pa_substatus);
    fgets(line, 128, rfp);
    sscanf(line, "%s %s", name, sinfod->effective_date);
    sinfod = sinfod->next; //sinfod->next set up from the malloc
  } 
  }
  fclose(rfp);
  return((KEY *)NULL);
}

/* Function called on receipt of a sum_svc response message.
 * Called from respd().
*/
KEY *respdo_1(KEY *params)
{
  SUM_t *sum;
  SUM_info_t *sinfo;
  SUMOPENED *sumopened;
  char *wd;
  char **cptr;
  uint64_t *dsixpt;
  uint64_t dsindex;
  int i, reqcnt, reqcode;
  char name[128];
                                         
  sumopened = getsumopened(sumopened_hdr, getkey_uint64(params, "uid"));
  sum = (SUM_t *)sumopened->sum;
  if(sum == NULL) {
    printf("**Response from sum_svc does not have an opened SUM_t *sum\n");
    printf("**Don't know what this will do to the caller, but this is a logic bug\n");
    return((KEY *)NULL);
  }
  if(sum->debugflg) {
    printf("\nIn respdo_1() the keylist is:\n");
    keyiterate(printkey, params);
  }
  reqcnt = getkey_int(params, "reqcnt");
  reqcode = getkey_int(params, "REQCODE");
  sum->status = getkey_int(params, "STATUS");
  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  switch(reqcode) {
  case ALLOCDO:
    wd = getkey_str(params, "partn_name");
    dsindex = getkey_uint64(params, "ds_index");
    *cptr = wd;
    *dsixpt = dsindex;
    if(findkey(params, "ERRSTR")) {
      printf("%s\n", GETKEY_str(params, "ERRSTR"));
    }
    break;
  case GETDO:
    for(i = 0; i < reqcnt; i++) {
        sprintf(name, "wd_%d", i);
      wd = getkey_str(params, name);
      *cptr++ = wd;
      if(findkey(params, "ERRSTR")) {
        printf("%s\n", GETKEY_str(params, "ERRSTR"));
      }
    } 
    break;
  case INFODOX:
    if(findkey(params, "ERRSTR")) {
      printf("%s\n", GETKEY_str(params, "ERRSTR"));
    }
    sinfo = sum->sinfo;
    for(i = 0; i < reqcnt; i++) {  //do linked list in sinfo
      sprintf(name, "ds_index_%d", i);
      sinfo->sunum = getkey_uint64(params, name);
      sprintf(name, "online_loc_%d", i);
      strcpy(sinfo->online_loc, GETKEY_str(params, name));
      sprintf(name, "online_status_%d", i);
      strcpy(sinfo->online_status, GETKEY_str(params, name));
      sprintf(name, "archive_status_%d", i);
      strcpy(sinfo->archive_status, GETKEY_str(params, name));
      sprintf(name, "offsite_ack_%d", i);
      strcpy(sinfo->offsite_ack, GETKEY_str(params, name));
      sprintf(name, "history_comment_%d", i);
      strcpy(sinfo->history_comment, GETKEY_str(params, name));
      sprintf(name, "owning_series_%d", i);
      strcpy(sinfo->owning_series, GETKEY_str(params, name));
      sprintf(name, "storage_group_%d", i);
      sinfo->storage_group = getkey_int(params, name);
      sprintf(name, "bytes_%d", i);
      sinfo->bytes = getkey_double(params, name);
      sprintf(name, "creat_date_%d", i);
      strcpy(sinfo->creat_date, GETKEY_str(params, name));
      sprintf(name, "username_%d", i);
      strcpy(sinfo->username, GETKEY_str(params, name));
      sprintf(name, "arch_tape_%d", i);
      strcpy(sinfo->arch_tape, GETKEY_str(params, name));
      sprintf(name, "arch_tape_fn_%d", i);
      sinfo->arch_tape_fn = getkey_int(params, name);
      sprintf(name, "arch_tape_date_%d", i);
      strcpy(sinfo->arch_tape_date, GETKEY_str(params, name));
      sprintf(name, "safe_tape_%d", i);
      strcpy(sinfo->safe_tape, GETKEY_str(params, name));
      sprintf(name, "safe_tape_fn_%d", i);
      sinfo->safe_tape_fn = getkey_int(params, name);
      sprintf(name, "safe_tape_date_%d", i);
      strcpy(sinfo->safe_tape_date, GETKEY_str(params, name));
      sprintf(name, "pa_status_%d", i);
      sinfo->pa_status = getkey_int(params, name);
      sprintf(name, "pa_substatus_%d", i);
      sinfo->pa_substatus = getkey_int(params, name);
      sprintf(name, "effective_date_%d", i);
      strcpy(sinfo->effective_date, GETKEY_str(params, name));
      if(!(sinfo = sinfo->next)) { 
        if(i+1 != reqcnt) {
          printf("ALERT: #of info requests received differs from reqcnt\n");
          break;	//don't agree w/reqcnt !ck this out
        }
      }
    } 
    break;
  case INFODO:
    //add_keys(infoparams, &params);
    sinfo = sum->sinfo;
    sinfo->sunum = getkey_uint64(params, "SUNUM");
    strcpy(sinfo->online_loc, GETKEY_str(params, "online_loc"));
    strcpy(sinfo->online_status, GETKEY_str(params, "online_status"));
    strcpy(sinfo->archive_status, GETKEY_str(params, "archive_status"));
    strcpy(sinfo->offsite_ack, GETKEY_str(params, "offsite_ack"));
    strcpy(sinfo->history_comment, GETKEY_str(params, "history_comment"));
    strcpy(sinfo->owning_series, GETKEY_str(params, "owning_series"));
    sinfo->storage_group = getkey_int(params, "storage_group");
    sinfo->bytes = getkey_double(params, "bytes");
    strcpy(sinfo->creat_date, GETKEY_str(params, "creat_date"));
    strcpy(sinfo->username, GETKEY_str(params, "username"));
    strcpy(sinfo->arch_tape, GETKEY_str(params, "arch_tape"));
    sinfo->arch_tape_fn = getkey_int(params, "arch_tape_fn");
    strcpy(sinfo->arch_tape_date, GETKEY_str(params, "arch_tape_date"));
    strcpy(sinfo->safe_tape, GETKEY_str(params, "safe_tape"));
    sinfo->safe_tape_fn = getkey_int(params, "safe_tape_fn");
    strcpy(sinfo->safe_tape_date, GETKEY_str(params, "safe_tape_date"));
    sinfo->pa_status = getkey_int(params, "pa_status");
    sinfo->pa_substatus = getkey_int(params, "pa_substatus");
    strcpy(sinfo->effective_date, GETKEY_str(params, "effective_date"));
    break;
  case PUTDO:
    break;
  default:
    printf("**Unexpected REQCODE in respdo_1()\n");
    break;
  }
  return((KEY *)NULL);
}


/* This is the dispatch routine for the registered RESPPROG, suidback.
 * Called when a server sends a response that it is done with the original 
 * call that we made to it. 
 * This routine is called by svc_getreqset() in getanymsg().
*/
static void respd(rqstp, transp)
  struct svc_req *rqstp;
  SVCXPRT *transp;
{
  union __svcargun {
    Rkey respdo_1_arg;
  } argument;
  char *result;
  bool_t (*xdr_argument)(), (*xdr_result)();
  char *(*local)();

  switch (rqstp->rq_proc) {
  case NULLPROC:
    (void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
    return;
  case RESPDO:
    xdr_argument = xdr_Rkey;
    xdr_result = xdr_void;
    local = (char *(*)()) respdo_1;
    RESPDO_called = 1;
    break;
  case RESPDOARRAY:
    xdr_argument = xdr_Rkey;
    xdr_result = xdr_void;
    local = (char *(*)()) respdoarray_1;
    RESPDO_called = 1;
    break;
  default:
    svcerr_noproc(transp);
    return;
  }
  bzero((char *)&argument, sizeof(argument));
  if(!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
    svcerr_decode(transp);
    return;
  }
  /* send immediate ack back */
  if(!svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL)) {
    svcerr_systemerr(transp);
    return;
  }
  result = (*local)(&argument, rqstp);	/* call the fuction */

  if(!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
    printf("unable to free arguments in respd()\n");
    /*svc_unregister(RESPPROG, mytid);*/
  }
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". */
char *datestring(void)
{
  time_t t;
  char *str;

  t = time(NULL);
  str = ctime(&t);
  str[19] = 0;
  return str+4;          /* isolate the mmm dd hh:mm:ss */
}

