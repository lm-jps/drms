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
#include <pwd.h>
#include <sum_rpc.h>
#include <printk.h>
#include "serverdefs.h"
#include "cJSON.h"
#include "hcontainer.h"
#include "timer.h"

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define IS_ALIVE "is-alive"

static HContainer_t *gMTCallMap = NULL;

/* Enum defining types of API functions supported by MT SUMS. */
enum MTSums_CallType_enum
{   kMTSums_CallType_None = 0,
    kMTSums_CallType_Open = 1,
    kMTSums_CallType_Close = 2,
    kMTSums_CallType_Rollback = 3,
    kMTSums_CallType_Info = 4,
    kMTSums_CallType_Get = 5,
    kMTSums_CallType_Alloc = 6,
    kMTSums_CallType_Alloc2 = 7,
    kMTSums_CallType_Put = 8,
    kMTSums_CallType_Deleteseries = 9,
    kMTSums_CallType_Ping = 10,
    kMTSums_CallType_Poll = 11,
    kMTSums_CallType_END = 12,
};
typedef enum MTSums_CallType_enum MTSums_CallType_t;

char *MTSums_CallType_strings[] =
{
    "none",
    "open",
    "close",
    "rollback",
    "info",
    "get",
    "alloc",
    "alloc2",
    "put",
    "deleteseries",
    "ping",
    "poll"
};

static MTSums_CallType_t CallTypeFromString(const char *str)
{
    MTSums_CallType_t rv = kMTSums_CallType_None;
    void **node = NULL;
    int iCall = 0;
    
    if (!gMTCallMap)
    {
        gMTCallMap = hcon_create(sizeof(int), 64, NULL, NULL, NULL, NULL, 0);
        
        if (gMTCallMap)
        {
            for (iCall = 0; iCall < kMTSums_CallType_END; iCall++)
            {
                hcon_insert(gMTCallMap, MTSums_CallType_strings[iCall], &iCall);
            }
        }
    }

    if (gMTCallMap)
    {
        node = hcon_lookup(gMTCallMap, str);
        if (node && *node)
        {
            rv = *(int *)node;
        }
    }
    
    return rv;
}

// Typedefs for jsonizer opaque data type and pointer
struct JSONIZER_DATA_struct; // Incomplete type.
typedef struct JSONIZER_DATA_struct JSONIZER_DATA_t;

/* No data is sent to the MT SUMS server for the SUM_open() call. */
/* No extra data is sent to the MT SUMS server for the SUM_close() call. */
struct JSONIZER_DATA_INFO_struct
{
    uint64_t *sunums;
    size_t nSus;
};
typedef struct JSONIZER_DATA_INFO_struct JSONIZER_DATA_INFO_t;

struct JSONIZER_DATA_GET_struct
{
    int touch;
    int retrieve;
    int retention;
    uint64_t *sunums;
    size_t nSus;
};
typedef struct JSONIZER_DATA_GET_struct JSONIZER_DATA_GET_t;

struct JSONIZER_DATA_ALLOC_struct
{
    int sugroup;
    double numBytes;
};
typedef struct JSONIZER_DATA_ALLOC_struct JSONIZER_DATA_ALLOC_t;

struct JSONIZER_DATA_ALLOC2_struct
{
    uint64_t sunum;
    int sugroup;
    double numBytes;
};
typedef struct JSONIZER_DATA_ALLOC2_struct JSONIZER_DATA_ALLOC2_t;

struct JSONIZER_DATA_PUT_struct
{
    uint64_t *sunums;
    char **sudirs;
    size_t nSus;
    char *series;
    int retention;
    char *archiveType;
};
typedef struct JSONIZER_DATA_PUT_struct JSONIZER_DATA_PUT_t;

struct JSONIZER_DATA_DELETESERIES_struct
{
    char *series;
};
typedef struct JSONIZER_DATA_DELETESERIES_struct JSONIZER_DATA_DELETESERIES_t;

struct JSONIZER_DATA_POLL_struct
{
    char *requestID;
};
typedef struct JSONIZER_DATA_POLL_struct JSONIZER_DATA_POLL_t;

static int callMTSums(SUM_t *sums, MTSums_CallType_t callType, JSONIZER_DATA_t *data, int (*history)(const char *fmt, ...));
static int jsonizeRequest(SUM_t *sums, MTSums_CallType_t type, JSONIZER_DATA_t *data, char **json, int (*history)(const char *fmt, ...));
static int unjsonizeResponse(SUM_t *sums, MTSums_CallType_t type, const char *msg, int (*history)(const char *fmt, ...));

#endif

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

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
// All these global RPC clients are never used in any RPC functions. Consider them variables that point
// to CLIENT structs in the SUM_struct struct. There is no reason they need to be global, and in fact
// there is no reason for them to exist at all.
static CLIENT *cl = NULL;
static CLIENT *clalloc = NULL;
static CLIENT *clget = NULL;
static CLIENT *clput = NULL;
static CLIENT *clinfo = NULL;
static CLIENT *cldelser = NULL;
static CLIENT *clopen = NULL;
static CLIENT *clopen1 = NULL;
static CLIENT *clopen2 = NULL;
static CLIENT *clopen3 = NULL;
static CLIENT *clopen4 = NULL;
static CLIENT *clopen5 = NULL;
static CLIENT *clopen6 = NULL;
static CLIENT *clopen7 = NULL;
// Never used.
static CLIENT *clclose = NULL;

/* Aw, crap. I think the SVC servers link to these variables. */
static SVCXPRT *transp[MAXSUMOPEN];
static SUMID_t transpid[MAXSUMOPEN];
#endif

// clprev keeps track of the last RPC client used. I (Art) assume that this means that you cannot call an 
// asynchronous client, like SUM_get(), and then call other clients while you wait for the asynchronous client
// to complete.
// If the current SUMS API function is an MT SUMS function, then clprev is NULL.
static CLIENT *clprev = NULL;
static int numopened = 0;
static int numSUM = 0;
static int taperdon_cleared = 0;
//KEY *infoparams;

int rr_random(int min, int max)
{
  return rand() % (max - min + 1) + min;
}


#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
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
#endif

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS

#define MSGLEN_NUMBYTES 8
#define MAX_MSG_BUFSIZE 4096
#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) ((a) < (b) ? (a) : (b))

/* There could be NULL chars in the message to send, so the caller must provide the message length in msgLen. */
static int sendMsg(SUM_t *sums, const char *msg, uint32_t msgLen, int (*history)(const char *fmt, ...))
{
    size_t bytesSentTotal;
    size_t bytesSent;
    char numBytesMesssage[MSGLEN_NUMBYTES + 1];
    const char *pBuf = NULL;
    int sockfd = -1;
    int err = 0;
    
    if (msg && *msg && sums)
    {
        /* Make a socket to MT Sums and connect to it. */
        sockfd = (int)sums->mSumsClient;
        
        if (sockfd != -1)
        {
            /* First send the length of the message, msgLen. */
            bytesSentTotal = 0;
            snprintf(numBytesMesssage, sizeof(numBytesMesssage), "%08x", msgLen);
        
            while (bytesSentTotal < MSGLEN_NUMBYTES)
            {
                pBuf = numBytesMesssage + bytesSentTotal;
                bytesSent = send(sockfd, pBuf, strlen(pBuf), 0);
            
                if (!bytesSent)
                {
                    (*history)("Socket to MT SUMS broken.\n");
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
                        (*history)("Socket to MT SUMS broken.\n");
                        err = 1;
                        break;
                    }
            
                    bytesSentTotal += bytesSent;
                }
            }
        }
        else
        {
            err = 1;
            (*history)("Not connected to MT SUMS.\n");
        }
    }
    else
    {
        (*history)("Invalid arguments to sendMsg().\n");
    }
    
    return err;
}

/* 0 - success
 * 1 - error
 * 2 - timeout
 */
static int receiveMsg(SUM_t *sums, char **out, size_t *outLen, int (*history)(const char *fmt, ...))
{
    size_t bytesReceivedTotal;
    ssize_t sizeTextRecvd;
    char recvBuffer[MAX_MSG_BUFSIZE];
    char numBytesMessage[MSGLEN_NUMBYTES + 1];
    unsigned int sizeMessage;
    char *allTextReceived = NULL;
    int sockfd = -1;
    struct timeval tv;
    fd_set readfds;
    int nReady;
    int err = 0;
    
    if (!out)
    {
        err = 1;
        (*history)("'out' must not be NULL in receiveMsg().\n");
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
        *recvBuffer = '\0';
        
        FD_ZERO(&readfds);
        FD_SET(sockfd, &readfds);
        
        while (bytesReceivedTotal < MSGLEN_NUMBYTES && bytesReceivedTotal < MAX_MSG_BUFSIZE)
        {
            tv.tv_sec = 10; /* timeout in 10 seconds. */
            tv.tv_usec = 0;
            nReady = select(sockfd + 1, &readfds, NULL, NULL, &tv);
            
            if (nReady == 0)
            {
                (*history)("Timeout receiving message-length data from sumsd.py.\n");
                err = 2;
                break;
            }
            
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
        *numBytesMessage = '\0';
        snprintf(numBytesMessage, sizeof(numBytesMessage), "%s", recvBuffer);
        sscanf(numBytesMessage, "%08x", &sizeMessage);
        
        allTextReceived = calloc(1, sizeMessage + 1);
        
        if (!allTextReceived)
        {
            (*history)("Out of memory in receiveMsg().\n");
            err = 1;
        }
        else
        {
            /* Receive the message. */
            bytesReceivedTotal = 0;
        
            *recvBuffer = '\0';
            while (bytesReceivedTotal < sizeMessage)
            {
                tv.tv_sec = 10; /* timeout in 10 seconds. */
                tv.tv_usec = 0;
                nReady = select(sockfd + 1, &readfds, NULL, NULL, &tv);
                
                if (nReady == 0)
                {
                    (*history)("Timeout receiving message data from sumsd.py.\n");
                    err = 2;
                    break;
                }

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

/* Returns json string in json. Handles Unicode, but UTF-8 only. So we can use strlen(json) to get the string length.
 *
 * Send:
 * {
 *    "pid": 12345, 
 *    "user": arta
 * }
 */
static int jsonizeClientInfo(SUM_t *sums, pid_t pid, const char *username, char **json, int (*history)(const char *fmt, ...))
{
    int err = 1;
    
    if (json)
    {
        cJSON *root = NULL;
        
        root = cJSON_CreateObject();
        if (!root)
        {
            (*history)("Out of memory calling cJSON_CreateObject().\n");
        }
        else
        {
            /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
            cJSON_AddNumberToObject(root, "pid", (double)pid);
            cJSON_AddStringToObject(root, "user", username);
        
            *json = cJSON_Print(root);
            
            if (*json)
            {
                err = 0;
            }
            
            cJSON_Delete(root);
        }
    }
    
    return err;
}

static MSUMSCLIENT_t ConnectToMtSums(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    MSUMSCLIENT_t msums = -1;
    int sockfd = -1;
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    char service[16];
    struct sockaddr_in server;
    
    if (sums && sums->mSumsClient == -1)
    {
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_flags = 0; /* A field to make this as complicated as possible. */
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0; /* Another field to make this as complicated as possible. */

        snprintf(service, sizeof(service), "%d", SUMSD_LISTENPORT);
        
        // We could override SUMSERVER with the same-named env var.
        if (getaddrinfo(SUMSERVER, service, &hints, &result) != 0)
        {
            (*history)("Unable to get SUMS server address.\n");
        }
        else
        {
            struct addrinfo *address = NULL;
            
            for (address = result; address != NULL; address = address->ai_next)
            {
                if ((sockfd = socket(address->ai_family, address->ai_socktype, address->ai_protocol)) == -1)
                {
                    (*history)("Unable to create socket to sumsd.py with address.\n");
                    continue;
                }
                else
                {
                    /* connect the socket to the server's address */
                    if (connect(sockfd, result->ai_addr, result->ai_addrlen) == -1)
                    {
                        (*history)("Unable to connect to SUMS server with address (errno = %d).\n", errno);
                        close(sockfd);
                        sockfd = -1;
                    }
                    else
                    {
                        msums = (MSUMSCLIENT_t)sockfd;
                        break;
                    }
                }
            }            
        }
        
        freeaddrinfo(result);
        
        /* Every time we connect to MT SUMS, we send the server client information. */
        if (msums != -1)
        {
            pid_t pid;
            struct passwd *pwd = NULL;
            char *json = NULL;
            
            /* Need to set mSumsClient for the sendMsg() call. */
            sums->mSumsClient = msums;
            
            pid = getpid();
            pwd = getpwuid(geteuid());
            
            if (jsonizeClientInfo(sums, pid, pwd->pw_name, &json, history))
            {
                (*history)("Unable to collect client info.\n");
                shutdown(msums, SHUT_RDWR);
                close(msums);
                msums = -1;
                sums->mSumsClient = -1;
            }
            else
            {
                /* Send json. */
                if (sendMsg(sums, json, strlen(json), history))
                {
                    shutdown(msums, SHUT_RDWR);
                    close(msums);
                    msums = -1;
                    sums->mSumsClient = -1;
                }
            }
            
            if (json)
            {
                free(json);
                json = NULL;
            } 
        }
    }
    else
    {
        (*history)("Unable to connect to MT SUMS.\n");
    }
        
    return sums->mSumsClient;
}

static void DisconnectFromMtSums(SUM_t *sums)
{
    if (sums && sums->mSumsClient != -1)
    {
        shutdown(sums->mSumsClient, SHUT_RDWR);
        close(sums->mSumsClient);
        sums->mSumsClient = -1;
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
 
 // db, the database HOST, is not USED. The SUMS server does not read this argument. It uses SUMS_DB_HOST
 // in SUMLIB_PgConnect.c.
 
 /* RPC and MT SUMS server SUM_open() */
SUM_t *sumsopenOpen(const char *server, const char *db, int (*history)(const char *fmt, ...))
{
  CLIENT *clopx = NULL;
  KEY *klist = NULL;
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
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
#else // sum_svc SUM_open() was called (branch above)
    numSUM = SUM_NUMSUM;
    clprev = NULL;
#endif
  
    if(numSUM > SUM_MAXNUMSUM) 
    {
        (*history)("**ERROR: #of sum_svc > SUM_MAXNUMSUM (%d)\n", SUM_MAXNUMSUM);
        (*history)("This is a fatal sum_svc configuration error\n");
        return(0);
    }

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
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
#endif // Not using MT SUMS SUM_open()

    if (!db)
    {
        if (!(db = getenv("SUMDB")))
        {
            char *dbInternal = alloca(sizeof(SUMS_DB_HOST)+1);
            strcpy(dbInternal, SUMS_DB_HOST);
            db = dbInternal;
        }
    }
    
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    klist = newkeylist();
    setkey_str(&klist, "db_name", db);
    setkey_str(&klist, "USER", username);
    /* get a unique id from sum_svc for this open */
    rr = rr_random(0, numSUM-1);
    
    switch(rr) 
    {
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
    
    // This makes the SUM_close() client the same as the SUM_open() client.
    // It sets clprev to this client as well.
    if((sumid = sumrpcopen_1(klist, clopx, history)) == 0) 
    {
        (*history)("Failed to get SUMID from sum_svc\n");
        clnt_destroy(cl);
        freekeylist(&klist);
        return(NULL);
    }
    
    sumptr = (SUM_t *)calloc(1, sizeof(SUM_t)); //NULL filled
    
#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
    sumptr->mSumsClient = msums;
#endif
#else
    // MT SUMS SUM_open() - returns sumid.
    SUM_t *sums = NULL;
    int err;
    
    err = ((sums = (SUM_t *)calloc(1, sizeof(SUM_t))) == NULL); //NULL filled
    
    if (err)
    {
        (*history)("Cannot alloc SUM_t - out of memory.\n");
    }
    else
    {
        sums->mSumsClient = msums;

        err = callMTSums(sums, kMTSums_CallType_Open, NULL, history);
    }
    
    if (err)
    {
        (*history)("Cannot connect to MT SUMS.\n");
        return NULL;
    }
    
    /* sums->uid now contains sumsid. */
    sumid = sums->uid;
    sumptr = sums;
    clprev = NULL;
#endif
    
    numopened++;

    /* Save the OPEN client used to open this SUMS connection. It must be used to closed the SUMS connection.
     * If we are using the MT SUMS server exclusively, then clopx == NULL.
     */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clclose = clopx;
#endif
    sumptr->sinfo = NULL;
  
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->cl = cl; // This is the client that is connected to sum_svc.
#endif

for(j=0; j < numSUM; j++) {
  switch(j) {
  case 0:
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen = cl;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
    sumptr->clalloc = cl;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
    sumptr->clget = cl;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
    sumptr->clput = cl;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
    sumptr->clinfo = cl;
#endif
    break;
  case 1:	//this is the case w/e.g. Salloc and Salloc1
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen = clopen;
    sumptr->clopen1 = clopen1;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen2 = clopen2;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen3 = clopen3;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen4 = clopen4;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen5 = clopen5;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen6 = clopen6;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->clopen7 = clopen7;
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_DELETESUS) || !SUMS_USEMTSUMS_DELETESUS)
    if(numSUM == 1) 
    {
        cldelser = cl; // Same as client for SUM_open() and SUM_close()
    }
    else 
    {
        cldelser = clnt_create(server_name, SUMDELSER, SUMDELSERV, "tcp");
        if(!cldelser) 
        {
            clnt_pcreateerror("Can't get client handle to sum_svc SUMDELSER");
            (*history)("sum_svc error on handle to SUMDELSER on %s\n", server_name);
            sleep(1);
            cldelser = clnt_create(server_name, SUMDELSER, SUMDELSERV, "tcp");
            if(!cldelser) 
            { 
              clnt_pcreateerror("Can't get client handle to sum_svc SUMDELSER");
              (*history)("sum_svc error on handle to SUMDELSER on %s\n", server_name);
              return(NULL);
            }
        }
    }

    sumptr->cldelser = cldelser;
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
    sumptr->uid = sumid;
#endif
  sumptr->username = username;
  sumptr->tdays = 0;
  sumptr->debugflg = 0;		/* default debug off */
  sumptr->storeset = JSOC;	/* default storage set */
  sumptr->numSUM = numSUM;	/* # of sum servers available */
  sumptr->dsname = "<none>";
  sumptr->history_comment = NULL;
    /* always alloc these two, just in case we are performing some RPC SUMS call that needs them; for MT SUMS calls that use
     * these, the memory will first be deallocated, then*/
    sumptr->dsix_ptr = (uint64_t *)malloc(sizeof(uint64_t) * SUMARRAYSZ);
    sumptr->wd = (char **)calloc(SUMARRAYSZ, sizeof(char *));

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
  setsumopened(&sumopened_hdr, sumid, sumptr, username); //put in open list
#endif
  if (klist)
  {
      freekeylist(&klist);
      klist = NULL;
  }
  return(sumptr);
}

/* RPC and MT SUMS SUM_close()/SUMS SUM_rollback() */
/* Close this session with the SUMS. Return non 0 on error.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
int sumsopenClose(SUM_t *sums, int (*history)(const char *fmt, ...))
#else
int sumsopenClose(SUM_t *sums, MTSums_CallType_t callType, int (*history)(const char *fmt, ...))
#endif
{
  KEY *klist = NULL;
  char *call_err;
  static char res;
  enum clnt_stat status;
  int i, stat;
  int errflg = 0;

  if(sums->debugflg) {
    (*history)("SUM_close() call: uid = %lu\n", sums->uid);
  }

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sums->uid); 
  setkey_int(&klist, "DEBUGFLG", sums->debugflg);
  setkey_int(&klist, "REQCODE", CLOSEDO);
  setkey_str(&klist, "USER", sums->username);

  if(sums->mode & TAPERDON) {
    if(!taperdon_cleared) {		//tape rd is still active. Notify sum_svc
      setkey_int(&klist, "TAPERDACTIVE", 1);
    }
  }
  bzero((char *)&res, sizeof(res));
  //Use the same process that we opened with
  clprev = sums->clclose;
  status = clnt_call(sums->clclose, CLOSEDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_void, &res, TIMEOUT);

/* NOTE: These rtes seem to return after the reply has been received despite
 * the timeout value. If it did take longer than the timeout then the timeout
 * error status is set but it should be ignored.
*/
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sums->clopen, "Err clnt_call for CLOSEDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      errflg = 1;
    }
  }

  stat = getmsgimmed();		//clean up pending response

  (void)pmap_unset(RESPPROG, sums->uid); /* unreg response server */
  remsumopened(&sumopened_hdr, sums->uid); /* rem from linked list */
  
    /* RPC SUM_close() */
#else 
    /* MT SUM_close() or SUM_rollback() */
    int err;
    
    /* jsonize request */
    err = callMTSums(sums, callType, (JSONIZER_DATA_t *)NULL, history);
    
    errflg = (err == 1);
    
    if (gMTCallMap)
    {
        hcon_destroy(&gMTCallMap);
    }
#endif /* MT SUM_close() */

  if(numSUM == 1) {
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
    clnt_destroy(sums->cl);	//don't close the same connec more than once
#endif
  }
  else {
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
  clnt_destroy(sums->cl);	/* destroy handle to sum_svc */
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
  if(sums->clopen) clnt_destroy(sums->clopen);
  if(sums->clopen1) clnt_destroy(sums->clopen1);
  if(sums->clopen2) clnt_destroy(sums->clopen2);
  if(sums->clopen3) clnt_destroy(sums->clopen3);
  if(sums->clopen4) clnt_destroy(sums->clopen4);
  if(sums->clopen5) clnt_destroy(sums->clopen5);
  if(sums->clopen6) clnt_destroy(sums->clopen6);
  if(sums->clopen7) clnt_destroy(sums->clopen7);
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
  if(sums->clalloc) clnt_destroy(sums->clalloc);
  if(sums->clalloc1) clnt_destroy(sums->clalloc1);
  if(sums->clalloc2) clnt_destroy(sums->clalloc2);
  if(sums->clalloc3) clnt_destroy(sums->clalloc3);
  if(sums->clalloc4) clnt_destroy(sums->clalloc4);
  if(sums->clalloc5) clnt_destroy(sums->clalloc5);
  if(sums->clalloc6) clnt_destroy(sums->clalloc6);
  if(sums->clalloc7) clnt_destroy(sums->clalloc7);
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
  if(sums->clget) clnt_destroy(sums->clget);
  if(sums->clget1) clnt_destroy(sums->clget1);
  if(sums->clget2) clnt_destroy(sums->clget2);
  if(sums->clget3) clnt_destroy(sums->clget3);
  if(sums->clget4) clnt_destroy(sums->clget4);
  if(sums->clget5) clnt_destroy(sums->clget5);
  if(sums->clget6) clnt_destroy(sums->clget6);
  if(sums->clget7) clnt_destroy(sums->clget7);
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
  if(sums->clput) clnt_destroy(sums->clput);
  if(sums->clput1) clnt_destroy(sums->clput1);
  if(sums->clput2) clnt_destroy(sums->clput2);
  if(sums->clput3) clnt_destroy(sums->clput3);
  if(sums->clput4) clnt_destroy(sums->clput4);
  if(sums->clput5) clnt_destroy(sums->clput5);
  if(sums->clput6) clnt_destroy(sums->clput6);
  if(sums->clput7) clnt_destroy(sums->clput7);
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
  if(sums->clinfo) clnt_destroy(sums->clinfo);
  if(sums->clinfo1) clnt_destroy(sums->clinfo1);
  if(sums->clinfo2) clnt_destroy(sums->clinfo2);
  if(sums->clinfo3) clnt_destroy(sums->clinfo3);
  if(sums->clinfo4) clnt_destroy(sums->clinfo4);
  if(sums->clinfo5) clnt_destroy(sums->clinfo5);
  if(sums->clinfo6) clnt_destroy(sums->clinfo6);
  if(sums->clinfo7) clnt_destroy(sums->clinfo7);
#endif
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_DELETESUS) || !SUMS_USEMTSUMS_DELETESUS)
  if(sums->cldelser) clnt_destroy(sums->cldelser);
#endif
  }
  
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
  for(i=0; i < MAXSUMOPEN; i++) {
    if(transpid[i] == sums->uid) {
      svc_destroy(transp[i]);
      --numopened;
      break;
    }
  }
#endif

    if (sums->dsix_ptr)
    {
        free(sums->dsix_ptr);
        sums->dsix_ptr = NULL;
    }
    
    if (sums->wd)
    {
        free(sums->wd);
        sums->wd = NULL;
    }
  
  if(sums->sinfo) free(sums->sinfo);
  free(sums);
  if (klist)
  {
      freekeylist(&klist);
      klist = NULL;
  }
  if(errflg) return(4);
  return(0);
}

/* SUMS services that cannot be configured by the client. The RPC versions of SUM_nop() and SUM_poll() must be*/
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
/* See if sum_svc is still alive. Return 0 if ok, 1 on timeout,
 * 4 on error (like unable to connect, i.e. the sum_svc is gone),
 * 5 tape_svc is gone (new 03Mar2011).
 * Calls the sums process of the last api call that was made.
*/

/* SUM_nop() pings the sum_svc server. If the clnt_call() does not error-out, then the sum_svc server is considered to be
 * functioning. From what I (Art) can tell, sum_svc does not actually check to see if tape_svc is gone. So I doubt that
 * SUM_nop() can return code 5.
 *
 * It pings the sum_svc server identified by clprev, which is supposed to be the last sum_svc server used for a SUMS request.
 * This means that you cannot make any other SUMS requests after you have made a SUMS requests and before you call SUM_nop().
 */
static int sumsopenNopRPC(SUM_t *sum, int (*history)(const char *fmt, ...))
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


/* Check if the response for a  previous request is complete.
 * Return 0 = msg complete, the sum has been updated
 * TIMEOUTMSG = msg still pending, try again later
 * ERRMESS = fatal error (!!TBD find out what you can do if this happens)
 * NOTE: Upon msg complete return, sum->status != 0 if error anywhere in the 
 * path of the request that initially returned the RESULT_PEND status.
*/

/* We basically check to see if sum_svc has finished our request by polling to see
 * if it, in its RPC-client capacity, has sent results to OUR RPC-client request. So, sum_svc
 * sees our request, processes it, then makes an RPC request back to us (we act as a server
 * with the RESPPROG service) to give us results. 
 *
 * As far as I (Art) can tell, this means that you can never make any other requests to sum_svc
 * while you are polling with SUM_poll() for a previous request. Otherwise, the result
 * RPC-client requests from sum_svc could get mixed up. In other words, SUM_poll() is completely
 * useless for DRMS because you can't do anything else, SUMS-wise, while you are waiting for 
 * your SUMS request to complete. 
 */
static int sumsopenPollRPC(SUM_t *sum)
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
#endif

#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
/* noop return values:
 *   0 - SUMS running
 *   1 - timeout
 *   4 - SUMS server error (maybe SUMS MT is not running)
 *   5 - cannot talk to tape service - not currently supported.
 */
static int sumsopenNopMT(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    int err;
    
    /* callMTSums rv: 0 - successful call (SUMS running), 1 - internal error, 2 - timeout */
    err = callMTSums(sums, kMTSums_CallType_Ping, NULL, history);
    
    if (err == 0)
    {
        return 0;
    }
    else if (err == 1)
    {
        return 4;
    }
    else if (err == 2)
    {
        return 1;
    }
    else
    {
        (*history)("Unknown SUMS return code %d.\n", err);
        return 4;
    }
}

/* Return values:
 *   0 - SUMS call complete.
 *   3 (TIMEOUTMSG) - The underlying select() call timed-out, so this call timed-out too. This means, SUMS is still staging one or more tapes!
 *   4 (ERRMSS) - The underlying select() call failed, so this call failed as well.
 */
static int sumsopenPollMT(SUM_t *sums)
{
    int err;
    
    /* callMTSums rv: 0 - successful call (SUMS running), 1 - internal error, 2 - timeout */
    err = callMTSums(sums, kMTSums_CallType_Poll, NULL, printkerr);
    
    if (err == 0)
    {
        /* SUMS call complete (tape read complete, if a tape read happened). */
        return 0;
    }
    else if (err == 1)
    {
        /* Internal error */
        return ERRMESS;
    }
    else if (err == 2)
    {
        /* Timeout talking to MT SUMS. */
        return TIMEOUTMSG;
    }
    else if (err == RESULT_PEND)
    {
        /* SUM_get() returns this if a tape read is pending. */
        return TIMEOUTMSG;
    }
    else
    {
        printkerr("Unknown SUMS return code %d.\n", err);
        return ERRMESS;
    }
}
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
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
    
    /* I think we are actually setting up a karfin' SVC server here! It looks like we do this to receive result messages 
     * from the SUMS SVC servers (sum_svc, Sopen, ...). The SUMS SVC servers make client calls to this RESPPROG SVC server
     * to send results back to the DRMS program. The getanymsg() function reads all RPC requests from the SUMS SVC servers
     * (who make requests with clnt_call(..., RESPDOARRAY, ...) and clnt_call(..., RESPDO, ...). getanymsg() calls
     * svc_getreqset(), which calls respd()
     */
    if (!svc_register(xtp, RESPPROG, suidback, respd, IPPROTO_TCP)) {
      (*history)("unable to register RESPPROG in sumrpcopen_1()\n");
      return(0);
    }
    transp[numopened] = xtp;
    transpid[numopened] = suidback;
  }
  return (suidback);
}
#endif /* RPC SUM_open() */


#if defined(SUMS_USEMTSUMS) && SUMS_USEMTSUMS
static cJSON *unjsonizeResponseParse(const char *msg, int (*history)(const char *fmt, ...))
{
    cJSON *response = NULL;

    response = cJSON_Parse(msg);
    
    if (!response)
    {
        (*history)("The SUMINFO response is not valid JSON.");
    }
    
    return response;
}

static int unjsonizeResponseCheckStatus(cJSON *response, int (*history)(const char *fmt, ...))
{
    int err = 0;
    cJSON *value = NULL;
    
    err = ((value = cJSON_GetObjectItem(response, "status")) == NULL);
    if (!err)
    {
        err = (value->type != cJSON_String);
    }
        
    if (err)
    {
        (*history)("Invalid 'status' attribute.\n");
    }
    else
    {
        err = (strcasecmp(value->valuestring, "ok") != 0);
        if (err)
        {
            (*history)("The SUMS service failed with status %s.\n", value->valuestring);
            
            if ((value = cJSON_GetObjectItem(response, "errmsg")) != NULL)
            {
                if (value->type == cJSON_String)
                {
                    (*history)("Error message: %s.\n", value->valuestring);
                }
            }
        }
    }
    
    return err;
}

/* 0 ==> not alive
 * 1 ==> error
 * 2 ==> alive
 */
static int unjsonizeResponseCheckIsAlive(cJSON *response, int (*history)(const char *fmt, ...))
{
    int isAlive = 0;
    cJSON *value = NULL;
    
    value = cJSON_GetObjectItem(response, IS_ALIVE);
    
    if (value)
    {
        if (value->type == cJSON_True)
        {
            isAlive = 2;
        }
        else if (value->type == cJSON_False) 
        {
            isAlive = 0;        
        }
        else
        {
            (*history)("unexpected data type for isAlive element\n");
            isAlive = 1;
        }
    }
    
    return isAlive;
}

#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
/* The request JSON looks like this:
 * {
 *    "reqtype" : "open"
 * }
 */
 
 /* Ignore server and db. */
static int jsonizeSumopenRequest(char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    int err;
    
    root = NULL;
    err = 0;

    if (!json)
    {
        (*history)("Invalid argument(s) to 'jsonizeSumopenRequest'.\n");
        err = 1;
    }
    
    if (!err)
    {
        err = ((root = cJSON_CreateObject()) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateObject().\n");
        }
    }
    
    if (!err)
    {   
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        err = ((jsonValue = cJSON_CreateString("open")) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateString().\n");
        }
    }
    
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
    }
    
    if (!err)
    {   
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON in this format.
 *
 * In the SUMS DB, sumsid is a 64-bit signed integer. Since the SUMS daemon is a Python 3
 * script, the sumsid remains a 64-bit signed integer. However, JSON parsers do not
 * necessarily support 64-bit integers. In particular, the one used by DRMS, cJSON, does not -
 * all number strings, whether they represent integer or floating point numbers, are
 * stored in a C double variable, which has 53 bits of precision, not the needed 64 bits.
 * To cope with this, we must pass 64-bit integers as strings from SUMS to DRMS via JSON.
 * The current SUMS-DRMS implementation uses hexadecimal strings to represent all 64-bit 
 * integers.
 *
 * Now, the data type chosen for the sumsid was an unsigned 32-bit integer (unfortunately).
 * It seems like we will never have more than 2^32 - 1 SUMS session, but that isn't guaranteed.
 * So, the definition of SUMID_t is incorrect, and it should be a signed 64-bit number, although
 * an unsigned 64-bit number would work too, as long as the sign is checked in the server and/or
 * client.
 *
 * ART - the data type of SUMID_t should be changed.
 

sumsid is a 32-bit number, so it is passed as a JSON number):
 * {
 *    "status" : "ok",
 *    "sessionid" : "1AE2FC"
 * }
 *
 * We need to assign sessionid to sums->uid.
 */
static int unjsonizeSumopenResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    cJSON *response = NULL;
    cJSON *jsonValue = NULL;
    SUMID_t sumsid;
    int err;
    
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (!err)
    {
        err = ((jsonValue = cJSON_GetObjectItem(response, "sessionid")) == NULL);
        if (err)
        {
            (*history)("Unable to unjsonize sessionid.\n");
        }
    }

    if (!err)
    {
        err = (jsonValue->type != cJSON_String);
        if (err)
        {
            (*history)("Unexpected data type for sessionid.");
        }
    }

    if (!err)
    {
        /* Convert a hex string to an unsigned 32-bit integer. Again, this is not 100% correct, but
         * it is unlikely that the server will ever return anything but a string that 
         * represents an unsigned 32-bit number. */

        /* %llx converts a unsigned hex string to an unsigned 64-bit integer */
        uint64_t uid;

        err = (sscanf(jsonValue->valuestring, "%llx", &uid) != 1);
        
        /* uid may not be a 32-bit number */
        sums->uid = (uint32_t)uid;
    }
  
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}

/* The request JSON looks like this:
 * {
 *    "reqtype" : "close",
 *    "sessionid" : "1AE2FC"
 * }
 */
static int jsonizeSumcloseRequest(SUM_t *sums, SUMID_t sessionid, char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    int err;
    char numBuf[64];

    root = NULL;    
    err = (json == NULL);

    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeSumcloseRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateObject().\n");
        }
    }
    
    if (!err)
    {   
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        err = ((jsonValue = cJSON_CreateString("close")) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateString().\n");
        }
    }
        
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
    }
    
    if (!err)
    {
        /* since SUMS is a Python 3 script, its JSON parser supports 64-bit numbers; ultimately, 
         * sessionid will be a 64-bit number (if the SUMID_t definition is fixed and made a 
         * 64-bit number); however, the cJSON_CreateNumber() does not support 64-bit integers, since
         * a 64-bit argument is cast to a double; so, we have to send the session ID as a string
         * (and we choose a hex string) */
         
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
    }
        
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
    }
    
    if (!err)
    {   
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON in this format:
 * {
 *    "status" : "ok"
 * }
 */

static int unjsonizeSumcloseResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    /* There really isn't anything to return, but we need to know if the server succeeded, so we should return a 
     * status value. Unlike requests that read only, SUM_close() has to write to the db, so it could fail.
     */
    int err;
    cJSON *response = NULL;
    
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);

    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}

/* The request JSON looks like this:
 * {
 *    "reqtype" : "rollback",
 *    "sessionid" : "1AE2FC"
 * }
 */
static int jsonizeSumrollbackRequest(SUM_t *sums, SUMID_t sessionid, char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    int err;
    char numBuf[64];

    root = NULL;    
    err = (json == NULL);

    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeSumrollbackRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateObject().\n");
        }
    }
    
    if (!err)
    {   
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        err = ((jsonValue = cJSON_CreateString("rollback")) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateString().\n");
        }
    }
        
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
    }
    
    if (!err)
    {
        /* since SUMS is a Python 3 script, its JSON parser supports 64-bit numbers; ultimately, 
         * sessionid will be a 64-bit number (if the SUMID_t definition is fixed and made a 
         * 64-bit number); however, the cJSON_CreateNumber() does not support 64-bit integers, since
         * a 64-bit argument is cast to a double; so, we have to send the session ID as a string
         * (and we choose a hex string) */
         
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
    }
        
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
    }
    
    if (!err)
    {   
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON in this format:
 * {
 *    "status" : "ok"
 * }
 */
static int unjsonizeSumrollbackResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    /* There really isn't anything to return, but it would be good to know if the server succeeded, so we should return a 
     * status value. 
     */
    int err;
    cJSON *response = NULL;
    
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);

    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;     
}
#endif

#if defined(SUMS_USEMTSUMS_INFO) && SUMS_USEMTSUMS_INFO
/* The request JSON looks like this (since JSON does not support 64-bit numbers, send SUNUMs as hexadecimal strings):
 *   {
 *      "reqtype" : "info",
 *      "sessionid" : "1AE2FC",
 *      "sus" : ["1DE2D412", "1AA72414"]
 *   }
 */
static int jsonizeSuminfoRequest(SUM_t *sums, SUMID_t sessionid, uint64_t *sunums, size_t nSus, char **json, int (*history)(const char *fmt, ...))
{
    int isu;
    uint64_t sunum;
    char numBuf[64];
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    cJSON *jsonArrayElement = NULL;
    int err;
    
    err = (!sums || !sunums || nSus == 0 || !json);
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }    
    else
    {
        err = ((jsonValue = cJSON_CreateString("info")) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
        
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);        
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        err = ((jsonValue = cJSON_CreateArray()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateArray().\n");
    }
    else
    {
        for (isu = 0; isu < nSus; isu++)
        {
            sunum = sunums[isu];
        
            /* PyList_SET_ITEM steals the reference to the Py object provided in its third 
             * argument. To free the memory allocated in the Py environment for the 
             * list reference and all the references in the list, you simply have to
             * decrement the reference on the list (and not the items in the list). */
            snprintf(numBuf, sizeof(numBuf), "%llx", sunum); /* No padding, no minimum string length. */
            
            err = ((jsonArrayElement = cJSON_CreateString(numBuf)) == NULL);
            if (err)
            {
                (*history)("Out of memory calling cJSON_CreateString().\n");
                break;
            }
            
            cJSON_AddItemToArray(jsonValue, jsonArrayElement);
        }
    }
    
    if (!err)
    {   
        /* Add the SUNUM array to the root object. */
        cJSON_AddItemToObjectCS(root, "sus", jsonValue);
        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON in this format:
 * {
 *    "status" : "ok",
 *    "suinfo" : 
 *     [
 *        {
 *          "sunum" : "2B13493A",
 *          "onlineLoc" : "/SUM50/D722684218",
 *          "onlineStatus" : "Y",
 *          "archiveStatus" : "N",
 *          "offsiteAck" : "N",
 *          "historyComment" : null,
 *          "owningSeries" : "hmi.M_45s",
 *          "storageGroup" : 1001,
 *          "bytes" : "F0D115A",
 *          "creatDate" : "2015-07-20 13:43:27",
 *          "username" : "jsocprod",
 *          "archTape" : null,
 *          "archTapeFn" : null,
 *          "archTapeDate" : null,
 *          "safeTape" : null,
 *          "safeTapeFn" : null,
 *          "safeTapeDate" : null,
 *          "effectiveDate" : "204212051243",
 *          "paStatus" : 4,
 *          "paSubstatus" : 128
 *        },
 *        {
 *          "sunum" : "392FA",
 *          "onlineLoc" : "/SUM5/D2608803/D234234",
 *          "onlineStatus" : "N",
 *          "archiveStatus" : "Y",
 *          "offsiteAck" : "N",
 *          "historyComment" : null,
 *          "owningSeries" : "ds_mdi.fd_M_01h_lev1_8",
 *          "storageGroup" : 1,
 *          "bytes" : "227FF5",
 *          "creatDate" : "2006-12-05 10:46:31",
 *          "username" : "jeneen",
 *          "archTape" : "012762L4",
 *          "archTapeFn" : 236,
 *          "archTapeDate" : "2008-06-04 12:58:44",
 *          "safeTape" : null,
 *          "safeTapeFn" : null,
 *          "safeTapeDate" : null,
 *          "effectiveDate" : null,
 *          "paStatus" : null,
 *          "paSubstatus" : null
 *        },
 *        ...
 *     ]
 * }
 */
static int unjsonizeSuminfoResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err;
    cJSON *response = NULL;
    cJSON *jsonArray = NULL;
    cJSON *jsonArrayElement = NULL;
    cJSON *jsonValue = NULL;
    SUM_info_t *elem = NULL;
    int64_t numBytes;
    int nElems;
    int iElem;
        
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }

    if (!err)
    {
        err = ((jsonArray = cJSON_GetObjectItem(response, "suinfo")) == NULL);
        if (err)
        {
            (*history)("Invalid 'suinfo' attribute.\n");
        }
    }

    if (!err)
    {
        nElems = cJSON_GetArraySize(jsonArray);
        sums->sinfo = (SUM_info_t *)calloc(nElems, sizeof(SUM_info_t));
        
        err = (sums->sinfo == NULL);

        if (err)
        {
            (*history)("Out of memory calling calloc().\n");
        }
        else
        {
            for (iElem = 0; iElem < nElems; iElem++)
            {
                elem = &(sums->sinfo[iElem]);
                jsonArrayElement = cJSON_GetArrayItem(jsonArray, iElem);
                
                if (iElem < nElems - 1)
                {
                    elem->next = sums->sinfo + iElem + 1;
                }
                
                /* sunum */
                err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "sunum")) == NULL) || jsonValue->type != cJSON_String);

                if (err)
                {
                    (*history)("Unable to unjsonize sunum.\n");
                    break;
                }                
                else
                {
                    /* Convert hexadecimal string to 64-bit number. */
                    err = (sscanf(jsonValue->valuestring, "%llx", &(elem->sunum)) != 1);
                }
                
                if (err)
                {
                    (*history)("Unable to parse sunum.\n");
                    break;
                }
                else
                {
                    /* online_loc */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "onlineLoc")) == NULL) ||jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize onlineLoc.\n");
                    break;
                }
                else
                {
                    snprintf(elem->online_loc, sizeof(elem->online_loc), "%s", jsonValue->valuestring);

                    /* online_status */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "onlineStatus")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize onlineStatus.\n");
                    break;
                }
                else
                {
                    snprintf(elem->online_status, sizeof(elem->online_status), "%s", jsonValue->valuestring);

                    /* archive_status */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "archiveStatus")) == NULL) || jsonValue->type != cJSON_String);
                    
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize archiveStatus.\n");
                    break;
                }
                else
                {
                    snprintf(elem->archive_status, sizeof(elem->archive_status), "%s",jsonValue->valuestring);

                    /* offsite_ack */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "offsiteAck")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize offsiteAck.\n");
                    break;
                }
                else
                {
                    snprintf(elem->offsite_ack, sizeof(elem->offsite_ack), "%s", jsonValue->valuestring);

                    /* history_comment */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "historyComment")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize historyComment.\n");
                    break;
                }
                else
                {
                    snprintf(elem->history_comment, sizeof(elem->history_comment), "%s", jsonValue->valuestring);
            
                    /* owning_series */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "owningSeries")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize owningSeries.\n");
                    break;
                }
                else
                {
                    snprintf(elem->owning_series, sizeof(elem->owning_series), "%s", jsonValue->valuestring);

                    /* storage_group */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "storageGroup")) == NULL) || jsonValue->type != cJSON_Number);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize storageGroup.\n");
                    break;
                }
                else
                {
                    elem->storage_group = jsonValue->valueint;
                    
                    /* bytes */
                    /* bytes is a double in SUM_info_t, but it is a 64-bit integer in sum_main (and is reported as a 64-bit integer by sumsd.py). 
                     * But because JSON-parsers do not typically support 64-bit integers, convert to hexadecimal string. */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "bytes")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize bytes.\n");
                    break;
                }
                else
                {
                    /* Convert hexadecimal string to 64-bit number. */
                    err = (sscanf(jsonValue->valuestring, "%llx", &numBytes) != 1);
                }
                
                if (err)
                {
                    (*history)("Unable to parse bytes string.");
                    break;
                }
                else
                {
                    /* WARNING: a loss of precision can result here. */
                    elem->bytes = (double)numBytes;
                    
                    /* Skip createSumid. Yay! This is another 64-bit integer in sum_main. */

                    /* creatDate */                    
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "creatDate")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize creatDate.\n");
                    break;
                }
                else
                {
                    snprintf(elem->creat_date, sizeof(elem->creat_date), "%s", jsonValue->valuestring);
                
                    /* username */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "username")) == NULL) || jsonValue->type != cJSON_String);
                }
                
                if (err)
                {
                    (*history)("Unable to unjsonize username.\n");
                    break;
                }
                else
                {
                    snprintf(elem->username, sizeof(elem->username), "%s", jsonValue->valuestring);

                    /* arch_tape */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "archTape")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize archTape.\n");
                    break;
                }
                else
                {
                    snprintf(elem->arch_tape, sizeof(elem->arch_tape), "%s", jsonValue->valuestring);            

                    /* arch_tape_fn */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "archTapeFn")) == NULL) || jsonValue->type != cJSON_Number);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize archTapeFn.\n");
                    break;
                }
                else
                {
                    elem->arch_tape_fn = jsonValue->valueint;

                    /* arch_tape_date */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "archTapeDate")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize archTapeDate.\n");
                    break;
                }
                else
                {
                    snprintf(elem->arch_tape_date, sizeof(elem->arch_tape_date), "%s", jsonValue->valuestring);

                    /* safe_tape */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "safeTape")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize safeTape.\n");
                    break;
                }
                else
                {
                    snprintf(elem->safe_tape, sizeof(elem->safe_tape), "%s", jsonValue->valuestring);

                    /* safe_tape_fn */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "safeTapeFn")) == NULL) || jsonValue->type != cJSON_Number);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize safeTapeFn.\n");
                    break;
                }
                else
                {
                    elem->safe_tape_fn = jsonValue->valueint;

                    /* safe_tape_date */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "safeTapeDate")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize safeTapeDate.\n");
                    break;
                }
                else
                {
                    snprintf(elem->safe_tape_date, sizeof(elem->safe_tape_date), "%s", jsonValue->valuestring);

                    /* effective_date */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "effectiveDate")) == NULL) || jsonValue->type != cJSON_String);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize effectiveDate.\n");
                    break;
                }
                else
                {
                    snprintf(elem->effective_date, sizeof(elem->effective_date), "%s", jsonValue->valuestring);

                    /* pa_status */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "paStatus")) == NULL) || jsonValue->type != cJSON_Number);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize paStatus.\n");
                    break;
                }
                else
                {
                    elem->pa_status = jsonValue->valueint;           

                    /* pa_substatus */
                    err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "paSubstatus")) == NULL) || jsonValue->type != cJSON_Number);
                }

                if (err)
                {
                    (*history)("Unable to unjsonize paSubstatus.\n");
                    break;
                }
                else
                {
                    elem->pa_substatus = jsonValue->valueint;
                }
            }
        }
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}

#endif

#if defined(SUMS_USEMTSUMS_GET) && SUMS_USEMTSUMS_GET
/* The request JSON looks like this (since JSON does not support 64-bit numbers, send SUNUMs as hexadecimal strings):
 *   {
 *      "reqtype" : "get",
 *      "sessionid" : "1AE2FC",
 *      "touch" : true,
 *      "retrieve" : false,
 *      "retention" : 60,
 *      "sus" : ["1DE2D412", "1AA72414"]
 *   }
 *
 * For the RPC SUMS, if mode | RETRIEVE and the SUNUM is a valid SUNUM and the SU is offline, then a request would be sent to the tape
 * system. This is true for ANY DRMS, which is not correct. The MT SUMS code should check the SUMS_TAPE_AVAILABLE DRMS parameter and
 * if it is set to 0, and the RETRIEVE mode flag is set, then an error should be returned. If SUMS_TAPE_AVAILABLE is set to 1, then
 * the MT SUMS code should print a test message saying that the SU is being retrieved (although it is not). The MT SUMS server will
 * spawn a thread that will eventually handle an asynchronous interaction with the tape system. For now, the thread will simply log 
 * "waiting for the tape system request to complete" several times. Then it will terminate. The client, which will call SUM_poll() in 
 * a loop, will get a response code to indicate that this tape thread has terminated, and it will get some dummy path for the 
 * previously offline SUs. For all online SUs in the SUM_get() request, the client will receive real SU paths.
 *
 * If the requested SUs are all online, then this call is synchronous (the response from the MT SUMS server contains the SU paths
 * for all the requested SUs), otherwise it is asynchronous (the caller must poll the MT SUMS server, providing it a request ID). In 
 * the latter case, until the SUs are ALL online, the server returns the request ID and a code indicating the request is being processed.
 * When ALL SUs are online, the server returns a code indicating that the request is complete, and it returns the SU paths for
 * all requested SUs.
 */
static int jsonizeSumgetRequest(SUM_t *sums, SUMID_t sessionid, int touch, int retrieve, int retention, uint64_t *sunums, size_t nSus, char **json, int (*history)(const char *fmt, ...))
{
    int isu;
    uint64_t sunum;
    char numBuf[64];
    cJSON *root = NULL;
    cJSON *jsonArray = NULL;
    cJSON *jsonArrayElement = NULL;
    cJSON *jsonValue = NULL;
    int err;
    
    err = (!sums || !sunums || nSus == 0 || !json);
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }
    else
    {
        err = ((jsonValue = cJSON_CreateString("get")) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
        
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        
        err = ((jsonValue = cJSON_CreateBool(touch)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateBool().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "touch", jsonValue);
    }
    
    if (!err)
    {
        err = ((jsonValue = cJSON_CreateBool(retrieve)) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateBool().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "retrieve", jsonValue);
    }
    
    if (!err)
    {
        err = ((jsonValue = cJSON_CreateNumber((double)retention)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {        
        cJSON_AddItemToObjectCS(root, "retention", jsonValue);
    }
    
    if (!err)
    {
        err = ((jsonArray = cJSON_CreateArray()) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateArray().\n");
    }
    else
    {
        for (isu = 0; isu < nSus; isu++)
        {
            sunum = sunums[isu];
        
            /* PyList_SET_ITEM steals the reference to the Py object provided in its third 
             * argument. To free the memory allocated in the Py environment for the 
             * list reference and all the references in the list, you simply have to
             * decrement the reference on the list (and not the items in the list). */
            snprintf(numBuf, sizeof(numBuf), "%llx", sunum); /* No padding, no minimum string length. */
            
            err = ((jsonArrayElement = cJSON_CreateString(numBuf)) == NULL);
            if (err)
            {
                (*history)("Out of memory calling cJSON_CreateString().\n");
                break;
            }
            else
            {
                cJSON_AddItemToArray(jsonArray, jsonArrayElement);
            }
        }
    }
    
    if (!err)
    {   
        /* Add the SUNUM array to the root object. */
        cJSON_AddItemToObjectCS(root, "sus", jsonArray);
        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON in this format:
 * {
 *    "status" : "ok",
 *    "supaths" : 
 *     [
 *        {
 *          "sunum" : "2B13493A",
 *          "path" : "/SUM19/D854870270/S00027"
 *        },
 *        {
 *          "sunum" : "18E34EA7",
 *          "path" : "/SUM7/D854871818/S00009"
 *        },
 *        {
 *          "sunum" : "2E6945AB",
 *          "path" " null         # An invalid/unknown SU.
 *        }
 *     ]
 *
 * OR, if a tape read occurs:
 * {
 *    "status" : "ok",
 *    "taperead-requestid" : "123e4567-e89b-12d3-a456-426655440000"
 * }
 *
 * We need to assign the paths to sums->wd.
 */
static int unjsonizeSumgetResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err;
    cJSON *response = NULL;
    cJSON *jsonArray = NULL;
    cJSON *jsonArrayElement = NULL;
    cJSON *jsonValue = NULL;
    SUM_info_t *elem = NULL;
    int doingTapeRead = 0;
    int64_t numBytes;
    int nElems;
    int iElem;
        
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (!err)
    {
        jsonValue = cJSON_GetObjectItem(response, "taperead-requestid");
        if (jsonValue)
        {
            doingTapeRead = 1;
            err = (jsonValue->type != cJSON_String);
            if (err)
            {
                (*history)("Invalid 'taperead-requestid' attribute.\n");
            }
            else
            {
                /* Store the request ID in sums->dsname (re-purpose this field). And the call type that triggered this 
                 * in sums->reqcnt. */
                
                /* If this field was used in previous calls to this function, free the allocated string now. */
                if (sums->dsname)
                {
                    free(sums->dsname);
                    sums->dsname = NULL;
                }
        
                sums->dsname = strdup(jsonValue->valuestring);
                sums->reqcnt = kMTSums_CallType_Get;
                
                /* RESULT_PEND means pending tape read. */
                return RESULT_PEND;
            }
        }
    }

    /* Not performing tape read. */
    if (!err)
    {
        err = ((jsonArray = cJSON_GetObjectItem(response, "supaths")) == NULL);
        if (err)
        {
            (*history)("Invalid 'supaths' attribute.\n");
        }
    }

    if (!err)
    {
        nElems = cJSON_GetArraySize(jsonArray);
        if (nElems != sums->reqcnt)
        {
            (*history)("Invalid number of paths (%d) returned from server; expecting %d paths.\n", nElems, sums->reqcnt);
            err = 1;
        }
        
        if (sums->wd)
        {
            /* this may have been allocated from a previous call */
            free(sums->wd);
            sums->wd = NULL;
        }
        
        sums->wd = (char **)calloc(nElems, sizeof(char *));
        
        err = (sums->wd == NULL);

        if (err)
        {
            (*history)("Out of memory calling calloc().\n");
        }
        
        if (!err)
        {
            for (iElem = 0; iElem < nElems; iElem++)
            {
                jsonArrayElement = cJSON_GetArrayItem(jsonArray, iElem);
                
                /* If an SU is offline, or the SU is invalid, then the path could be JSON null. */
                
                
                /* We can ignore sunum. The order of array items matches the order of the sunums sent in the request. */
                err = (((jsonValue = cJSON_GetObjectItem(jsonArrayElement, "path")) == NULL));
                if (err)
                {
                    (*history)("Unable to unjsonize path - path attribute is missing.\n");
                    break;
                }
                else
                {
                    if (jsonValue->type == cJSON_NULL)
                    {
                        sums->wd[iElem] = strdup("");
                    }
                    else if (jsonValue->type == cJSON_String)
                    {
                        sums->wd[iElem] = strdup(jsonValue->valuestring);
                    }
                    else
                    {
                        err = 1;
                        (*history)("Unable to unjsonize path - invalid data type for path-attribute.\n");
                        break;
                    }
                }
            }
        }
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }

    return err;
}

#endif

#if defined(SUMS_USEMTSUMS_ALLOC) && SUMS_USEMTSUMS_ALLOC 
 /* The request JSON looks like this (since JSON does not support 64-bit numbers, send sunum as hexadecimal strings):
 *   {
 *      "reqtype" : "alloc",
 *      "sessionid" : "1AE2FC",
 *      "sunum" : "82C5E02A",
 *      "sugroup" : 22,
 *      "numbytes" : 1024
 *   }
 */ 
static int jsonizeSumallocSumalloc2Request(SUM_t *sums, SUMID_t sessionid, uint64_t *sunum, int sugroup, double numBytes, char **json, int (*history)(const char *fmt, ...))
{
    char numBuf[64];
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    int err;
    
    err = (!sums || !json);
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }
    else
    {
        /* reqtype */
        
        /* The cJSON library doesn't provide a way to check if this worked. We'll know when we print out the json string. */
        cJSON_AddItemToObjectCS(root, "reqtype", cJSON_CreateString("alloc"));
        
        /* uid */
        if (!err)
        {
            snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
            err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
        }
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
        else
        {
            cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        }
        
        /* sunum */
        if (sunum)
        {
            /* SUM_alloc() */
            snprintf(numBuf, sizeof(numBuf), "%llx", *sunum);

            err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
            if (err)
            {
                (*history)("Out of memory calling cJSON_CreateString().\n");
            }
            else
            {
                cJSON_AddItemToObjectCS(root, "sunum", jsonValue);
            }
        }
        else
        {
            /* SUM_alloc2() */
            cJSON_AddItemToObjectCS(root, "sunum", cJSON_CreateNull());
        }

        /* group */
        if (!err)
        {
            err = ((jsonValue = cJSON_CreateNumber((double)sugroup)) == NULL);
        }
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
        else
        {
            cJSON_AddItemToObjectCS(root, "sugroup", jsonValue);
        }
        
        /* bytes */
        if (!err)
        {
            err = ((jsonValue = cJSON_CreateNumber(numBytes)) == NULL);
        }
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
        else
        {
            cJSON_AddItemToObjectCS(root, "numbytes", jsonValue);
        }        
    }
    
    if (!err)
    {        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON, for both alloc and alloc2 calls, in this format:
 * {
 *    "status" : "ok",
 *    "sunum" : "2B13493A",
 *    "sudir" : "/SUM19/D722684218"
 * }
 *
 * We need to assign sunum to sums->dsix_ptr[0], and sudir to sums->wd[0].
 */
static int unjsonizeSumallocSumalloc2Response(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err = 0;
    cJSON *response = NULL;
    cJSON *value = NULL;
        
    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }

    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }

    if (!err)
    {
        err = ((value = cJSON_GetObjectItem(response, "sunum")) == NULL);
        if (err)
        {
            (*history)("Missing 'sunum' attribute.\n");
        }
    }

    if (!err)
    {
        err = (value->type != cJSON_String);
        if (err)
        {
            (*history)("Unexpected data type for sunum.");
        }
    }

    if (!err)
    {
        if (sums->dsix_ptr)
        {
            free(sums->dsix_ptr);
            sums->dsix_ptr = NULL;
        }
    
        err = ((sums->dsix_ptr = calloc(1, sizeof(uint64_t))) == NULL);
        
        if (err)
        {
            (*history)("out of memory");
        }
        else
        {
            /* Convert hexadecimal string to 64-bit number. */
            err = (sscanf(value->valuestring, "%llx", &(sums->dsix_ptr[0])) != 1);
            if (err)
            {
                (*history)("Invalid sunum string.");
            }
        }
    }
    
    if (!err)
    {
        err = ((value = cJSON_GetObjectItem(response, "sudir")) == NULL);
        if (err)
        {
            (*history)("Missing 'sudir' attribute.\n");
        }
    }

    if (!err)
    {
        err = (value->type != cJSON_String);    
        if (err)
        {
            (*history)("Unexpected data type for sudir.");
        }
    }

    if (!err)
    {
        if (sums->wd)
        {
            /* this may have been allocated from a previous call */
            free(sums->wd);
            sums->wd = NULL;
        }
        
        err = ((sums->wd = (char **)calloc(1, sizeof(char *))) == NULL);
        
        if (err)
        {
            (*history)("Out of memory.");
        }
        else
        {    
            err = ((sums->wd[0] = strdup(value->valuestring)) == NULL);
            if (err)
            {
                (*history)("Out of memory.");
            }
        }
    }    
    
    if (response)
    {
        cJSON_Delete(response);
    }

    return err;
}

static int jsonizeSumallocRequest(SUM_t *sums, SUMID_t sessionid, int sugroup, double numBytes, char **json, int (*history)(const char *fmt, ...))
{
    return jsonizeSumallocSumalloc2Request(sums, sessionid, NULL, sugroup, numBytes, json, history);
}

static int unjsonizeSumallocResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    return unjsonizeSumallocSumalloc2Response(sums, msg, history);
}

/* Stub out functions that should not be used if MT-SUMS alloc2 is not defined. */
static int jsonizeSumalloc2Request(SUM_t *sums, SUMID_t sessionid, uint64_t sunum, int sugroup, double numBytes, char **json, int (*history)(const char *fmt, ...))
{
    return jsonizeSumallocSumalloc2Request(sums, sessionid, &sunum, sugroup, numBytes, json, history);
}
static int unjsonizeSumalloc2Response(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    return unjsonizeSumallocSumalloc2Response(sums, msg, history);
}
#endif

#if defined(SUMS_USEMTSUMS_PUT) && SUMS_USEMTSUMS_PUT
/* The request JSON looks like this (since JSON does not support 64-bit numbers, send sunum as hexadecimal strings):
 *   {
 *      "reqtype" : "put",
 *      "sessionid" : "1AE2FC",
 *      "sudirs" : [ {"2B13493A" : "/SUM19/D722684218"}, {"2B15A227" : "/SUM12/D722838055"} ],
 *      "series" : "hmi.M_720s",
 *      "retention" : 14,
 *      "archivetype" : "temporary+archive"
 *   }
 */
static int jsonizeSumputRequest(SUM_t *sums, SUMID_t sessionid, uint64_t *sunums, char **sudirs, size_t nSus, const char *series, int retention, const char *archiveType, char **json, int (*history)(const char *fmt, ...))
{
    char numBuf[64];
    cJSON *root = NULL;
    cJSON *jsonArray = NULL;
    cJSON *jsonArrayElement = NULL;
    cJSON *jsonValue = NULL;
    int isu;
    int err;
    
    err = (!sums || !sunums || !sudirs || nSus < 1 || !series || series[0] == '\0' || !json);
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }
    else
    {
        err = ((jsonValue = cJSON_CreateString("put")) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);

        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
    }
        
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        
        err = ((jsonArray = cJSON_CreateArray()) == NULL);
    }
    
    if (err)
    {
        (*history)("out of memory calling cJSON_CreateArray()\n");
    }
    else
    {
        /* sudirs */
        for (isu = 0; isu < nSus; isu++)
        {
            err = ((jsonArrayElement = cJSON_CreateObject()) == NULL);
            
            if (err)
            {
                (*history)("out of memory calling cJSON_CreateObject()\n");
                break;
            }
            else
            {
                char *sunumStr = NULL;
                
                snprintf(numBuf, sizeof(numBuf), "%llx", sunums[isu]);
                err = ((sunumStr = strdup(numBuf)) == NULL);

                if (err)
                {
                    (*history)("out of memory calling strdup()\n");
                    break;
                }                
                else
                {
                    err = ((jsonValue = cJSON_CreateString(sudirs[isu])) == NULL);

                    if (err)
                    {
                        (*history)("out of memory calling cJSON_CreateString()\n");
                        break;
                    }
                    else
                    {
                        /* do not use cJSON_AddItemToObjectCS() with the same buffer - if you overwrite
                         * the buffer contents, then you will change the value of the attribute;
                         *
                         * sunumStr will be freed by cjson
                         */
                        cJSON_AddItemToObject(jsonArrayElement, sunumStr, jsonValue);
                    }
             
                    cJSON_AddItemToArray(jsonArray, jsonArrayElement);
                }
            }
        }
    }
    
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "sudirs", jsonArray);
    }
    
    /* series */
    if (!err)
    {
        err = ((jsonValue = cJSON_CreateString(series)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "series", jsonValue);
    }
    
    /* touch */
    if (!err)
    {
        err = ((jsonValue = cJSON_CreateNumber(retention)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "retention", jsonValue);
    }
    
    if (!err)
    {
        err = ((jsonValue = cJSON_CreateString(archiveType)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "archivetype", jsonValue);
    }
    
    if (!err)
    {        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON, for the put call, in this format:
 * {
 *    "status" : "ok",
 * }
 */
static int unjsonizeSumputResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err = 0;
    cJSON *response = NULL;

    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}
#endif

#if defined(SUMS_USEMTSUMS_DELETESUS) && SUMS_USEMTSUMS_DELETESUS
/* The request JSON looks like this:
 *   {
 *      "reqtype" : "deleteseries",
 *      "sessionid" : "1AE2FC",
 *      "series" : "hmi.M_720s"
 *   }
 */
static int jsonizeSumdeleteseriesRequest(SUM_t *sums, SUMID_t sessionid, const char *series, char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    char numBuf[64];
    int err;
    
    err = (!sums || !series || series[0] == '\0');
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateObject().\n");
        }
    }

    if (!err)
    {
        err = ((jsonValue = cJSON_CreateString("deleteseries")) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateString().\n");
        }
    }
    
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
        
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateNumber().\n");
        }
    }
            
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        
        err = ((jsonValue = cJSON_CreateString(series)) == NULL);
        
        if (err)
        {
            (*history)("Out of memory calling cJSON_CreateString().\n");
        }
    }
        
    if (!err)
    {
        cJSON_AddItemToObjectCS(root, "series", jsonValue);
    }
    
    if (!err)
    {        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON, for the deleteseries call, is in this format:
 * {
 *    "status" : "ok",
 * }
 */
static int unjsonizeSumdeleteseriesResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err = 0;
    cJSON *response = NULL;

    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}
#endif

/* SUMS_USEMTSUMS is defined here. */

/* The request JSON looks like this:
 *   {
 *      "reqtype" : "ping",
 *      "sessionid" : "1AE2FC"
 *   }
 */
static int jsonizeSumnopRequest(SUM_t *sums, SUMID_t sessionid, char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    char numBuf[64];
    int err;
    
    err = (!sums || !json);
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }
    else
    {
        err = ((jsonValue = cJSON_CreateString("ping")) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
        
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
    }
    
    if (!err)
    {        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* SUMS_USEMTSUMS is defined here. */

/* The MT SUMS daemon returns JSON, for the ping call, in this format:
 * {
 *    "status" : "ok",
 * }
 */
static int unjsonizeSumnopResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    int err = 0;
    cJSON *response = NULL;

    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (response)
    {
        cJSON_Delete(response);
    }
    
    return err;
}

/* SUMS_USEMTSUMS is defined here. */

/* The request JSON looks like this:
 *   {
 *      "reqtype" : "poll",
 *      "sessionid" : "1AE2FC",
 *      "requestid" : "123e4567-e89b-12d3-a456-426655440000"
 *   }
 */
static int jsonizeSumpollRequest(SUM_t *sums, SUMID_t sessionid, const char *requestID, char **json, int (*history)(const char *fmt, ...))
{
    cJSON *root = NULL;
    cJSON *jsonValue = NULL;
    char numBuf[64];
    int err;
    
    err = (!sums || !requestID || requestID[0] == '\0');
    
    if (err)
    {
        (*history)("Invalid argument(s) to 'jsonizeRequest'.\n");
    }
    else
    {
        err = ((root = cJSON_CreateObject()) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateObject().\n");
    }
    else
    {
        err = ((jsonValue = cJSON_CreateString("poll")) == NULL);
    }
    
    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "reqtype", jsonValue);
        
        snprintf(numBuf, sizeof(numBuf), "%llx", (uint64_t)sessionid);
        err = ((jsonValue = cJSON_CreateString(numBuf)) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateNumber().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "sessionid", jsonValue);
        
        err = ((jsonValue = cJSON_CreateString(requestID)) == NULL);
    }

    if (err)
    {
        (*history)("Out of memory calling cJSON_CreateString().\n");
    }
    else
    {
        cJSON_AddItemToObjectCS(root, "requestid", jsonValue);
    }
    
    if (!err)
    {        
        *json = cJSON_Print(root);
    }
    
    if (root)
    {
        cJSON_Delete(root);
    }
    
    return err;
}

/* The MT SUMS daemon returns JSON, for the poll call, in the exact same format as the get call:
 * {
 *    "reqtype" : "get",
 *    "status" : "ok",
 *    "supaths" : 
 *     [
 *        {
 *          "sunum" : "2B13493A",
 *          "path" : "/SUM19/D854870270/S00027"
 *        },
 *        {
 *          "sunum" : "18E34EA7",
 *          "path" : "/SUM7/D854871818/S00009"
 *        }
 *     ]
 *
 * OR, if a tape read is still pending:
 * {
 *    "reqtype" : "get",
 *    "status" : "taperead",
 *    "taperead-requestid" : "123e4567-e89b-12d3-a456-426655440000"
 * }
 */
static int unjsonizeSumpollResponse(SUM_t *sums, const char *msg, int (*history)(const char *fmt, ...))
{
    /* Ack - in theory, SUM_poll() could be used for any SUMS API call (but in practice, it is available for 
     * SUM_get() only). */
    int err = 0;
    cJSON *response = NULL;
    cJSON *value = NULL;
    MTSums_CallType_t origType;

    err = ((response = unjsonizeResponseParse(msg, history)) == NULL);
    
    if (!err)
    {
        err = unjsonizeResponseCheckStatus(response, history);
    }
    
    if (!err)
    {
        err = unjsonizeResponseCheckIsAlive(response, history);
    }
    
    if (!err)
    {
        err = ((value = cJSON_GetObjectItem(response, "reqtype")) == NULL);
        if (err)
        {
            (*history)("Missing 'reqtype' attribute.\n");
        }
    }

    if (!err)
    {
        err = (value->type != cJSON_String);
        if (err)
        {
            (*history)("Unexpected data type for reqtype.\n");
        }
    }

    if (!err)
    {
        /* Map to MTSums_CallType_t */
        err = ((origType = CallTypeFromString(value->valuestring)) == kMTSums_CallType_None);
        if (err)
        {
            (*history)("Unknown MT SUMS call type %s.\n", value->valuestring);
        }
    }    
    
    if (response)
    {
        cJSON_Delete(response);
    }

    if (!err)
    {
        return unjsonizeResponse(sums, origType, msg, history);
    }
    
    return err;
}

int jsonizeRequest(SUM_t *sums, MTSums_CallType_t type, JSONIZER_DATA_t *data, char **json, int (*history)(const char *fmt, ...))
{
    int err;
    
    err = 0;
    
    if (0)
    {
        err = 1;
    }
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    else if (type == kMTSums_CallType_Open)
    {
        err = jsonizeSumopenRequest(json, history);
    }
    else if (type == kMTSums_CallType_Close)
    {
        SUMID_t sessionid = 0;

        sessionid = sums->uid;
        err = jsonizeSumcloseRequest(sums, sessionid, json, history);
    }
    else if (type == kMTSums_CallType_Rollback)
    {
        SUMID_t sessionid = 0;

        sessionid = sums->uid;
        err = jsonizeSumrollbackRequest(sums, sessionid, json, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_INFO) && SUMS_USEMTSUMS_INFO
    else if (type == kMTSums_CallType_Info)
    {
        JSONIZER_DATA_INFO_t *infoData = NULL;
        SUMID_t sessionid = 0;
        uint64_t *sunums = NULL;
        size_t nSus = 0;
        
        infoData = (JSONIZER_DATA_INFO_t *)data;
        sessionid = sums->uid;
        sunums = infoData->sunums;
        nSus = infoData->nSus;
        err = jsonizeSuminfoRequest(sums, sessionid, sunums, nSus, json, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_GET) && SUMS_USEMTSUMS_GET
    else if (type == kMTSums_CallType_Get)
    {
        JSONIZER_DATA_GET_t *getData = NULL;
        SUMID_t sessionid = 0;
        int touch = 0;
        int retrieve = 0;
        int retention = 0;
        uint64_t *sunums = NULL;
        size_t nSus = 0;
        
        getData = (JSONIZER_DATA_GET_t *)data;
        sessionid = sums->uid;
        touch = getData->touch;
        retrieve = getData->retrieve;
        retention = getData->retention;
        sunums = getData->sunums;
        nSus = getData->nSus;
        err = jsonizeSumgetRequest(sums, sessionid, touch, retrieve, retention, sunums, nSus, json, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_ALLOC) && SUMS_USEMTSUMS_ALLOC
    else if (type == kMTSums_CallType_Alloc)
    {
        JSONIZER_DATA_ALLOC_t *allocData = NULL;
        SUMID_t sessionid = 0;
        int sugroup = -1;
        double numBytes = 0;      

        allocData = (JSONIZER_DATA_ALLOC_t *)data;
        sessionid = sums->uid;
        sugroup = allocData->sugroup;
        numBytes = allocData->numBytes;
        err = jsonizeSumallocRequest(sums, sessionid, sugroup, numBytes, json, history);
    }
    else if (type == kMTSums_CallType_Alloc2)
    {
        JSONIZER_DATA_ALLOC2_t *alloc2Data = NULL;
        SUMID_t sessionid = 0;
        uint64_t sunum = 0; /* stupid - because this is unsigned, we can't make an invalid sunum for the default value. */
        int sugroup = -1;
        double numBytes = 0;
        
        alloc2Data = (JSONIZER_DATA_ALLOC2_t *)data;
        sessionid = sums->uid;
        sunum = alloc2Data->sunum;
        sugroup = alloc2Data->sugroup;
        numBytes = alloc2Data->numBytes;        
        err = jsonizeSumalloc2Request(sums, sessionid, sunum, sugroup, numBytes, json, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_PUT) && SUMS_USEMTSUMS_PUT
    else if (type == kMTSums_CallType_Put)
    {
        JSONIZER_DATA_PUT_t *putData = NULL;
        SUMID_t sessionid = 0;
        uint64_t *sunums;
        char **sudirs;
        size_t nSus;
        char *series;
        int retention;
        const char *archiveType;
        
        putData = (JSONIZER_DATA_PUT_t *)data;
        sessionid = sums->uid;
        sunums = putData->sunums;
        sudirs = putData->sudirs;
        nSus = putData->nSus;
        series = putData->series;
        retention = putData->retention;
        archiveType = putData->archiveType;
        err = jsonizeSumputRequest(sums, sessionid, sunums, sudirs, nSus, series, retention, archiveType, json, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_DELETESUS) && SUMS_USEMTSUMS_DELETESUS
    else if (type == kMTSums_CallType_Deleteseries)
    {
        JSONIZER_DATA_DELETESERIES_t *deleteseriesData = NULL;
        SUMID_t sessionid = 0;
        char *series = NULL;
        
        deleteseriesData = (JSONIZER_DATA_DELETESERIES_t *)data;
        sessionid = sums->uid;
        series = deleteseriesData->series;
        err = jsonizeSumdeleteseriesRequest(sums, sessionid, series, json, history);
    }
#endif
    else if (type == kMTSums_CallType_Ping)
    {
        SUMID_t sessionid = 0;
        
        sessionid = sums->uid;
        err = jsonizeSumnopRequest(sums, sessionid, json, history);
    }
    else if (type == kMTSums_CallType_Poll)
    {
        JSONIZER_DATA_POLL_t *pollData = NULL;
        SUMID_t sessionid = 0;
        char *requestID = NULL;
        
        pollData = (JSONIZER_DATA_POLL_t *)data;
        sessionid = sums->uid;
        requestID = pollData->requestID;
        err = jsonizeSumpollRequest(sums, sessionid, requestID, json, history);
    }
    else
    {
        (*history)("Unsupported MT SUMS request %d.\n", type);
        err = 1;
    }

    return err;
}

/* 0 ==> successfully parsed response
 * 1 ==> error
 * 2 ==> is alive (pending)
 */
int unjsonizeResponse(SUM_t *sums, MTSums_CallType_t type, const char *msg, int (*history)(const char *fmt, ...))
{
    int err;
    
    err = 0;
    
    if (0)
    {
        err = 1;
    }
#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
    else if (type == kMTSums_CallType_Open)
    {
        err = unjsonizeSumopenResponse(sums, msg, history);
    }
    else if (type == kMTSums_CallType_Close)
    {
        err = unjsonizeSumcloseResponse(sums, msg, history);
    }
    else if (type == kMTSums_CallType_Rollback)
    {
        err = unjsonizeSumrollbackResponse(sums, msg, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_INFO) && SUMS_USEMTSUMS_INFO
    else if (type == kMTSums_CallType_Info)
    {
        err = unjsonizeSuminfoResponse(sums, msg, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_GET) && SUMS_USEMTSUMS_GET
    else if (type == kMTSums_CallType_Get)
    {
        err = unjsonizeSumgetResponse(sums, msg, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_ALLOC) && SUMS_USEMTSUMS_ALLOC
    else if (type == kMTSums_CallType_Alloc)
    {
        err = unjsonizeSumallocResponse(sums, msg, history);
    }
    else if (type == kMTSums_CallType_Alloc2)
    {
        err = unjsonizeSumalloc2Response(sums, msg, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_PUT) && SUMS_USEMTSUMS_PUT
    else if (type == kMTSums_CallType_Put)
    {
        err = unjsonizeSumputResponse(sums, msg, history);
    }
#endif
#if defined(SUMS_USEMTSUMS_DELETESUS) && SUMS_USEMTSUMS_DELETESUS
    else if (type == kMTSums_CallType_Deleteseries)
    {
        err = unjsonizeSumdeleteseriesResponse(sums, msg, history);
    }
#endif
    /* SUMS_USEMTSUMS is defined here, which means we always have to provide the MT SUM noop call. */
    else if (type == kMTSums_CallType_Ping)
    {
        err = unjsonizeSumnopResponse(sums, msg, history);
    }
    else if (type == kMTSums_CallType_Poll)
    {
        err = unjsonizeSumpollResponse(sums, msg, history);
    }
    else
    {
        (*history)("Unsupported MT SUMS request %d.\n", type);
        err = 1;
    }

    return err;
}

int callMTSums(SUM_t *sums, MTSums_CallType_t callType, JSONIZER_DATA_t *data, int (*history)(const char *fmt, ...))
{
    char *request = NULL;
    char *response = NULL;
    size_t rspLen = 0;
    int err;
    TIMER_t *timer = NULL;
    
    clprev = NULL;

    err = jsonizeRequest(sums, callType, data, &request, history);
    
    if (!err)
    {
#if (defined(SUMS_USEMTSUMS_ALL) && SUMS_USEMTSUMS_ALL)
        /* pure MT SUMS */
        if (callType == kMTSums_CallType_Open)
#else
        /* mixed MT and RPC SUMS */
#endif
        {
            if (!sums || sums->mSumsClient != -1 || ConnectToMtSums(sums, history) == -1)
            {
                /* there should be no existing connection to SUMS if we are calling SUM_open() */
                err = 4; /* "SUMS dead" */
            }
        }
    }
    
    if (!err)
    {
        /* send request */
        /* if this is a SUM_rollback() or SUM_close(), then this will
         * tell the MT SUMS server to end the SUMS DB transaction.
         */
        if (sendMsg(sums, request, strlen(request), history))
        {
            err = 4;
        }
    }

    if (!err)
    {    
        /* receive response - return value of 0 is success, return value of 1 is internal/client error, 
         * return value of 2 is timeout. */
        timer = CreateTimer();
        while (1)
        {
            err = receiveMsg(sums, &response, &rspLen, history);
            
            if (err)
            {
                /* break out if a timeout (err == 2) or error (err == 1) occurred */
                break;
            }
            
            /* successfully received response (err == 0), but it could be an is-alive response */
        
            err = unjsonizeResponse(sums, callType, response, history);    
            
            if (err == 0 /* got final response */ || err == 1 /* error */)
            {
                break;
            }
            else if (err == 2)
            {
                /* got isAlive response; wait for final response after checking for timeout (5m) */
                if (GetElapsedTime(timer) > 300.0)
                {
                    /* timeout */
                    err = 1;
                    (*history)("timeout waiting for %s response from SUMS\n", MTSums_CallType_strings[callType]);
                    break;
                }
                
                err = 0;
                continue;
            }
            else
            {
                /* unexpected status - error */
                err = 1;
                break;
            }
        }
        
        DestroyTimer(&timer);
    }    

#if (defined(SUMS_USEMTSUMS_ALL) && SUMS_USEMTSUMS_ALL)
    /* pure MT SUMS - if SUM_open() fails, then disconnect from MT SUMS (shutdown the socket connection) */
    if ((callType == kMTSums_CallType_Open && sums->uid == 0) || callType == kMTSums_CallType_Close || callType == kMTSums_CallType_Rollback)
#else
    /* mixed MT and RPC SUMS */
#endif
    {
        DisconnectFromMtSums(sums);
    }

    if (response)
    {
        free(response);
        response = NULL;
    }
    
    if (request)
    {
        free(request);
        request = NULL;
    }
    
    return err;
}


#if defined(SUMS_USEMTSUMS_CONNECTION) && SUMS_USEMTSUMS_CONNECTION
/* Arguments to send to MT server:
 *   None
 */
 
/* The original API requires two arguments, which are no longer used:
 *   server - the machine hosting the SUMS service (NOT USED).
 *   db - the machine hosting the DB used by MT SUMS (NOT USED).
 */

SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))
{
    // Both server and db are ignored for MT SUMS. The DRMS makes and tears-down a SUMS connection
    // for each call to SUMS. We need to connect to MT SUMS for the SUM_open() call so MT SUMS
    // can create an entry in the sum_open DB table and so it can return a sumid, which is then
    // used to later to delete the record in the sum_open table.
    SUM_t *sums = NULL;
    
    /* Must call sumsopenOpen() to create the SUM_t first. If the client is using MT SUM server exclusively, 
     * then sumsopenOpen() is virtually a noop. Otherwise, it creates the RPC clients needed for the various services. */
    sums = sumsopenOpen(server, db, history);    

    return sums;
}

 /* Arguments to send to MT server:
 *   sessionid (int) - passed-in in sums->uid argument (uint32_t)
 */
int SUM_close(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    return sumsopenClose(sums, kMTSums_CallType_Close, history);
}

int SUM_rollback(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    return sumsopenClose(sums, kMTSums_CallType_Rollback, history);
}

#else /* MT SUMS CONNECTION family */
/* RPC SUMS is used for the CONNECTION family (but MT SUMS provides at least one service) */


#endif /* RPC SUMS CONNECTION family */

#if defined(SUMS_USEMTSUMS_INFO) && SUMS_USEMTSUMS_INFO
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

/* Arguments to send to MT server:
 *   sus (array of hex strs) - passed-in in sunums/reqcnt arguments (array uint64_t, int)
 */
int SUM_infoArray(SUM_t *sums, uint64_t *sunums, int reqcnt, int (*history)(const char *fmt, ...))
{
    JSONIZER_DATA_INFO_t data;

    if (reqcnt > MAX_MTSUMS_NSUS) 
    {
        (*history)("Too many SUs in request (maximum of %d allowed).\n", reqcnt, MAX_MTSUMS_NSUS);
        return 1; /* means 'internal error', which isn't a great description of the error. */
    }
    
    /* jsonize request */
    data.sunums = sunums;
    data.nSus = reqcnt;
    
    return callMTSums(sums, kMTSums_CallType_Info, (JSONIZER_DATA_t *)(&data), history);
}
#else /* MT SUMS INFO family */
/* RPC SUMS is used for the INFO family. */

#endif /* RPC SUMS INFO family */

#if defined(SUMS_USEMTSUMS_GET) && SUMS_USEMTSUMS_GET
/* For some reason, in the original implementation, the SUNUMs are passed via the SUM_t argument, and not as separate 
 * arguments. The original API functions handle the SUNUMs inconsistently, so we have to preserve that in the
 * new API functions.
 *
 * Arguments to send to MT server:
 *   touch (Bool)- passed-in in sums->mode argument (a single bit in an int field)
 *   retrieve (Bool) - passed-in in sums->mode argument (a single bit in an int field)
 *   retention (numeric) - passed-in in sums->tdays argument (int)
 *   sus (array of hex strs) - passed-in in sums->dsix_ptr and sums->reqcnt arguments (uint64_t and int)
 *
 * SUM_get() is an asynchronous call IFF a tape read happens. If a tape read is happening, then 
 * return RESULT_PEND. Otherwise, return 0 if the path was obtained and there were no errors. Otherwise,
 * return non-zero. The client must then call SUM_nop() to determine if SUMS has crashed and if the client
 * should try again.
 */
int SUM_get(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    JSONIZER_DATA_GET_t data;
    int rv = 0;
    
    if (sums->reqcnt > MAX_MTSUMS_NSUS) 
    {
        (*history)("Too many SUs in request (maximum of %d allowed).\n", sums->reqcnt, MAX_MTSUMS_NSUS);
        return 1; /* means 'internal error', which isn't a great description of the error. */
    }
    
    /* jsonize request */
    data.touch = sums->mode & TOUCH;
    data.retrieve = sums->mode & RETRIEVE;
    data.retention = sums->tdays;
    data.sunums = sums->dsix_ptr;
    data.nSus = sums->reqcnt;
    
    /* If a tape read results, then we have to save the tape-read requestID for future calls to SUM_poll(). Re-purpose
     * sums->dsname - initialize the value to NULL so when know when to clean dsname. */
     sums->dsname = NULL;
    
    /* 0 - success, RESULT_PEND - pending tape read, everything else - error. */
    return callMTSums(sums, kMTSums_CallType_Get, (JSONIZER_DATA_t *)(&data), history);
}
#else /* MT SUMS GET family */
/* RPC SUMS is used for the GET family. */
#endif /* RPC SUMS GET family */

#if defined(SUMS_USEMTSUMS_ALLOC) && SUMS_USEMTSUMS_ALLOC
/* Arguments to send to MT server:
 *   sunum (hex str) - passed-in in sunum argument (uint64_t)
 *   group (numeric) - passed-in in sums->group argument (int)
 *   sessionid (numeric) = - passed-in in sums->uid argument (uint32_t)
 *   bytes (numeric) - passed-in in sums->bytes argument (double). bytes is seriously whacked. It is a double in the SUM_t struct, and a 
 *     double in the SUMS db. But it should really be an integer. JSON uses double-precision floating-point numbers for all 
 *     numeric values.
 */
 
/* SUM_alloc() performs these tasks:
 *    1. It selects a partition to create an SUDIR in. It does this by mapping the group number to a partition set with the
 *       sum_arch_group table, then randomly choosing one of the suitable partitions.
 *    2. It creates the SUDIR with a mkdir/chmod combo.
 *    3. It inserts a record in sum_partn_alloc that contains mostly default values. The only values that matter are the
 *       status value, which is set to DARW (read-write SUDIR), the SUMS sessionid, the tape group, and ds_index, which 
 *       is the SUNUM for the SU. With RPC SUMS, this insertion was completely obsolete. It inserted dummy values for the
 *       tape group, ds_index and all other columns, except the status and sessionid columns. The MT SUM_alloc() stores the
 *       tape group and ds_index so that SUM_put() can later use those values for selecting a tape group for the SU.
 *       SUM_put() inserts a new record in sum_partn_alloc with real column values, and then SUM_close() deletes 
 *       the record inserted by SUM_alloc().
 */ 
 
int SUM_alloc(SUM_t *sums, int (*history)(const char *fmt, ...))
{
    JSONIZER_DATA_ALLOC_t data;

    data.sugroup = sums->group;
    data.numBytes = sums->bytes;
    
    return callMTSums(sums, kMTSums_CallType_Alloc, (JSONIZER_DATA_t *)(&data), history);
}

int SUM_alloc2(SUM_t *sums, uint64_t sunum, int (*history)(const char *fmt, ...))
{
    JSONIZER_DATA_ALLOC2_t data;

    data.sunum = sunum;
    data.sugroup = sums->group;
    data.numBytes = sums->bytes;
    
    return callMTSums(sums, kMTSums_CallType_Alloc2, (JSONIZER_DATA_t *)(&data), history);
}
#else /* MT SUMS ALLOC family */
/* RPC SUMS is used for the ALLOC family. */
#endif /* RPC SUMS ALLOC family */

#if defined(SUMS_USEMTSUMS_PUT) && SUMS_USEMTSUMS_PUT
/* Arguments to send to MT server:
 *   sudirs (obj of hex str : sudir) - passed-in in sums->dsix_ptr, sums->wd, sums->reqcnt
 *   series (str) - passed-in in sums->dsname
 *   retention (numeric)- passed-in in sums->tdays
 *   archivetype (str) - passed in in sums->mode argument (as 3 different bits: ARCH, TEMP, PERM - these all conflict with
 *                       each other, PERM takes precedence over TEMP, which takes precedence over ARCH.)
 */
 
/* SUM_put() performs these tasks:
 *    1. It inserts a record for this SU into sum_main. 
 *    2. It inserts a record for this SU into sum_partn_alloc, just like SUM_alloc() does. However, SUM_alloc() inserts a status
 *       of DARW (read-write SUDIR)and SUM_put() inserts a status of DAAP (archive-pending SUDIR) or DADP (delete-pending SUDIR).
 *       SUMS select DAAP if the SUMS has a tape system. Otherwise, it selects DADP (the SU gets deleted after retention has expired).
 *       SUM_close() deletes the record inserted by SUM_alloc() by deleting all records where the status is DARW or DARO.
 */

/* Storage group and storage set are passed in by the client, but they should not be. They are passed-in by the client during the
 * SUM_alloc() call and used to determine a SUMS partition. But they are not saved into the SUMS DB during the SUM_alloc() call.
 * They are saved to the SUMS db during the SUM_put() call. They are saved into both sum_main and sum_partn_alloc - the values 
 * in sum_main are never used, but the ones in sum_partn_alloc are used when the SUs are archived to tape. 
 *
 * ARGH!!! The group/storeset values used in SUM_alloc() are not saved anywhere. And there is no way to check that the group 
 * passed in to SUM_alloc() matches the group passed  in to SUM_put(). Look at it this way - the group value passed in 
 * to SUM_alloc() is used for selecting a SUMS partition. The group value passed in to SUM_put() is used by the tape system 
 * to group SUs into tape files. So, the same group number is used for different purposes. Ideally, SUs that share a SUMS
 * partition also share a tape file. But because the group passed into SUM_alloc() is not saved, this cannot be enforced.
 * This is an (original) SUMS flaw.
 *
 * To remedy this, the MT SUMS SUM_alloc() stores the group number in sum_partn_alloc. The RPC SUMS stores a dummy group number 
 * of 0, but the MT SUMS stores the actual group number. Then when the MT SUM_put() is called, it ignores sums->group and uses
 * the value in sum_partn_alloc.
 */
 int SUM_put(SUM_t *sums, int (*history)(const char *fmt, ...))
 {
    JSONIZER_DATA_PUT_t data;
    int err;

    data.sunums = sums->dsix_ptr;
    data.sudirs = sums->wd;
    data.nSus = sums->reqcnt;
    data.series = sums->dsname;
    data.retention = sums->tdays;
    /* The original RPC SUMS has no default archivetype!! It could be some random int. */
    data.archiveType = (sums->mode & PERM) ? "permanent+archive" : ((sums->mode & TEMP) ? "temporary+noarchive" : ((sums->mode & ARCH) ? "temporary+archive" : "temporary+noarchive"));
    
    err = callMTSums(sums, kMTSums_CallType_Put, (JSONIZER_DATA_t *)(&data), history);
    
    return err;
 }
 
#else /* MT SUMS PUT family */
/* RPC SUMS is used for the PUT family. */
#endif /* RPC SUMS PUT family */

#if defined(SUMS_USEMTSUMS_DELETESUS) && SUMS_USEMTSUMS_DELETESUS
/* Arguments to send to MT server:
 *   series - passed-in in seriesname argument
 */
 
/* SUM_delete_series() performs these tasks:
 *    1. It queries sum_main to determine if any SUs belong to the series being deleted. If not, it is a no-op and returns.
 *    2. For each SU that belongs to the series (determined by querying the owning_series column in sum_main), it 
 *       changes the status in sum_partn_alloc to DADP and the substatus to DADPDELSU, and it sets the retention for the 
 *       SU to 0 days. It does this change on chunks of SUs, not single SUs.
 */
  
/* The RPC version of SUM_delete_series() does some really inefficient stuff:
 *    1. It passes all SUNUMs from DRMS to SUMS, regardless of the existence of the corresponding SUs. There is
 *       actually no reason to pass the SUNUMs from DRMS to SUMS to begin with, since SUMS knows which SUs
 *       belong to which series.
 *    2. It passes these SUNUMs via text files, each of which occupies an SU.
 *    3. For each SU passed to SUMS, it does a single SQL command to change the retention to 0 (even if the SU
 *       does not exist, the query is performed). 
 *
 * For the MT version, the filename argument is no longer used. There is no reason to pass SUs to SUMS at all, and
 * if there were, using a file is a poor option.
 */
int SUM_delete_series(SUM_t *sums, char *filename, char *seriesname, int (*history)(const char *fmt, ...))
{
    JSONIZER_DATA_DELETESERIES_t data;

    data.series = seriesname;

    return callMTSums(sums, kMTSums_CallType_Deleteseries, (JSONIZER_DATA_t *)(&data), history);
}
#else /* MT SUMS DELETESUS family */
/* RPC SUMS is used for the DELETESUS family. */
#endif /* RPC SUMS DELETESUS family */

#if defined(SUMS_USEMTSUMS_ALL) && SUMS_USEMTSUMS_ALL
int SUM_nop(SUM_t *sum, int (*history)(const char *fmt, ...))
{
    return sumsopenNopMT(sum, history);
}

int SUM_poll(SUM_t *sum)
{
    return sumsopenPollMT(sum);
}
#else 
int SUM_nop(SUM_t *sum, int (*history)(const char *fmt, ...))
{
    if (clprev != NULL)
    {
        return sumsopenNopRPC(sum, history);
    }
    else
    {
        return sumsopenNopMT(sum, history);
    }
}

int SUM_poll(SUM_t *sum)
{
    if (clprev != NULL)
    {
        return sumsopenPollRPC(sum);
    }
    else
    {
        return sumsopenPollMT(sum);
    }
}

#endif


/* For SUM_nop() and SUM_poll(), we need to use the RPC SUMS server if the the SUMS request being 
 * currently processed is being handled by an RPC SUMS server. Otherwise, we need to use the MT SUMS 
 * server. The clprev global is used to indicate which SUMS server is processing the current request.
 * If clprev == NULL, then the MT SUMS server is processing the current request. Otherwise, the 
 * value of clprev is the RPC client that is connected to the RPC server that is processing the
 * current request. If the MT SUMS server should handle the SUM_nop() call, then send a nop JSON request
 * to the MT SUMS server. Otherwise, call sumopenNopRPC() and let it make a SUMS request using the
 * RPC client in clprev.
 */


#endif /* Use MT SUMS (for at least one SUMS API function) */

/* RPC SUMS CONNECTION family */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_CONNECTION) || !SUMS_USEMTSUMS_CONNECTION)
SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))
{
    return sumsopenOpen(server, db, history);
}

int SUM_close(SUM_t *sum, int (*history)(const char *fmt, ...))
{
    return sumsopenClose(sum, history);
}
#endif /* RPC SUMS CONNECTION family */

/* RPC SUMS INFO family */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)

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
#endif /* RPC SUMS INFO family */

/* RPC SUMS GET family */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
/* Get the wd of the storage units given in dsix_ptr of the given sum.
 * Return 0 on success w/data available, 1 on error, 4 on connection reset
 * by peer (sum_svc probably gone) or RESULT_PEND (32)
 * when data will be sent later and caller must do a sum_wait() or sum_poll() 
 * when he is ready for it. You can't make another SUM_get() with the
 * samd SUM_t handle while one is still pending.
*/

/* WTAF - why are the SUNUMs passed via the SUM_t argument, and not as separate arguments as is done for
 * SUM_info() - why the inconsistency? */
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
#endif /* RPC SUMS GET family */

 /* RPC SUMS ALLOC family*/
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif /* RPC SUMS ALLOC family */

/* RPC SUMS PUT family */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_PUT) || !SUMS_USEMTSUMS_PUT)
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
  /* uid is not provided by the caller of SUM_put(). */
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_str(&klist, "dsname", sum->dsname);
  setkey_str(&klist, "history_comment", sum->history_comment);
  setkey_str(&klist, "username", sum->username);
  setkey_int(&klist, "group", sum->group);
  
  /* storeset got set by a previous SUM_alloc/SUM_alloc2 call! The caller of SUM_put does NOT provide
   * a storeset value. */
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
#endif /* RPC SUMS PUT family */

/* RPC SUMS DELETESUS family */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_DELETESUS) || !SUMS_USEMTSUMS_DELETESUS)
/* Called by the delete_series program before it deletes the series table.
 * Called with a pointer to a full path name that contains the sunums
 * that are associated with the series about to be deleted.
 * Returns 1 on error, else 0.
 * NOTE: error 4 is Connection reset by peer, sum_svc probably gone.
*/
// WTAF! Why is there no SUM_t passed into the function! Added by Art.
int SUM_delete_series(SUM_t *sum, char *filename, char *seriesname, int (*history)(const char *fmt, ...))
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
  clprev = sum->cldelser;
  status = clnt_call(sum->cldelser, DELSERIESDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
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
#endif /* RPC SUMS DELETESUS family */

/* SUMS services that cannot be configured by the client. The RPC versions of SUM_nop() and SUM_poll() must be*/
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
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
#endif

/* If we are not using MT SUMS, then we have to define SUM_open() here and have it always
 * use the RPC server. Otherwise, if MT SUMS is not used for all calls, then we need to use 
 * the SUM_open() definition that selects either the RPC or MT
 * version, depending on which server was used for the current SUMS request. If MT SUMS is
 * used for all calls, then we need to use the SUM_open() definition that uses the MT server
 * only.
 */
#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS)
int SUM_nop(SUM_t *sum, int (*history)(const char *fmt, ...))
{
    return sumsopenNopRPC(sum, history);
}

int SUM_poll(SUM_t *sum)
{
    return sumsopenPollRPC(sum);
}
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
// Don't use any of this RPC-based interface calls if the client is solely using the MT SUMS interface.
// There is no reason to expose the API calls. The MT SUMS server reads the partition table
// each time it need to use it. 

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

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALLOC) || !SUMS_USEMTSUMS_ALLOC)
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
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_GET) || !SUMS_USEMTSUMS_GET)
    
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
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_INFO) || !SUMS_USEMTSUMS_INFO)
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
#endif

#if (!defined(SUMS_USEMTSUMS) || !SUMS_USEMTSUMS) || (!defined(SUMS_USEMTSUMS_ALL) || !SUMS_USEMTSUMS_ALL)
// Don't use any of the RPC-based interface if the client is configured to solely use MT SUMS.

/**************************************************************************/

/* Attempt to get any sum_svc completion msg.
 * If block = 0 will timeout after 0.5 sec, else will wait until a msg is
 * received.
 * Returns the type of msg or timeout status.
*/

/* svc_fdset is a global RPC variable. It is an array that contains a struct element for each connection from sum_svc 
 * to the DRMS module (which is acting as the SVC server). For each connection, svc_getreqset() calls the
 * RESPPROG program, which is respd(), when it receives an RPC request. resp() will then call one of two 
 * functions, respdo_1() or respdoarray_1(), to handle these requests. Each of these functions will put
 * results in the SUM_t struct. 
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

/* This is the function where the SUM_t::sinfo list is created. It looks like initially
 * xdr_array was used to create this list, but then the developer couldn't figure out how to do 
 * it, so they used this function instead. However, they left all the xdr stuff laying about.
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
  
    if (reqcode == ALLOCDO || reqcode == GETDO)
    {
        /* these two calls will set the SUdirs, and we do not know exactly how many bytes were allocated to sum->wd */
        if (sum->wd)
        {
            /* this may have been allocated from a previous call */
            free(sum->wd);
            sum->wd = NULL;
        }

        sum->wd = (char **)calloc(reqcnt, sizeof(char *));
    }
    
    if (reqcode == ALLOCDO)
    {
        /* this call is also going to set the SUNUM */    
        if (sum->dsix_ptr)
        {
            /* this may have been allocated from a previous call */
            free(sum->dsix_ptr);
            sum->dsix_ptr = NULL;
        }

        sum->dsix_ptr = (uint64_t *)calloc(reqcnt, sizeof(uint64_t));
    }

  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  switch(reqcode) {
  case ALLOCDO:
    /* wd is actually a SUDIR, not a partn_name (a partn_name example is /SUM23, and SUDIR is /SUM23/D93925033) */
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
  bool_t (*xdr_argument)(void), (*xdr_result)(void);
  char *(*local)(void *, void *);

  switch (rqstp->rq_proc) {
  case NULLPROC:
    (void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
    return;
  case RESPDO:
    xdr_argument = xdr_Rkey;
    xdr_result = xdr_void;
    local = (char *(*)(void *, void *)) respdo_1;
    RESPDO_called = 1;
    break;
  case RESPDOARRAY:
    xdr_argument = xdr_Rkey;
    xdr_result = xdr_void;
    local = (char *(*)(void *, void *)) respdoarray_1;
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
#endif

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

