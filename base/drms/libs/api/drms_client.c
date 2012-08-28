#include "drms.h"
#include "drms_priv.h"
//#include "xmem.h"
#include "db.h"
// #define DEBUG

/* Do not localize the version table. */
#define DRMS_MINVERSTABLE "drms.minvers"
#define DRMS_OLDCODETABLE "drms.oldcode"

/* Returns 1 if the table exists, 0 if it doesn't, and -1 if an error occurred. */
static int TableExists(DRMS_Session_t *session, const char *schema, const char *table)
{
    int rv = 0;
    DB_Text_Result_t *qres = NULL;
    char query[256];
    
    snprintf(query, sizeof(query), "SELECT * FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", schema, table);
    
    if ((qres = drms_query_txt(session, query)) == NULL)
    {
        /* Error */
        fprintf(stderr, "Invalid database query: %s.\n", query);
        rv = -1;
    }
    else
    {
        if (qres->num_rows == 1) 
        {
            rv = 1;
        }
        
        db_free_text_result(qres);
    }
    
    return rv;
}

/* returns 1 on success, 0 on failure */
static int GetDBMinVersion(DRMS_Session_t *session, char **versout)
{
    int rv = 1;
    char *schema = NULL;
    char *table = NULL;
    char query[256];
    DB_Text_Result_t *qres = NULL;
    
    if (!versout)
    {
        fprintf(stderr, "Invalid argument to GetDBMinVersion().\n");
        rv = 0;
    }
    else
    {
        if (get_namespace(DRMS_MINVERSTABLE, &schema, &table))
        {
            fprintf(stderr, "Out of memory in GetDBMinVersion().\n");
            rv = 0;
        }
        else
        {
            /* If the version table, su_production.minvers at Stanford, doesn't exist, bail out. */
            if (!TableExists(session, schema, table))
            {
                fprintf(stderr, "Missing database table %s.\n", DRMS_MINVERSTABLE);
                rv = 0;
            }
            else
            {
                /* Fetch the version from the DB version table. */
                snprintf(query, sizeof(query), "SELECT minversion FROM %s", DRMS_MINVERSTABLE);
                
                if ((qres = drms_query_txt(session, query)) == NULL)
                {
                    /* Error */
                    fprintf(stderr, "Invalid database query: %s.\n", query);
                    rv = 0;
                }
                else
                {
                    if (qres->num_rows == 0)
                    {
                        fprintf(stderr, "DRMS requires a record in drms.minvers.\n");
                        rv = 0;
                    }
                    else if (qres->num_rows != 1)
                    {
                        fprintf(stderr, "Unexpected db response to query %s\n", query);
                        rv = 0;
                    }
                    else
                    {
                        const char *version = qres->field[0][0];
                        
                        *versout = strdup(version);
                    }

                    db_free_text_result(qres);
                }
            }
            
            free(schema);
            free(table);
        }
    }
    
    return rv;
}

DRMS_Session_t *drms_connect(const char *host)
{
  struct sockaddr_in server;
  struct hostent *he;
  DRMS_Session_t *session;
  int denied;
  char *port = NULL;
  char *hostname = NULL;
  char *pport = NULL;
  unsigned short portnum;
  char *sudirrecv = NULL;
  struct sockaddr *serverp = NULL;

  if (host)
  {
     hostname = strdup(host);

     /* extract port from host */
     if ((pport = strchr(host, ':')) != NULL)
     {
        hostname[pport - host] = '\0';
        port = strdup(pport + 1);
     }
     else
     {
        fprintf(stderr, "Port number missing from host.\n");
        goto bailout1;
     }

     if (port)
     {
        sscanf(port, "%hu", &portnum);
     }
  }

  /* get the host info */
  if ((he=gethostbyname(hostname)) == NULL) {  
    herror("gethostbyname");
    if (port)
    {
       free(port);
    }
    if (hostname)
    {
       free(hostname);
    }

    fprintf(stderr, "Error calling gethostbyname().\n");
    return NULL;
  }
  
  session = malloc(sizeof(DRMS_Session_t));
  XASSERT(session);
  session->db_direct = 0;

  /* set up the transport end point */
  if ( (session->sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
  {
    perror("socket call failed");
    goto bailout1;
  }

  {
    int rcvbuf, sndbuf;
    db_getsocketbufsize(session->sockfd, &sndbuf,&rcvbuf);
#ifdef DEBUG
    printf("Original socket buffer sizes: SO_SNDBUF = %d, SO_RCVBUF = %d\n",
	   sndbuf,rcvbuf);
#endif
    if (sndbuf<65536)
    {
      sndbuf = 65536/2;
      db_setsocketbufsize(session->sockfd, sndbuf, rcvbuf/2);
    }
    db_getsocketbufsize(session->sockfd, &sndbuf,&rcvbuf);
#ifdef DEBUG
    printf("New socket buffer sizes: SO_SNDBUF = %d, SO_RCVBUF = %d\n",
	   sndbuf,rcvbuf);
#endif      
  }

  memset(&server, 0, sizeof(server));  
  server.sin_family = AF_INET;     /* host byte order */
  server.sin_port = htons(portnum); /* short, network byte order */
  server.sin_addr = *((struct in_addr *)he->h_addr);

  serverp = (struct sockaddr *)&server;
  /* connect the socket to the server's address */
  if ( connect(session->sockfd, serverp, sizeof(struct sockaddr_in)) == -1 )
  {
    perror("connect");
    fprintf(stderr,"Failed to connect to server, please check that host name, "
	    "port number and password are correct.\n");
    goto bailout;
  }
  strncpy(session->hostname, hostname, DRMS_MAXHOSTNAME);
  session->port = portnum;
  /* Turn off Nagle's algorithm. */
  /*
  if (setsockopt(session->sockfd,  IPPROTO_TCP,    
		 TCP_NODELAY,  (char *) &flag, 
		 sizeof(int)) < 0)
    return NULL;
*/

  /* Receive anxiously anticipaqted reply from the server. If denied=1
     we are doomed, but if denied=0 we are on our way to the green
     pastures of DRMS. */
  denied = Readint(session->sockfd);
  if (denied)
  {
     fprintf(stderr, "drms_server didn't accept the attempt to connect with it.\n");
     goto bailout;
  }
  session->clientid = Readint(session->sockfd);
  session->sessionid = Readlonglong(session->sockfd);
  session->sessionns = receive_string(session->sockfd);
  session->sunum = Readlonglong(session->sockfd);

  /* Big pain in the ass - this is the SUDIR of the log directory. We don't always 
   * create such a directory, and the code checks for NULL session->sudir in many
   * cases. But there is no way that the client (sock module) knows whether or
   * not there is such an SUDIR in use by the server (drms_server), so we always
   * have to send SOME string. The policy is: if drms_server has a log SUDIR, then
   * send the path to that, otherwise, send the string "NOLOGSUDIR". Then in the 
   * client code, drop NOLOGSUIDR and set session->sudir to NULL.
   */
  sudirrecv = receive_string(session->sockfd);

  if (sudirrecv && strcasecmp(sudirrecv, kNOLOGSUDIR))
  {
     session->sudir = sudirrecv;
  }
  else
  {
     free(sudirrecv);
     session->sudir = NULL;  
  }

#ifdef DEBUG
  printf("got sudir=%s\n",session->sudir);
#endif
  return session;
  
 bailout:
  close(session->sockfd);
 bailout1:
  free(session);
  if (port)
  {
     free(port);
  }
  if (hostname)
  {
     free(hostname);
  }
  return NULL;
}

int drms_send_commandcode (int sockfd, int command) {
  int echo;
  Writeint (sockfd, command);
  echo = Readint (sockfd);
  if (echo != command) {
    printf ("FATAL ERROR: The DRMS server echoed a different command code (%d)\n"
        "  from the one sent (%d).\n"
	"This usually indicates that the module generated an invalid command\n"
	"  that caused the DRMS server to terminate the connection.\n"
	"It can also mean that the DRMS server crashed.\nAborting.\n",
	echo, command);
    close (sockfd);
    exit (1);	   /*  FIXME: Should call some user supplied abort callback  */
  }
  return 0;
}  

int drms_send_commandcode_noecho (int sockfd, int command) {
  Writeint (sockfd, command);
  return 0;
}  

#ifndef DRMS_CLIENT
DRMS_Session_t *drms_connect_direct(const char *dbhost, const char *dbuser, 
				    const char *dbpasswd, const char *dbname,
				    const char *sessionns)
{
  DRMS_Session_t *session;

  session = malloc(sizeof(DRMS_Session_t));
  XASSERT(session);
  memset(session, 0, sizeof(DRMS_Session_t));
  session->db_direct = 1;

  /* Set client variables to some null values. */
  session->port = -1;
  session->sockfd = -1;

  /* Authenticate and connect to the database. */
  if ((session->db_handle = db_connect(dbhost,dbuser,dbpasswd,dbname,1)) 
      == NULL)
  {
    fprintf(stderr,"Couldn't connect to database.\n");
    free(session);
    session = NULL;
  }
  else if (0)
  {
     /* Print out some diagnostic information */
  }
    
    /* Check the version of this module against the minimum version required by the DRMS database. */
    char *minversion = NULL;
    char jsocversion[128];
    
    if (!GetDBMinVersion(session, &minversion))
    {        
        /* No transaction to rollback. */
        db_disconnect(&session->db_handle);
        free(session);
        session = NULL;
    }
    else
    {
        jsoc_getversion(jsocversion, sizeof(jsocversion), NULL);
        
        if (!base_isvers(jsocversion, minversion))
        {
            char *schema = NULL;
            char *table = NULL;
            
            /* Save some information about which executable was too old. */
            if (get_namespace(DRMS_OLDCODETABLE, &schema, &table))
            {
                fprintf(stderr, "Out of memory in drms_connect_direct().\n");
            }
            else
            {
                /* If the old-code table doesn't exist, skip this. This is for finding people 
                 * with old code. */
                if (TableExists(session, schema, table))
                {
                    char binpath[PATH_MAX] = {0};
                    char query[1024];
                    struct timeval tv;
                    char *date = NULL;
                    int rowexists = 0;
                    DB_Text_Result_t *qres = NULL;
                    
                    if (readlink("/proc/self/exe", binpath, sizeof(binpath)) == -1)
                    {
                        fprintf(stderr, "Cannot obtain path to running binary.\n");
                    }
                    else
                    {
                        /* Check for existence of a record for this binary. Update the 
                         * date and version if the record does exist. */
                        snprintf(query, sizeof(query), "SELECT path FROM %s WHERE path = '%s'", DRMS_OLDCODETABLE, binpath);
                        
                        if ((qres = drms_query_txt(session, query)) == NULL)
                        {
                            /* Error */
                            fprintf(stderr, "Invalid database query: %s.\n", query);
                        }
                        else
                        {
                            rowexists = qres->num_rows > 0;
                            db_free_text_result(qres);
                            
                            gettimeofday(&tv, NULL);
                            date = ctime(&tv.tv_sec);
                            
                            if (rowexists)
                            {
                                /* Update */
                                snprintf(query, sizeof(query), "UPDATE %s set date = '%s', version = '%s' WHERE path = '%s'", DRMS_OLDCODETABLE, date, jsocversion, binpath);
                            }
                            else
                            {
                                /* Fetch the version from the DB version table. */
                                snprintf(query, sizeof(query), "INSERT INTO %s (path, date, version) VALUES ('%s', '%s', '%s')", DRMS_OLDCODETABLE, binpath, date, jsocversion);
                            }
                            
                            if (drms_dms(session, NULL, query))
                            {
                                fprintf(stderr, "Invalid database query: %s.\n", query);
                            }
                        }
                    }
                }
                
                free(table);
                free(schema);
            }
            
            fprintf(stderr, "Your DRMS module/client code (version %s) is incompatible with the current version of DRMS (which requires version %s). Please update.\n", jsocversion, minversion);
            /* No transaction to rollback. */
            db_disconnect(&session->db_handle);
            free(session);
            session = NULL;
            
            
        }
    }
  
    if (minversion)
    {
       free(minversion);
    }

    if (session)
    {
        if (sessionns) {
            session->sessionns = strdup(sessionns);
        } else {
            // get the default session namespace
            DB_Text_Result_t *tresult = NULL;
            char query[1024];
            
            // test existance of sessionns table
            sprintf(query, "select c.relname from pg_class c, pg_namespace n where n.oid = c.relnamespace and n.nspname='admin' and c.relname='sessionns'");
            tresult = db_query_txt(session->db_handle, query);
            if (tresult->num_rows) {
                db_free_text_result(tresult);    
                sprintf(query, "select sessionns from admin.sessionns where username='%s'", session->db_handle->dbuser);
                tresult = db_query_txt(session->db_handle, query);
                if (tresult->num_rows) {
                    session->sessionns = strdup(tresult->field[0][0]);
                } else {
                    goto bailout;
                }
            }
            db_free_text_result(tresult);
            return session;
        bailout:
            fprintf(stderr, "Can't get default session namespace\n");
            free(session);
            session = NULL;
            db_free_text_result(tresult);
        }
    }
    
  return session;
}
#endif

/* Clean up shutdown -- arta 4/28/2009
 * Server processes never exit this way - they call drms_server_abort, or 
 * drms_server_commit so make this call client-only. Also, 
 * remove drms_disconnect_now, which was a duplicate of drms_disconnect.
 * So, drms_disconnect means to disconnect from the server (eg, drms_server app). 
 * It won't affect the server, unless the client experienced an error while
 * running. If it passes abort == 1, then this will cause the server
 * (eg, drms_server) to terminate, but only after all other clients
 * have disconnected. */
#ifdef DRMS_CLIENT
void drms_disconnect(DRMS_Env_t *env, int abort)
{
   drms_send_commandcode(env->session->sockfd, DRMS_DISCONNECT);
   Writeint(env->session->sockfd, abort);
}

void drms_disconnect_now(DRMS_Env_t *env, int abort)
{
  drms_send_commandcode_noecho(env->session->sockfd, DRMS_DISCONNECT);
  Writeint(env->session->sockfd, abort);
}
#endif

int drms_commit(DRMS_Env_t *env)
{
  int status;

#ifndef DRMS_CLIENT
  if (env->session->db_direct)
  {
    return db_commit(env->session->db_handle);
  }
  else
#else
    XASSERT(env->session->db_direct==0);
#endif
  {
    drms_send_commandcode(env->session->sockfd, DRMS_COMMIT);
    status = Readint(env->session->sockfd);
    return status;
  }
}



int drms_rollback(DRMS_Session_t *session)
{
  int status;

#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_rollback(session->db_handle);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_ROLLBACK);
    status = Readint(session->sockfd);
    return status;
  }
}

DB_Text_Result_t *drms_query_txt(DRMS_Session_t *session,  char *query)
{
    char *errmsg = NULL;
    DB_Text_Result_t *rv = NULL;
#ifdef DEBUG
  printf("drms_query_txt: query = %s\n",query);
#endif
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_query_txt(session->db_handle, query);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
   drms_send_commandcode(session->sockfd, DRMS_TXTQUERY);
   rv = db_client_query_txt(session->sockfd, query, 0, &errmsg);
      
   if (errmsg)
   {
       snprintf(session->db_handle->errmsg, sizeof(session->db_handle->errmsg), "%s", errmsg);
       free(errmsg);
   }
      
      return rv;
  }
}

DB_Binary_Result_t *drms_query_bin(DRMS_Session_t *session,  char *query)
{
    char *errmsg = NULL;
    DB_Binary_Result_t *rv = NULL;
#ifdef DEBUG
  printf("drms_query_bin: query = %s\n",query);
#endif

#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_query_bin(session->db_handle, query);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_BINQUERY);
    rv = db_client_query_bin(session->sockfd, query, 0, &errmsg);
      
    if (errmsg)
    {
        snprintf(session->db_handle->errmsg, sizeof(session->db_handle->errmsg), "%s", errmsg);
        free(errmsg);
    }
      
    return rv;
  }
}



DB_Binary_Result_t *drms_query_binv(DRMS_Session_t *session,  char *query,
				    ...)
{
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int n,i;
  char *q;
  DB_Binary_Result_t *result;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 

  va_list ap;
  va_start(ap, query);   


#ifdef DEBUG
  printf("drms_query_binv: query = %s\n",query);
#endif
  
  /* Collect arguments. */
  q = (char *)query;
  n = 0;
  while (*q)
  {
    if (*q == '?')
    {
      if (n>=MAXARG)
      {
	fprintf(stderr,"drms_query_binv: Maximum number of arguments "
		"exceeded.\n");
	result = NULL;
	goto failure;
      }
      n++;
    }
    q++;
  }

  for (i=0; i<n; i++)
  {
    intype[i] = va_arg(ap, DB_Type_t);
    switch(intype[i])
    {
    case DB_CHAR:
      tchar = (db_char_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_char_t));
      memcpy(argin[i], &tchar, sizeof(db_char_t));
      break;
    case DB_INT1:
      tint1 = (db_int1_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_int1_t));
      memcpy(argin[i], &tint1, sizeof(db_int1_t));
      break;
    case DB_INT2:
      tint2 = (db_int2_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_int2_t));
      memcpy(argin[i], &tint2, sizeof(db_int2_t));
      break;
    case DB_INT4:
      tint4 = va_arg(ap, db_int4_t);
      argin[i] = alloca(sizeof(db_int4_t));
      memcpy(argin[i], &tint4, sizeof(db_int4_t));
      break;
    case DB_INT8:
      tint8 = va_arg(ap, db_int8_t);
      argin[i] = alloca(sizeof(db_int8_t));
      memcpy(argin[i], &tint8, sizeof(db_int8_t));
      break;
    case DB_FLOAT:
      tfloat = (db_float_t) va_arg(ap,double);/* Float is promoted to double */
      argin[i] = alloca(sizeof(db_float_t));
      memcpy(argin[i], &tfloat, sizeof(db_float_t));
      break;
    case DB_DOUBLE:
      tdouble = va_arg(ap, db_double_t);
      argin[i] = alloca(sizeof(db_double_t));
      memcpy(argin[i], &tdouble, sizeof(db_double_t));
      break;
    case DB_STRING:
    case DB_VARCHAR:
      argin[i] = va_arg(ap, db_string_t);
      break;
    }
  }
  result = drms_query_bin_array(session, query, n, intype, argin);

 failure:
  va_end(ap);
  return result;
}




DB_Binary_Result_t *drms_query_bin_array(DRMS_Session_t *session,  char *query,
					 int n_args, DB_Type_t *intype, 
					 void **argin)
{
#ifdef DEBUG
  printf("drms_query_bin_array: query = %s\n",query);
#endif
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_query_bin_array(session->db_handle,query, n_args, intype, 
			      argin);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_BINQUERY_ARRAY);
    return db_client_query_bin_array(session->sockfd, query, 0, n_args, intype,
				     argin);
  }
}



int drms_dms(DRMS_Session_t *session, int *row_count,  char *query)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_dms(session->db_handle, row_count, query);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_DMS);
    return db_client_dms(session->sockfd, row_count, query);
  }
}


/* DMS function with variable argument list. Collects arguments in
   list of void * and calls db_dms_array. */
int drms_dmsv(DRMS_Session_t *session, int *row_count,  char *query, 
	      int n_rows, ...)
{
  int status=1;
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int n,i;
  char *q;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 

  va_list ap;
  va_start(ap, n_rows);   
  
  /* Collect arguments. */
  q = (char *)query;
  n = 0;
  while (*q)
  {
    if (*q == '?')
    {
      if (n>=MAXARG)
      {
	fprintf(stderr,"ERRO in drms_dmsv: Maximum number of arguments "
		"exceeded.\n");
	goto failure;
      }
      n++;
    }
    q++;
  }

  for (i=0; i<n; i++)
  {
    intype[i] = va_arg(ap, DB_Type_t);
    if (n_rows == -1) /* Ac single set of arguments passed by value. */
    {    
      switch(intype[i])
      {
      case DB_CHAR:
	tchar = (db_char_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_char_t));
	memcpy(argin[i], &tchar, sizeof(db_char_t));
	break;
      case DB_INT1:
	tint1 = (db_int1_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int1_t));
	memcpy(argin[i], &tint1, sizeof(db_int1_t));
	break;
      case DB_INT2:
	tint2 = (db_int2_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int2_t));
	memcpy(argin[i], &tint2, sizeof(db_int2_t));
	break;
      case DB_INT4:
	tint4 = va_arg(ap, db_int4_t);
	argin[i] = alloca(sizeof(db_int4_t));
	memcpy(argin[i], &tint4, sizeof(db_int4_t));
	break;
      case DB_INT8:
	tint8 = va_arg(ap, db_int8_t);
	argin[i] = alloca(sizeof(db_int8_t));
	memcpy(argin[i], &tint8, sizeof(db_int8_t));
	break;
      case DB_FLOAT:
	tfloat = (db_float_t) va_arg(ap,double);
	argin[i] = alloca(sizeof(db_float_t));
	memcpy(argin[i], &tfloat, sizeof(db_float_t));
	break;
      case DB_DOUBLE:
	tdouble = va_arg(ap, db_double_t);
	argin[i] = alloca(sizeof(db_double_t));
	memcpy(argin[i], &tdouble, sizeof(db_double_t));
	break;
      case DB_STRING:
      case DB_VARCHAR:
	argin[i] = alloca(sizeof(char **));
	*((char **)argin[i]) = va_arg(ap, char *);
	break;
      }
    }
    else
    {
      argin[i] = va_arg(ap, void *);
    }
  }

  if (n_rows == -1)
    n_rows = 1;

  status = drms_dms_array(session, row_count, query, n_rows, n,
			  intype, argin );
 failure:
  va_end(ap);
  return status;
}


int drms_dms_array(DRMS_Session_t *session, int *row_count, 
		   char *query, int n_rows, int n_args, 
		   DB_Type_t *intype, void **argin )
{  
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_dms_array(session->db_handle, row_count, query, 
			n_rows, n_args, intype, argin );
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_DMS_ARRAY);
    return db_client_dms_array(session->sockfd, row_count, query, 
			       n_rows, n_args, intype, argin );
  }
}

/* DMS function with variable argument list. Collects arguments in
   list of void * and calls db_dms_array. */
int drms_bulk_insertv(DRMS_Session_t *session, char *table, 
		      int n_rows, int n_cols, ...)
{
  int status=1;
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int i;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 

  va_list ap;
  va_start(ap, n_cols);   
  
  for (i=0; i<n_cols; i++)
  {
    intype[i] = va_arg(ap, DB_Type_t);
    if (n_rows == -1) /* Ac single set of arguments passed by value. */
    {    
      switch(intype[i])
      {
      case DB_CHAR:
	tchar = (db_char_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_char_t));
	memcpy(argin[i], &tchar, sizeof(db_char_t));
	break;
      case DB_INT1:
	tint1 = (db_int1_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int1_t));
	memcpy(argin[i], &tint1, sizeof(db_int1_t));
	break;
      case DB_INT2:
	tint2 = (db_int2_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int2_t));
	memcpy(argin[i], &tint2, sizeof(db_int2_t));
	break;
      case DB_INT4:
	tint4 = va_arg(ap, db_int4_t);
	argin[i] = alloca(sizeof(db_int4_t));
	memcpy(argin[i], &tint4, sizeof(db_int4_t));
	break;
      case DB_INT8:
	tint8 = va_arg(ap, db_int8_t);
	argin[i] = alloca(sizeof(db_int8_t));
	memcpy(argin[i], &tint8, sizeof(db_int8_t));
	break;
      case DB_FLOAT:
	tfloat = (db_float_t) va_arg(ap,double);
	argin[i] = alloca(sizeof(db_float_t));
	memcpy(argin[i], &tfloat, sizeof(db_float_t));
	break;
      case DB_DOUBLE:
	tdouble = va_arg(ap, db_double_t);
	argin[i] = alloca(sizeof(db_double_t));
	memcpy(argin[i], &tdouble, sizeof(db_double_t));
	break;
      case DB_STRING:
      case DB_VARCHAR:
	argin[i] = alloca(sizeof(char **));
	*((char **)argin[i]) = va_arg(ap, char *);
	break;
      }
    }
    else
    {
      argin[i] = va_arg(ap, void *);
    }
  }

  if (n_rows == -1)
    n_rows = 1;

  status = drms_bulk_insert_array(session, table, n_rows, n_cols,
				  intype, argin );
  va_end(ap);
  return status;
}



int drms_bulk_insert_array(DRMS_Session_t *session,
			   char *table, int n_rows, int n_args, 
			   DB_Type_t *intype, void **argin )
{  
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_bulk_insert_array(session->db_handle, table, 
				n_rows, n_args, intype, argin );
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_BULK_INSERT_ARRAY);
    return db_client_bulk_insert_array(session->sockfd,table, 
				       n_rows, n_args, intype, argin );
  }
}


int drms_sequence_drop(DRMS_Session_t *session,  char *table)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_sequence_drop(session->db_handle, table);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_SEQUENCE_DROP);
    return db_client_sequence_drop(session->sockfd, table);
  }
}

int drms_sequence_create(DRMS_Session_t *session,  char *table)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_sequence_create(session->db_handle, table);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_SEQUENCE_CREATE);
    return db_client_sequence_create(session->sockfd, table);
  }
}


long long *drms_sequence_getnext(DRMS_Session_t *session,  char *table, int n)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_sequence_getnext_n(session->db_handle, table, n);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_SEQUENCE_GETNEXT);
    return db_client_sequence_getnext_n(session->sockfd, table, n); 
  }
}




long long drms_sequence_getcurrent(DRMS_Session_t *session,  char *table)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_sequence_getcurrent(session->db_handle, table);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_SEQUENCE_GETCURRENT);
    return db_client_sequence_getcurrent(session->sockfd, table);
  }
}



long long drms_sequence_getlast(DRMS_Session_t *session,  char *table)
{
#ifndef DRMS_CLIENT
  if (session->db_direct)
  {
    return db_sequence_getlast(session->db_handle, table);
  }
  else
#else
    XASSERT(session->db_direct==0);
#endif
  {
    drms_send_commandcode(session->sockfd, DRMS_SEQUENCE_GETLAST);
    return db_client_sequence_getlast(session->sockfd, table);
  }
}




long long *drms_alloc_recnum(DRMS_Env_t *env,  char *series, 
			     DRMS_RecLifetime_t lifetime, int n)
{
  int i, status;
  long long *seqnums;
  struct iovec vec[4];
  int tmp[2], len;

#ifndef DRMS_CLIENT
  if (env->session->db_direct)
  {
    seqnums = db_sequence_getnext_n(env->session->db_handle, series, n);
    if (lifetime == DRMS_TRANSIENT)
    {
       drms_server_transient_records(env, series, n, seqnums);
    }
    return seqnums;
  }
  else
#else
    XASSERT(env->session->db_direct==0);
#endif
  {
    int sockfd = env->session->sockfd;
    drms_send_commandcode(sockfd, DRMS_ALLOC_RECNUM);
    vec[1].iov_len = strlen(series);
    vec[1].iov_base = series;
    len = htonl(vec[1].iov_len);
    vec[0].iov_len = sizeof(len);
    vec[0].iov_base = &len;
    tmp[0] = htonl(n);
    vec[2].iov_len = sizeof(tmp[0]);
    vec[2].iov_base = &tmp[0];
    tmp[1] = htonl((int)lifetime);
    vec[3].iov_len = sizeof(tmp[1]);
    vec[3].iov_base = &tmp[1];
    Writevn(sockfd, vec, 4);
    
    status = Readint(sockfd);
    if (status==0)
    {
      seqnums = malloc(n*sizeof(long long));
      XASSERT(seqnums);
      for (i=0; i<n; i++)
	seqnums[i] = (long long)Readlonglong(sockfd);
      return seqnums;
    }
    else
      return NULL;
  }
}




/****** Client interface to Storage Units and Storage Unit Slots. *******/


/* Retrieve the storage unit with unique id given by sunum for reading. 
   The storage unit is retrieved by calling SUMS, if this has not
   already been done.
   Returns storage unit directory in su struct. */
DRMS_StorageUnit_t *drms_getunit_internal(DRMS_Env_t *env, char *series,
                                          long long sunum, int retrieve, 
                                          int gotosums, int *status)
{
  HContainer_t *scon=NULL;
  int stat;
  DRMS_StorageUnit_t *su;
  char hashkey[DRMS_MAXHASHKEYLEN];
  DRMS_Record_t *template;
#ifdef DRMS_CLIENT
  char *sudir;
  XASSERT(env->session->db_direct==0);
#endif
  
  if ((su = drms_su_lookup(env, series, sunum, &scon)) == NULL)
  {
    /* We didn't find the storage unit in the cache so we have to ask 
       the server or SUMS for it... */
    if (!scon)
    {
      scon = hcon_allocslot(&env->storageunit_cache,series);    
      hcon_init(scon, sizeof(DRMS_StorageUnit_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_su_freeunit, NULL);
    }
    /* Get a slot in the cache to insert a new entry in. */
    sprintf(hashkey,DRMS_SUNUM_FORMAT, sunum);
    su = hcon_allocslot(scon,hashkey);
#ifdef DEBUG
      printf("getunit: Got su = %p. Now has %d slots from '%s'\n",su, 
	     hcon_size(scon), series);
#endif

    /* Populate the fields in the struct. */
    su->sunum = sunum;
    su->mode = DRMS_READONLY; /* All storage units previously archived 
				 by SUMS are read-only. */
    su->nfree = 0;
    su->state = NULL;
    su->recnum = NULL;
    su->refcount = 0;
    /* Get a template for series info. */
    if ((template = drms_template_record(env, series,&stat)) == NULL)
      goto bailout;
    su->seriesinfo = template->seriesinfo;

    if (gotosums)
    {
       /* Talk to SUMS ONLY if the caller of this function permits it. */
#ifndef DRMS_CLIENT
       /* Send a query to SUMS for the storage unit directory. */

       stat = drms_su_getsudir(env, su, retrieve);
#ifdef DEBUG
       printf("drms_getunit: Got sudir from SUMS = '%s'\n",su->sudir);
#endif
#else
       {
          int len, retrieve_tmp;
          long long sunum_tmp;
          struct iovec vec[4];

          drms_send_commandcode(env->session->sockfd, DRMS_GETUNIT);
      
          /* Send seriesname,  sunum, and retrieve flag */
          vec[1].iov_len = strlen(series);
          vec[1].iov_base = series;
          len = htonl(vec[1].iov_len);
          vec[0].iov_len = sizeof(len);
          vec[0].iov_base = &len;
          sunum_tmp = htonll(sunum);
          vec[2].iov_len = sizeof(sunum_tmp);
          vec[2].iov_base = &sunum_tmp;
          retrieve_tmp = htonl(retrieve);
          vec[3].iov_len = sizeof(retrieve_tmp);
          vec[3].iov_base = &retrieve_tmp;
          Writevn(env->session->sockfd, vec, 4);
      
          stat = Readint(env->session->sockfd);
          if (stat == DRMS_SUCCESS)
          {
             sudir = receive_string(env->session->sockfd);
#ifdef DEBUG
             printf("drms_getunit: Got sudir from DRMS server = '%s', stat = %d\n",
                    sudir,stat);
#endif
             strncpy(su->sudir, sudir, sizeof(su->sudir));
             free(sudir);
          }
       }
#endif

       if (!strlen(su->sudir)) {
          hcon_remove(scon, hashkey);
          su = NULL;      
       }
    } /* if (gotosums) */
   
    if (stat)
    {
      hcon_remove(scon, hashkey);
      su = NULL;
    }
  }      
  else
    stat = DRMS_SUCCESS;
 
 bailout:
  if (status)
    *status = stat;
  return su;
}

DRMS_StorageUnit_t *drms_getunit(DRMS_Env_t *env, char *series,
				 long long sunum, int retrieve, int *status)
{
   /* OK to talk to SUMS if needed. */
   return drms_getunit_internal(env, series, sunum, retrieve, 1, status);
}

DRMS_StorageUnit_t *drms_getunit_nosums(DRMS_Env_t *env, char *series,
                                        long long sunum, int *status)
{
   /* Will NOT talk to SUMS. */
   return drms_getunit_internal(env, series, sunum, 0, 0, status);
}

/* series can be NULL, in which case the resulting storage units seriesinfo fields 
 * will also be NULL. */
int drms_getunits_internal(DRMS_Env_t *env, 
                           int n, 
                           DRMS_SuAndSeries_t *suandseries,
                           int retrieve,
                           int dontwait)
{
  HContainer_t *scon=NULL;
  int stat = DRMS_SUCCESS;
  DRMS_StorageUnit_t *su;
  char hashkey[DRMS_MAXHASHKEYLEN];
  DRMS_Record_t *template;
#ifdef DRMS_CLIENT
  char *sudir;
  XASSERT(env->session->db_direct==0);
#endif

  DRMS_StorageUnit_t **su_nc; // not cached su's
  int cnt;

#ifdef DRMS_CLIENT
  char **tosend = (char **)malloc(n * sizeof(char *));
#endif

  su_nc = malloc(n*sizeof(DRMS_StorageUnit_t *));
  XASSERT(su_nc);
#ifdef DEBUG
      printf("getunit: Called, n=%d, series=%s\n", n, series);
#endif
  /* Get a template for series info. */

  cnt = 0;
  for (int i = 0; i < n; i++) {
     if ((template = drms_template_record(env, suandseries[i].series, &stat)) == NULL)
       goto bailout;

    if ((su = drms_su_lookup(env, suandseries[i].series, suandseries[i].sunum, &scon)) == NULL) {
      if (!scon)
	{
	  scon = hcon_allocslot(&env->storageunit_cache, suandseries[i].series);    
	  hcon_init(scon, sizeof(DRMS_StorageUnit_t), DRMS_MAXHASHKEYLEN, 
		    (void (*)(const void *)) drms_su_freeunit, NULL);
	}
      /* Get a slot in the cache to insert a new entry in. */
      sprintf(hashkey,DRMS_SUNUM_FORMAT, suandseries[i].sunum);
      su = hcon_allocslot(scon,hashkey);
#ifdef DEBUG
      printf("getunit: Got su = %p. Now has %d slots from '%s'\n",su, 
	     hcon_size(scon), suandseries[i].series);
#endif

      /* Populate the fields in the struct. */
      su->sunum = suandseries[i].sunum;
      su->sudir[0] = '\0';
      su->mode = DRMS_READONLY; /* All storage units previously archived 
				   by SUMS are read-only. */
      su->nfree = 0;
      su->state = NULL;
      su->recnum = NULL;
      su->refcount = 0;
      su->seriesinfo = template->seriesinfo;
      su_nc[cnt] = su;

#ifdef DRMS_CLIENT
      /* need to send series names to drms_server - but only the ones for sunums that were not
       * already cached. */
      tosend[cnt] = suandseries[i].series;
#endif

      cnt++;
    } 
  }

#ifndef DRMS_CLIENT
    /* Send a query to SUMS for the storage unit directory. */
  if (cnt) {
    stat = drms_su_getsudirs(env, cnt, su_nc, retrieve, dontwait);
  }
#else
  int icnt;
  if (cnt) {
    long long *sunum_tmp;
    struct iovec *vec;

    drms_send_commandcode(env->session->sockfd, DRMS_GETUNITS);

    /* Send cnt, series names, sunums, retrieve, and dontwait to drms_server */
    
    /* pass n first */
    Writeint(env->session->sockfd, cnt);

    /* pass n seriesnames next */
    for (icnt = 0; icnt < cnt; icnt++)
    {
       send_string(env->session->sockfd, tosend[icnt]);
    }

    /* pass n sunums */
    vec = malloc(cnt*sizeof(struct iovec)); 
    XASSERT(vec);     
    sunum_tmp = malloc(cnt*sizeof(long long));
    XASSERT(sunum_tmp);
    for (int i = 0; i < cnt; i++) {
      sunum_tmp[i] = htonll(su_nc[i]->sunum);
      vec[i].iov_len = sizeof(sunum_tmp[i]);
      vec[i].iov_base = &sunum_tmp[i];
    }
    Writevn(env->session->sockfd, vec, cnt);
    free(sunum_tmp);
    free(vec);

    /* retrieve */
    Writeint(env->session->sockfd, retrieve);

    /* dontwait */
    Writeint(env->session->sockfd, dontwait);
      
    stat = Readint(env->session->sockfd);
    if (stat == DRMS_SUCCESS) {
      if (!dontwait) {
	for (int i = 0; i < cnt; i++) {
	  sudir = receive_string(env->session->sockfd);
	  strncpy(su_nc[i]->sudir, sudir, sizeof(su->sudir));
	  free(sudir);
	}
      }
    } 
  }
#endif
  for (int i = 0; i < cnt; i++) {
    if (stat || !strlen(su_nc[i]->sudir)) {
      drms_su_lookup(env, su_nc[i]->seriesinfo->seriesname, su_nc[i]->sunum, &scon);
      sprintf(hashkey,DRMS_SUNUM_FORMAT, su_nc[i]->sunum);
      hcon_remove(scon, hashkey);
    }
  }

 bailout:
  free(su_nc);

  return stat;
}

int drms_getunits(DRMS_Env_t *env, 
                  char *series,
		  int n, 
                  long long *sunum, 
		  int retrieve,
		  int dontwait)
{
   DRMS_SuAndSeries_t *arr = malloc(n * sizeof(DRMS_SuAndSeries_t));
   int isu;
   char *dup = strdup(series);
   int ret = 0;

   if (arr && dup)
   {
      for (isu = 0; isu < n; isu++)
      {
         arr[isu].sunum = sunum[isu];
         arr[isu].series = dup;
      }

      ret = drms_getunits_internal(env, n, arr, retrieve, dontwait);

      free(dup);
      free(arr);
   }

   return ret;
}

/* Can specify SUNUMs from more than one series. */
int drms_getunits_ex(DRMS_Env_t *env, 
                     int num, 
                     DRMS_SuAndSeries_t *suandseries, 
                     int retrieve,
                     int dontwait)
{
   return drms_getunits_internal(env, num, suandseries, retrieve, dontwait);
}

int drms_getsuinfo(DRMS_Env_t *env, long long *sunums, int nReqs, SUM_info_t **infostructs)
{
   int status = DRMS_SUCCESS;

#ifndef DRMS_CLIENT
   /* Send a query to SUMS for the storage unit directory. */
   status = drms_su_getinfo(env, sunums, nReqs, infostructs);
#else
   DB_Type_t type;

   SUM_info_t *info = NULL;

   int isunum;
   long long onesunum;
   long long sunum;
   char *online_loc = NULL;
   char *online_status = NULL;
   char *archive_status = NULL;
   char *offsite_ack = NULL;
   char *history_comment= NULL;
   char *owning_series = NULL;
   int storage_group;
   double *pbytes = NULL;
   double bytes;
   char *creat_date = NULL;
   char *username = NULL;
   char *arch_tape = NULL;
   int arch_tape_fn;
   char *arch_tape_date = NULL;
   char *safe_tape = NULL;
   int safe_tape_fn;
   char *safe_tape_date = NULL;
   int pa_status;
   int pa_substatus;
   char *effective_date = NULL;

   drms_send_commandcode(env->session->sockfd, DRMS_GETSUINFO);

   Writeint(env->session->sockfd, nReqs);
   for (isunum = 0; isunum < nReqs; isunum++)
   {
      onesunum = sunums[isunum];
      Writelonglong(env->session->sockfd, onesunum);
   }

   status = Readint(env->session->sockfd);

   if (status == DRMS_SUCCESS)
   {
      for (isunum = 0; isunum < nReqs; isunum++)
      {
         /* Have to convert each field in the SUM_info_t structure from network to host byte order. */
         sunum = Readlonglong(env->session->sockfd);
         online_loc = receive_string(env->session->sockfd); /* must free mem */
         online_status = receive_string(env->session->sockfd); /* must free mem */
         archive_status = receive_string(env->session->sockfd); /* must free mem */
         offsite_ack = receive_string(env->session->sockfd); /* must free mem */
         history_comment = receive_string(env->session->sockfd); /* must free mem */
         owning_series = receive_string(env->session->sockfd); /* must free mem */
         storage_group = Readint(env->session->sockfd);
         pbytes = (double *)Read_dbtype(&type, env->session->sockfd); /* must free mem */
         bytes = *pbytes;
         creat_date = receive_string(env->session->sockfd); /* must free mem */
         username = receive_string(env->session->sockfd); /* must free mem */
         arch_tape = receive_string(env->session->sockfd); /* must free mem */
         arch_tape_fn = Readint(env->session->sockfd);
         arch_tape_date = receive_string(env->session->sockfd); /* must free mem */
         safe_tape = receive_string(env->session->sockfd); /* must free mem */
         safe_tape_fn = Readint(env->session->sockfd);
         safe_tape_date = receive_string(env->session->sockfd); /* must free mem */
         pa_status = Readint(env->session->sockfd);
         pa_substatus = Readint(env->session->sockfd);
         effective_date = receive_string(env->session->sockfd); /* must free mem */

         infostructs[isunum] = (SUM_info_t *)malloc(sizeof(SUM_info_t));
         info = infostructs[isunum];

         info->sunum = sunum;
         snprintf(info->online_loc, sizeof(info->online_loc), "%s", online_loc);
         snprintf(info->online_status, sizeof(info->online_status), "%s", online_status);
         snprintf(info->archive_status, sizeof(info->archive_status), "%s", archive_status);
         snprintf(info->offsite_ack, sizeof(info->offsite_ack), "%s", offsite_ack);
         snprintf(info->history_comment, sizeof(info->history_comment), "%s", history_comment);
         snprintf(info->owning_series, sizeof(info->owning_series), "%s", owning_series);
         info->storage_group = storage_group;
         info->bytes = bytes;
         snprintf(info->creat_date, sizeof(info->creat_date), "%s", creat_date);
         snprintf(info->username, sizeof(info->username), "%s", username);
         snprintf(info->arch_tape, sizeof(info->arch_tape), "%s", arch_tape);
         info->arch_tape_fn = arch_tape_fn;
         snprintf(info->arch_tape_date, sizeof(info->arch_tape_date), "%s", arch_tape_date);
         snprintf(info->safe_tape, sizeof(info->safe_tape), "%s", safe_tape);
         info->safe_tape_fn = safe_tape_fn;      
         snprintf(info->safe_tape_date, sizeof(info->safe_tape_date), "%s", safe_tape_date);
         info->pa_status = pa_status;
         info->pa_substatus = pa_substatus;
         snprintf(info->effective_date, sizeof(info->effective_date), "%s", effective_date);

         free(online_loc);
         free(online_status);
         free(archive_status);
         free(offsite_ack);
         free(history_comment);
         free(owning_series);
         free(pbytes);
         free(creat_date);
         free(username);
         free(arch_tape);
         free(arch_tape_date);
         free(safe_tape);
         free(safe_tape_date);
         free(effective_date);

         /* That was fun! */
      }
   }
#endif

   return status;
}

int drms_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retrieve)
{
   int status = DRMS_SUCCESS;

#ifdef DRMS_CLIENT
   XASSERT(env->session->db_direct==0);
#endif
  
#ifndef DRMS_CLIENT
   /* Send a query to SUMS for the storage unit directory. */
   status = drms_su_getsudir(env, su, retrieve);
#else
   {
      char *sudir;

      drms_send_commandcode(env->session->sockfd, DRMS_GETSUDIR);
      
      /* Send SUNUM and retrieve */
      /* The goal of this function is to write the storage unit directory into 
       * the DRMS_StorageUnit_t passed into it. But we cannot pass the DRMS_StorageUnit_t *
       * to drms_server since the latter is in a different process.
       *
       * So, just pass the essential bit of information - the SUNUM. In drms_server
       * construct a DRMS_StorageUnit_t that contains this SUNUM, then 
       * call drms_su_getsudir() with this  DRMS_StorageUnit_t.  drms_server
       * will then pass the SUDIR back, and that will be stuffed into su.
       */
      //Writen_ntoh(env->session->sockfd, su, sizeof(DRMS_StorageUnit_t *));
      Writelonglong(env->session->sockfd, su->sunum);
      Writeint(env->session->sockfd, retrieve);

      sudir = receive_string(env->session->sockfd);
      status = Readint(env->session->sockfd);

      if (status == DRMS_SUCCESS)
      {
         snprintf(su->sudir, DRMS_MAXPATHLEN, "%s", sudir);
      }
      else if (status == DRMS_REMOTESUMS_TRYLATER)
      {
         *(su->sudir) = '\0';
      }
   }
#endif

   return status;
}

int drms_getsudirs(DRMS_Env_t *env, DRMS_StorageUnit_t **su, int num, int retrieve, int dontwait)
{
   int status = DRMS_SUCCESS;

#ifdef DRMS_CLIENT
   XASSERT(env->session->db_direct==0);
#endif
  
#ifndef DRMS_CLIENT
   /* Send a query to SUMS for the storage unit directory. */
   status = drms_su_getsudirs(env, num, su, retrieve, dontwait);
#else
   {
      char *sudir;
      DRMS_StorageUnit_t *onesu = NULL;
      int isu;

      drms_send_commandcode(env->session->sockfd, DRMS_GETSUDIRS);
      
      /* Send SUNUM and retrieve */
      /* The goal of this function is to write the storage unit directory into 
       * the DRMS_StorageUnit_t passed into it. But we cannot pass the DRMS_StorageUnit_t *
       * to drms_server since the latter is in a different process.
       *
       * So, just pass the essential bit of information - the SUNUM. In drms_server
       * construct a DRMS_StorageUnit_t that contains this SUNUM, then 
       * call drms_su_getsudir() with this  DRMS_StorageUnit_t.  drms_server
       * will then pass the SUDIR back, and that will be stuffed into su.
       */
      Writeint(env->session->sockfd, num);
      for (isu = 0; isu < num; isu++)
      {
         onesu = su[isu];
         Writelonglong(env->session->sockfd, onesu->sunum);
      }

      Writeint(env->session->sockfd, retrieve);
      Writeint(env->session->sockfd, dontwait);

      status = Readint(env->session->sockfd);

      for (isu = 0; isu < num; isu++)
      {
         onesu = su[isu];
         sudir = receive_string(env->session->sockfd);

         if (status == DRMS_SUCCESS)
         {
            snprintf(onesu->sudir, DRMS_MAXPATHLEN, "%s", sudir);
         }
         else if (status == DRMS_REMOTESUMS_TRYLATER)
         {
            *(onesu->sudir) = '\0';
         }
      }
   }
#endif

   return status;
}


/* Client version. */
int drms_newslots_internal(DRMS_Env_t *env, int n, char *series, long long *recnum,
                           DRMS_RecLifetime_t lifetime, int *slotnum, 
                           DRMS_StorageUnit_t **su,
                           int createslotdirs,
                           int gotosums)
{
  int status, i;
  long long sunum;
  DRMS_Record_t *template;
  char *sudir;
  char hashkey[DRMS_MAXHASHKEYLEN];
  HContainer_t *scon=NULL;

  if (!slotnum || !su || !series)
    return 1;
  
#ifndef DRMS_CLIENT
  if (env->session->db_direct)
  {
     if (gotosums)
     {
        /* Allows SUMS access. */
        return drms_su_newslots(env, n, series, recnum, lifetime, slotnum, su, createslotdirs);
     }
     else
     {
        /* Does not allow SUMS access. */
        return drms_su_newslots_nosums(env, n, series, recnum, lifetime, slotnum, su, createslotdirs);
     }
  }
  else
#else
    XASSERT(env->session->db_direct==0);
#endif
  {
    int tmp[6], *t;
    long long *ltmp, *lt;
    struct iovec *vec, *v;

    drms_send_commandcode(env->session->sockfd, DRMS_NEWSLOTS);
    if (n==0)
    {
      send_string(env->session->sockfd, series); 
      Writeint(env->session->sockfd, n);
      return 0;
    }

    ltmp = malloc(n*sizeof(long long));
    XASSERT(ltmp);
    vec = malloc((n+6)*sizeof(struct iovec));
    XASSERT(vec);

    /* Send series, n, lifetime, createslotdirs flag, and the record numbers with a
       single writev system call. */
    t = tmp; v = vec;
    net_packstring(series, t++, v); v+=2;
    net_packint(n, t++, v++);
    net_packint((int)lifetime, t++, v++);
    net_packint(createslotdirs, t++, v++);

    /* ART - Add gotosums flag */
    net_packint(gotosums, t++, v++);

    lt = ltmp;
    for (i=0; i<n; i++)
      net_packlonglong(recnum[i], lt++, v++);
    Writevn(env->session->sockfd, vec, n+6);
    free(vec);
    free(ltmp);
   
    status = Readint(env->session->sockfd);
    if (status==DRMS_SUCCESS)
    {
      if ((template = drms_template_record(env, series, &status))==NULL)
	return status;
      for (i=0; i<n; i++)
      {
	sunum = Readlonglong(env->session->sockfd);      
	sudir = receive_string(env->session->sockfd);
	slotnum[i] = Readint(env->session->sockfd);      

	/* Find the associated storage unit in the cache. */
	scon = NULL;
	if ((su[i] = drms_su_lookup(env, series, sunum, &scon)) == NULL)
	{
	  /* We didn't find the storage unit in the cache. 
	     Insert a new entry... */
	  if (!scon)
	  {
	    scon = hcon_allocslot(&env->storageunit_cache,series);    
	    hcon_init(scon, sizeof(DRMS_StorageUnit_t), DRMS_MAXHASHKEYLEN, 
		      (void (*)(const void *)) drms_su_freeunit, NULL);
	  }
	  /* Get a cache slot to insert a new entry in. */
	  sprintf(hashkey,DRMS_SUNUM_FORMAT, sunum);
	  su[i] = hcon_allocslot(scon,hashkey);
	  
	  /* Initialize SU structure. */
	  su[i]->sunum = sunum;
	  strncpy(su[i]->sudir, sudir, sizeof(su[i]->sudir));
	  su[i]->seriesinfo = template->seriesinfo;
	  su[i]->mode = DRMS_READWRITE;
	  su[i]->nfree = 0; /* The client should never look at this. */
	  su[i]->state = NULL; /* The client should never look at this. */
	  su[i]->recnum = NULL; /* The client should never look at this. */
	  su[i]->refcount = 0;
	}
	else
	{
	  if (strcmp(su[i]->sudir, sudir))
	  {
	    fprintf(stderr,"ERROR: Storage unit %s:#%lld seen with different "
		    "sudirs: '%s' != '%s'", 
		    series, sunum, su[i]->sudir, sudir);
	    return 1;
	  }
	}
#ifdef DEBUG
	printf("Client received sunum = %lld\n",su[i]->sunum);
	printf("Client received sudir = '%s'\n",sudir);
	printf("Client: su->sudir = '%s'\n",su[i]->sudir);
#endif
	free(sudir);
      }
    }
    else
      for (i=0; i<n; i++)	
	su[i] = NULL;
  }

  return status;
}

int drms_newslots(DRMS_Env_t *env, int n, char *series, long long *recnum,
		  DRMS_RecLifetime_t lifetime, int *slotnum, 
		  DRMS_StorageUnit_t **su,
                  int createslotdirs)
{
   /* Allow SUMS access if necessary (it won't always be necessary). */
   return drms_newslots_internal(env, n, series, recnum, lifetime, slotnum, su, createslotdirs, 1);
}

int drms_newslots_nosums(DRMS_Env_t *env, int n, char *series, long long *recnum,
                         DRMS_RecLifetime_t lifetime, int *slotnum, 
                         DRMS_StorageUnit_t **su,
                         int createslotdirs)
{
   /* Does NOT allow SUMS access. Returns an error if SUMS access is required for successful completion. */
   return drms_newslots_internal(env, n, series, recnum, lifetime, slotnum, su, createslotdirs, 1);
}


/* Mark a storage unit slot as either DRMS_SLOT_FREE, DRMS_SLOT_FULL, or
   DRMS_SLOT_TEMP. */
int drms_slot_setstate(DRMS_Env_t *env, char *series, long long sunum, 
		       int slotnum, int state)
{ 
  
#ifndef DRMS_CLIENT
  if (env->session->db_direct)
  {
    if (state == DRMS_SLOT_FREE)
      return drms_su_freeslot(env, series, sunum, slotnum);
    else
      return drms_su_markslot(env, series, sunum, slotnum, &state) != NULL;
  }
  else
#else
    XASSERT(env->session->db_direct==0);
#endif
  {
    int len,tmp[3];
    long long sunum_tmp;
    struct iovec vec[5];

    drms_send_commandcode(env->session->sockfd, DRMS_SLOT_SETSTATE);

    /* Send seriesname and sunum */
    vec[1].iov_len = strlen(series);
    vec[1].iov_base = series;
    len = htonl(vec[1].iov_len);
    vec[0].iov_len = sizeof(len);
    vec[0].iov_base = &len;
    sunum_tmp = htonll(sunum);
    vec[2].iov_len = sizeof(sunum_tmp);
    vec[2].iov_base = &sunum_tmp;
    tmp[1] = htonl(slotnum);
    vec[3].iov_len = sizeof(tmp[1]);
    vec[3].iov_base = &tmp[1];
    tmp[2] = htonl(state);
    vec[4].iov_len = sizeof(tmp[2]);
    vec[4].iov_base = &tmp[2];
    Writevn(env->session->sockfd, vec, 5);

    return Readint(env->session->sockfd);
  }    
}

int drms_dropseries(DRMS_Env_t *env, const char *series, DRMS_Array_t *vec)
{
#ifndef DRMS_CLIENT
   return drms_server_dropseries_su(env, series, vec);
#else
   XASSERT(env->session->db_direct==0);
        
   if (!env->session->db_direct)
   {
      /* This is a DRMS client - must remove deleted series from 
         DRMS server cache. This will also call SUM_delete_series via
         drms_server_dropseries_su() (which can be called by DRMS servers ONLY). */
      drms_send_commandcode(env->session->sockfd, DRMS_DROPSERIES);
      send_string(env->session->sockfd, series);

      /* If this is a sock-module, must pass the vector SUNUM by SUNUM 
       * to drms_server. */
      int irow = -1;
      long long *data = (long long *)vec->data;

      /* Write number of rows */
      Writeint(env->session->sockfd, vec->axis[1]);

      /* only one column*/
      for (irow = 0; irow < vec->axis[1]; irow++)
      {
         Writelonglong(env->session->sockfd, data[irow]);
      }
   }

   return 0;
#endif
}

/* Create a new series in th database using the given record as template 
   and insert the template in the series cache. */
int drms_create_series(DRMS_Record_t *rec, int perms)
{
  int status;
  DRMS_Record_t *template;
  DRMS_Env_t *env;
  char *series;

  series = rec->seriesinfo->seriesname;
  env = rec->env;
  //if (hcon_member_lower(&env->series_cache, series))
  /* Must use drms_template_record() in case series has not yet been cached in series_cache. This function 
   * will cache it. */
  if (drms_template_record(env, series, &status) != NULL)
  {
    fprintf(stderr,"drms_create_series(): "
	    "ERROR: Cannot create series '%s' because it already exists.\n", series);
    return 1;
  }
  template = (DRMS_Record_t *)hcon_allocslot_lower(&env->series_cache, series);
  drms_copy_record_struct(template, rec);  

  // Set pidx keywords pointers 
  for (int i = 0; i < template->seriesinfo->pidx_num; i++) {
    template->seriesinfo->pidx_keywords[i] = drms_keyword_lookup(template, rec->seriesinfo->pidx_keywords[i]->info->name, 0);
  }

  status = drms_insert_series(env->session, 0, template, perms);
  if (!status && !env->session->db_direct)
  {
    drms_send_commandcode(env->session->sockfd, DRMS_NEWSERIES);
    send_string(env->session->sockfd, series);
  }
  return status;
}


/* Update an existing series and replace the template in the series cache. */
int drms_update_series(DRMS_Record_t *rec, int perms)
{
  int status;
  DRMS_Record_t *template;
  DRMS_Env_t *env;
  char *series;

  series = rec->seriesinfo->seriesname;
  env = rec->env;

  /* Must use drms_template_record() in case series has not yet been cached in series_cache. This function 
   * will cache it. */
  template = drms_template_record(env, series, &status);

  if (!template)
  {
    fprintf(stderr,"ERROR: Cannot update series '%s' because it does not "
	    "exists.\n", series);
    return 1;
  }
  drms_copy_record_struct(template, rec);  
  status = drms_insert_series(env->session, 1, template, perms);

  /* If this is a client then tell the server that a new series 
     now exists. */     
  if (!status && !env->session->db_direct)
  {
    drms_send_commandcode(env->session->sockfd, DRMS_NEWSERIES);
    send_string(env->session->sockfd, series);
  }
  return status;
}

/* Create a new series on-the-fly, using a template record. This template
 * must have been created with drms_create_recproto().  This function 
 * owns template. 
 */
int drms_create_series_fromprototype(DRMS_Record_t **prototype, 
				     const char *outSeriesName, 
				     int perms)
{
   int status = DRMS_SUCCESS;
   
   if (prototype && *prototype && strlen(outSeriesName) < DRMS_MAXSERIESNAMELEN)
   {
      char *user = getenv("USER");
      DRMS_Record_t *proto = *prototype;

      strcpy(proto->seriesinfo->seriesname, outSeriesName);

      // Discard "Owner", fill it with the dbuser
      if (proto->env->session->db_direct && 
          proto->env->session->db_handle->dbuser && 
          strlen(proto->env->session->db_handle->dbuser)) 
      {
	 strcpy(proto->seriesinfo->owner, proto->env->session->db_handle->dbuser);
      }
      else if (!proto->seriesinfo->owner || strlen(proto->seriesinfo->owner) == 0)
      {
         if (user && strlen(user) < DRMS_MAXOWNERLEN)
         {
            strcpy(proto->seriesinfo->owner, user);
         }
         else
         {
            strcpy(proto->seriesinfo->owner, "unknown");
         }
      }

      status = drms_create_series(proto, perms);
   }
   else
   {
      status = DRMS_ERROR_INVALIDDATA;
   }

   if (prototype)
   {
      drms_destroy_recproto(prototype);
   }

   return status;
}

int drms_series_hastableprivs(DRMS_Env_t *env, const char *schema, const char *table, const char *priv)
{
   char query[DRMS_MAXQUERYLEN];
   int result = 0;
   DB_Text_Result_t *qres = NULL;

   sprintf(query, "select * from information_schema.table_privileges where table_schema = '%s' and table_name ~~* '%s' and privilege_type = '%s'", schema, table, priv);

   if ((qres = drms_query_txt(env->session, query)) != NULL && qres->num_rows != 0) 
   {
      result = 1;
   }

   if (qres)
   {
      db_free_text_result(qres);
   }

   return result;
}

static int drms_series_candosomething(DRMS_Env_t *env, const char *series, const char *perm)
{
   int result = 0;
   char *namespace = NULL;
   char *sname = NULL;

   if (!get_namespace(series, &namespace, &sname))
   {
      result = drms_series_hastableprivs(env, namespace, sname, perm);
   }

   if (namespace)
   {
      free(namespace);
   }

   if (sname)
   {
      free(sname);
   }

   return result;
}

int drms_series_cancreaterecord(DRMS_Env_t *env, const char *series)
{
   return drms_series_candosomething(env, series, "INSERT");
}

int drms_series_candeleterecord(DRMS_Env_t *env, const char *series)
{
    return drms_series_candosomething(env, series, "DELETE");
}

int drms_series_canupdaterecord(DRMS_Env_t *env, const char *series)
{
   return drms_series_candosomething(env, series, "UPDATE");
}

int drms_query_tabexists(DRMS_Session_t *session, const char *ns, const char *tab, int *status)
{
   int result = 0;
   char query[DRMS_MAXQUERYLEN];
   DB_Text_Result_t *qres = NULL;

   if (status)
   {
      *status = DRMS_SUCCESS;
   }

   snprintf(query, sizeof(query), "SELECT * FROM pg_tables WHERE schemaname ILIKE '%s' AND tablename ILIKE '%s'", ns, tab);

   if ((qres = drms_query_txt(session, query)) == NULL)
   {
      if (status)
      {
         *status = DRMS_ERROR_BADDBQUERY;
      }

      fprintf(stderr, "Bad database query '%s'\n", query);
   }
   else if (qres->num_rows > 0)
   {
      result = 1;
   }

   if (qres)
   {
      db_free_text_result(qres);
      qres = NULL;
   }

   return result;
}

/*
  SELECT pg_catalog.pg_get_userbyid(T1.relowner) AS owner 
     FROM pg_catalog.pg_class AS T1, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'su_arta') AS T2 
     WHERE T1.relnamespace = T2.oid AND T1.relname = 'fd_m_96m_lev18'
*/
int drms_series_isdbowner(DRMS_Env_t *env, const char *series, int *status)
{
   int isowner = 0;
   char *nspace = NULL;
   char *relname = NULL;
   int istat = DRMS_SUCCESS;
   DB_Text_Result_t *qres = NULL;

   if (!get_namespace(series, &nspace, &relname))
   {
      char query[1024];
      strtolower(nspace);
      strtolower(relname);

      snprintf(query, sizeof(query), "SELECT pg_catalog.pg_get_userbyid(T1.relowner) AS owner FROM pg_catalog.pg_class AS T1, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '%s') AS T2 WHERE T1.relnamespace = T2.oid AND T1.relname = '%s'", nspace, relname);

      if ((qres = drms_query_txt(env->session, query)) != NULL)
      {
         if (qres->num_cols == 1 && qres->num_rows == 1)
         {
            char *dbuser = NULL;

#ifndef DRMS_CLIENT
            dbuser = env->session->db_handle->dbuser;
#else
            drms_send_commandcode(env->session->sockfd, DRMS_GETDBUSER);
            dbuser = receive_string(env->session->sockfd);
#endif
            if (strcmp(qres->field[0][0], dbuser) == 0)
            {
               isowner = 1;
            }

#ifdef DRMS_CLIENT
            free(dbuser);
            dbuser = NULL;
#endif

         }
         else
         {
            fprintf(stderr, "Unexpected database response to query '%s'.\n", query);
            istat = DRMS_ERROR_BADDBQUERY;
         }

         db_free_text_result(qres);
         qres = NULL;
      }
      else
      {
         fprintf(stderr, "Invalid query '%s'.\n", query);
         istat = DRMS_ERROR_BADDBQUERY;
      }

      free(nspace);
      free(relname);
   }
   else
   {
      istat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = istat;
   }

   return isowner;
}

/* Table su_production.produsers has a single column, "user". This table is on the hmidb host, not the hmidb2 host. */
int drms_client_isproduser(DRMS_Env_t *env, int *status)
{
    int isproduser = 0;
    char query[1024];
    char *dbuser = NULL;
    char *dbhost = NULL; /* The host without the port. */
    char *pc = NULL;
    DB_Text_Result_t *qres = NULL;
    int istat = DRMS_SUCCESS;
    int forceconn = 0;
    DB_Handle_t *dbh = NULL;
    
#ifdef PRODUSER_DBHOST
    /* Derive the db host machine from the PRODUSER_DBHOST string (which has the format host:port). */
    dbhost = strdup(PRODUSER_DBHOST);
    
    if (!dbhost)
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    else
    {
        pc = strchr(dbhost, ':');
        if (pc)
        {
            *pc = '\0';
        }    
    }
        
    if (istat == DRMS_SUCCESS)
    {
#ifndef DRMS_CLIENT
        dbuser = env->session->db_handle->dbuser;
        
        /* For a server module, hostname is in drms_env->session->db_handle->dbhost. */
        forceconn = (strcasecmp(dbhost, env->session->db_handle->dbhost) != 0);
#else
        drms_send_commandcode(env->session->sockfd, DRMS_GETDBUSER);
        dbuser = receive_string(env->session->sockfd);
        
        if (!dbuser)
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        /* For a sock module, the db host to which it has access is the db host that the 
         * serving drms_server is connected to. And I don't think there is a way to 
         * determine to which db host drms_server is connected, so we'll HAVE TO 
         * connect to PRODUSER_DBHOST here, regardless of the existing connection between 
         * drms_server and a db. */
        forceconn = 1;
#endif
    }
    
    if (istat == DRMS_SUCCESS)
    {
        if (forceconn)
        {
            /* The caller wants to obtain the list of production users on a db host to which 
             * this module has no connection */
            
            /* Use db_connect to connect to the PRODUSER_DBNAME db on PRODUSER_DBHOST. Steal dbuser from the existing, 
             * irrelevant db connection. */
            if ((dbh = db_connect(PRODUSER_DBHOST, dbuser, NULL, PRODUSER_DBNAME, 1)) == NULL)
            {
                fprintf(stderr,"Couldn't connect to %s database on %s.\n", PRODUSER_DBNAME, PRODUSER_DBHOST);
                istat = DRMS_ERROR_CANTCONNECTTODB;
            }
        }
        else
        {
            dbh = env->session->db_handle;
        }
    }
    
    if (istat == DRMS_SUCCESS)
    {
        snprintf(query, sizeof(query), "SELECT %s FROM %s WHERE %s = '%s'", PRODUSER_COLUSER, PRODUSER_PRODTAB, PRODUSER_COLUSER, dbuser);
        
        if ((qres = db_query_txt(dbh, query)) != NULL)
        {
            if (qres->num_cols == 1 && qres->num_rows == 1)
            {
                isproduser = 1;
            }
            
            db_free_text_result(qres);
        }
        else
        {
            fprintf(stderr, "Unexpected database response to query '%s'.\n", query);
            
            /* Don't set an error status - the site may not have a PRODUSER table at all. The behavior in that
             * case should be identical to the behavior where the table exists, but is empty. */
        }
    }
    
    if (forceconn && dbh)
    {
        /* Need to terminate new db connection. */
        db_disconnect(&dbh);
    }
    
#ifdef DRMS_CLIENT
    if (dbuser)
    {
        free(dbuser);
        dbuser = NULL;
    }
#endif

    if (status)
    {
        *status = istat;
    }
#endif
    
    return isproduser;
}

#ifdef DRMS_CLIENT

sem_t *gShutdownsem = NULL; /* synchronization among signal thread, main thread, 
                               sums thread, and server threads during shutdown */
DRMS_Shutdown_State_t gShutdown; /* shudown state following receipt by DRMS of a signal that 
                                 * causes module termination */

void drms_lock_client(DRMS_Env_t *env)
{
  if (env->drms_lock == NULL)
  {
     return;
  }
  else
  {
     pthread_mutex_lock(env->drms_lock);
  }
}

void drms_unlock_client(DRMS_Env_t *env)
{
  if (env->drms_lock == NULL)
  {
     return;
  }
  else
  {
     pthread_mutex_unlock( env->drms_lock );
  }
}

/* 0 means success - lock was acquired */
int drms_trylock_client(DRMS_Env_t *env)
{
   if (env->drms_lock == NULL)
   {
      return 0;
   }
   else
   {
      return pthread_mutex_trylock(env->drms_lock);
   }
}

sem_t *drms_client_getsdsem(void)
{
   return gShutdownsem;
}

void drms_client_initsdsem(void)
{
  /* create shutdown (unnamed POSIX) semaphore */
  gShutdownsem = malloc(sizeof(sem_t));

  /* Initialize semaphore to 1 - "unlocked" */
  sem_init(gShutdownsem, 0, 1);
  
  /* Initialize shutdown state to kSHUTDOWN_UNINITIATED - no shutdown requested */
  gShutdown = kSHUTDOWN_UNINITIATED;
}

void drms_client_destroysdsem(void)
{
   if (gShutdownsem)
   {
      sem_destroy(gShutdownsem);
      free(gShutdownsem);
      gShutdownsem = NULL;
   }
}

DRMS_Shutdown_State_t drms_client_getsd(void)
{
   return gShutdown;
}

void drms_client_setsd(DRMS_Shutdown_State_t st)
{
   gShutdown = st;
}

int drms_client_registercleaner(DRMS_Env_t *env, CleanerData_t *data)
{
   int gotlock = 0;
   int ok = 1;

   gotlock = (drms_trylock_client(env) == 0);

   if (gotlock)
   {
      if (env->cleaners == NULL)
      {
         /* No cleaner nodes yet - create list and add first one. */
         env->cleaners = list_llcreate(sizeof(CleanerData_t), NULL);
         if (!env->cleaners)
         {
            fprintf(stderr, "Can't register cleaner.\n");
            ok = 0;
         }
      }

      if (ok)
      {
         if (!list_llinserttail(env->cleaners, data))
         {
            fprintf(stderr, "Can't register cleaner.\n");
            ok = 0;
         }
      }

      drms_unlock_client(env);
   }
   else
   {
      fprintf(stderr, "Can't register doit cleaner function. Unable to obtain mutex.\n");
   }
  
   return ok;
}

static void ArrivederciBaby(DRMS_Env_t *env)
{
   if (gShutdownsem)
   {
      sem_wait(gShutdownsem);
   }

   gShutdown = kSHUTDOWN_ABORT;

   if (gShutdownsem)
   {
      sem_post(gShutdownsem);
   }

   /* Does not shutdown drms_server */
   /* Need to lock env so that main thread and signal thread don't contend for it. */
   drms_abort_now(env);

   /* don't wait for self-start drms_server */
   exit(1);
}

/* Code for handling signals, some of which shut down the program */
void *drms_signal_thread(void *arg)
{
  int status, signo;
  DRMS_Env_t *env = (DRMS_Env_t *) arg;
  int doexit = 0;

#ifdef DEBUG
  printf("drms_signal_thread started.\n");
  fflush(stdout);
#endif

  /* Block signals. */
  /* It is necessary to block a signal that the thread is going to wait for.  Blocking
   * will prevent the delivery of those signals that are blocked. However, sigwait
   * will still return when a signal becomes pending. */
  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, NULL)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);
    exit(1);
  }

  for (;;)
  {
    if ((status = sigwait(&env->signal_mask, &signo)))
    {
      if (status == EINTR)
      {
	fprintf(stderr, "sigwait error, errcode=%d.\n", status);
	continue;
      }
      else
      {
	fprintf(stderr,"sigwait error, errcode=%d.\n", status);
        signo = SIGTERM; /* Act like we got an abort signal. */
      }
    }

    switch(signo)
    {
       case SIGINT:
         fprintf(stderr,"WARNING: jsoc_main received SIGINT...exiting.\n");
         break;
       case SIGTERM:
         fprintf(stderr,"WARNING: jsoc_main received SIGTERM...exiting.\n");
         break;
       case SIGQUIT:
         fprintf(stderr,"WARNING: jsoc_main received SIGQUIT...exiting.\n");
         break; 
       case SIGUSR2:
         if (env->verbose) 
         {
            fprintf(stdout,"signal thread received SIGUSR2 (main shutting down)...exiting.\n");
         }
         pthread_exit(NULL);
         break;
       default:
         fprintf(stderr,"WARNING: DRMS server received signal no. %d...exiting.\n", 
                 signo);

         signo = SIGTERM; /* Act like we got an abort signal. */
         break;
    }

    switch(signo)
    {
       case SIGINT:
       case SIGTERM:
       case SIGQUIT:
         /* No need to acquire lock to look at env->shutdownsem, which was either 
          * created or not before the signal_thread existed. By the time
          * execution gets here, shutdownsem is read-only */
         if (gShutdownsem)
         {
            /* acquire shutdown lock */
            sem_wait(gShutdownsem);

            if (gShutdown == kSHUTDOWN_UNINITIATED)
            {
               /* There is no shutdown in progress - okay to start shutdown */
               gShutdown = kSHUTDOWN_ABORTINITIATED; /* Shutdown initiated */

               fprintf(stderr, "Shutdown initiated.\n");

               /* Allow DoIt() function a chance to clean up. */
               /* Call user-registered callback, if such a callback was registered, that cleans up 
                * resources used in the DoIt() loop */
               if (env->cleaners)
               {
                  ListNode_t *lnode = NULL;
                  CleanerData_t *acleaner = NULL;

                  list_llreset(env->cleaners);
                  while ((lnode = list_llnext(env->cleaners)) != NULL)
                  {
                     acleaner = lnode->data;
                     (*(acleaner->cb))(acleaner->data);
                     list_llremove(env->cleaners, lnode);
                     list_llfreenode(&lnode);
                  }
               }

               /* release shutdown lock */
               sem_post(gShutdownsem);
           
               doexit = 1;
            }
            else
            {
               /* release - shutdown has already been initiated */
               sem_post(gShutdownsem);
            }

            /* DoIt has been optionally cleaned up. */
            /* This will cause the SUMS thread to be killed, and the dbase to abort.  */
            if (doexit)
            {
               ArrivederciBaby(env);
               return NULL; /* kill the signal thread - only thread left */
            }
         }
         else
         {
            ArrivederciBaby(env);
            return NULL; /* kill the signal thread - only thread left */
         }
      
         break;      
    }
  }
}

#endif /* DRMS_CLIENT */
