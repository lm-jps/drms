/**
\defgroup masterlists masterlists - create a database namespace and DRMS master tables for that namespace
@ingroup su_admin

\brief Create DB namespace and master DRMS tables.

\par Synopsis:

\code
masterlists [JSOC_DBHOST=] [JSOC_DBNAME=] [dbuser=<user>] namespace=<ns> nsgrp=<user|sys>
\endcode

This program is used only to create a new empty set of DRMS database tables.
This program prompts you for postgres DB password.

\par Flags:
\c -h: print brief usage information.
\c -g: this is a guest user, with assumed namespace su_guest and nsgrp=user

\param JSOC_DBHOST Specifies (overrides) the database host to connect to. Default is ::SERVER.
\param JSOC_DBNAME Specifies (overrides) the database name to use. Default is ::DBNAME.
\param dbuser Specifies DB user who owns master tables drms_*. Default is the unix user who runs \ref masterlists.
\param namespace Specifies the namespace where the master tables reside.
\param nsgrp     Specifies the namespace group, must be either 'user'
or 'sys'. For personal or test usage, please use 'user', and 'sys' is reserved for project like HMI.
\sa
create_series describe_series delete_series modify_series show_info 

*/
#include <pwd.h>
#include "drms.h"
#include "serverdefs.h"
#include "cmdparams.h"

#define GUESTSPACE "su_guest"
#define GUESTGROUP "user"

CmdParams_t cmdparams;
ModuleArgs_t module_args[] = {
  {ARG_FLAG, "g", NULL, "guest"},
  {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;
int main(int argc, char **argv) {
  DB_Handle_t *db_handle;
  char stmt[8000]={0}, *p;
    const char *dbhost;
    char *dbuser, *dbpasswd, *dbname, *namespace, *nsgrp;
    char *dbport = NULL;
    char dbHostAndPort[64];
  DB_Text_Result_t *qres;
  char *tn[] = {"drms_keyword", "drms_link", "drms_segment", "drms_series"};
  int guest = 0;

  /* Parse command line parameters. */
  if (cmdparams_parse (&cmdparams, argc, argv) == -1) goto usage;
  if (cmdparams_exists (&cmdparams,"h")) goto usage;
  guest = cmdparams_isflagset(&cmdparams, "g");  /*turns guest=1 */
 
  /* Get hostname, user, passwd and database name for establishing 
     a connection to the DRMS database server. */
    
    /* SERVER does not contain port information. Yet when dbhost is used in db_connect(), that function
     * parses the value looking for an optional port number. So if you didn't provide the JSOC_DBHOST
     * then there was no way to connect to the db with a port other than the default port that the
     * db listens on for incoming connections (which is usually 5432).
     *
     * I changed this so that masterlists uses the DRMSPGPORT macro to define the port to connect to.
     * If by chance somebody has appeneded the port number to the server name in SERVER, and that
     * conflicts with the value in DRMSPGPORT, then DRMSPGPORT wins, and a warning message is printed.
     *
     * --ART (2014.08.20)
     */
    
  if ((dbhost = cmdparams_get_str(&cmdparams, "JSOC_DBHOST", NULL)) == NULL)
    {
        const char *sep = NULL;
        
        dbhost =  SERVER;
        dbport = DRMSPGPORT;
        
        /* Check for conflicting port numbers. */
        if ((sep = strchr(dbhost, ':')) != NULL)
        {
            if (strcmp(sep + 1, dbport) != 0)
            {
                char *tmpBuf = strdup(dbhost);
                
                if (tmpBuf)
                {
                    tmpBuf[sep - dbhost] = '\0';
                    fprintf(stderr, "WARNING: the port number in the SERVER localization parameter (%s) and in DRMSPGPORT (%s) conflict.\nThe DRMSPGPORT value will be used.\n", sep + 1, DRMSPGPORT);
                    
                    snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s:%s", tmpBuf, dbport);
                    free(tmpBuf);
                    tmpBuf = NULL;
                }
                else
                {
                    fprintf(stderr, "Out of memory.\n");
                    goto failure;
                }
            }
            else
            {
                snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s", dbhost);
            }
        }
        else
        {
            snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s:%s", dbhost, dbport);
        }
    }
    else
    {
        snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s", dbhost);
    }
    
    
  if ((dbname = cmdparams_get_str(&cmdparams, "JSOC_DBNAME", NULL)) == NULL)
    dbname = DBNAME;
  if ((dbuser = cmdparams_get_str(&cmdparams, "dbuser", NULL)) == NULL) {
    struct passwd *pwd = getpwuid(geteuid());
    dbuser = pwd->pw_name;
  }

  if ((namespace = cmdparams_get_str(&cmdparams, "namespace", NULL)) == NULL) {
    if (!guest) {
      fprintf(stderr, "Missing namespace (namespace=)\n");
      goto usage;
    } else {
      namespace = GUESTSPACE;
    }
  }

  namespace = strdup(namespace);

  if (!strcmp(namespace, "public")) {
    fprintf(stderr, "Can't create DRMS master tables in namespace public\n");
    goto usage;
  }
  if (guest) { 
     if (!strcmp(namespace, "su_guest")) {
       printf("You are adding a user to the GUEST namespace. This is a shared-access namespace.\n");
       printf("Please encourage users in this namespace to name series as xxx_seriesname, where xxx are user initials.\n");
     } else {
       fprintf(stderr, "Can only create DRMS guest users in namespace %s\n", GUESTSPACE);
       goto usage;
     }
  }

  strtolower(namespace);

  if ((nsgrp = cmdparams_get_str(&cmdparams, "nsgrp", NULL)) == NULL) {
    if (!guest) {
      fprintf(stderr, "Missing namespace group (nsgrp=)\n");
      goto usage;
    } else {
      nsgrp = GUESTGROUP;     /* All guests are automatically in the user group */
    }
  }

  nsgrp = strdup(nsgrp);

  strtolower(nsgrp);
  if (strcmp(nsgrp, "user") &&
      strcmp(nsgrp, "sys")) {
    fprintf(stderr, "Namespace group must be either user or sys\n");
    goto usage;
  }

  dbpasswd = getpass ("Please type the password for database user \"postgres\":");
  if ((db_handle = db_connect (dbHostAndPort,"postgres",dbpasswd,dbname,0)) == NULL) {
    fprintf (stderr, "Failure to connect to DB server.\n");
    return 1;
  }
  printf("Connected to database '%s' on host '%s' and port '%s' as "
	 "user '%s'.\n",db_handle->dbname, db_handle->dbhost, db_handle->dbport,
	 db_handle->dbuser);

  if (db_isolation_level (db_handle, DB_TRANS_SERIALIZABLE) ) {
    fprintf (stderr,"Failed to set database isolation level.\n");
    goto failure;
  }
  if ( db_start_transaction(db_handle))
  {
    fprintf(stderr,"Couldn't start database transaction.\n");
    goto failure;
  }


  // check to see if namespace already exists; if a guest, then skip the check and the creation of the schema
  if (!guest) {
       sprintf(stmt, "select * from pg_namespace where nspname = '%s'", namespace);
       if ( (qres = db_query_txt(db_handle, stmt)) != NULL) {
         if (qres->num_rows > 0) {
           fprintf(stderr, "Namespace %s already exists.\n", namespace);
           goto failure;
         }
       }

       sprintf(stmt, "create schema %s", namespace);
       if (db_dms(db_handle, NULL, stmt)) {
         fprintf(stderr, "Failed to create schema %s.\n", namespace);
         goto failure;
       }
       sprintf(stmt, "insert into admin.ns values ('%s', '%s', '%s')", namespace, nsgrp, dbuser);
       if (db_dms(db_handle, NULL, stmt)) {
         fprintf(stderr, "Failed to record namespace.\nStatement was %s\n", stmt);
         goto failure;
       }
  }

  p = stmt;
  p += sprintf(p, "grant create, usage on schema %s to %s;", namespace, dbuser);
  if (!guest) {
      p += sprintf(p, "grant usage on schema %s to public", namespace);
  } else {
  /* Allow any guest users access to the su_guest namespace */
      p += sprintf(p, "grant guest to %s", dbuser);
  }
  if (db_dms(db_handle, NULL, stmt)) {
    fprintf(stderr, "Failed to grant privileges.\n statement was %s\n", stmt);
    goto failure;
  }

  sprintf(stmt, "set search_path to %s", namespace);
  if (db_dms(db_handle, NULL, stmt)) {
    fprintf(stderr, "Failed to set search_path to %s\n", namespace);
    goto failure;
  }

  sprintf(stmt, "set role %s", dbuser);
  if (db_dms(db_handle, NULL, stmt)) {
    fprintf(stderr, "Failed to set role %s\n", dbuser);
    goto failure;
  }

  /* Create master tables. */
  if (!guest) {
      p = stmt;
      p += sprintf(p,"create table %s (\n",DRMS_MASTER_SERIES_TABLE);
      p += sprintf(p,"seriesname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
      p += sprintf(p,"author %s not null,\n",db_stringtype_maxlen(255));
      p += sprintf(p,"owner %s not null,\n",db_stringtype_maxlen(255));
      p += sprintf(p,"unitsize %s not null,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      p += sprintf(p,"archive %s not null,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      p += sprintf(p,"retention %s not null,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      p += sprintf(p,"tapegroup %s not null,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      p += sprintf(p,"primary_idx %s not null,\n",db_stringtype_maxlen(1024));
      p += sprintf(p,"created %s not null,\n",db_stringtype_maxlen(30));
      p += sprintf(p,"description %s,\n",db_stringtype_maxlen(4000));
      p += sprintf(p,"dbidx %s,\n",db_stringtype_maxlen(1024));
      p += sprintf(p,"version %s,\n",db_stringtype_maxlen(1024));
      p += sprintf(p,"primary key (seriesname));\n");
      p += sprintf(p, "grant select on %s to public", DRMS_MASTER_SERIES_TABLE);

      if (db_dms(db_handle,NULL, stmt))
      {
        fprintf(stderr, "failed to create '" DRMS_MASTER_SERIES_TABLE "'\n");
        goto failure;
      }
      printf("Created new "DRMS_MASTER_SERIES_TABLE"...\n");

       /* Create master keyword table. */
       p = stmt;
       p += sprintf(p,"create table " DRMS_MASTER_KEYWORD_TABLE  " (\n");
       p += sprintf(p,"seriesname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
       p += sprintf(p,"keywordname %s not null,\n",db_stringtype_maxlen(DRMS_MAXKEYNAMELEN));
       p += sprintf(p,"linkname %s ,\n",db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
       p += sprintf(p,"targetkeyw %s ,\n",db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
       p += sprintf(p,"type %s,\n",db_stringtype_maxlen(20));
       p += sprintf(p,"defaultval %s ,\n",db_stringtype_maxlen(4000));
       p += sprintf(p,"format %s ,\n",db_stringtype_maxlen(20));
       p += sprintf(p,"unit %s ,\n",db_stringtype_maxlen(64));
       p += sprintf(p,"islink %s default 0 ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"isconstant %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"persegment %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"description %s ,\n",db_stringtype_maxlen(4000));
       p += sprintf(p,"primary key (seriesname, keywordname));\n");
       p += sprintf(p,"grant select on %s to public", DRMS_MASTER_KEYWORD_TABLE);

       if (db_dms(db_handle,NULL, stmt))
       {
         fprintf(stderr, "failed to create '" DRMS_MASTER_KEYWORD_TABLE"'\n");
         goto failure;
       }
       printf("Created new '"DRMS_MASTER_KEYWORD_TABLE"'...\n");

       /* Create master link table. */
       p = stmt;
       p += sprintf(p,"create table "DRMS_MASTER_LINK_TABLE" (\n");
       p += sprintf(p,"seriesname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
       p += sprintf(p,"linkname %s not null,\n",db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
       p += sprintf(p,"target_seriesname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
       p += sprintf(p,"type %s ,\n",db_stringtype_maxlen(20));
       p += sprintf(p,"description %s ,\n",db_stringtype_maxlen(4000));
       p += sprintf(p,"primary key (seriesname, linkname));\n");
       p += sprintf(p,"grant select on %s to public", DRMS_MASTER_LINK_TABLE);

       if (db_dms(db_handle,NULL, stmt))
       {
         fprintf(stderr, "failed to create '"DRMS_MASTER_LINK_TABLE"'\n");
         goto failure;
       }
       printf("Created new '"DRMS_MASTER_LINK_TABLE"'...\n");


       /* Create master segment table. */
       p = stmt;
       p += sprintf(p,"create table "DRMS_MASTER_SEGMENT_TABLE" (\n");
       p += sprintf(p,"seriesname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
       p += sprintf(p,"segmentname %s not null,\n",db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
       p += sprintf(p,"segnum %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"scope %s ,\n",db_stringtype_maxlen(10));
       p += sprintf(p,"type %s,\n",db_stringtype_maxlen(20));
       p += sprintf(p,"naxis %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"axis %s ,\n",db_stringtype_maxlen(4000));
       p += sprintf(p,"unit %s ,\n",db_stringtype_maxlen(64));
       p += sprintf(p,"protocol %s ,\n",db_stringtype_maxlen(64));
       p += sprintf(p,"description %s ,\n",db_stringtype_maxlen(4000));
       p += sprintf(p,"islink %s default 0 ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"linkname %s ,\n",db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
       p += sprintf(p,"targetseg %s, \n",db_stringtype_maxlen(DRMS_MAXSEGNAMELEN));
       p += sprintf(p,"cseg_recnum bigint default 0 ,\n");
       p += sprintf(p,"primary key (seriesname, segmentname));\n");
       p += sprintf(p,"grant select on %s to public", DRMS_MASTER_SEGMENT_TABLE);

       if (db_dms(db_handle,NULL, stmt))
       {
         fprintf(stderr, "failed to create '"DRMS_MASTER_SEGMENT_TABLE"'\n");
         goto failure;
       }
       printf("Created new '" DRMS_MASTER_SEGMENT_TABLE"'...\n");

       /* Session tables. */
       p = stmt;
       p += sprintf(p,"create table "DRMS_SESSION_TABLE" (\n");
       p += sprintf(p,"sessionid %s not null,\n",db_type_string(drms2dbtype(DRMS_TYPE_LONGLONG)));
       p += sprintf(p,"hostname %s ,\n",db_stringtype_maxlen(30));
       p += sprintf(p,"port %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"pid %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"sunum %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_LONGLONG)));
       p += sprintf(p,"sudir %s ,\n",db_stringtype_maxlen(DRMS_MAXPATHLEN));
       p += sprintf(p,"username %s ,\n",db_stringtype_maxlen(30));
       p += sprintf(p,"starttime %s ,\n",db_stringtype_maxlen(30));
       p += sprintf(p,"lastcontact %s ,\n",db_stringtype_maxlen(30));
       p += sprintf(p,"endtime %s ,\n",db_stringtype_maxlen(30));
       p += sprintf(p,"clients %s ,\n",db_type_string(drms2dbtype(DRMS_TYPE_INT)));
       p += sprintf(p,"status %s ,\n",db_stringtype_maxlen(200));
       p += sprintf(p,"sums_thread_status %s ,\n",db_stringtype_maxlen(200));
       p += sprintf(p,"jsoc_version %s ,\n",db_stringtype_maxlen(200));
       p += sprintf(p,"primary key (sessionid));");
       p += sprintf(p,"grant select on %s to public", DRMS_SESSION_TABLE);
       if (db_dms(db_handle,NULL, stmt))
       {
         fprintf(stderr, "failed to create '"DRMS_SESSION_TABLE"'.\n");
         goto failure;
       }
       printf("Created new '"DRMS_SESSION_TABLE"'...\n");

       db_sequence_create(db_handle, "drms_sessionid");
       printf("Created new drms_sessionid_seq sequence...\n");
  }

  sprintf(stmt, "set search_path to default");
  if (db_dms(db_handle, NULL, stmt)) {
    fprintf(stderr, "Failed to set search_path to default\n");
    goto failure;
  }

  sprintf(stmt, "set role none");
  if (db_dms(db_handle, NULL, stmt)) {
    fprintf(stderr,"Failed to set role none\n");
    goto failure;
  }

  if (0) {
  // updating views
  sprintf(stmt, "select name from admin.ns where nsgrp='%s'", nsgrp);
  if ( (qres = db_query_txt(db_handle, stmt)) != NULL) {
    for(int i = 0; i < 4; i++) {
      p = stmt;
      p += sprintf(p, "create or replace view %s_%s as\n\t(select * from %s.%s) ",
		   nsgrp, tn[i], qres->field[0][0], tn[i]);
      for (int j = 1; j < qres->num_rows; j++) {
	p += sprintf(p, "union\n\t(select * from %s.%s) ", qres->field[j][0], tn[i]);
      }
      if (db_dms(db_handle, NULL, stmt))
	{
	  fprintf(stderr, "Failed to update view.\nStatement was %s\n", stmt);
	  goto failure;
	}
    }

    // session
    p = stmt;
    p += sprintf(p, "create or replace view %s_drms_session as\n\t"
		 "(select '%s'::text as sessionns, * from %s.drms_session) ",
		 nsgrp, qres->field[0][0], qres->field[0][0]);
    for (int j = 1; j < qres->num_rows; j++) {
      p += sprintf(p, "union\n\t(select '%s'::text as sessionns, * from %s.drms_session) ", 
		   qres->field[j][0], qres->field[j][0]);
    }
    if (db_dms(db_handle, NULL, stmt))
      {
	fprintf(stderr, "Failed to update view.\nStatement was%s\n", stmt);
	goto failure;
      }
  }
  }
  printf("Commiting...\n");
  db_commit(db_handle);
  db_disconnect(&db_handle);
  printf("Done.\n");

  if (namespace)
  {
     free(namespace);
  }

  if (nsgrp)
  {
     free(nsgrp);
  }


  return 0;
 failure:
  printf("Aborting...\n");
  db_disconnect(&db_handle);
  printf("Failed to create masterlists.\n");

  if (namespace)
  {
     free(namespace);
  }

  if (nsgrp)
  {
     free(nsgrp);
  }

  return 1;
 usage:
  printf("Usage:    %s [-h]\n"
	 "          %s [JSOC_DBHOST=] [JSOC_DBNAME=] [dbuser=] namespace= nsgrp=\n"
	 "Options:  -h:        print this help message.\n"
         "Options:  -g:        add a guest user, only dbuser parameter required; default namespace=su_guest, nsgrp=user \n"
	 "          JSOC_DBHOST: DB host to connect to. default is 'hmidb'.\n"
	 "          JSOC_DBNAME: DB name to connect to. default is 'jsoc'.\n"
	 "          dbuser:    DB user who owns master tables drms_*. \n"
	 "                     default is the unix username.\n"
         "          namespace: namespace where the master tables reside.\n"
         "          nsgrp:     namespace group, must be either 'user' or 'sys'.\n",
	 argv[0], argv[0]);
  return 1;
}
    
