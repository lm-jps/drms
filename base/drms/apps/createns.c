#include <sys/types.h>
#include <pwd.h>
#include "jsoc_main.h"

char *module_name = "createns";

typedef enum
{
   kCrnsErr_Success = 0,
   kCrnsErr_NsExists,
   kCrnsErr_DBQuery,
   kCrnsErr_Argument,
   kCrnsErr_FileIO
} CrnsError_t;

#define kNamespace     "ns"
#define kNsGroup       "nsgroup"
#define kDbUser        "dbusr"
#define kFile          "file"
#define kNotSpec       "NOTSPECIFIED"

ModuleArgs_t module_args[] =
{
   {ARG_STRING,  kNamespace,  NULL,  "DRMS namespace to create.",                                          NULL},
   {ARG_STRING,  kNsGroup,    NULL,  "DRMS namespace group in which new namespace will have membership.",  NULL},
   {ARG_STRING,  kDbUser,     NULL,  "DRMS database user that owns newly created namespace.",              NULL},
   {ARG_STRING,  kFile,       kNotSpec,  "Optional output file (output printed to stdout otherwise).",     NULL},
   {ARG_END}
};

static CrnsError_t CreateSQLMasterSeriesTable(FILE *fptr)
{
  CrnsError_t err = kCrnsErr_Success;
  
  fprintf(fptr,"CREATE TABLE %s (\n", DRMS_MASTER_SERIES_TABLE);
  fprintf(fptr,"  seriesname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
  fprintf(fptr,"  author %s NOT NULL,\n", db_stringtype_maxlen(255));
  fprintf(fptr,"  owner %s NOT NULL,\n", db_stringtype_maxlen(255));
  fprintf(fptr,"  unitsize %s NOT NULL,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
  fprintf(fptr,"  archive %s NOT NULL,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
  fprintf(fptr,"  retention %s NOT NULL,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
  fprintf(fptr,"  tapegroup %s NOT NULL,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
  fprintf(fptr,"  primary_idx %s NOT NULL,\n", db_stringtype_maxlen(1024));
  fprintf(fptr,"  created %s NOT NULL,\n", db_stringtype_maxlen(30));
  fprintf(fptr,"  description %s,\n", db_stringtype_maxlen(4000));
  fprintf(fptr,"  dbidx %s,\n", db_stringtype_maxlen(1024));
  fprintf(fptr,"  version %s,\n", db_stringtype_maxlen(1024));
  fprintf(fptr,"  PRIMARY KEY (seriesname));\n");

  fprintf(fptr, "GRANT select ON %s TO public;\n", DRMS_MASTER_SERIES_TABLE);

  return err;
}

static CrnsError_t CreateSQLMasterKeywordTable(FILE *fptr)
{
   CrnsError_t err = kCrnsErr_Success;

   fprintf(fptr,"CREATE TABLE %s (\n", DRMS_MASTER_KEYWORD_TABLE);
   fprintf(fptr,"  seriesname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
   fprintf(fptr,"  keywordname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXKEYNAMELEN));
   fprintf(fptr,"  linkname %s ,\n", db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
   fprintf(fptr,"  targetkeyw %s ,\n", db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
   fprintf(fptr,"  type %s,\n", db_stringtype_maxlen(20));
   fprintf(fptr,"  defaultval %s ,\n", db_stringtype_maxlen(4000));
   fprintf(fptr,"  format %s ,\n", db_stringtype_maxlen(20));
   fprintf(fptr,"  unit %s ,\n", db_stringtype_maxlen(64));
   fprintf(fptr,"  islink %s default 0 ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  isconstant %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  persegment %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  description %s ,\n", db_stringtype_maxlen(4000));
   fprintf(fptr,"  PRIMARY KEY (seriesname, keywordname));\n");

   fprintf(fptr,"GRANT select ON %s TO public;\n", DRMS_MASTER_KEYWORD_TABLE);
       
   return err;
}

static CrnsError_t CreateSQLMasterLinkTable(FILE *fptr)
{
   CrnsError_t err = kCrnsErr_Success;

   fprintf(fptr,"CREATE TABLE %s (\n", DRMS_MASTER_LINK_TABLE);
   fprintf(fptr,"  seriesname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
   fprintf(fptr,"  linkname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
   fprintf(fptr,"  target_seriesname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
   fprintf(fptr,"  type %s ,\n", db_stringtype_maxlen(20));
   fprintf(fptr,"  description %s ,\n", db_stringtype_maxlen(4000));
   fprintf(fptr,"  PRIMARY KEY (seriesname, linkname));\n");

   fprintf(fptr,"GRANT select ON %s TO public;\n", DRMS_MASTER_LINK_TABLE);

   return err;
}

static CrnsError_t CreateSQLMasterSegmentTable(FILE *fptr)
{
   CrnsError_t err = kCrnsErr_Success;

   fprintf(fptr,"CREATE TABLE %s (\n", DRMS_MASTER_SEGMENT_TABLE);
   fprintf(fptr,"  seriesname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
   fprintf(fptr,"  segmentname %s NOT NULL,\n", db_stringtype_maxlen(DRMS_MAXSERIESNAMELEN));
   fprintf(fptr,"  segnum %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  scope %s ,\n", db_stringtype_maxlen(10));
   fprintf(fptr,"  type %s,\n", db_stringtype_maxlen(20));
   fprintf(fptr,"  naxis %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  axis %s ,\n", db_stringtype_maxlen(4000));
   fprintf(fptr,"  unit %s ,\n", db_stringtype_maxlen(64));
   fprintf(fptr,"  protocol %s ,\n", db_stringtype_maxlen(64));
   fprintf(fptr,"  description %s ,\n", db_stringtype_maxlen(4000));
   fprintf(fptr,"  islink %s default 0 ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  linkname %s ,\n", db_stringtype_maxlen(DRMS_MAXLINKNAMELEN));
   fprintf(fptr,"  targetseg %s, \n", db_stringtype_maxlen(DRMS_MAXSEGNAMELEN));
   fprintf(fptr,"  cseg_recnum bigint default 0 ,\n");
   fprintf(fptr,"  PRIMARY KEY (seriesname, segmentname));\n");

   fprintf(fptr,"GRANT select ON %s TO public;\n", DRMS_MASTER_SEGMENT_TABLE);

   return err;
}

static CrnsError_t CreateSQLMasterSessionTables(FILE *fptr)
{
   CrnsError_t err = kCrnsErr_Success;

   fprintf(fptr,"CREATE TABLE %s (\n", DRMS_SESSION_TABLE);
   fprintf(fptr,"  sessionid %s NOT NULL,\n", db_type_string(drms2dbtype(DRMS_TYPE_LONGLONG)));
   fprintf(fptr,"  hostname %s ,\n", db_stringtype_maxlen(30));
   fprintf(fptr,"  port %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  pid %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  sunum %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_LONGLONG)));
   fprintf(fptr,"  sudir %s ,\n", db_stringtype_maxlen(DRMS_MAXPATHLEN));
   fprintf(fptr,"  username %s ,\n", db_stringtype_maxlen(30));
   fprintf(fptr,"  starttime %s ,\n", db_stringtype_maxlen(30));
   fprintf(fptr,"  lastcontact %s ,\n", db_stringtype_maxlen(30));
   fprintf(fptr,"  endtime %s ,\n", db_stringtype_maxlen(30));
   fprintf(fptr,"  clients %s ,\n", db_type_string(drms2dbtype(DRMS_TYPE_INT)));
   fprintf(fptr,"  status %s ,\n", db_stringtype_maxlen(200));
   fprintf(fptr,"  sums_thread_status %s ,\n", db_stringtype_maxlen(200));
   fprintf(fptr,"  jsoc_version %s ,\n", db_stringtype_maxlen(200));
   fprintf(fptr,"  PRIMARY KEY (sessionid));\n");

   fprintf(fptr,"GRANT select ON %s TO public;\n", DRMS_SESSION_TABLE);
   fprintf(fptr, "CREATE SEQUENCE drms_sessionid_seq;\n");

   return err;
}

static CrnsError_t CreateSQL(FILE *fptr, const char *ns, const char *nsgrp, const char *dbusr)
{
   CrnsError_t err = kCrnsErr_Success;

   fprintf(fptr, "CREATE SCHEMA %s;\n", ns);
   /* Do not insert into admin.sessionns (which lists the default namespace to use for each user). The
    * ns and dbusr provided to this module implies that the ns is owned by the dbusr. It does not
    * imply that the ns is the default namespace for the dbusr. You can run createns many times, one
    * for each of several namespaces, providing the same dbusr for each ns. You don't want to keep
    * updating the default namespace each time the module runs - in fact the default namespace
    * for dbusr might not be any ns created by createns (saying what the default ns is is a manual step). */
   fprintf(fptr, "INSERT INTO admin.ns VALUES ('%s', '%s', '%s');\n", ns, nsgrp, dbusr);
   fprintf(fptr, "GRANT create, usage ON SCHEMA %s TO %s;\n", ns, dbusr);
   fprintf(fptr, "GRANT usage ON SCHEMA %s TO public;\n", ns);

   fprintf(fptr, "SET search_path TO %s;\n", ns);
   fprintf(fptr, "SET ROLE %s;\n", dbusr);

   err = CreateSQLMasterSeriesTable(fptr);

   if (!err)
   {
      err = CreateSQLMasterKeywordTable(fptr);
   }

   if (!err)
   {
      err = CreateSQLMasterLinkTable(fptr);
   }

   if (!err)
   {
      err = CreateSQLMasterSegmentTable(fptr);
   }

   if (!err)
   {
      err = CreateSQLMasterSessionTables(fptr);
   }

   if (!err)
   {
      fprintf(fptr, "SET ROLE NONE;\n");
      fprintf(fptr, "SET search_path TO default;\n");
   }

   return err;
}

int DoIt(void) 
{
   CrnsError_t err = kCrnsErr_Success;

   const char *ns = NULL;
   const char *nsgrp = NULL;
   const char *dbusr = NULL;
   const char *file = NULL;
   char *grp = NULL;
   FILE *fptr = NULL;

   char query[DRMS_MAXQUERYLEN];
   DB_Text_Result_t *qres = NULL;

   /* Namespace (required) */
   if (!err)
   {
      ns = cmdparams_get_str(&cmdparams, kNamespace, NULL);
      if (!strcmp(ns, "public")) 
      {
         fprintf(stderr, "Can't create DRMS master tables in namespace public.\n");
         err = kCrnsErr_Argument;
      }
   }

   /* Namespace group (required, but not used in DRMS) */
   if (!err)
   {
      nsgrp = cmdparams_get_str(&cmdparams, kNsGroup, NULL);
      grp = strdup(nsgrp);
      strtolower(grp);
      if (strcmp(grp, "user") != 0 && strcmp(grp, "sys") != 0) 
      {
         fprintf(stderr, "Namespace group ('%s') must be either user or sys.\n", nsgrp);
         err = kCrnsErr_Argument;
      }
   }

   /* DB user (linux user assumed if not specified) who owns the namespace */
   if (!err)
   {
      dbusr = cmdparams_get_str(&cmdparams, kDbUser, NULL);

      if (strcmp(dbusr, kNotSpec) == 0)
      {
         struct passwd *pwd = getpwuid(geteuid());
         dbusr = pwd->pw_name;
      }
   }

   /* Output file */
   if (!err)
   {
      file = cmdparams_get_str(&cmdparams, kFile, NULL);
      if (strcmp(file, kNotSpec) == 0)
      {
         fptr = stdout;
      }
      else
      {
         fptr = fopen(file, "w");
         if (!fptr)
         {
            fprintf(stderr, "Unable to open file '%s'.\n", file);
            err = kCrnsErr_FileIO;
         }
      }
   }
   
   if (!err)
   {
      err = CreateSQL(fptr, ns, grp, dbusr);
   }

   if (!err)
   {
      fflush(fptr);
   }

   if (grp)
   {
      free(grp);
   }

   return err;
}
