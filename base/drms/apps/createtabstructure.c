/**
@defgroup createtabstructure createtabstructure - Generate SQL that describes how to create the specified series in DRMS
@ingroup su_admin

@brief Generate SQL that describes how to create the specified series in DRMS
@par Synopsis:

@code
createtabstructure in=<series> [archive=<archive>] [retention=<days>] [tapegroup=<groupnum>] [file=<outputfile>]
@endcode

Given an input DRMS series name, createtabstructure generates SQL statements that, when evaluated, reproduce the 
input series. The statements modify the structure of several DRMS tables and create a new table to effect this. Specifically,
they insert one series-specific row into the namespace's drms_series table, one row per series link into the drms_link 
table, one row per series keyword into the drms_keyword table, and one row per segment into the drms_segment table.
The statements then create the series table itself (eg., mdi.fd_M_96m_lev18) and the corresponding sequence
(eg., mdi.fd_M_96m_lev18_seq). Now rows are added to the series table -
the goal is to simply reproduce the minimal structure so that processes like remoteDRMS can then populate the
series table. Additionally, no attempt is made to create the drms_series, drms_link, drms_keyword, or drms_segment
tables. Those are assumed to exist. They can be created with the masterlists programs (which also makes the drms_session
and drms_sessionid_seq tables).

To use this module for remoteDRMS processing, first run masterlists at the remote, slony-log-receving site to 
create the appropriate namespaces and drms_* tables and sequence. Then run createtabstructure at the slony-log-shipping
site to create the SQL file that, when run on the slony-log-receiving site, populates the drms_* tables and creates the
series table and sequence. Next, start ingesting slony logs at the remote site. These logs will populate the 
series table.

@param in The name of the DRMS series to reproduce (required).
@param archive If specified, this will override the existing archive flag. Valid values are 1, 0, or -1 (optional).
@param retention If specified, this will override the existing retention flag. This is the minimum number of days the the associated storage units are guaranteed to remain online (optional).
@param tapegroup. If specified, this will override the existing tapegroup ID. Relevant to NetDRMS sites that have tape systems (optional).
@param file If specified, the SQL generated will be written to the file. Otherwise, the SQL will be printed to stdout (optional).

@par Return Value
If execution proceeds with no errors, kCrtabErr_Success is returned. Otherwise, if the @a in argument is missing, then 
kCrtabErr_MissingSeriesName is returned. If @a in is an invalid DRMS series, then kCrtabErr_UnknownSeries is returned.
If a problem occurs while evaluating a database query, then kCrtabErr_DBQuery is returned. If an invalid argument is
supplied, kCrtabErr_Argument is returned, and if a file IO error occurs, kCrtabErr_FileIO is returned. 

@par GEN_FLAGS:
@ref jsoc_main

@sa
create_series masterlists
*/

#include "jsoc_main.h"

char *module_name = "createtabstructure";

typedef enum
{
   kCrtabErr_Success = 0,
   kCrtabErr_MissingSeriesName,
   kCrtabErr_DBQuery,
   kCrtabErr_Argument,
   kCrtabErr_FileIO,
   kCrtabErr_UnknownSeries
} CrtabError_t;

#define kSeriesin      "in"
#define kArchive       "archive"
#define kRetention     "retention"
#define kTapegroup     "tapegroup"
#define kFile          "file"
#define kNotSpec       "NOTSPECIFIED"

ModuleArgs_t module_args[] =
{
   {ARG_STRING, kSeriesin,  kNotSpec,   "Series for which to make SQL that generates structure."},
   {ARG_STRING, kArchive,   kNotSpec,   "Series archive value override."},
   {ARG_STRING, kRetention, kNotSpec,   "Series retetnion value override."},
   {ARG_STRING, kTapegroup, kNotSpec,   "Series tapgegroup value override."},
   {ARG_STRING, kFile,      kNotSpec,   "Optional output file (output printed to stdout otherwise)."},
   {ARG_END}
};

static CrtabError_t GetTableOID(DRMS_Env_t *env, const char *ns, const char *table, char **oid)
{
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;
   CrtabError_t err = kCrtabErr_Success;

   snprintf(query, sizeof(query), "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^(%s)$' AND n.nspname ~ '^(%s)$' ORDER BY 2, 3", table, ns);
   
   if (!oid)
   {
      fprintf(stderr, "Missing required argument 'oid'.\n");
      err = kCrtabErr_Argument;
   }
   else if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      if (qres->num_rows != 1)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = kCrtabErr_DBQuery;
      }
      else
      {
         /* row 1, column 1 */
         char ioid[8];
         *oid = malloc(sizeof(char) * 64);

         /* qres will think OID is of type string, but it is not. It is a 32-bit big-endian number.
          * So, must convert the four bytes into a 32-bit number (swapping bytes if the machine is 
          * a little-endian machine) */
         memcpy(ioid, qres->column->data, 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
         db_byteswap(DB_INT4, 1, ioid);
#endif

         snprintf(*oid, 64, "%d", *((int *)ioid));
         db_free_binary_result(qres);
      }
   }

   return err;
}

static CrtabError_t GetColumnLists(DRMS_Env_t *env, 
                                   const char *oid, 
                                   char **collist,
                                   char **colnames)
{
   CrtabError_t err = kCrtabErr_Success;
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;

   snprintf(query, sizeof(query), "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) as defval, a.attnotnull FROM pg_catalog.pg_attribute a WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", oid);

   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      if (qres->num_cols != 4)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = kCrtabErr_DBQuery;
      }
      else
      {
         char *list = NULL;
         int irow;
         size_t strsize = DRMS_MAXQUERYLEN;
         char colname[512];

         if (collist)
         {
            char buf[512];
            char dtype[512];
            char defval[512] = {0};
            char notnull[512] = {0};

            list = malloc(sizeof(char) * strsize);
            memset(list, 0, sizeof(char) * strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  base_strcatalloc(list, ", ", &strsize);
               }

               /* row irow + 1, column 1 */
               db_binary_field_getstr(qres, irow, 0, sizeof(colname), colname);
               db_binary_field_getstr(qres, irow, 1, sizeof(dtype), dtype);
               db_binary_field_getstr(qres, irow, 2, sizeof(buf), buf);

               *defval = '\0';
               *notnull = '\0';

               if (strlen(buf) > 0)
               {
                  snprintf(defval, sizeof(defval), "default %s", buf);
               }

               db_binary_field_getstr(qres, irow, 3, sizeof(buf), buf);

               /* The "not null" column is actually a db boolean, which isn't
                * supported by DRMS. Instead, qres contains a single byte, 
                * with the value 0x01 if true, and 0x00 if false */
               if ((int)(*((int *)buf)) == 1)
               {
                  snprintf(notnull, sizeof(notnull), "not null");
               }

               snprintf(buf, sizeof(buf), "%s %s %s %s", colname, dtype, defval, notnull);
               base_strcatalloc(list, buf, &strsize);
            }

            *collist = list;
            list = NULL;
         }

         if (colnames)
         {
            list = malloc(sizeof(char) * strsize);
            memset(list, 0, sizeof(char) * strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  base_strcatalloc(list, ", ", &strsize);
               }

               /* row irow + 1, column 1 */
               db_binary_field_getstr(qres, irow, 0, sizeof(colname), colname);
               base_strcatalloc(list, colname, &strsize);
            }

            *colnames = list;
            list = NULL;
         }

         db_free_binary_result(qres);
      }
   }
   
   return err;
}

static CrtabError_t GetRows(DRMS_Env_t *env, 
                            const char *series, 
                            const char *ns, 
                            const char *table, 
                            char *colnames, 
                            DB_Binary_Result_t **res)
{
   CrtabError_t err = kCrtabErr_Success;
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;

   snprintf(query, sizeof(query), "SELECT %s FROM %s.%s WHERE seriesname = '%s'", colnames, ns, table, series);
   
   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      /* series table might be emtpy */
      *res = qres;
   }

   return err;
}

#if 0
   if (rowinslist)
         {
            /*
              "(seriesname, keywordname, linkname, targetkeyw, type, "
              "defaultval, format, unit, description, islink, "
              "isconstant, persegment) values (?,?,?,?,?,?,?,?,?,?,?,?)"
            */
            char colname[512];

            list = malloc(sizeof(char) * strside);
            memset(list, 0, sizeof(char) * strsize);
            base_strcatalloc(list, "(", &strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  base_strcatalloc(list, ", ", &strsize);
               }

               /* row irow + 1, column 1 */
               db_binary_field_getstr(qres, irow, 0, sizeof(colname), colname);
               base_strcatalloc(list, colname, &strsize);
            }

            base_strcatalloc(list, ") values (", &strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  base_strcatalloc(list, ",", &strsize);
               }

               base_strcatalloc(list, "?", &strsize);               
            }

            base_strcatalloc(list, ")", &strsize);

            *rowinslist = list;
            list = NULL;
         }
#endif


static CrtabError_t CreateSQLTable(FILE *fptr, const char *table, const char *collist)
{
   CrtabError_t err = kCrtabErr_Success;
  
   /* Don't create primary key - CreateSQLIndices will do that when it copies the ones from 
    * the source table. */
   fprintf(fptr, "CREATE TABLE %s (%s);\n", table, collist);
   
   return err;
}

static CrtabError_t CreateSQLSequence(FILE *fptr, const char *table)
{
   CrtabError_t err = kCrtabErr_Success;
   fprintf(fptr, "CREATE SEQUENCE %s_seq;\n", table);
   return err;
}

static CrtabError_t CreateSQLIndices(FILE *fptr, DRMS_Env_t *env, const char *oid)
{
   CrtabError_t err = kCrtabErr_Success;
   DRMS_Session_t *session = env->session;
   char query[DRMS_MAXQUERYLEN];
   char createbuf[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   int irow;

   snprintf(query, sizeof(query), "SELECT c2.relname, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i WHERE c.oid = '%s' AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND i.indisvalid = 't' ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname", oid);

   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      if (qres->num_cols != 2)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = kCrtabErr_DBQuery;
      }
      else
      {
         for (irow = 0; irow < qres->num_rows; irow++)
         {
            /* The column pg_get_indexdef (col 2) has the query that creates the indices. */
            db_binary_field_getstr(qres, irow, 1, sizeof(createbuf), createbuf); 
            fprintf(fptr, "%s;\n", createbuf);
         }
      }

      db_free_binary_result(qres);
   }

   return err;
}

static CrtabError_t CreateSQLGrantPerms(FILE *fptr, DRMS_Env_t *env, const char *series, const char *ns)
{
   CrtabError_t err = kCrtabErr_Success;
   DRMS_Session_t *session = env->session;
   char grantbuf[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;

   fprintf(fptr, "GRANT SELECT ON %s TO public;\n", series);
   fprintf(fptr, "GRANT SELECT ON %s_seq TO public;\n", series);
   fprintf(fptr, "GRANT DELETE ON %s TO sumsadmin;\n", series);

   sprintf(grantbuf, "SELECT owner FROM admin.ns WHERE name = '%s'", ns);
   if ((qres = drms_query_bin(session, grantbuf)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", grantbuf);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      if (qres->num_rows != 1)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", grantbuf);
         err = kCrtabErr_DBQuery;
      }
      else
      {
         char nsowner[512];

         db_binary_field_getstr(qres, 0, 0, sizeof(nsowner), nsowner);

         fprintf(fptr, "GRANT select, insert, update, delete ON %s TO %s;\n", series, nsowner);
         fprintf(fptr, "GRANT update ON %s_seq TO %s;\n", series, nsowner);
      }

      db_free_binary_result(qres);
   }

   return err;
}

static CrtabError_t CreateSQLInsertIntoTable(FILE *fptr, 
                                             DRMS_Env_t *env, 
                                             const char *series, 
                                             const char *ns, 
                                             const char *table)
{
   CrtabError_t err = kCrtabErr_Success;
   int irow;
   int icol;
   size_t strsize = 1024;
   char *list = NULL;
   char val[1024];
   char *oid = NULL;
   char *colnames = NULL;
   DB_Binary_Result_t *rows = NULL;

   err = GetTableOID(env, ns, table, &oid);

   if (!err)
   {
      err = GetColumnLists(env, oid, NULL, &colnames);
   }

   err = GetRows(env, series, ns, table, colnames, &rows);

   for (irow = 0; irow < rows->num_rows; irow++)
   {
      list = malloc(sizeof(char) * strsize);
      memset(list, 0, sizeof(char) * strsize);

      for (icol = 0; icol < rows->num_cols; icol++)
      {
         if (icol)
         {
            base_strcatalloc(list, ", ", &strsize);
         }

         db_binary_field_getstr(rows, irow, icol, sizeof(val), val);
         if (((rows->column)[icol]).type == DB_STRING)
         {
            base_strcatalloc(list, "'", &strsize);
            base_strcatalloc(list, val, &strsize);
            base_strcatalloc(list, "'", &strsize);
         }
         else
         {
            base_strcatalloc(list, val, &strsize);
         }
      }

      fprintf(fptr, "INSERT INTO %s VALUES (%s);\n", table, list);
      free(list);
   }

   if (oid)
   {
      free(oid);
   }

   if (colnames)
   {
      free(colnames);
   }

   if (rows)
   {
      db_free_binary_result(rows);
   }

   return err;
}

static CrtabError_t CreateSQLUpdateTable(FILE *fptr, 
                                         DRMS_Env_t *env,
                                         const char *ns,
                                         const char *table, 
                                         const char *colname, 
                                         const char *colvalue,
                                         const char *where)
{
   CrtabError_t err = kCrtabErr_Success;
   DRMS_Session_t *session = env->session;
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;

   /* Ensure that where clause selects one record - otherwise, bail. */
   snprintf(query, sizeof(query), "SELECT count(*) from %s.%s where %s", ns, table, where);
   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = kCrtabErr_DBQuery;
   }
   else
   {
      if (qres->num_rows == 0)
      {
         fprintf(stderr, "where clause '%s' does not select a record.\n", where);
         err = kCrtabErr_DBQuery;
      }
      if (qres->num_rows != 1)
      {
         fprintf(stderr, "where clause '%s' selects more than one record.\n", where);
         err = kCrtabErr_DBQuery;
      }
      else
      {
         fprintf(fptr, "UPDATE %s SET %s = %s where %s;\n", table, colname, colvalue, where);         
      }

      db_free_binary_result(qres);
   }

   return err;
}

static int CreateSQL(FILE *fptr, DRMS_Env_t *env, 
                     const char *seriesin, 
                     const char *archive, 
                     const char *retention, 
                     const char *tapegroup)
{
   CrtabError_t err = kCrtabErr_Success;
   char *series = NULL;
   char *oid = NULL;
   char *ns = NULL;
   char *table = NULL;
   char *collist = NULL;

   series = strdup(seriesin);
   strtolower(series);

   get_namespace(series, &ns, &table);
   if (!ns || !table)
   {
      fprintf(stderr, "Invalid argument 'seriesin' (%s).\n", seriesin);
      err = kCrtabErr_Argument;
   }
   else
   {
      err = GetTableOID(env, ns, table, &oid);

      if (!err)
      {
         err = GetColumnLists(env, oid, &collist, NULL);
      }

      if (!err)
      {
         /* set search path to namespace - needed for the remainder of the SQL */
         fprintf(fptr, "SET search_path TO %s;\n", ns);
      }

      /* First, create the series table and associated sequence table */
      if (!err)
      {
         err = CreateSQLTable(fptr, table, collist);

         /* Create indices in the series table (the other tables will have been created by masterlists) */
         if (!err)
         {
            err = CreateSQLIndices(fptr, env, oid);
         }

         /* sequence table */
         if (!err)
         {
            err = CreateSQLSequence(fptr, table);
         }

         /* Grant select to public and delete to sumsadmin */
         /* Grant select, insert, update, delete to owner of the namespace */
         if (!err)
         {
            err = CreateSQLGrantPerms(fptr, env, series, ns);
         }
      }
   
      /* Second, insert rows into drms_series */
      if (!err)
      {
         char where[512];

         snprintf(where, sizeof(where), "seriesname = '%s'", seriesin);
         err = CreateSQLInsertIntoTable(fptr, env, seriesin, ns, DRMS_MASTER_SERIES_TABLE);
         
         /* override archive, retention, and tapegroup if present */
         if (!err)
         {
            err = CreateSQLUpdateTable(fptr, env, ns, DRMS_MASTER_SERIES_TABLE, "archive", archive, where);
         }

         if (!err)
         {
            err = CreateSQLUpdateTable(fptr, env, ns, DRMS_MASTER_SERIES_TABLE, "retention", retention, where);
         }

         if (!err)
         {
            err = CreateSQLUpdateTable(fptr, env, ns, DRMS_MASTER_SERIES_TABLE, "tapegroup", tapegroup, where);
         }
      }

      /* Third, insert rows into drms_links */
      if (!err)
      {
         err = CreateSQLInsertIntoTable(fptr, env, seriesin, ns, DRMS_MASTER_LINK_TABLE);
      }

      /* Fourth, insert rows into drms_keywords */
      if (!err)
      {
         err = CreateSQLInsertIntoTable(fptr, env, seriesin, ns, DRMS_MASTER_KEYWORD_TABLE);
      }

      /* Fifth, insert rows into drms_segments */
      if (!err)
      {
         err = CreateSQLInsertIntoTable(fptr, env, seriesin, ns, DRMS_MASTER_SEGMENT_TABLE);
      }

      free(ns);
      free(table);

      if (series)
      {
         free(series);
      }

      if (oid)
      {
         free(oid);
      }

      if (collist)
      {
         free(collist);
      }
   }

   fflush(fptr);

   return err;
}

int DoIt(void) 
{
   CrtabError_t err = kCrtabErr_Success;
   int drmsstat = DRMS_SUCCESS;
   char *series = NULL;
   char *archive = NULL;
   char *retention = NULL;
   char *tapegroup = NULL;
   char *file = NULL;
   FILE *fptr = NULL;

   series = cmdparams_get_str(&cmdparams, kSeriesin, NULL);

   /* these should all override the values in drms_series */
   archive = cmdparams_get_str(&cmdparams, kArchive, NULL);
   if (strcmp(archive, kNotSpec) == 0)
   {
      archive = NULL;
   }

   retention = cmdparams_get_str(&cmdparams, kRetention, NULL);
   if (strcmp(retention, kNotSpec) == 0)
   {
      retention = NULL;
   }

   tapegroup = cmdparams_get_str(&cmdparams, kTapegroup, NULL);
   if (strcmp(tapegroup, kNotSpec) == 0)
   {
      tapegroup = NULL;
   }

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
         err = kCrtabErr_FileIO;
      }
   }

   if (!err)
   {
      if (strcmp(series, kNotSpec) == 0 && (cmdparams_numargs(&cmdparams) < 1 || (series = cmdparams_getarg(&cmdparams, 1)) == NULL))
      {
         fprintf(stderr, "Missing argument {series=}<seriesname>.\n");
         err = kCrtabErr_Argument;
      }
   }

   if (!err)
   {
      if (drms_series_exists(drms_env, series, &drmsstat))
      {
         CreateSQL(fptr, drms_env, series, archive, retention, tapegroup);
      }
      else
      {
         fprintf(stdout, "Unknown series '%s'.\n", series);
         err = kCrtabErr_UnknownSeries;
      }
   }

   return err;
}
