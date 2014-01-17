//#define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include <regex.h>
#include "atoinc.h"

/* There were problems coping with concurrent access to this table. For now, we're abandoning
 * using the TOC. We can revisit this in the future if counting is too slow. */

#if (defined TOC && TOC)
#define kTableOfCountsNS   "public"
#define kTableOfCountsTab  "recordcounts"
#define kTableOfCountsColSeries "seriesname"
#define kTableOfCountsColNRecs "nrecords"
#endif

#define kShadowSuffix "_shadow"
#define kShadowColRecnum "recnum"
#define kShadowColNRecs "nrecords"
#define kShadowTrig "updateshadowtrig"
#define kShadowTrigFxn "updateshadow"
#define kLimitCutoff 100000

#if (defined TRACKSHADOWS && TRACKSHADOWS)
    #define kShadowTrackTab "drms.shadowtrack"
    #define kshadowTrackerFxn "drms.shadowtrackfxn"
#endif

#if (defined TOC && TOC)
static int TocExists(DRMS_Env_t *env, int *status)
{
   int istat = DRMS_SUCCESS;
   int tabexists = 0;

   tabexists = drms_query_tabexists(env->session, kTableOfCountsNS, kTableOfCountsTab, &istat);

   if (status)
   {
      *status = istat;
   }

   return tabexists;
}

static int CreateTableOfCounts(DRMS_Env_t *env)
{
   char createstmt[8192];
   int status = DRMS_SUCCESS;

   /* Create the table of counts. */
   snprintf(createstmt, sizeof(createstmt), "CREATE TABLE %s (%s text not null, %s bigint not null, PRIMARY KEY (seriesname))", kTableOfCountsTab, kTableOfCountsColSeries, kTableOfCountsColNRecs);

   if (drms_dms(env->session, NULL, createstmt))
   {
      fprintf(stderr, "Failed: %s\n", createstmt);
      status = DRMS_ERROR_BADDBQUERY;
   }
   else
   {
      snprintf(createstmt, sizeof(createstmt), "GRANT ALL ON %s TO public", kTableOfCountsTab);

      if (drms_dms(env->session, NULL, createstmt))
      {
         fprintf(stderr, "Failed: %s\n", createstmt);
         status = DRMS_ERROR_BADDBQUERY;
      }
      else
      {
         /* Create an lower-case index on seriesname. This will speed up queries which will use seriesname
          * as the search criterion. */
         snprintf(createstmt, sizeof(createstmt), "CREATE INDEX %s_%s_lower ON %s.%s (lower(%s))", kTableOfCountsTab, kTableOfCountsColSeries, kTableOfCountsNS, kTableOfCountsTab, kTableOfCountsColSeries);

         if (drms_dms(env->session, NULL, createstmt))
         {
            fprintf(stderr, "Failed: %s\n", createstmt);
            status = DRMS_ERROR_BADDBQUERY;
         }
      }
   }

   return status;
}

static long long CountRecordGroups(DRMS_Env_t *env,
                                   const char *series,
                                   int *status)
{
   int istat = DRMS_SUCCESS;
   char query[DRMS_MAXQUERYLEN];
   char *pklist = NULL;
   size_t stsz = DRMS_MAXQUERYLEN;
   DB_Text_Result_t *tres = NULL;
   long long count = -1;
   int ipkey;

   char *lcseries = strdup(series);

   if (lcseries)
   {
      strtolower(lcseries);
      pklist = malloc(stsz);

      if (pklist)
      {
         DRMS_Record_t *template = drms_template_record(env, series, &istat);

         *pklist = '\0';

         if (istat == DRMS_SUCCESS)
         {
            /* manually cons together list of prime-key names */
            for (ipkey = 0; ipkey < template->seriesinfo->pidx_num; ipkey++)
            {
               pklist = base_strcatalloc(pklist, lcseries, &stsz);
               pklist = base_strcatalloc(pklist, ".", &stsz);
               pklist = base_strcatalloc(pklist, template->seriesinfo->pidx_keywords[ipkey]->info->name, &stsz);

               if (ipkey < template->seriesinfo->pidx_num - 1)
               {
                  pklist = base_strcatalloc(pklist, ", ", &stsz);
               }
            }

            snprintf(query, sizeof(query), "SELECT count(*) FROM (SELECT max(recnum) FROM %s GROUP BY %s) AS T1", lcseries, pklist);

            tres = drms_query_txt(env->session, query);

            if (tres)
            {
               if (tres->num_rows == 1 && tres->num_cols == 1)
               {
                  count = atoll(tres->field[0][0]);
               }

               db_free_text_result(tres);
               tres = NULL;
            }
            else
            {
               fprintf(stderr, "Failed: %s\n", query);
               istat = DRMS_ERROR_BADDBQUERY;
            }
         }
      }
      else
      {
         istat = DRMS_ERROR_OUTOFMEMORY;
      }
   }
   else
   {
      istat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = istat;
   }

   return count;
}

/* Determine if 'series' is present in ns.tab */
static int IsSeriesPresent(DRMS_Env_t *env, const char *ns, const char *tab, const char *series, int *status)
{
   char *query = NULL;
   size_t stsz = 8192;
   int istat = DRMS_SUCCESS;
   int ans = -1;
   DB_Text_Result_t *tres = NULL;
   char *lcseries = strdup(series);

   if (lcseries)
   {
      strtolower(lcseries);

      query = malloc(stsz);
      if (query)
      {
         *query = '\0';

         query = base_strcatalloc(query, "SELECT seriesname FROM ", &stsz);
         query = base_strcatalloc(query, ns, &stsz);
         query = base_strcatalloc(query, ".", &stsz);
         query = base_strcatalloc(query, tab, &stsz);
         query = base_strcatalloc(query, " WHERE lower(seriesname) = '", &stsz);
         query = base_strcatalloc(query, lcseries, &stsz);
         query = base_strcatalloc(query, "'", &stsz);

         tres = drms_query_txt(env->session, query);

         if (tres)
         {
            if (tres->num_rows == 1)
            {
               ans = 1;
            }
            else if (tres->num_rows == 0)
            {
               ans = 0;
            }
            else
            {
               istat = DRMS_ERROR_BADDBQUERY;
            }

            db_free_text_result(tres);
            tres = NULL;
         }
         else
         {
            fprintf(stderr, "Failed: %s\n", query);
            istat = DRMS_ERROR_BADDBQUERY;
         }

         free(query);
         query = NULL;
      }
      else
      {
         istat = DRMS_ERROR_OUTOFMEMORY;
      }

      free(lcseries);
      lcseries = NULL;
   }
   else
   {
      istat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = istat;
   }

   return ans;
}

/* Insert the tuple (series, count) into table ns.tab. */
static int InsertSeries(DRMS_Env_t *env, const char *ns, const char *tab, const char *series, long long count)
{
   char stmnt[8192];
   int status = DRMS_SUCCESS;

   snprintf(stmnt, sizeof(stmnt), "INSERT INTO %s.%s (%s, %s) VALUES ('%s', %lld)", ns, tab, "seriesname", "nrecords", series, count);

   if (drms_dms(env->session, NULL, stmnt))
   {
      fprintf(stderr, "Failed: %s\n", stmnt);
      status = DRMS_ERROR_BADDBQUERY;
   }

   return status;
}

static int AdvanceGroupCount(DRMS_Env_t *env,
                             const char *ns,
                             const char *tab,
                             const char *series)
{
   // UPDATE public.artatest SET nrecords = nrecords + 1 WHERE lower(seriesname) = 'su_arta.fd_m_96m_lev18';
   char *query = NULL;
   size_t stsz = 8192;
   char *lcseries = NULL;
   int status = DRMS_SUCCESS;

   lcseries = strdup(series);
   if (lcseries)
   {
      strtolower(lcseries);

      query = malloc(stsz);
      if (query)
      {
         *query = '\0';

         if (query)
         {
            query = base_strcatalloc(query, "UPDATE ", &stsz);
            query = base_strcatalloc(query, ns, &stsz);
            query = base_strcatalloc(query, ".", &stsz);
            query = base_strcatalloc(query, tab, &stsz);
            query = base_strcatalloc(query, " SET nrecords = nrecords + 1 WHERE lower(seriesname) = '", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, "'", &stsz);

            if (drms_dms(env->session, NULL, query))
            {
               fprintf(stderr, "Failed: %s\n", query);
               status = DRMS_ERROR_BADDBQUERY;
            }

            free(query);
            query = NULL;
         }
      }
      else
      {
         status = DRMS_ERROR_OUTOFMEMORY;
      }
      free(lcseries);
      lcseries = NULL;
   }
   else
   {
      status = DRMS_ERROR_OUTOFMEMORY;
   }

   return status;
}
#endif

/* Free the prime-keyword-name strings stored in the linked list. */
void FreeKey(void *data)
{
    if (data)
    {
        if (*(char **)data)
        {
            free(*(char **)data);   
        }
    }
}

static int ShadowExists(DRMS_Env_t *env, const char *series, int *status)
{
    int istat = DRMS_SUCCESS;
    int tabexists = 0;
    char *namespace = NULL;
    char *table = NULL;
    char shadowtable[DRMS_MAXSERIESNAMELEN];
    DRMS_Record_t *template = NULL;
    
    template = drms_template_record(env, series, &istat);
    
    if (istat == DRMS_SUCCESS)
    {
        /* First check template->seriesinfo->hasshadow. */
        if (template->seriesinfo->hasshadow == 1)
        {
            return 1;
        }
        else if (template->seriesinfo->hasshadow == 0)
        {
            return 0;
        }
        
        /* else fall through and check for the existing of the table. */
        
        if (!get_namespace(series, &namespace, &table))
        {
            snprintf(shadowtable, sizeof(shadowtable), "%s%s", table, kShadowSuffix);
            tabexists = drms_query_tabexists(env->session, namespace, shadowtable, &istat);
            
            free(namespace);
            free(table);
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        template->seriesinfo->hasshadow = tabexists;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return tabexists;
}

/* Returns 1 on error, 0 otherwise. */
static int GetTempTable(char *tabname, int size)
{
   static int id = 0;

   if (id < INT_MAX)
   {
      snprintf(tabname, size, "shadowtemp%03d", id);
      id++;
      return 0;
   }

   return 1;
}

/* If this is a big table, then we don't want to do any of this on the client side. Just derive a SQL statement 
 * and let the server manage memory and other resources. */
// INSERT INTO <shadow table> (pkey1, pkey2, ..., nrecords, recnum) SELECT T1.pkey1, T1.pkey2, ..., T2.c, T2.m FROM <series table> AS T1, (SELECT count(*) AS c, max(recnum) AS m FROM <series table> GROUP BY pkey1, pkey2, ...) AS T2 WHERE T1.recnum = T2.m 
static int PopulateShadow(DRMS_Env_t *env, const char *series, const char *tname)
{
    int status = DRMS_SUCCESS;
    char *lcseries = NULL;
    char *pklist = NULL; /* comma-separated list of lower-case prime-key keyword names */
    char *tpklist = NULL;
    size_t stsz = DRMS_MAXQUERYLEN;
    size_t stsz2 = DRMS_MAXQUERYLEN;
    int ipkey;
    char stmnt[8192];
    DRMS_Keyword_t *key = NULL;
    char shadow[DRMS_MAXSERIESNAMELEN * 2];
    
    lcseries = strdup(series);
    pklist = malloc(stsz);
    tpklist = malloc(stsz2);
    
    if (lcseries && pklist && tpklist)
    {
        DRMS_Record_t *template = drms_template_record(env, series, &status);
        char lckeyname[DRMS_MAXKEYNAMELEN + 1];
        
        *pklist = '\0';
        *tpklist = '\0';

        if (status == DRMS_SUCCESS)
        {
            /* manually cons together list of prime-key names */
            for (ipkey = 0; ipkey < template->seriesinfo->pidx_num; ipkey++)
            {
                key = template->seriesinfo->pidx_keywords[ipkey];
                snprintf(lckeyname, sizeof(lckeyname), "%s", key->info->name);
                strtolower(lckeyname);
                
                pklist = base_strcatalloc(pklist, lckeyname, &stsz);
                tpklist = base_strcatalloc(tpklist, "T1.", &stsz2);
                tpklist = base_strcatalloc(tpklist, lckeyname, &stsz2);
                
                if (ipkey < template->seriesinfo->pidx_num - 1)
                {
                    pklist = base_strcatalloc(pklist, ", ", &stsz);
                    tpklist = base_strcatalloc(tpklist, ", ", &stsz2);
                }
            }
            
            /* Generate the name of the shadow table. */
            if (tname == NULL)
            {
                snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            }
            else
            {
                char *lctname = strdup(tname);
                
                strtolower(lctname);
                snprintf(shadow, sizeof(shadow), "%s", lctname);
            }
            
            snprintf(stmnt, sizeof(stmnt), "INSERT INTO %s (%s, %s, %s) SELECT %s, T2.c, T2.m FROM %s AS T1, (SELECT count(*) AS c, max(recnum) AS m FROM %s GROUP BY %s) AS T2 WHERE T1.recnum = T2.m", shadow, pklist, kShadowColNRecs, kShadowColRecnum, tpklist, lcseries, lcseries, pklist);
            
            if (drms_dms(env->session, NULL, stmnt))
            {
                fprintf(stderr, "Failed: %s\n", stmnt);
                status = DRMS_ERROR_BADDBQUERY;
            }
        }
    }
    else
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }
    
    return status;
}

/* Update an EXISTING record in the shadow table. */
static int UpdateShadow(DRMS_Env_t *env,
                        const char *series,
                        char **pkeynames,
                        int ncols,
                        long long recnum,
                        int added)
{
    char *lcseries = NULL;
    char *namespace = NULL;
    char *table = NULL;
    char *query = NULL;
    size_t stsz = 1024;
    char srecnum[32];
    char smaxrec[32];
    char scolnum[8];
    DB_Binary_Result_t *qres = NULL;
    long long maxrec = -1;
    int status = DRMS_SUCCESS;
    
    /* Find the existing record in shadow table, using the series name. */
    lcseries = strdup(series);
    query = malloc(stsz);
    
    if (lcseries && query)
    {
        *query = '\0';
        
        if (!get_namespace(series, &namespace, &table))
        {
            int icol;
            
            /* Is the record just inserted the newest version?
             *
             * This query simply finds the maximum record number in the series table of all records that compose
             * the DRMS record that just got a new version record inserted into it (or that just got a version
             * deleted from it).
             *
             * SELECT max(T1.recnum) FROM <series> as T1, (SELECT <pkey1> AS p1, <pkey2> AS p2, ... FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.<pkey1> = T2.p1 AND T1.<pkey2> = T2.p2 AND ...
             *
             * if (recnum > maxrec) newest = 1;
             */
            snprintf(srecnum, sizeof(srecnum), "%lld", recnum);
            
            query = base_strcatalloc(query, "SELECT max(T1.recnum) FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, " AS T1, (SELECT ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = base_strcatalloc(query, pkeynames[icol], &stsz);
                query = base_strcatalloc(query, " AS p", &stsz);
                query = base_strcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = base_strcatalloc(query, ", ", &stsz);
                }
            }
            
            query = base_strcatalloc(query, " FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
            query = base_strcatalloc(query, srecnum, &stsz);
            query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = base_strcatalloc(query, "T1.", &stsz);
                query = base_strcatalloc(query, pkeynames[icol], &stsz);
                query = base_strcatalloc(query, " = T2.p", &stsz);
                query = base_strcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = base_strcatalloc(query, " AND ", &stsz);
                }
            }
            
            if ((qres = drms_query_bin(env->session, query)) != NULL)
            {
                if (qres->num_cols == 1 && qres->num_rows == 1)
                {
                    maxrec = db_binary_field_getlonglong(qres, 0, 0);
                }
                else
                {
                    status = DRMS_ERROR_BADDBQUERY;
                }
                
                db_free_binary_result(qres);
                qres = NULL;
            }
            else
            {
                status = DRMS_ERROR_BADDBQUERY;
            }
            
            if (status == DRMS_SUCCESS)
            {
                if (added && recnum == maxrec)
                {
                    /* The record just inserted into series is the newest version. Update the corresponding record's
                     * shadow-table record, using the prime-key values to locate the shadow-table record.
                     *
                     * UPDATE <shadow> AS T1 SET recnum = <recnum>, nrecords = T1.nrecords + 1 FROM (SELECT pkey1, pkey2 FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.pkey1=T2.pkey1 AND T1.pkey2=T2.pkey2 AND ...
                     *
                     * test - UPDATE su_arta.fd_m_96m_lev18_4 as T1 SET car_rot = 10000 FROM (select t_rec_index FROM su_arta.fd_m_96m_lev18 where recnum = 8592) AS T2 WHERE T1.t_rec_index = T2.t_rec_index;
                     */
                    *query = '\0';
                    query = base_strcatalloc(query, "UPDATE ", &stsz);
                    query = base_strcatalloc(query, namespace, &stsz);
                    query = base_strcatalloc(query, ".", &stsz);
                    query = base_strcatalloc(query, table, &stsz);
                    query = base_strcatalloc(query, kShadowSuffix, &stsz);
                    query = base_strcatalloc(query, " AS T1 SET ", &stsz);
                    query = base_strcatalloc(query, kShadowColRecnum, &stsz);
                    query = base_strcatalloc(query, " = ", &stsz);
                    query = base_strcatalloc(query, srecnum, &stsz);
                    query = base_strcatalloc(query, ", ", &stsz);
                    query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                    query = base_strcatalloc(query, " = T1.", &stsz);
                    query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                    query = base_strcatalloc(query, " + 1 FROM (SELECT ", &stsz);
                    
                    for (icol = 0; icol < ncols; icol++)
                    {
                        query = base_strcatalloc(query, pkeynames[icol], &stsz);
                        
                        if (icol < ncols - 1)
                        {
                            query = base_strcatalloc(query, ", ", &stsz);
                        }
                    }

                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);                    
                    query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
                    query = base_strcatalloc(query, srecnum, &stsz);
                    query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);
                    
                    for (icol = 0; icol < ncols; icol++)
                    {
                        snprintf(scolnum, sizeof(scolnum), "%d", icol);
                        query = base_strcatalloc(query, "T1.", &stsz);
                        query = base_strcatalloc(query, pkeynames[icol], &stsz);
                        query = base_strcatalloc(query, " = T2.", &stsz);
                        query = base_strcatalloc(query, pkeynames[icol], &stsz);
                        
                        if (icol < ncols - 1)
                        {
                            query = base_strcatalloc(query, " AND ", &stsz);
                        }
                    }
                    
                    if (drms_dms(env->session, NULL, query))
                    {
                        /* If the shadow table doesn't exist, or if the series' record does not exist in the shadow table,                   
                         * then this query will fail. */
                        fprintf(stderr, "Failed: %s\n", query);
                        status = DRMS_ERROR_BADDBQUERY;
                    }
                }

                if (!added)
                {
                   /* A record was just deleted from the DRMS record group. */

                   if (maxrec > recnum)
                   {
                       /* An obsolete version of the DRMS record was deleted. Update the nrecords column only.                                                                                                  
                        *                                                                                  
                        * UPDATE <shadow> AS T1 SET nrecords = T1.nrecords - 1 FROM (SELECT pkey1, pkey2 FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.pkey1=T2.pkey1 AND T1.pkey2=T2.pkey2 AND ...     
                        */
                      *query = '\0';
                      query = base_strcatalloc(query, "UPDATE ", &stsz);
                      query = base_strcatalloc(query, namespace, &stsz);
                      query = base_strcatalloc(query, ".", &stsz);
                      query = base_strcatalloc(query, table, &stsz);
                      query = base_strcatalloc(query, kShadowSuffix, &stsz);
                      query = base_strcatalloc(query, " AS T1 SET ", &stsz);
                      query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                      query = base_strcatalloc(query, " = T1.", &stsz);
                      query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                      query = base_strcatalloc(query, " - 1 FROM (SELECT ", &stsz);

                      for (icol = 0; icol < ncols; icol++)
                      {
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);

                         if (icol < ncols - 1)
                         {
                            query = base_strcatalloc(query, ", ", &stsz);
                         }
                      }

                      query = base_strcatalloc(query, " FROM ", &stsz);
                      query = base_strcatalloc(query, lcseries, &stsz);
                      query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
                      query = base_strcatalloc(query, srecnum, &stsz);
                      query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);

                      for (icol = 0; icol < ncols; icol++)
                      {
                         snprintf(scolnum, sizeof(scolnum), "%d", icol);
                         query = base_strcatalloc(query, "T1.", &stsz);
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);
                         query = base_strcatalloc(query, " = T2.", &stsz);
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);

                         if (icol < ncols - 1)
                         {
                            query = base_strcatalloc(query, " AND ", &stsz);
                         }
                      }

                      if (drms_dms(env->session, NULL, query))
                      {
                          /* If the shadow table doesn't exist, or if the series' record does not exist in the shadow table, 
                           * then this query will fail. */
                          fprintf(stderr, "Failed: %s\n", query);
                          status = DRMS_ERROR_BADDBQUERY;
                      }

                   }
                   else if (maxrec < recnum)
                   {
                      /* The current version of the DRMS record was deleted. Update both the nrecords column
                       * and the recnum column.
                       *                                                                                  
                       * UPDATE <shadow> AS T1 SET recnum = <maxrec>, nrecords = T1.nrecords - 1 FROM (SELECT pkey1, pkey2 FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.pkey1=T2.pkey1 AND T1.pkey2=T2.pkey2 AND ...                                                                                       
                       */
                      snprintf(smaxrec, sizeof(smaxrec), "%lld", maxrec);

                      *query = '\0';
                      query = base_strcatalloc(query, "UPDATE ", &stsz);
                      query = base_strcatalloc(query, namespace, &stsz);
                      query = base_strcatalloc(query, ".", &stsz);
                      query = base_strcatalloc(query, table, &stsz);
                      query = base_strcatalloc(query, kShadowSuffix, &stsz);
                      query = base_strcatalloc(query, " AS T1 SET ", &stsz);
                      query = base_strcatalloc(query, kShadowColRecnum, &stsz);
                      query = base_strcatalloc(query, " = ", &stsz);
                      query = base_strcatalloc(query, smaxrec, &stsz);
                      query = base_strcatalloc(query, ", ", &stsz);
                      query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                      query = base_strcatalloc(query, " = T1.", &stsz);
                      query = base_strcatalloc(query, kShadowColNRecs, &stsz);
                      query = base_strcatalloc(query, " - 1 FROM (SELECT ", &stsz);

                      for (icol = 0; icol < ncols; icol++)
                      {
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);

                         if (icol < ncols - 1)
                         {
                            query = base_strcatalloc(query, ", ", &stsz);
                         }
                      }

                      query = base_strcatalloc(query, " FROM ", &stsz);
                      query = base_strcatalloc(query, lcseries, &stsz);
                      query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
                      query = base_strcatalloc(query, srecnum, &stsz);
                      query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);

                      for (icol = 0; icol < ncols; icol++)
                      {
                         snprintf(scolnum, sizeof(scolnum), "%d", icol);
                         query = base_strcatalloc(query, "T1.", &stsz);
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);
                         query = base_strcatalloc(query, " = T2.", &stsz);
                         query = base_strcatalloc(query, pkeynames[icol], &stsz);

                         if (icol < ncols - 1)
                         {
                            query = base_strcatalloc(query, " AND ", &stsz);
                         }
                      }

                      if (drms_dms(env->session, NULL, query))
                      {
                          /* If the shadow table doesn't exist, or if the series' record does not exist in the shadow table,                                                                                    
                           * then this query will fail. */
                         fprintf(stderr, "Failed: %s\n", query);
                         status = DRMS_ERROR_BADDBQUERY;
                      }
                   }
                   else
                   {
                      /* Can't happen - duplicate recnums are not allowed. */
                   }
                }
            }
            
            free(namespace);
            free(table);
        }
        else
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(query);
        free(lcseries);
    }
    else
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }
    
    return status;
}

static char *PrependWhere(DRMS_Env_t *env, const char *pkwhere, const char *series, const char *prefix, int *status)
{
    int istat = DRMS_SUCCESS;
    DRMS_Record_t *template = drms_template_record(env, series, &istat);
    char *ret = NULL;
    char *qualpkwhere = NULL;
    char *orig = NULL;
    char lckeyname[DRMS_MAXKEYNAMELEN];
    char repl[DRMS_MAXKEYNAMELEN + 16];
    int ipkey;
    DRMS_Keyword_t *key = NULL;
    
    if (istat == DRMS_SUCCESS)
    {
        if (template->seriesinfo->pidx_num > 0)
        {
            qualpkwhere = strdup(pkwhere);
            if (qualpkwhere)
            {
                orig = qualpkwhere;
                
                for (ipkey = 0; ipkey < template->seriesinfo->pidx_num; ipkey++)
                {
                    key = template->seriesinfo->pidx_keywords[ipkey];
                    snprintf(lckeyname, sizeof(lckeyname), "%s", key->info->name);
                    strtolower(lckeyname);
                    snprintf(repl, sizeof(repl), "%s%s", prefix, lckeyname);
                    orig = qualpkwhere;
                    qualpkwhere = base_strcasereplace(orig, lckeyname, repl);
                    if (!qualpkwhere)
                    {
                        /* This means that there were no instances of lckeyname to 
                         * replace. */
                        qualpkwhere = orig;
                        orig = NULL;
                        continue;
                    }
                    free(orig);
                    orig = NULL;
                }
                
                if (orig)
                {
                    free(orig);
                    orig = NULL;
                }
                
                ret = qualpkwhere;
            }
            else
            {
                istat = DRMS_ERROR_OUTOFMEMORY;
            }
        }
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return ret;
}

/* INSERT INTO <shadow> (pkey1, pkey2, ..., kShadowColRecnum, kShadowColNrecs) SELECT pkey1, pkey2, ..., recnum, 1 FROM <series> WHERE recnum = <recnum> */
int InsertIntoShadow(DRMS_Env_t *env,
                     const char *series,
                     char **pkeynames,
                     int ncols,
                     long long recnum)
{
    char *lcseries = NULL;
    char *query = NULL;
    size_t stsz = 1024;
    char srecnum[32];
    int icol;
    int status = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    query = malloc(stsz);
    
    if (lcseries && query)
    {
        *query = '\0';
        snprintf(srecnum, sizeof(srecnum), "%lld", recnum);
        
        query = base_strcatalloc(query, "INSERT INTO ", &stsz);
        query = base_strcatalloc(query, lcseries, &stsz);
        query = base_strcatalloc(query, kShadowSuffix, &stsz);
        query = base_strcatalloc(query, " (", &stsz);
        
        for (icol = 0; icol < ncols; icol++)
        {
            query = base_strcatalloc(query, pkeynames[icol], &stsz);
            query = base_strcatalloc(query, ", ", &stsz);
        }
        
        query = base_strcatalloc(query, kShadowColRecnum, &stsz);
        query = base_strcatalloc(query, ", ", &stsz);
        query = base_strcatalloc(query, kShadowColNRecs, &stsz);
        query = base_strcatalloc(query, ") SELECT ", &stsz);
        
        for (icol = 0; icol < ncols; icol++)
        {
            query = base_strcatalloc(query, pkeynames[icol], &stsz);
            query = base_strcatalloc(query, ", ", &stsz);
        }
        
        query = base_strcatalloc(query, "recnum, 1 FROM ", &stsz);
        query = base_strcatalloc(query, lcseries, &stsz);
        query = base_strcatalloc(query, "WHERE recnum = ", &stsz);
        query = base_strcatalloc(query, srecnum, &stsz);
        
        if (drms_dms(env->session, NULL, query))
        {
            status = DRMS_ERROR_BADDBQUERY;
        }
        
        free(query);
        free(lcseries);
    }
    else
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }
    
    return status;
}

/*                                                                                                       
 * SELECT count(*) FROM <series> as T1, (SELECT <pkey1>, <pkey2>, ... FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.<pkey1> = T2.<pkey1>, T1.<pkey2> = T2.<pkey2>, ...;                                  
*/
static int GroupExists(DRMS_Env_t *env,
                       long long recnum,
                       const char *series,
                       char **pkeynames,
                       int ncols,
                       int *status)
{
   char *query = NULL;
   size_t stsz = 4096;
   char srecnum[32];
   DB_Text_Result_t *tres = NULL;
   int ans = -1;
   int icol;
   char *lcseries = NULL;
   int istat = DRMS_SUCCESS;


   snprintf(srecnum, sizeof(srecnum), "%lld", recnum);

   lcseries = strdup(series);

   if (lcseries)
   {
      strtolower(lcseries);
      query = malloc(stsz);
      if (query)
      {
         *query = '\0';
         query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
         query = base_strcatalloc(query, lcseries, &stsz);
         query = base_strcatalloc(query, " AS T1, (SELECT ", &stsz);

         for (icol = 0; icol < ncols; icol++)
         {
            query = base_strcatalloc(query, pkeynames[icol], &stsz);

            if (icol < ncols - 1)
            {
               query = base_strcatalloc(query, ", ", &stsz);
            }
         }

         query = base_strcatalloc(query, " FROM ", &stsz);
         query = base_strcatalloc(query, lcseries, &stsz);
         query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
         query = base_strcatalloc(query, srecnum, &stsz);
         query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);

         for (icol = 0; icol < ncols; icol++)
         {
            query = base_strcatalloc(query, "T1.", &stsz);
            query = base_strcatalloc(query, pkeynames[icol], &stsz);
            query = base_strcatalloc(query, " = T2.", &stsz);
            query = base_strcatalloc(query, pkeynames[icol], &stsz);

            if (icol < ncols - 1)
            {
               query = base_strcatalloc(query, " AND ", &stsz);
            }
         }

         tres = drms_query_txt(env->session, query);

         if (tres)
         {
            if (tres->num_rows == 0)
            {
               ans = 0;
            }
            else
            {
               ans = 1;
            }

            db_free_text_result(tres);
         }
         else
         {
            fprintf(stderr, "Failed: %s\n", query);
            istat = DRMS_ERROR_BADDBQUERY;
         }

         free(query);
      }

      free(lcseries);
   }

   if (status)
   {
      *status = istat;
   }

   return ans;
}

static int WasGroupDeleted(DRMS_Env_t *env,
                           long long recnum,
                           const char *series,
                           char **pkeynames,
                           int ncols,
                           int *status)
{
   return (!GroupExists(env, recnum, series, pkeynames, ncols, status));
}

/* DELETE FROM <shadow> USING (SELECT <pkey1>, <pkey2>, ... FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.<pkey1> = T2.p1 AND T1.<pkey2> = T2.p2 AND ...*/
static int DeleteFromShadow(DRMS_Env_t *env,
                            const char *series,
                            char **pkeynames,
                            int ncols,
                            long long recnum)
{
   char *lcseries = NULL;
   char *query = NULL;
   size_t stsz = 1024;
   char srecnum[32];
   int icol;
   int status = DRMS_SUCCESS;

   lcseries = strdup(series);
   query = malloc(stsz);

   if (lcseries && query)
   {
      snprintf(srecnum, sizeof(srecnum), "%lld", recnum);
      *query = '\0';

      query = base_strcatalloc(query, "DELETE FROM ", &stsz);
      query = base_strcatalloc(query, lcseries, &stsz);
      query = base_strcatalloc(query, kShadowSuffix, &stsz);
      query = base_strcatalloc(query, " USING (SELECT ", &stsz);

      for (icol = 0; icol < ncols; icol++)
      {
         query = base_strcatalloc(query, pkeynames[icol], &stsz);

         if (icol < ncols - 1)
         {
            query = base_strcatalloc(query, ", ", &stsz);
         }
      }

      query = base_strcatalloc(query, " FROM ", &stsz);
      query = base_strcatalloc(query, lcseries, &stsz);
      query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
      query = base_strcatalloc(query, srecnum, &stsz);
      query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);

      for (icol = 0; icol < ncols; icol++)
      {
         query = base_strcatalloc(query, "T1.", &stsz);
         query = base_strcatalloc(query, pkeynames[icol], &stsz);
         query = base_strcatalloc(query, " = T2.", &stsz);
         query = base_strcatalloc(query, pkeynames[icol], &stsz);

         if (icol < ncols - 1)
         {
            query = base_strcatalloc(query, " AND ", &stsz);
         }
      }

      if (drms_dms(env->session, NULL, query))
      {
          /* If the shadow table doesn't exist, or if the series' record does not exist in the shadow table,                                                                                                      
           * then this query will fail. */
         fprintf(stderr, "Failed: %s\n", query);
         status = DRMS_ERROR_BADDBQUERY;
      }

      free(query);
      free(lcseries);
   }
   else
   {
      status = DRMS_ERROR_OUTOFMEMORY;
   }

   return status;
}



/* To copy privileges from one PG table to another, you have to
 * parse this acl format <db user>=<priv array>. This is PG-specific, and this is not a good way to do things (because as
 * a client, we don't know what the grammar is for privilege strings). But there doesn't appear to be an alternative.
 *
 * SELECT pg_catalog.array_to_string(T1.relacl,E'\n') AS acl FROM pg_catalog.pg_class T1, (SELECT oid FROM pg_catalog.pg_namespace WHE RE nspname = 'su_arta') AS T2 WHERE T1.relnamespace = T2.oid AND T1.relname = 'fd_m_96m_lev18'
 *
 * This function inserts into privs a comma-separated list of privileges, keyed by the db user that has
 * those privileges.
 */
#define kPrivStringLen 32
#define kPrivCodeLen 1
#define kPrivStringList 256
#define kPrivContainerUserLen 64

typedef enum ExtPrivState_enum
{
    kEPStateNext = 0,
    kEPStateUser = 1,
    kEPStatePrivs = 2,
    kEPStateGrantor = 3,
    kEPStateErr = 4
} ExtPrivState_t;

static int ExtractPrivileges(DRMS_Env_t *env, const char *ns, const char *tab, HContainer_t *privs)
{
    static HContainer_t *privmap = NULL;
    
    int status = DRMS_SUCCESS;
    char *lcns = NULL;
    char *lctab = NULL;
    char val[kPrivStringLen];
    char *query = NULL;
    size_t stsz = 512;
    DB_Text_Result_t *qres = NULL;
    char *row = NULL;
    char aprivcode[2];
    char *apriv = NULL;
    char *pch = NULL;
    char user[256];
    char *privlist = NULL;
    
    /* Always prepare a valid return container. */
    hcon_init(privs, kPrivStringList, kPrivContainerUserLen, NULL, NULL);
    
    if (!privmap)
    {
        privmap = hcon_create(kPrivStringLen, kPrivCodeLen, NULL, NULL, NULL, NULL, 0);
    }
    
    if (privmap)
    {
        snprintf(val, sizeof(val), "%s", "SELECT");
        hcon_insert(privmap, "r", val);
        snprintf(val, sizeof(val), "%s", "UPDATE");
        hcon_insert(privmap, "w", val);
        snprintf(val, sizeof(val), "%s", "INSERT");
        hcon_insert(privmap, "a", val);
        snprintf(val, sizeof(val), "%s", "DELETE");
        hcon_insert(privmap, "d", val);
        snprintf(val, sizeof(val), "%s", "TRUNCATE");
        hcon_insert(privmap, "D", val);
        snprintf(val, sizeof(val), "%s", "REFERENCES");
        hcon_insert(privmap, "x", val);
        snprintf(val, sizeof(val), "%s", "TRIGGER");
        hcon_insert(privmap, "t", val);
        snprintf(val, sizeof(val), "%s", "EXECUTE");
        hcon_insert(privmap, "X", val);
        snprintf(val, sizeof(val), "%s", "USAGE");
        hcon_insert(privmap, "U", val);
        snprintf(val, sizeof(val), "%s", "CREATE");
        hcon_insert(privmap, "C", val);
        snprintf(val, sizeof(val), "%s", "CONNECT");
        hcon_insert(privmap, "c", val);
        snprintf(val, sizeof(val), "%s", "TEMPORARY");
        hcon_insert(privmap, "T", val);
    }
    else
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }   
    
    if (status == DRMS_SUCCESS)
    {
        query = malloc(stsz);
        lcns = strdup(ns);
        lctab = strdup(tab);
        
        if (query && lcns && lctab)
        {
            strtolower(lcns);
            strtolower(lctab);
            *query = '\0';
            query = base_strcatalloc(query, "SELECT T1.relacl AS acl FROM pg_catalog.pg_class T1, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '", &stsz);
            query = base_strcatalloc(query, lcns, &stsz);
            query = base_strcatalloc(query, "\') AS T2 WHERE T1.relnamespace = T2.oid AND T1.relname = \'", &stsz);
            query = base_strcatalloc(query, lctab, &stsz);
            query = base_strcatalloc(query, "\'", &stsz);
            
            if ((qres = drms_query_txt(env->session, query)) != NULL)
            {
                if (qres->num_cols == 1 && qres->num_rows == 1)
                {
                    ExtPrivState_t state;
                    char *puser = NULL;
                    
                    row = strdup(qres->field[0][0]);
                    
                    if (row)
                    {
                        /* row looks like {arta=arwd/postgres,=r/postgres}. All privs get put into a                                         
                         * single row. We need to extract the                                                                                
                         * db user (left of the '=') and the privilege codes (right of the '=', before the '/'). */
                        pch = row;

                        while (*pch)
                        {
                            if (*pch == ' ')
                            {
                                pch++;
                            }
                            else
                            {
                                break;
                            }
                        }
                        
                        if (*pch == '{')
                        {
                            pch++;
                            state = kEPStateNext;
                            aprivcode[1] = '\0';
                            
                            while (*pch)
                            {
                                if (state == kEPStateErr)
                                {
                                    status = DRMS_ERROR_BADDBQUERY;
                                    break;
                                }
                                else if (state == kEPStateNext)
                                {
                                    if (*pch == '=')
                                    {
                                        snprintf(user, sizeof(user), "%s", "PUBLIC");
                                        state = kEPStatePrivs;
                                        pch++;
                                    }
                                    else if (*pch == '/' || *pch == ',')
                                    {
                                        fprintf(stderr, "Invalid user string.\n");
                                        state = kEPStateErr;
                                    }
                                    else if (*pch == '}')
                                    {
                                        /* done! */
                                        break;
                                    }
                                    else
                                    {
                                        state = kEPStateUser;
                                        puser = user;
                                    }
                                }
                                else if (state == kEPStateUser)
                                {
                                    if (*pch == '=')
                                    {
                                        *puser = '\0';
                                        puser = user;
                                        pch++;
                                        state = kEPStatePrivs;
                                    }
                                    else if (*pch == '/' || *pch == ',' || *pch == '}')
                                    {
                                        fprintf(stderr, "Invalid user string.\n");
                                        state = kEPStateErr;
                                    }
                                    else
                                    {
                                        *puser++ = *pch++;
                                    }
                                }
                                else if (state == kEPStatePrivs)
                                {
                                    if (*pch == '/')
                                    {
                                        /* end of privs */
                                        state = kEPStateGrantor;
                                        pch++;
                                    }
                                    else if (*pch == '=' || *pch == ',' || *pch == '}')
                                    {
                                        fprintf(stderr, "Invalid grantor string.\n");
                                        state = kEPStateErr;
                                    }
                                    else
                                    {
                                        aprivcode[0] = *pch;
                                        
                                        if ((apriv = (char *)hcon_lookup(privmap, aprivcode)) != NULL)
                                        {
                                            if ((privlist = (char *)hcon_lookup(privs, user)) != NULL)
                                            {
                                                /* If there is already a list established for this user,                                           
                                                 * append to it. */
                                                base_strlcat(privlist, ",", kPrivStringList);
                                                base_strlcat(privlist, apriv, kPrivStringList);
                                            }
                                            else
                                            {
                                                /* Otherwise, create a list for this user and populate it with                                     
                                                 * a single privilege string. */
                                                hcon_insert(privs, user, apriv);
                                            }
                                            
                                            pch++;
                                        }
                                        else
                                        {
                                            fprintf(stderr, "Unsupported privilege code '%s'.\n", aprivcode);
                                            state = kEPStateErr;
                                        }
                                    }
                                }
                                else if (state == kEPStateGrantor)
                                {
                                    if (*pch == ',')
                                    {
                                        /* end of grantor */
                                        state = kEPStateNext;
                                        pch++;
                                    }
                                    else if (*pch == '}')
                                    {
                                        /* end of grantor */
                                        state = kEPStateNext;
                                    }
                                    else if (*pch == '=' || *pch == '/')
                                    {
                                        fprintf(stderr, "Invalid grantor string.\n");
                                        state = kEPStateErr;
                                    }
                                    else
                                    {
                                        /* don't need the grantor - just skip past it */
                                        pch++;
                                    }
                                }
                            }
                        }
                        else
                        {
                            /* No privileges to copy! */
                        }
                        
                        free(row);
                        row = NULL;
                    }
                    else
                    {
                        status = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
                else
                {
                    fprintf(stderr, "Unexpected database response to query '%s'.\n", query);
                    status = DRMS_ERROR_BADDBQUERY;
                }
                
                db_free_text_result(qres);
                qres = NULL;
            }
            else
            {
                fprintf(stderr, "Invalid query '%s'.\n", query);
                status = DRMS_ERROR_BADDBQUERY;
            }
            
            free(lctab);
            lctab = NULL;
            free(lcns);
            lcns = NULL;
            free(query);
            query = NULL;
        }
        else
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
    }
    
    return status;
}

/* This function will not create the shadow table if: 1. it already exists, or
 * 2. the table doesn't exist and the env->createshadows flag is not set. */
static int CreateShadow(DRMS_Env_t *env, const char *series, const char *tname, int *created)
{
    int status = DRMS_SUCCESS;
    int ipkey;
    char *pklist = NULL; /* comma-separated list of lower-case prime-key keyword names */
    char *pkdatatype = NULL; /* comma-separated list of lower-case data_type strings of the prime-key columns in the shadow table. */
    char *indexquery = NULL;
    size_t stsz = 256;
    size_t stsz2 = 512;
    size_t stsz3 = 512;
    char *lcseries = strdup(series);
    char query[1024];
    DRMS_Keyword_t *key = NULL;
    char dtypestr[32];
    
    if (created)
    {
        *created = 0;
    }

    if (lcseries)
    {
        strtolower(lcseries);
        pklist = malloc(stsz);
        pkdatatype = malloc(stsz2);
        *pklist = '\0';
        *pkdatatype = '\0';
        
        if (pklist && pkdatatype)
        {
            DRMS_Record_t *template = drms_template_record(env, series, &status);
            char lckeyname[DRMS_MAXKEYNAMELEN + 1];
            
            if (status == DRMS_SUCCESS)
            {
                if (template->seriesinfo->pidx_num > 0)
                {
                    indexquery = calloc(1, sizeof(char) * stsz3);
                    
                    if (!indexquery)
                    {
                        status = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
            }
            else
            {
                fprintf(stderr, "Series %s not found.\n", series);
            }
            
            if (status == DRMS_SUCCESS)
            {
                if (tname == NULL)
                {
                    if (template->seriesinfo->hasshadow == -1)
                    {
                        /* Haven't checked to see if the shadow table exists - it might. */
                        /* This call will set template->seriesinfo->hasshadow. */
                        ShadowExists(env, series, &status);
                        
                        if (status == DRMS_ERROR_BADDBQUERY)
                        {
                            fprintf(stderr, "Unable to check database for the existence of shadow table.\n");
                        }
                    }
                }
            }

            if (status == DRMS_SUCCESS)
            {
                char *shnamens = NULL;
                char *shnametab = NULL;
                
                if (tname != NULL)
                {
                    if (get_namespace(tname, &shnamens, &shnametab))
                    {
                        status = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
                
                if (status == DRMS_SUCCESS)
                {
                    if (template->seriesinfo->hasshadow == 0 || (tname != NULL && !drms_query_tabexists(env->session, shnamens, shnametab, &status)))
                    {
                        if (status != DRMS_SUCCESS)
                        {
                            fprintf(stderr, "Unable to check for the existence of the shadow table %s.\n", tname);
                        }
                        
                        if (status == DRMS_SUCCESS)
                        {
                            if (!env->createshadows)
                            {
                                status = DRMS_ERROR_CANTCREATESHADOW;
                                fprintf(stderr, "Environment does not permit shadow-table creation.\n");
                            }
                        }
                        
                        if (status == DRMS_SUCCESS)
                        {
                            char *ns = NULL;
                            char *tab = NULL;
                            size_t shnamestsz;
                            
                            if (!get_namespace(lcseries, &ns, &tab))
                            {
                                if (tname == NULL)
                                {
                                    shnamens = strdup(ns);
                                    shnametab = strdup(tab);
                                    shnamestsz = strlen(shnametab) + 1;
                                    shnametab = base_strcatalloc(shnametab, kShadowSuffix, &shnamestsz);
                                }
                            }
                            else
                            {
                                status = DRMS_ERROR_OUTOFMEMORY;
                            }
                            
                            if (status == DRMS_SUCCESS)
                            {
                                /* Generate lists of prime-key names and datatypes. These are needed when the shadow table is
                                 * created. */
                                for (ipkey = 0; ipkey < template->seriesinfo->pidx_num; ipkey++)
                                {
                                    key = template->seriesinfo->pidx_keywords[ipkey];
                                    snprintf(lckeyname, sizeof(lckeyname), "%s", key->info->name);
                                    strtolower(lckeyname);
                                    
                                    pklist = base_strcatalloc(pklist, lckeyname, &stsz);
                                    
                                    if (key->info->type == DRMS_TYPE_STRING)
                                    {
                                        snprintf(dtypestr, sizeof(dtypestr), "%s", db_stringtype_maxlen(4000));
                                    }
                                    else
                                    {
                                        snprintf(dtypestr, sizeof(dtypestr), "%s", db_type_string(drms2dbtype(key->info->type)));
                                    }
                                    
                                    pkdatatype = base_strcatalloc(pkdatatype, lckeyname, &stsz2);
                                    pkdatatype = base_strcatalloc(pkdatatype, " ", &stsz2);
                                    pkdatatype = base_strcatalloc(pkdatatype, dtypestr, &stsz2);
                                    
                                    if (ipkey < template->seriesinfo->pidx_num - 1)
                                    {
                                        pklist = base_strcatalloc(pklist, ", ", &stsz);
                                        pkdatatype = base_strcatalloc(pkdatatype, ", ", &stsz2);
                                    }
                                    
                                    /* The queries on the shadow tables will sometimes involve where clauses containing the individual 
                                     * prime-key-constituent columns. To optimize queries, we need to create indexes for these
                                     * columns. The CREATE TABLE command will create a composite index on all prime-key member columns.
                                     *
                                     * This statement is not executed at this point. It is run after the CREATE TABLE statement
                                     * has succeeded. 
                                     */
                                    indexquery = base_strcatalloc(indexquery, "CREATE INDEX ", &stsz3);
                                    indexquery = base_strcatalloc(indexquery, shnametab, &stsz3);
                                    indexquery = base_strcatalloc(indexquery, "_", &stsz3);
                                    indexquery = base_strcatalloc(indexquery, lckeyname, &stsz3);
                                    indexquery = base_strcatalloc(indexquery, " ON ", &stsz3);
                                    indexquery = base_strcatalloc(indexquery, shnamens, &stsz3);
                                    indexquery = base_strcatalloc(indexquery, ".", &stsz3);
                                    indexquery = base_strcatalloc(indexquery, shnametab, &stsz3);
                                    indexquery = base_strcatalloc(indexquery, " (", &stsz3);
                                    indexquery = base_strcatalloc(indexquery, lckeyname, &stsz3);
                                    indexquery = base_strcatalloc(indexquery, ");", &stsz3);                                                
                                }
                            }
                            
                            /* Create the shadow table. */
                            
                            /* The owner of the new shadow table will be the user running 
                             * this code, but the permissions on the shadow table will match 
                             * the permissions on the original table. */
                            if (status == DRMS_SUCCESS)
                            {
                                snprintf(query, sizeof(query), "CREATE TABLE %s.%s (%s, nrecords integer, recnum bigint, PRIMARY KEY (%s))", shnamens, shnametab, pkdatatype, pklist);
                                
                                if (drms_dms(env->session, NULL, query))
                                {
                                    fprintf(stderr, "Failed: %s\n", query);
                                    status = DRMS_ERROR_BADDBQUERY;
                                }
                                else
                                {
                                    /* Copy the privileges of the original table to the newly created shadow table. */
                                    HContainer_t privs;
                                    const char *user = NULL;
                                    char *privlist = NULL;
                                    
                                    /* Create the indexes on the individual prime-key members. */
                                    if (drms_dms(env->session, NULL, indexquery))
                                    {
                                        fprintf(stderr, "Failed: %s\n", indexquery);
                                        status = DRMS_ERROR_BADDBQUERY;   
                                    }
                                    else
                                    {
                                        status = ExtractPrivileges(env, ns, tab, &privs);
                                        
                                        if (status == DRMS_SUCCESS)
                                        {
                                            HIterator_t *hit = hiter_create(&privs);
                                            
                                            if (hit)
                                            {
                                                while ((privlist = hiter_extgetnext(hit, &user)) != NULL)
                                                {
                                                    snprintf(query, sizeof(query), "GRANT %s ON %s.%s TO %s", privlist, shnamens, shnametab, user);
                                                    
                                                    if (drms_dms(env->session, NULL, query))
                                                    {
                                                        fprintf(stderr, "Failed: %s\n", query);
                                                        status = DRMS_ERROR_BADDBQUERY;
                                                    }
                                                }
                                                
                                                hiter_destroy(&hit);
                                            }
                                            else
                                            {
                                                status = DRMS_ERROR_OUTOFMEMORY;
                                            }
                                            
                                            hcon_free(&privs);
                                        }
                                    }
                                    
                                    /* Create the updateshadowtrig trigger on the series table. This trigger 
                                     * will call the updateshadow() function after an INSERT or DELETE on the 
                                     * series table. updateshadow() will update the shadow table (if it exists) - 
                                     * it will either update a row in, delete a row from , or insert a row into, 
                                     * the shadow table. 
                                     *
                                     * CREATE TRIGGER updateshadowtrig AFTER INSERT OR DELETE on <TABLE>
                                     *    FOR EACH ROW EXECUTE PROCEDURE updateshadow();
                                     */
                                    if (status == DRMS_SUCCESS)
                                    {
                                        if (tname == NULL)
                                        {
                                            /* We should create the trigger only if the shadow table created has the sanctioned name. 
                                             * Otherwise, we'll run the updateshadow db-server function, which would then fail because
                                             * it would look for a shadow table that did not exist. */
                                            snprintf(query, sizeof(query), "CREATE TRIGGER %s AFTER INSERT OR DELETE on %s FOR EACH ROW EXECUTE PROCEDURE %s()", kShadowTrig, lcseries, kShadowTrigFxn);
                                            
                                            if (drms_dms(env->session, NULL, query))
                                            {
                                                fprintf(stderr, "Failed: %s\n", query);
                                                status = DRMS_ERROR_BADDBQUERY;   
                                            }
                                        }
                                    }
                                    
#if (defined TRACKSHADOWS && TRACKSHADOWS)
#error
                                    /* Create a temporary trigger on the ORIGINAL series table. This trigger will intercept
                                     * record insertions and deletions and execute a plpgsql function that will insert 
                                     * one record for each record inserted/deleted into a shadow-table tracker. This table */
                                    if (status == DRMS_SUCCESS)
                                    {
                                        snprintf(query, 
                                                 sizeof(query), 
                                                 "DROP TRIGGER IF EXISTS shadowtracker on %s; CREATE TRIGGER shadowtracker AFTER INSERT OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE %s()",
                                                 lcseries,
                                                 lcseries,
                                                 kshadowTrackerFxn);
                                        
                                        if (drms_dms(env->session, NULL, query))
                                        {
                                            fprintf(stderr, "Failed: %s\n", query);
                                            status = DRMS_ERROR_BADDBQUERY;   
                                        }
                                    }
#endif
                                }
                            }
                            
                            /* Populate the shadow table with data from the original series table. 
                             * Do this last, otherwise we could spend hours populating the table, just to have 
                             * one of the other steps fail. */
                            if (status == DRMS_SUCCESS)
                            {
                                status = PopulateShadow(env, series, tname);
                            }
                            
                            if (status == DRMS_SUCCESS)
                            {
                                template->seriesinfo->hasshadow = 1;
                                
                                if (created)
                                {
                                    *created = 1;
                                }
                            }
                            
                            if (ns)
                            {
                                free(ns);
                            }
                            
                            if (tab)
                            {
                                free(tab);
                            }
                        }
                    }
                    else
                    {
                        fprintf(stderr, "Shadow table %s%s already exists.\n", lcseries, kShadowSuffix);
                    }
                }
                
                if (shnamens)
                {
                    free(shnamens);
                }
                
                if (shnametab)
                {
                    free(shnametab);
                }
            }
            
            free(pklist);
            pklist = NULL;
            free(indexquery);
            indexquery = NULL;
            free(pkdatatype);
            pkdatatype = NULL;
        }
        else
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }
    
    return status;
}

/* Drops the shadow for series series. 
 *
 * If tname != NULL, then this drops the table specified in tname, and it does not drop any triggers that may exist
 * on the original series table. */
static int DropShadow(DRMS_Env_t *env, const char *series, const char *tname, int *dropped)
{
    int status = DRMS_SUCCESS;
    int exists = 0;
    char cmd[1024];
    char *lcseries = strdup(series);
    char *shadow = NULL;
    
    if (dropped)
    {
        *dropped = 0;
    }

    /* First, figure out if the shadow table exists already. */
    if (tname != NULL)
    {
        shadow = strdup(tname);
    }
    else
    {
        shadow = malloc(strlen(lcseries) + strlen(kShadowSuffix) + 1);
        if (shadow)
        {
            snprintf(shadow, strlen(lcseries) + strlen(kShadowSuffix) + 1, "%s%s", lcseries, kShadowSuffix);
        }
    }
    
    if (!shadow)
    {
        status = DRMS_ERROR_OUTOFMEMORY;
    }
    else
    {
        strtolower(shadow);
    }
    
    if (status == DRMS_SUCCESS)
    {
        /* Gotta split-up the table name - uh. */
        char *shnamens = NULL;
        char *shnametab = NULL;
        
        if (get_namespace(shadow, &shnamens, &shnametab))
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
        else
        {
            exists = drms_query_tabexists(env->session, shnamens, shnametab, &status);
        }
    }
    
    if (status == DRMS_SUCCESS)
    {
        if (lcseries)
        {
            strtolower(lcseries);
        }
        else
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
    }

    /* Drop the trigger on the series table. This trigger runs the function that updates the shadow table. */
    if (status == DRMS_SUCCESS)
    {
        if (exists)
        {
            if (tname == NULL)
            {
                /* The trigger exists only if the caller did not provide a shadow-table name when creating the shadow
                 * table. */
                snprintf(cmd, sizeof(cmd), "DROP TRIGGER IF EXISTS %s ON %s", kShadowTrig, lcseries);
                
                if (drms_dms(env->session, NULL, cmd))
                {
                    fprintf(stderr, "Failed: %s\n", cmd);
                    status = DRMS_ERROR_BADDBQUERY;   
                }
            }
        }
    }
    
    /* Drop the shadow - the existing records in this table and the indexes on this table will be
     * dropped as well. */
    if (status == DRMS_SUCCESS)
    {
        if (exists)
        {
            snprintf(cmd, sizeof(cmd), "DROP TABLE IF EXISTS %s CASCADE", shadow);
            
            if (drms_dms(env->session, NULL, cmd))
            {
                fprintf(stderr, "Failed: %s\n", cmd);
                status = DRMS_ERROR_BADDBQUERY;   
            }
            else
            {
                if (dropped)
                {
                    *dropped = 1;
                }
            }
        }
    }
    
    /* Clean up. */
    if (shadow)
    {
        free(shadow);
        shadow = NULL;
    }
    
    if (lcseries)
    {
        free(lcseries);
        lcseries = NULL;
    }
    
    return status;
}

/* The record just added to the series table is in a new group if there is only one record in that group. */
/* "SELECT count(*) FROM <series> as T1, (SELECT <pkey1> AS p1, <pkey2> AS p2, ... FROM <series> WHERE recnum = <recnum>) AS T2 WHERE T1.<pkey1> = T2.p1, T1.<pkey2> = T2.p2, ...";                                                                                          
 */
static int IsGroupNew(DRMS_Env_t *env,
                      long long recnum,
                      const char *series,
                      char **pkeynames,
                      int ncols,
                      int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char scolnum[8];
    char srecnum[32];
    DB_Text_Result_t *tres = NULL;
    int ans = -1;
    int icol;
    char *lcseries = NULL;
    int istat = DRMS_SUCCESS;
    
    snprintf(srecnum, sizeof(srecnum), "%lld", recnum);
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            
            query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, " AS T1, (SELECT ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = base_strcatalloc(query, pkeynames[icol], &stsz);
                query = base_strcatalloc(query, " AS p", &stsz);
                query = base_strcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = base_strcatalloc(query, ", ", &stsz);
                }
            }
            
            query = base_strcatalloc(query, " FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, " WHERE recnum = ", &stsz);
            query = base_strcatalloc(query, srecnum, &stsz);
            query = base_strcatalloc(query, ") AS T2 WHERE ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = base_strcatalloc(query, "T1.", &stsz);
                query = base_strcatalloc(query, pkeynames[icol], &stsz);
                query = base_strcatalloc(query, " = T2.p", &stsz);
                query = base_strcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = base_strcatalloc(query, " AND ", &stsz);
                }
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    /* Execute the query. */
    if (istat == DRMS_SUCCESS)
    {
        tres = drms_query_txt(env->session, query);
        
        if (tres)
        {
            if (tres->num_rows == 1 && tres->num_cols == 1)
            {
                long long num = atoll(tres->field[0][0]);
                
                if (num < 1)
                {
                    /* This implies that the record just added doesn't exist */
                    fprintf(stderr, "Unexpected record count %lld; should be at least one.\n", num);
                    istat = DRMS_ERROR_BADFIELDCOUNT;
                }
                else
                {
                    ans = (num == 1) ? 1 : 0;
                }
            }
            
            db_free_text_result(tres);
        }
        else
        {
            fprintf(stderr, "Failed: %s\n", query);
            istat = DRMS_ERROR_BADDBQUERY;
        }
        
        free(query);
        query = NULL;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return ans;
}

static char *PrependFields(const char *list, const char *prefix, int *status)
{
    char *pnext = NULL;
    char *fieldscp = NULL;
    char *qualfields = NULL;
    char *pch = NULL;
    size_t stsz2 = 4096;
    int istat = DRMS_SUCCESS;
    
    qualfields = malloc(stsz2);
    fieldscp = strdup(list);
    
    if (qualfields && fieldscp)
    {
        *qualfields = '\0';
        
        pnext = fieldscp;
        
        while ((pch = strchr(pnext, ',')) != NULL)
        {
            *pch = '\0';
            qualfields = base_strcatalloc(qualfields, prefix, &stsz2);
            qualfields = base_strcatalloc(qualfields, pnext, &stsz2);
            qualfields = base_strcatalloc(qualfields, ", ", &stsz2);
            pch++;
            
            while (*pch == ' ')
            {
                pch++;
            }
            
            pnext = pch;
        }
        
        /* There is still one more column that has not been processed yet. */
        qualfields = base_strcatalloc(qualfields, prefix, &stsz2);
        qualfields = base_strcatalloc(qualfields, pnext, &stsz2);
        
        free(fieldscp);
        fieldscp = NULL;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return qualfields;
}

static char *CreatePKeyList(DRMS_Env_t *env, const char *series, const char *prefix, const char *suffix, char *pkeyarr[], int *npkey, int doTypes, int *status)
{
    int istat = DRMS_SUCCESS;
    int ipkey;
    char lckeyname[DRMS_MAXKEYNAMELEN];
    DRMS_Keyword_t *key = NULL;
    DRMS_Record_t *template = drms_template_record(env, series, &istat);
    char *pklist = NULL;
    size_t stsz = 4096;
    
    if (istat == DRMS_SUCCESS)
    {
        if (template->seriesinfo->pidx_num > 0)
        {
            pklist = malloc(stsz);
            
            if (pklist)
            {
                *pklist = '\0';
                
                for (ipkey = 0; ipkey < template->seriesinfo->pidx_num; ipkey++)
                {
                    key = template->seriesinfo->pidx_keywords[ipkey];
                    snprintf(lckeyname, sizeof(lckeyname), "%s", key->info->name);
                    strtolower(lckeyname);
                    
                    if (prefix)
                    {
                        pklist = base_strcatalloc(pklist, prefix, &stsz);
                    }
                    
                    pklist = base_strcatalloc(pklist, lckeyname, &stsz);
                    
                    if (doTypes)
                    {
                        pklist = base_strcatalloc(pklist, " ", &stsz);
                        if (key->info->type == DRMS_TYPE_STRING)
                        {
                            pklist = base_strcatalloc(pklist, db_stringtype_maxlen(4000), &stsz);
                        }
                        else
                        {
                            pklist = base_strcatalloc(pklist, db_type_string(drms2dbtype(key->info->type)), &stsz);
                        }
                    }
                    
                    if (suffix)
                    {
                        pklist = base_strcatalloc(pklist, suffix, &stsz);
                    }
                    
                    if (ipkey < template->seriesinfo->pidx_num - 1)
                    {
                        pklist = base_strcatalloc(pklist, ", ", &stsz);
                    }
                    
                    /* Fill in the pkeyarr. */
                    if (pkeyarr)
                    {
                        pkeyarr[ipkey] = strdup(lckeyname);
                    }
                }
            }
            else
            {
                istat = DRMS_ERROR_OUTOFMEMORY;
            }
        }
    }
    
    if (npkey)
    {
        *npkey = template->seriesinfo->pidx_num;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return pklist;
}

char *drms_series_createPkeyList(DRMS_Env_t *env, const char *series, const char *prefix, const char *suffix, char *pkeyarr[], int *npkey, int *status)
{
    return CreatePKeyList(env, series, prefix, suffix, pkeyarr, npkey, 0, status);
}

char *drms_series_createPkeyColList(DRMS_Env_t *env, const char *series, const char *prefix, const char *suffix, char *pkeyarr[], int *npkey, int *status)
{
    return CreatePKeyList(env, series, prefix, suffix, pkeyarr, npkey, 1, status);
}

static void DetermineDefval(DRMS_Keyword_t *key, char *defvalout)
{
    int pret = 0;
    TIME interval;
    int internal;
    char defval[DRMS_DEFVAL_MAXLEN];
    
    if (defvalout)
    {
        if (key->info->type == DRMS_TYPE_TIME)
        {
            interval = atoinc(key->info->unit);
            internal = (interval > 0);
            
            pret = drms_sprintfval(defval, 
                                   key->info->type, 
                                   &key->value, 
                                   internal);
            XASSERT(pret < DRMS_DEFVAL_MAXLEN);
        }
        else
        {
            /* We want to store the default value as a text string, with canonical formatting.
             * The conversion used (in drms_record.c) when populating DRMS_Keyword_ts is:
             *   char -> none (first char in stored text string is used)
             *   short -> strtol(..., NULL, 0)
             *   int -> strtol(..., NULL, 0)
             *   long long -> strtoll(..., NULL, 0)
             *   float -> atof
             *   double -> atof
             *   time -> sscan_time
             *   string -> none (copy_string)
             */
            pret = drms_sprintfval(defval, 
                                   key->info->type,
                                   &key->value,	 
                                   0);
            XASSERT(pret < DRMS_DEFVAL_MAXLEN);
            
            /* If the resulting string is quoted, strip the quotes. */
            if (key->info->type == DRMS_TYPE_STRING)
            {
                size_t slen = strlen(defval);
                int ich;
                
                if (slen > 1)
                {
                    if ((defval[0] == '\'' && defval[slen - 1] == '\'') ||
                        (defval[0] == '\"' && defval[slen - 1] == '\"'))
                    {
                        ich = 0;
                        while(ich < slen - 2)
                        {
                            defval[ich] = defval[ich + 1];
                            ich++;
                        }
                        
                        defval[ich] = '\0';
                    }
                }
            }
        }
        
        snprintf(defvalout, DRMS_DEFVAL_MAXLEN, "%s", defval);
    }
}
/* 
extract namespace from series name. 
return error if no '.' present
*/ 
int get_namespace(const char *seriesname, char **namespace, char **shortname) {
  if (strchr(seriesname, '.')) {
    const char *p = seriesname;
    while (*p != '.') {
      p++;
    }
    *namespace = ns(seriesname);
    if (shortname) {
      *shortname = strdup(p+1);
    }
    return 0;
  } else {
    return 1;
  }
}

int drms_series_exists(DRMS_Env_t *drmsEnv, const char *sname, int *status)
{
   int ret = 0;

   if (sname != NULL && *sname != '\0')
   {
      drms_template_record(drmsEnv, sname, status);

      if (*status == DRMS_ERROR_UNKNOWNSERIES)
      {
	 ret = 0;
      }
      else if (*status != DRMS_SUCCESS)
      {
	 fprintf(stderr, "DRMS Error calling drms_series_exists.\n");
      }
      else
      {
	 ret = 1;
      }
   }

   return ret;
}

/* Create/update the database tables and entries for a new/existing series.
   The series is created from the information in the series template record
   given in the argument "template".
   If update=1 an existing series is updated. If update=0 a new series is 
   created. If update=0 and a series of the same name already exists an
   error code is returned.
*/
int drms_insert_series(DRMS_Session_t *session, int update, 
		       DRMS_Record_t *template, int perms)
{
  int i, len=0, segnum;
  char *pidx_buf=0, *dbidx_buf=0, scopestr[100], *axisstr=0;
  DRMS_SeriesInfo_t *si;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  DRMS_Link_t *link;
  HIterator_t hit;
  char *linktype,*p,*q;
  char dyn[]="dynamic";
  char stat[]="static";
  char defval[DRMS_DEFVAL_MAXLEN]={0};
  char *createstmt=0, *series_lower=0, *namespace=0;
  DB_Text_Result_t *qres;

  createstmt = malloc(30000);
  XASSERT(createstmt);

  /* Make sure links have pidx's set. */
  if (drms_link_getpidx(template) != DRMS_SUCCESS)
  {
     goto failure;
  }

  si = template->seriesinfo;
  // extract namespace from series name. default to 'public'
  if (get_namespace(si->seriesname, &namespace, &series_lower)) {
    fprintf(stderr, "Invalid seriesname: namespace missing\n");
    goto failure;
  }
  strtolower(namespace);
  /* series_lower does not contain namespace information. This is
     needed to create index */
  strtolower(series_lower);

  // namespace must exists
  sprintf(createstmt, "select * from pg_namespace where nspname = '%s'", namespace);
  if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
    if (qres->num_rows == 0) {
      fprintf(stderr, "Namespace %s does not exist.\n", namespace);
      goto failure;
    }
    db_free_text_result(qres);
  }

  // dbuser must have create privilege to namespace
  // this does not work when called from client module because there
  // is no dbuser information.
  if (session->db_direct) {
    sprintf(createstmt, "select has_schema_privilege('%s', '%s', 'create')", 
	    session->db_handle->dbuser,
	    namespace);
    if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
      if (qres->num_rows > 0) {
	if (qres->field[0][0][0] == 'f') {
	  fprintf(stderr, "dbuser %s does not have create privilege in namespace %s.\n",
		  session->db_handle->dbuser, namespace);
	  goto failure;
	}
      } else {
	fprintf(stderr, "Failed: %s\n", createstmt);
	goto failure;
      }
    } else {
      fprintf(stderr, "Failed: %s\n", createstmt);
      goto failure;
    }      
    db_free_text_result(qres);
  }

  sprintf(createstmt, "set search_path to %s", namespace);
  if(drms_dms(session, NULL, createstmt)) {
    fprintf(stderr, "Failed: %s\n", createstmt);
    goto failure;
  }

  if (si->pidx_num==0)
  {
    pidx_buf = malloc(2);
    XASSERT(pidx_buf);
    *pidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->pidx_num; i++)
      len += strlen((si->pidx_keywords[i])->info->name) + 3;
    pidx_buf = malloc(len+1);
    XASSERT(pidx_buf);
    memset(pidx_buf,0,len+1);
    p = pidx_buf;
    p += sprintf(p,"%s",(si->pidx_keywords[0])->info->name);
    for (i=1; i<si->pidx_num; i++)
      p += sprintf(p,", %s",(si->pidx_keywords[i])->info->name);
  }

  if (si->dbidx_num <= 0)
  {
    dbidx_buf = malloc(2);
    XASSERT(dbidx_buf);
    *dbidx_buf = 0;
  }
  else
  {
    len = 0;
    for (i=0; i<si->dbidx_num; i++)
      len += strlen((si->dbidx_keywords[i])->info->name) + 3;
    dbidx_buf = malloc(len+1);
    XASSERT(dbidx_buf);
    memset(dbidx_buf,0,len+1);
    p = dbidx_buf;
    p += sprintf(p,"%s",(si->dbidx_keywords[0])->info->name);
    for (i=1; i<si->dbidx_num; i++)
      p += sprintf(p,", %s",(si->dbidx_keywords[i])->info->name);
  }

  if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_SERIES_TABLE
		"(seriesname, description, author, owner, unitsize, archive,"
		"retention, tapegroup, version, primary_idx, dbidx, created) values (?,?,?,?,"
		"?,?,?,?,?,?,?,LOCALTIMESTAMP(0))", -1,
		DB_STRING, si->seriesname, DB_STRING, si->description, 
		DB_STRING, si->author, DB_STRING, si->owner,
		DB_INT4, si->unitsize, DB_INT4, si->archive, 
		DB_INT4, si->retention, DB_INT4, si->tapegroup, 
		DB_STRING, si->version,
		DB_STRING, pidx_buf,
		DB_STRING, dbidx_buf))
    goto failure;
  
  p = createstmt;
  /* Fixed fields. */
  p += sprintf(p,"create table %s (",series_lower); 
  p += sprintf(p,"recnum bigint not null"); 
  p += sprintf(p,", sunum bigint"); 
  p += sprintf(p,", slotnum integer"); 
  p += sprintf(p,", sessionid bigint"); 
  p += sprintf(p,", sessionns text");

  /* Link fields. */
  hiter_new_sort(&hit, &template->links, drms_link_ranksort); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      linktype = stat;
      p += sprintf(p,", ln_%s bigint",link->info->name);
    }
    else  /* Oh crap! A dynamic link... */
    {
      linktype = dyn;
      if (link->info->pidx_num) {
	p += sprintf(p,", ln_%s_isset %s default 0", link->info->name,  
		     db_type_string(drms2dbtype(DRMS_TYPE_INT)));
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
      {
	p += sprintf(p,", ln_%s_%s %s",link->info->name, link->info->pidx_name[i], 
		     db_type_string(drms2dbtype(link->info->pidx_type[i])));
      }
    }
    
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_LINK_TABLE
		  "(seriesname, linkname, target_seriesname, type, "
		  "description) values (?,?,?,?,?)", -1, 
		  DB_STRING, si->seriesname, DB_STRING, link->info->name,
		  DB_STRING, link->info->target_series, DB_STRING, linktype,
		  DB_STRING, link->info->description))
    {
        hiter_free(&hit);
        goto failure;
    }
  }
    
  hiter_free(&hit);
    
  /* Keyword fields. */
  hiter_new_sort(&hit, &template->keywords, drms_keyword_ranksort); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      if (key->info->type==DRMS_TYPE_STRING)
      {
	p += sprintf(p,", %s %s",key->info->name,
		     db_stringtype_maxlen(4000));
      }
      else
      {
	p += sprintf(p,", %s %s",key->info->name,
		     db_type_string(drms2dbtype(key->info->type)));
      }
    }
#ifdef DEBUG
    printf("keyword '%s'\n",key->info->name);
#endif

    /* key->info->per_segment overload: This will work on <= drms_series.c:1.23 because
     * in that version of DRMS, the version of drms_parser.c will not have the 
     * code that overloads key->info->per_segment.
     */
    if (drms_keyword_getperseg(key))
    {
      len = strlen(key->info->name);
      if (strcmp(key->info->name+len-4,"_000"))
	continue;
      key->info->name[len-4] = 0;
#ifdef DEBUG
      printf("Inserting per-segment keyword %s\n",key->info->name);
#endif
    }
      
      DetermineDefval(key, defval);

    /* The persegment column used to be either 0 or 1 and it said whether the keyword
     * was a segment-specific column or not.  But starting with series version 2.1, 
     * the persegment column was overloaded to hold all the keyword flags (including
     * per_seg).  So, the following code works on both < 2.1 series and >= 2.1 series.
     */
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_KEYWORD_TABLE
		  "(seriesname, keywordname, linkname, targetkeyw, type, "
		  "defaultval, format, unit, description, islink, "
		  "isconstant, persegment) values (?,?,?,?,?,?,?,?,?,?,?,?)",
		  -1,
		  DB_STRING, key->record->seriesinfo->seriesname, 
		  DB_STRING, key->info->name, DB_STRING, key->info->linkname, 
		  DB_STRING, key->info->target_key, 
		  DB_STRING, drms_type2str(key->info->type), DB_STRING, defval, 
		  DB_STRING, key->info->format, DB_STRING, key->info->unit,
		  DB_STRING, key->info->description,
		  DB_INT4, key->info->islink,DB_INT4, key->info->recscope, /* stored in the isconstant column of
                                                                            * drms_keyword. */
		  DB_INT4, key->info->kwflags))
    {
        hiter_free(&hit);
        goto failure;
    }

    /* key->info->per_segment overload: This will work on <= drms_series.c:1.23 because
     * in that version of DRMS, the version of drms_parser.c will not have the 
     * code that overloads key->info->per_segment.
     */
    if (drms_keyword_getperseg(key))
      key->info->name[len-4] = '_';

  }
    
  hiter_free(&hit);
    
  /* Segment fields. */
  hiter_new_sort(&hit, &template->segments, drms_segment_ranksort); /* Iterator for segment container. */
  segnum = 0;
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    switch(seg->info->scope)
    {
    case DRMS_CONSTANT:
      strcpy(scopestr,"constant");
      break;
    case DRMS_VARIABLE:
      strcpy(scopestr,"variable");
      break;
    case DRMS_VARDIM:
      strcpy(scopestr,"vardim");
      break;
    default:
      printf("ERROR: Invalid value of scope (%d).\n", (int)seg->info->scope);
      hiter_free(&hit);
      goto failure;
    }
    if (seg->info->naxis < 0 || seg->info->naxis>DRMS_MAXRANK)
    {
      printf("ERROR: Invalid value of rank (%d).\n",seg->info->naxis);
      hiter_free(&hit);
      goto failure;
    }
    else
    {
      axisstr = malloc(2*seg->info->naxis*20+1);
      XASSERT(axisstr);
      axisstr[0] = 0;
      q = axisstr;
      if (seg->info->naxis>0)
      {	
	q += sprintf(q,"%d",seg->axis[0]);
	for (i=1; i<seg->info->naxis; i++)
	  q+=sprintf(q,", %d",seg->axis[i]);
	if (seg->info->protocol == DRMS_TAS)
	for (i=0; i<seg->info->naxis; i++)
	  q+=sprintf(q,", %d",seg->blocksize[i]);	  
      }
    }
    
    if (drms_dmsv(session, NULL, "insert into " DRMS_MASTER_SEGMENT_TABLE
		  "(seriesname, segmentname, segnum, scope, type,"
		  " naxis, axis, unit, protocol, description, islink, linkname, targetseg)"
		  " values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
		  -1,
		  DB_STRING, seg->record->seriesinfo->seriesname,
		  DB_STRING, seg->info->name, DB_INT4, seg->info->segnum, 
		  DB_STRING, scopestr, 
		  DB_STRING, drms_type2str(seg->info->type), 
		  DB_INT4, seg->info->naxis,  DB_STRING, axisstr,
		  DB_STRING, seg->info->unit, 
		  DB_STRING, drms_prot2str(seg->info->protocol),
		  DB_STRING, seg->info->description,
		  DB_INT4, seg->info->islink,
	          DB_STRING, seg->info->linkname, 
		  DB_STRING, seg->info->target_seg))
    {
      free(axisstr);
      hiter_free(&hit);
      goto failure;
    }
    free(axisstr);

    /* All segments have an assciated file. */
    p += sprintf(p,", sg_%03d_file text", segnum);

    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
      {
	p += sprintf(p,", sg_%03d_axis%03d integer",segnum,i);
      }
    }

    segnum++;
  }
    
  hiter_free(&hit);
    
  p += sprintf(p,", primary key(recnum))");
#ifdef DEBUG
  printf("statement = '%s'\n",createstmt);
#endif

  /* Create the main table for the series. */
  if(drms_dms(session, NULL, createstmt))
    goto failure;

  /* 
     Backward compatibility: don't create composite index unless there
     is no DBIndex in jsd. 
     Since we are increasing the max number of prime keys allowed,
     make sure no composite index with > 5 keyword is made.
  */
  if (si->dbidx_num == -1 && si->pidx_num>0 && si->pidx_num <= 5)
  {
    /* Build an index of the primary index columns. */
    p = createstmt;
    p += sprintf(p,"create index %s_prime_idx on %s ( %s )",
		 series_lower, series_lower, pidx_buf);
    if(drms_dms(session, NULL, createstmt))
      goto failure;
  }

  if (si->dbidx_num > 0) {
    p = createstmt;
    for (i = 0; i < si->dbidx_num; i++) {
      char *dbidx_name = (si->dbidx_keywords[i])->info->name;
      p += sprintf(p,"create index %s_%s on %s ( %s );",
		   series_lower, dbidx_name, series_lower, dbidx_name);
    }
    if(drms_dms(session, NULL, createstmt))
      goto failure;
  }

  /* Create sequence for generating record numbers for the new series. */
  if (drms_sequence_create(session, template->seriesinfo->seriesname))
    goto failure;

  /* default to readable by public */
  p = createstmt;
  p += sprintf(p,"grant select on %s to public;",series_lower);
  p += sprintf(p,"grant select on %s_seq to public;",series_lower);
  p += sprintf(p,"grant delete on %s to sumsadmin;",series_lower);
  if(drms_dms(session, NULL, createstmt))
    goto failure;

  /* default permission for owner of the namespace */
  sprintf(createstmt, "select owner from admin.ns where name = '%s'", namespace);
  if ( (qres = drms_query_txt(session, createstmt)) != NULL) {
    if (qres->num_rows > 0) {
      char *nsowner = qres->field[0][0];
      p = createstmt;
      p += sprintf(p, "grant select, insert, update, delete on %s to %s;", series_lower, nsowner);
      p += sprintf(p, "grant update on %s_seq to %s;", series_lower, nsowner);
      if(drms_dms(session, NULL, createstmt))
	goto failure;
    } else {
      fprintf(stderr, "Failed: %s\n", createstmt);
      goto failure;
    }
  } else {
    fprintf(stderr, "Failed: %s\n", createstmt);
    goto failure;
  }      
  db_free_text_result(qres);

  /* The following is not well defined, hence leftout for now */
  if (0 && perms)
  {
    char permstr[30];
    p = permstr;
    if (perms & DB_PRIV_SELECT)
      p += sprintf(p,"select");
    if (perms & DB_PRIV_INSERT)
    {
      if (p!=permstr)
	p += sprintf(p,", ");
      p += sprintf(p,"insert");
    }
    if (perms & DB_PRIV_UPDATE)
    {
      if (p!=permstr)
	p += sprintf(p,", ");
      p += sprintf(p,"update");
    }
    sprintf(createstmt,"grant %s on %s to jsoc",permstr,series_lower);
#ifdef DEBUG
    printf("Setting permisions on table with '%s'\n",createstmt);
#endif
    /* Give table privileges to public. */
    if(drms_dms(session, NULL, createstmt))
      goto failure;

    sprintf(createstmt,"grant %s on %s_seq to jsoc",permstr,series_lower);
#ifdef DEBUG
    printf("Setting permisions on table with '%s'\n",createstmt);
#endif
    /* Give table privileges to public. */
    if(drms_dms(session, NULL, createstmt))
      goto failure;
    
    if (perms & DB_PRIV_SELECT)
    {
      sprintf(createstmt,"grant select on %s to jsoc_reader",series_lower);
      /* Give table privileges to public. */
      if(drms_dms(session, NULL, createstmt))
	goto failure;
      sprintf(createstmt,"grant select on %s_seq to jsoc_reader",series_lower);
      /* Give table privileges to public. */
      if(drms_dms(session, NULL, createstmt))
	goto failure;
    }
  }
    
    /* If the createshadow flag is set, then, at this time, we will create a shadow table. */
    if (si->createshadow)
    {
        int wascreated = 0;

        if (CreateShadow(template->env, si->seriesname, NULL, &wascreated) != DRMS_SUCCESS || !wascreated)
        {
            /* If CreateShadow() returned success, but the shadow table was not created, 
             * this means it had already existed. This shouldn't happen, because 
             * we're creating the original series now. */
            goto failure;
        }
    }
    
  free(namespace);
  free(series_lower);
  free(pidx_buf);
  free(dbidx_buf);
  free(createstmt);
  return 0;
 failure:
  fprintf(stderr,"drms_insert_series(): failed to insert series %s.\n",
	  template->seriesinfo->seriesname);
  free(namespace);
  free(series_lower);
  free(pidx_buf);
  free(dbidx_buf);
  free(createstmt);
  return 1;
}

static int RankReverseSort(const void *he1, const void *he2)
{
    DRMS_Keyword_t *k1 = (DRMS_Keyword_t *)hcon_getval(*((HContainerElement_t **)he1));
    DRMS_Keyword_t *k2 = (DRMS_Keyword_t *)hcon_getval(*((HContainerElement_t **)he2));
    
    XASSERT(k1 && k2);
    
    return (k1->info->rank < k2->info->rank) ? 1 : (k1->info->rank > k2->info->rank ? -1 : 0);
}
/* Returns -1 if the series does not have ranked keywords (with valid ranks), 
 * otherwise returns the 0-based rank. */
int drms_series_gethighestkeyrank(DRMS_Env_t *env, const char *series, int *status)
{
    int drmsstat = DRMS_SUCCESS;
    DRMS_Record_t *rec = NULL;
    HContainer_t *keys = NULL;
    DRMS_Keyword_t *key = NULL;
    HIterator_t hit;
    int rv = -1; /* The series does not have rankings. */
    
    rec = drms_template_record(env, series, &drmsstat);
    
    if (!rec || drmsstat)
    {
        if (!drmsstat)
        {
            drmsstat = DRMS_ERROR_UNKNOWNSERIES;
        }
        
        fprintf(stderr, "Unable to obtain template record for series %s; error %d.\n", series, drmsstat);
    }
    else
    {
        keys = &rec->keywords;
        if (hcon_size(keys) > 0)
        {
            hiter_new_sort(&hit, keys, RankReverseSort);
            key = (DRMS_Keyword_t *)hiter_getnext(&hit);
            if (!key)
            {
                fprintf(stderr, "Missing keyword.\n");
                drmsstat = DRMS_ERROR_UNKNOWNKEYWORD;
            }
            else
            {
                rv = key->info->rank;
            }
            
            hiter_free(&hit);
        }
    }
    
    if (status)
    {
        *status = drmsstat;
    }
    
    return rv;
}

/* This function will take as input a jsd keyword specification, and a series
 * name, and it will modify the series' db information to add the keywords listed
 * in the spec. 
 *
 * For now, the function will not add keys that are members of the prime-key set.
 * In fact, with this function, there is no way to specify that this key is 
 * a member of the prime-key set.
 *
 * sql will contain the SQL needed to add the columns to the series table, as well
 * as add the rows to the <ns>.drms_keyword table, IFF the series is a member of a
 * Slony replication-set. Otherwise, sql will contain NULL. These SQL statements must 
 * be executed on the Slony master. To do this, run a script that calls a DRMS module
 * that calls this function. The module should return to the script the SQL.
 */
int drms_addkeys_toseries(DRMS_Env_t *env, const char *series, const char *spec, char **sql)
{
    int drmsstat = DRMS_SUCCESS;
    DRMS_Record_t *seriestemp = NULL;
    HContainer_t *keys = NULL;
    int hirank = -1;
    HIterator_t *hit = NULL;
    DRMS_Keyword_t *key = NULL;
    int isrepl = 0; /* 1 if the series is under slony repication. */
    char defval[DRMS_DEFVAL_MAXLEN];
    DRMS_Session_t *session = env->session;
    char numbuf[32];
    size_t szalterbuf;
    char *tblactionbuf = NULL;
    size_t szbuf;
    char *sqlbuf = NULL;
    int first;
    int skipkey;
    int len;
    char *ns = NULL;
    char *tab = NULL;
    int nkeys;
    
    *sql = NULL;
    
    if (series && *series && spec && *spec)
    {
        seriestemp = drms_template_record(env, series, &drmsstat);
        
        if (seriestemp && drmsstat == DRMS_SUCCESS)
        {
            isrepl = drms_series_isreplicated(env, series);
            
            /* Parse spec with drms_parser code. This will return a container of DRMS_Keyword_t structs. */
            keys = drms_parse_keyworddesc(env, spec, &drmsstat);
            
            if (keys && !drmsstat && hcon_size(keys) >= 1)
            {
                /* hirank is a 0-based rank. */
                hirank = drms_series_gethighestkeyrank(env, series, &drmsstat);
                if (drmsstat)
                {
                    hirank = -1;
                    drmsstat = DRMS_SUCCESS;
                }
                
                if (isrepl) 
                {
                    szbuf = 1024;
                    sqlbuf = calloc(1, szbuf);
                    
                    if (!sqlbuf)
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                    else
                    {
                        szalterbuf = 512;
                        tblactionbuf = calloc(1, szalterbuf);
                        
                        if (!tblactionbuf)
                        {
                            drmsstat = DRMS_ERROR_OUTOFMEMORY;
                        }
                    }
                }
                else
                {
                   szalterbuf = 512;
                   tblactionbuf = calloc(1, szalterbuf);

                   if (!tblactionbuf)
                   {
                      drmsstat = DRMS_ERROR_OUTOFMEMORY;
                   }
                }
                
                if (drmsstat == DRMS_SUCCESS)
                {
                    if (get_namespace(series, &ns, &tab))
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
                
                if (drmsstat == DRMS_SUCCESS)
                {
                    /* Set the pointer from the key struct to the containing record. */
                    hit = hiter_create(keys);
                    
                    if (hit)
                    {
                        first = 1;
                        nkeys = 0;
                        while((key = (DRMS_Keyword_t *)hiter_getnext(hit)) != NULL)
                        {
                            /* First, ensure that the keyword to add does not already exist. */
                            if (drms_keyword_lookup(seriestemp, key->info->name, 0))
                            {
                                fprintf(stderr, "Cannot add keyword %s, skipping.\n", key->info->name);
                                continue;
                            }
                            
                            nkeys++;
                            
                            key->record = seriestemp;
                            
                            /* Set the keyword's rank. */
                            
                            /* Determine if this is an old series where the keyword rank was not inlcuded in the kwflags 
                             * column of drms_keyword. If so, do not set the rank bits of the kwflags column. Otherwise, 
                             * determine the rank of the highest-ranked keyword, and add one to that rank before setting
                             * the kwflags column value. REMEMBER: the rank in the DRMS_Keyword_t struct is 0-based, but 
                             * the rank in the kwflags column is 1-based. */
                            if (hirank >= 0)
                            {
                                key->info->rank = hirank; /* 0-based */
                                key->info->kwflags |= (hirank + 1) << 16; /* 1-based - goes directly into db. */
                                hirank++;
                            }
                            else
                            {
                                key->info->rank = -1;
                                key->info->kwflags &= 0x0000FFFF;
                            }
                            
                            DetermineDefval(key, defval);
                            
                            /* Create the actual SQL. */
                            
                            /* ALTER TABLE action statement */
                            /* Linked keywords and constant keywords have no associated columns int the series table. LINKS 
                             * themmselves to have associated columns in the series table, but this function does not
                             * add new links to series. */
                            if (!key->info->islink && !drms_keyword_isconstant(key))
                            {
                                if (first)
                                {
                                    first = 0;
                                }
                                else
                                {
                                    tblactionbuf = base_strcatalloc(tblactionbuf, ", ", &szalterbuf);
                                }
                                
                                tblactionbuf = base_strcatalloc(tblactionbuf, "ADD COLUMN ", &szalterbuf);
                                tblactionbuf = base_strcatalloc(tblactionbuf, key->info->name, &szalterbuf);
                                tblactionbuf = base_strcatalloc(tblactionbuf, " ", &szalterbuf);
                                
                                if (key->info->type == DRMS_TYPE_STRING)
                                {
                                    tblactionbuf = base_strcatalloc(tblactionbuf, db_stringtype_maxlen(4000), &szalterbuf);
                                }
                                else
                                {
                                    tblactionbuf = base_strcatalloc(tblactionbuf, db_type_string(drms2dbtype(key->info->type)), &szalterbuf);
                                }
                            }
                            
                            skipkey = 0;
                            if (drms_keyword_getperseg(key))
                            {
                                len = strlen(key->info->name);
                                if (strcmp(key->info->name + len - 4, "_000"))
                                {
                                    skipkey = 1;
                                }
                                else
                                {
                                    key->info->name[len - 4] = 0;
                                    skipkey = 0;
                                }
                            }
                            
                            if (isrepl)
                            {                                
                                /* ns.drms_keyword insert statement */
                                /* We add only a single row for a family of per-segment keywords. */
                                                                
                                if (!skipkey)
                                {
                                    sqlbuf = base_strcatalloc(sqlbuf, "INSERT INTO ", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ns, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ".", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, DRMS_MASTER_KEYWORD_TABLE, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "(seriesname, keywordname, linkname, targetkeyw, type, defaultval, format, unit, description, islink, isconstant, persegment) VALUES (", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "'", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->record->seriesinfo->seriesname, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->name, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->linkname, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->target_key, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, drms_type2str(key->info->type), &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, defval, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->format, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->unit, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "','", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->description, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, "',", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, key->info->islink ? "1" : "0", &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ",", &szbuf);
                                    snprintf(numbuf, sizeof(numbuf), "%d", (int)key->info->recscope);
                                    sqlbuf = base_strcatalloc(sqlbuf, numbuf, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ",", &szbuf);
                                    snprintf(numbuf, sizeof(numbuf), "%d", key->info->kwflags);
                                    sqlbuf = base_strcatalloc(sqlbuf, numbuf, &szbuf);
                                    sqlbuf = base_strcatalloc(sqlbuf, ")\n", &szbuf);
                                }
                            }
                            else
                            {
                                *sql = NULL;
                                
                                if (!skipkey)
                                {
                                    char ibuf[2048];
                                    snprintf(ibuf, sizeof(ibuf), "INSERT INTO %s." DRMS_MASTER_KEYWORD_TABLE
                                             "(seriesname, keywordname, linkname, targetkeyw, type, "
                                             "defaultval, format, unit, description, islink, "
                                             "isconstant, persegment) values (?,?,?,?,?,?,?,?,?,?,?,?)", ns);

                                    if (drms_dmsv(session, 
                                                  NULL, 
                                                  ibuf,
                                                  -1,
                                                  DB_STRING, key->record->seriesinfo->seriesname, 
                                                  DB_STRING, key->info->name, 
                                                  DB_STRING, key->info->linkname, 
                                                  DB_STRING, key->info->target_key, 
                                                  DB_STRING, drms_type2str(key->info->type), 
                                                  DB_STRING, defval, 
                                                  DB_STRING, key->info->format, 
                                                  DB_STRING, key->info->unit,
                                                  DB_STRING, key->info->description,
                                                  DB_INT4, key->info->islink,
                                                  DB_INT4, key->info->recscope, /* stored in the isconstant column of
                                                                                 * drms_keyword. */
                                                  DB_INT4, key->info->kwflags))
                                    {
                                        drmsstat = DRMS_ERROR_BADDBQUERY;
                                    }
                                }
                            }
                        } /* end while loop */
                        
                        if (drmsstat == DRMS_SUCCESS && nkeys > 0)
                        {
                            tblactionbuf = base_strcatalloc(tblactionbuf, "\n", &szalterbuf);
                            
                            szalterbuf = 2048;
                            char *alterbuf = calloc(1, szalterbuf);
                            
                            if (alterbuf)
                            {
                                alterbuf = base_strcatalloc(alterbuf, "ALTER TABLE ", &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, series, &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, " ", &szalterbuf);
                                alterbuf = base_strcatalloc(alterbuf, tblactionbuf, &szalterbuf);
                                
                                if (isrepl)
                                {
                                    /* If the table is being replicated, then we just concatenate the 
                                     * SQL that adds columns to the series table to the sqlbuf. */
                                    sqlbuf = base_strcatalloc(sqlbuf, alterbuf, &szbuf);
                                }
                                else
                                {
                                    /* If the table isn't being replicated, then we can go ahead and submit 
                                     * the SQL query that adds columns to the series table. */
                                    if (drms_dms(session, NULL, alterbuf))
                                    {
                                        drmsstat = DRMS_ERROR_BADDBQUERY;
                                    }
                                }
                                
                                free(alterbuf);
                                alterbuf = NULL;
                            }
                        }
                        
                        hiter_destroy(&hit);
                    }
                    else
                    {
                        drmsstat = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
            }
            else
            {
                fprintf(stderr, "Failed to parse keyword specification '%s'.\n", spec);
                drmsstat = DRMS_ERROR_BADJSD;
            }
            
            if (keys)
            {
                /* Free the keys container. This will deep-free any string values, but it will not free the info             
                 * struct. */
                hcon_destroy(&keys);
            }    
        }
        else
        {
            fprintf(stderr, "Unable to obtain template record for series %s.\n", series);
            drmsstat = DRMS_ERROR_UNKNOWNSERIES;
        }
    }
    else
    {
        drmsstat = DRMS_ERROR_INVALIDDATA;
    }
    
    if (drmsstat == DRMS_SUCCESS)
    {
        if (isrepl)
        {
            drmsstat = DRMS_ERROR_CANTMODPUBSERIES;
            
            if (sqlbuf && strlen(sqlbuf) > 0)
            {
                *sql = sqlbuf;
                sqlbuf = NULL;
            }
            else
            {
                drmsstat = DRMS_ERROR_BADJSD;
            }
        }
    }
    
    if (tblactionbuf)
    {
        free(tblactionbuf);
    }
    
    if (sqlbuf)
    {
        free(sqlbuf);
    }
    
    if (ns)
    {
        free(ns);
    }
    
    if (tab)
    {
        free(tab);
    }
    
    return drmsstat;
}

int drms_dropkeys_fromseries(DRMS_Env_t *env, const char *series, char **keys, int nkeys)
{
    int drmsstat = DRMS_SUCCESS;
    DRMS_Record_t *seriestemp = NULL;
    size_t szbuf;
    char *sqlbuf = NULL;
    int first;
    char *keyname = NULL;
    DRMS_Keyword_t *key = NULL;
    char ibuf[2048];
    char *lcseries = NULL;
    char *lckeyname = NULL;
    int ikey;
    int iseg;
    int nsegs;
    char *ns = NULL;
    DRMS_Session_t *session = NULL;
    
    if (series && *series && keys)
    {
        session = env->session;
        lcseries = strdup(series);
        
        if (lcseries)
        {
            strtolower(lcseries);
            get_namespace(lcseries, &ns, NULL);
            
            if (!ns)
            {
                drmsstat = DRMS_ERROR_OUTOFMEMORY;
            }
            else
            {
                /* ACK! For each persegment keyword, there exist only the expanded keywords in the regular
                 * series template. Use drms_create_jsdtemplate_record() instead to get the non-expanded
                 * persegment keywords. */
                seriestemp = drms_create_jsdtemplate_record(env, series, &drmsstat);
            }
        }
        else
        {
            drmsstat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        if (seriestemp && drmsstat == DRMS_SUCCESS)
        {
            if (drms_series_isreplicated(env, series))
            {
                fprintf(stderr, "Cannot drop keywords from replicated series %s.\n", series);
                drmsstat = DRMS_ERROR_CANTMODPUBSERIES;
            }
            else
            {
                szbuf = 1024;
                sqlbuf = calloc(1, szbuf);
                
                if (!sqlbuf)
                {
                    drmsstat = DRMS_ERROR_OUTOFMEMORY;
                }
            }
        }
        
        if (drmsstat == DRMS_SUCCESS)
        {
            for (ikey = 0, first = 1; ikey < nkeys; ikey++)
            {
                keyname = keys[ikey];
                
                if (lckeyname)
                {
                    free(lckeyname);
                }
                
                lckeyname = strdup(keyname);
                strtolower(lckeyname);
                                
                /* First, ensure that the keyword to drop actually exists. */
                if ((key = drms_keyword_lookup(seriestemp, keyname, 0)) == NULL)
                {
                    fprintf(stderr, "Cannot drop keyword %s because it does not exist in series %s, skipping.\n", keyname, series);
                    continue;
                }
                
                /* ALTER TABLE action statement - drop keys from the series table. */
                /* There are no columns in the series table for linked or constant keywords. */
                if (!key->info->islink && !drms_keyword_isconstant(key))
                {
                    if (drms_keyword_getperseg(key))
                    {
                        char keybuf[DRMS_MAXKEYNAMELEN + 4];
                        
                        /* We need to drop multiple columns for each per-segment keyword. */
                        nsegs = drms_record_numsegments(seriestemp);
                        for (iseg = 0; iseg < nsegs; iseg++)
                        {
                            snprintf(keybuf, sizeof(keybuf), "%s_%03d", keyname, iseg);
                            
                            if (first)
                            {
                                first = 0;
                            }
                            else
                            {
                                sqlbuf = base_strcatalloc(sqlbuf, ", ", &szbuf);
                            }
                            
                            /* If a db object depends on the column, then this SQL will fail. */
                            sqlbuf = base_strcatalloc(sqlbuf, "DROP COLUMN ", &szbuf);
                            sqlbuf = base_strcatalloc(sqlbuf, keybuf, &szbuf);
                        }                    
                    }
                    else
                    {
                        if (first)
                        {
                            first = 0;
                        }
                        else
                        {
                            sqlbuf = base_strcatalloc(sqlbuf, ", ", &szbuf);
                        }
                        
                        /* If a db object depends on the column, then this SQL will fail. */
                        sqlbuf = base_strcatalloc(sqlbuf, "DROP COLUMN ", &szbuf);
                        sqlbuf = base_strcatalloc(sqlbuf, keyname, &szbuf);
                    }
                }
                
                snprintf(ibuf, sizeof(ibuf), "DELETE FROM %s." DRMS_MASTER_KEYWORD_TABLE
                         " WHERE lower(seriesname) = '%s' AND lower(keywordname) = '%s'", ns, lcseries, lckeyname);
                
                if (drms_dms(session, NULL, ibuf))
                {
                    drmsstat = DRMS_ERROR_BADDBQUERY;
                    break;
                }
            } /* key loop */
            
            if (lckeyname)
            {
                free(lckeyname);
                lckeyname = NULL;
            }
            
            /* Drop columns from the series table. */
            if (sqlbuf && *sqlbuf)
            {
                size_t szalterbuf = 2048;
                char *alterbuf = calloc(1, szalterbuf);
                
                if (alterbuf)
                {
                    alterbuf = base_strcatalloc(alterbuf, "ALTER TABLE ", &szalterbuf);
                    alterbuf = base_strcatalloc(alterbuf, lcseries, &szalterbuf);
                    alterbuf = base_strcatalloc(alterbuf, " ", &szalterbuf);
                    alterbuf = base_strcatalloc(alterbuf, sqlbuf, &szalterbuf);
                    
                    if (drms_dms(session, NULL, alterbuf))
                    {
                        drmsstat = DRMS_ERROR_BADDBQUERY;
                    }
                    
                    free(alterbuf);
                    alterbuf = NULL;
                }
            }
        }
        
        if (seriestemp)
        {
            drms_destroy_jsdtemplate_record(&seriestemp);
        }
        
        if (lcseries)
        {
            free(lcseries);
            lcseries = NULL;
        }
    }
    else
    {
        drmsstat = DRMS_ERROR_INVALIDDATA;
    }
    
    return drmsstat;
}
/* cascade - I think this means to delete the PostgreSQL record-table and sequence
 * objects, and the SUs that the record table refers to. cascade == 0 when called 
 * from modify_series, but cascade == 1 when called from delete_series. 
 * modify_series copies the record-table and sequence to a new table and sequence
 * for the series. These copied records still refer to original SUs (so you
 * don't want to delete them). 
 * keepsums - If this is set, then the call to SUMS to delete the storage units
 * belonging to the series is not made. */
int drms_delete_series(DRMS_Env_t *env, const char *series, int cascade, int keepsums)
{
  char query[1024], *series_lower = NULL, *namespace = NULL;
  DB_Binary_Result_t *qres;
  DRMS_Session_t *session;
  int drmsstatus = DRMS_SUCCESS;
  DRMS_Array_t *array = NULL;
  int repl = 0;
  int retstat = -1;
  char shadow[DRMS_MAXSERIESNAMELEN];

  if (!env->session->db_direct && !env->selfstart)
  {
     fprintf(stderr, "Can't delete series if using drms_server. Please use a direct-connect modules, or a self-starting socket-connect module.\n");
     goto bailout;
  }

  series_lower = strdup(series);
  /* series_lower is fully qualified, i.e., it contains namespace */
  strtolower(series_lower);

  /*Check to see if the series exists first */
  if (!drms_series_exists(env, series_lower, &drmsstatus))
  {
     fprintf(stderr, "The series '%s' does not exist.  Please enter a valid series name.\n", series);
     goto bailout;
  }

  if (!drms_series_candeleterecord(env, series_lower))
  {
     fprintf(stderr, "Permission failure - cannot delete series '%s'.\n", series);
     goto bailout;
  }

  /* Don't delete the series if it is being slony-replicated (for now).
   * Eventually, we may want to allow deletion of such series under certain
   * circumstances. */
  if ((repl = drms_series_isreplicated(env, series)) == 0)
  {
     session = env->session;
     sprintf(query,"select seriesname from %s() where lower(seriesname) = '%s'",
             DRMS_MASTER_SERIES_TABLE, series_lower);
#ifdef DEBUG
     printf("drms_delete_series: query = %s\n",query);
#endif
     if ((qres = drms_query_bin(session, query)) == NULL)
     {
        printf("Query failed. Statement was: %s\n", query);
        goto bailout;
     }
#ifdef DEBUG
     db_print_binary_result(qres);
#endif
     if (qres->num_rows==1)
     {
        DB_Binary_Result_t *suqres = NULL;

        get_namespace(series, &namespace, NULL);

        if (!keepsums)
        {
           /* If we are planning on deleting SUs, but in fact there are 
            * no SUs to delete because the series has no segments, then
            * we are essentially keeping SUs (and not passing a vector
            * of SUs to delete to SUMS). */
           snprintf(query, sizeof(query), "SELECT segmentname FROM %s.%s where lower(seriesname) = '%s'", namespace, DRMS_MASTER_SEGMENT_TABLE, series_lower);

           if ((suqres = drms_query_bin(session, query)) == NULL)
           {
              fprintf(stderr, "Query failed. Statement was: %s\n", query);
              free(namespace);
              goto bailout;
           }

           if (suqres->num_rows == 0)
           {
              keepsums = 1;              
           }

           db_free_binary_result(suqres);
           suqres = NULL;
        }

        if (cascade && !keepsums) 
        {
           int irow;
           int nsunums;
           long long val;
           int axis[2];
           long long *llarr = NULL;

           /* Fetch an array of unique SUNUMs from the series table -
              the prime-key logic is NOT applied. */
           snprintf(query, sizeof(query), "SELECT DISTINCT sunum FROM %s ORDER BY sunum", series);

           /* Even on a very large table (100 M records), this query should return within 
            * a couple of minutes - if that isn't acceptable, then we can 
            * limit the code to series with segments. */
           if ((suqres = drms_query_bin(session, query)) == NULL)
           {
              fprintf(stderr, "Query failed. Statement was: %s\n", query);
              goto bailout;
           }

           nsunums = 0;

           if (suqres->num_rows > 0)
           {
              llarr = (long long *)malloc(sizeof(long long) * suqres->num_rows);

              for (irow = 0; irow < suqres->num_rows; irow++)
              {
                 val = db_binary_field_getlonglong(suqres, irow, 0);
                 if (val >= 0)
                 {
                    llarr[nsunums++] = val;
                 }
              }

              /* Stuff into a DRMS_Array_t */
              axis[0] = 1;
              axis[1] = nsunums;
              array = drms_array_create(DRMS_TYPE_LONGLONG, 2, axis, llarr, &drmsstatus);
           }

           if (nsunums == 0)
           {
              keepsums = 1;
           }

           db_free_binary_result(suqres);
           suqres = NULL;
        }

        /* This if statement just checks to make sure that if we are deleting SUs, then
         * we properly created the array of SUs that will be sent to SUMS for deletion. */
        if (keepsums || (!drmsstatus && array && array->naxis == 2 && array->axis[0] == 1))
        {
           if (cascade && !keepsums) {
              if (array->axis[1] > 0) {
                 /* Delete the SUMS files from SUMS. */
                 /* If this is a sock-module, must pass the vector SUNUM by SUNUM 
                  * to drms_server (drms_dropseries handles the sock-module case). */
                 if (drms_dropseries(env, series, array))
                 {
                    fprintf(stderr, "Unable to drop SUNUMS; failure calling SUMS.\n");
                    goto bailout;
                 }
              }
           }

           if (cascade) {
              sprintf(query,"drop table %s",series_lower);
              if (env->verbose)
              {
                 fprintf(stdout, "drms_delete_series(): %s\n", query);
              }

              if (drms_dms(session,NULL,query))
                goto bailout;
              if (drms_sequence_drop(session, series_lower))
                goto bailout;
           }
           sprintf(query, "set search_path to %s", namespace);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query)) {
              fprintf(stderr, "Failed: %s\n", query);
              goto bailout;
           }
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_LINK_TABLE,series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_KEYWORD_TABLE, series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_SEGMENT_TABLE, series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
           sprintf(query,"delete from %s where lower(seriesname) = '%s'",
                   DRMS_MASTER_SERIES_TABLE,series_lower);
           if (env->verbose)
           {
              fprintf(stdout, "drms_delete_series(): %s\n", query);
           }

           if (drms_dms(session,NULL,query))
             goto bailout;
            
            /* MUST DELETE ANY ASSOCIATED shadow table. If the user requires permissions
             * to delete the original table, then the user running this code must also 
             * have the permissions needed to delete the shadow table. */
            if (ShadowExists(env, series_lower, &drmsstatus))
            {
                snprintf(shadow, sizeof(shadow), "%s%s", series_lower, kShadowSuffix);
                snprintf(query, sizeof(query), "DROP TABLE %s", shadow);

                if (env->verbose)
                {
                    fprintf(stdout, "drms_delete_series(): %s\n", query);
                }
                
                if (drms_dms(session,NULL,query))
                {
                    goto bailout;
                }
            }
            
            /* If ShadowExists() fails, then it returns 0. */
            if (drmsstatus != DRMS_SUCCESS)
            {
                fprintf(stderr, "Unable to check for the existing of the shadow table for series %s.\n", series);
                goto bailout;
            }
            
           /* Both DRMS servers and clients have a series_cache (one item per
              each series in all of DRMS). So, always remove the deleted series
              from this cache, whether or not this is a server. */

           /* Since we are now caching series on-demand, this series may not be in the
            * series_cache, but hcon_remove handles this fine. */

           hcon_remove(&env->series_cache,series_lower);
        }
        else
        {
           fprintf(stderr, "Couldn't create vector of sunum keywords.\n");
           goto bailout;
        }

        free(namespace);
        namespace = NULL;
     }
     else if (qres->num_rows>1)
     {
        fprintf(stderr,"TOO MANY ROWS RETURNED IN DRMS_DELETE_SERIES\n"); 
        /* This should never happen since seriesname is a unique index on 
           the DRMS series table. */
        goto bailout;
     }
     else {
        /* The series doesn't exist. */
        fprintf(stderr, "Series '%s' does not exist\n", series);
        retstat = DRMS_ERROR_UNKNOWNSERIES;
        goto bailout;
     }
     db_free_binary_result(qres);
  
     if (array)
     {
        drms_free_array(array);
     }
  }
  else if (repl == -1)
  {
     /* There was a dbase query failure which killed the current dbase transaction; must quit 
      * program. */
     goto bailout;
  }
  else
  {
     fprintf(stderr, "Unable to delete series registered for replication.\n");
  }

  free(series_lower);

  return 0;
 bailout:
  fprintf(stderr,"drms_delete_series(): failed to delete series %s\n", series);
  if (series_lower)
  {
     free(series_lower);
  }

  if (namespace)
  {
     free(namespace);
  }

  if (array)
  {
     drms_free_array(array);
  }

  if (retstat != -1)
  {
     return retstat;
  }
  else
  {
     return 1;
  }
}

static int drms_series_keynamesort(const void *first, const void *second)
{
   if (first && second)
   {
      const char *rFirst = *((const char **)first);
      const char *rSecond = *((const char **)second);

      if (!rFirst && !rSecond)
      {
	 return 0;
      }
      else if (!rFirst)
      {
	 return 1;
      }
      else if (!rSecond)
      {
	 return -1;
      }
      else
      {
	 return strcmp(rFirst, rSecond);
      }
   }

   return 0;
}

static int drms_series_intpkeysmatch(DRMS_Record_t *recTemp1, 
				     char **pkArray1, 
				     int nPKeys1, 
				     DRMS_Record_t *recTemp2, 
				     char **pkArray2, 
				     int nPKeys2)
{
   int ret = 0;

   if (nPKeys1 == nPKeys2)
   {
      /* sort each series' keys */
      qsort(pkArray1, nPKeys1, sizeof(char *), drms_series_keynamesort);
      qsort(pkArray2, nPKeys2, sizeof(char *), drms_series_keynamesort);
      
      DRMS_Keyword_t *key1 = NULL;
      DRMS_Keyword_t *key2 = NULL;
      
      int i = 0;
      for (; i < nPKeys1; i++)
      {
	 if (strcmp(pkArray1[i], pkArray2[i]) == 0)
	 {
	    key1 = hcon_lookup_lower(&(recTemp1->keywords), pkArray1[i]);
	    key2 = hcon_lookup_lower(&(recTemp2->keywords), pkArray1[i]);
	    
	    XASSERT(key1 != NULL && key2 != NULL);
	    if (key1 != NULL && key2 != NULL)
	    {
	       if (key1->info->type != key2->info->type ||
		   key1->info->recscope != key2->info->recscope ||
                   drms_keyword_getperseg(key1) != drms_keyword_getperseg(key2))
	       {
		  break;
	       }
	    }
	    else
	    {
	       break;
	    }
	 }
	 else
	 {
	    break;
	 }
      } /* for */
      
      if (i == nPKeys1)
      {
	 ret = 1;
      }
   }

   return ret;
}

/* Returns 0 on error. */
/* Returns 1 if keys2 is equal to keys1. */
static int drms_series_pkeysmatch(DRMS_Env_t *drmsEnv, 
				  const char *series1, 
				  const char *series2, 
				  int *status)
{
   int ret = 0;

   int nPKeys1 = 0;
   int nPKeys2 = 0;

   DRMS_Record_t *recTemp1 = drms_template_record(drmsEnv, series1, status);
   DRMS_Record_t *recTemp2 = NULL;

   if (*status == DRMS_SUCCESS)
   {
      recTemp2 = drms_template_record(drmsEnv, series2, status);
   }

   if (*status == DRMS_SUCCESS)
   {
      char **pkArray1 = drms_series_createrealpkeyarray(drmsEnv, series1, &nPKeys1, status);
      char **pkArray2 = NULL;
      
      if (*status == DRMS_SUCCESS)
      {
	 pkArray2 = drms_series_createrealpkeyarray(drmsEnv, series2, &nPKeys2, status);
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 ret = drms_series_intpkeysmatch(recTemp1, pkArray1, nPKeys1, recTemp2, pkArray2, nPKeys2);

	 if (!ret)
	 {
	    fprintf(stdout, 
		    "Series %s prime key does not match series %s prime key.\n",
		    series1,
		    series2);
	 }
      }

      if (pkArray1)
      {
	 drms_series_destroypkeyarray(&pkArray1, nPKeys1);
      }

      if (pkArray2)
      {
	 drms_series_destroypkeyarray(&pkArray2, nPKeys2);
      }
   }
   
   return ret;
}

static int drms_series_intcreatematchsegs(DRMS_Env_t *drmsEnv, 
					  const char *series, 
					  DRMS_Record_t *recTempl,
					  HContainer_t *matchSegs, 
					  int *status)
{
   int nMatch = 0;

   DRMS_Record_t *seriesRec = drms_template_record(drmsEnv, series, status);

   if (*status ==  DRMS_SUCCESS)
   {
      HContainer_t *s1SegCont = &(seriesRec->segments);
      HContainer_t *s2SegCont = &(recTempl->segments);
      
      if (s1SegCont && s2SegCont)
      {
	 HIterator_t *s1Hit = hiter_create(s1SegCont);
	 HIterator_t *s2Hit = hiter_create(s2SegCont);
	 
	 if (s1Hit && s2Hit)
	 {
	    DRMS_Segment_t *s1Seg = NULL;
	    DRMS_Segment_t *s2Seg = NULL;
	    
	    while ((s1Seg = (DRMS_Segment_t *)hiter_getnext(s1Hit)) != NULL)
	    {
	       if ((s2Seg = 
		    (DRMS_Segment_t *)hcon_lookup_lower(s2SegCont, s1Seg->info->name)) != NULL)
	       {
		  /* Must check for segment equivalence. */
		  if (drms_segment_segsmatch(s1Seg, s2Seg))
		  {
		     nMatch++;
		     
		     if (nMatch == 1)
		     {
			hcon_init(matchSegs, 
				  DRMS_MAXSEGNAMELEN, 
				  DRMS_MAXSEGNAMELEN, 
				  NULL, 
				  NULL);
		     }
		     
		     char *newSeg = (char *)hcon_allocslot(matchSegs, s1Seg->info->name);
		     if (newSeg != NULL)
		     {
			strncpy(newSeg, s1Seg->info->name, DRMS_MAXSEGNAMELEN);
			newSeg[DRMS_MAXSEGNAMELEN - 1] = '\0';
		     }
		     else
		     {
			nMatch = -1;
			break;
		     }
		  }
	       }
	    }
	 }
	 
	 if (s1Hit)
	 {
	    hiter_destroy(&s1Hit);
	 }
	 
	 if (s2Hit)
	 {
	    hiter_destroy(&s2Hit);
	 }
      }
   }

   return nMatch;
}

/* Fills in matchSegs with pointers to template segments. */
static int drms_series_creatematchsegs(DRMS_Env_t *drmsEnv, 
				       const char *series1, 
				       const char *series2, 
				       HContainer_t *matchSegs, 
				       int *status)
{
   int nMatch = 0;

   if (matchSegs)
   {
      DRMS_Record_t *s2RecTempl = drms_template_record(drmsEnv, series2, status);   
     
      if (*status == DRMS_SUCCESS)
      {
	 nMatch = drms_series_intcreatematchsegs(drmsEnv,
						 series1,
						 s2RecTempl,
						 matchSegs,
						 status);

	 if (nMatch == 0)
	 {
	    fprintf(stdout, "Series %s and %s have no matching segments.\n", series1, series2);
	 }
      }
   }
   else
   {
      fprintf(stderr, "Must provide HContainer_t to CreateMatchingSegs\n");
   }

   return nMatch;
}

/* Slotted keywords associated with prime index keywords ARE prime 
 * keyword from the user's perspective.  So, put those in the array
 * returned.
 */
static char **drms_series_intcreatepkeyarray(DRMS_Record_t *recTempl, 
					     int *nPKeys,
					     DRMS_PrimeKeyType_t pktype,
					     int *status)
{
   char **ret = NULL;

   if (recTempl != NULL)
   {
      int nKeys = recTempl->seriesinfo->pidx_num;
      int iKey = 0;
      
      ret = (char **)malloc(sizeof(char *) * nKeys);
      
      if (ret != NULL)
      {
	 while (iKey < nKeys)
	 {
	    DRMS_Keyword_t *pkey = recTempl->seriesinfo->pidx_keywords[iKey];

	    if (drms_keyword_isindex(pkey) && pktype == kPkeysDRMSExternal)
	    {
	       /* Use slotted keyword */
	       pkey = drms_keyword_slotfromindex(pkey);
	       ret[iKey] = strdup(pkey->info->name);
	    }
	    else
	    {
	       ret[iKey] = strdup(pkey->info->name);
	    }
	    iKey++;
	 }
	 
	 *nPKeys = nKeys;
      }
      else
      {
	 *status = DRMS_ERROR_OUTOFMEMORY;
      }
   }
   else
   {
      *status = DRMS_ERROR_INVALIDDATA;
   }

   return ret;
}

/* INTERNAL only! */
char **drms_series_createrealpkeyarray(DRMS_Env_t *env, 
				       const char *seriesName, 
				       int *nPKeys,
				       int *status)
{
     char **ret = NULL;
     int stat = 0;

     DRMS_Record_t *template = drms_template_record(env, seriesName, &stat);

     if (template != NULL && stat == DRMS_SUCCESS)
     {
	ret = drms_series_intcreatepkeyarray(template, nPKeys, kPkeysDRMSInternal, &stat);
     }

     if (status)
     {
	*status = stat;
     }

     return ret;
}

/* External */
char **drms_series_createpkeyarray(DRMS_Env_t *env, 
				       const char *seriesName, 
				       int *nPKeys,
				       int *status)
{
     char **ret = NULL;
     int stat = 0;

     DRMS_Record_t *template = drms_template_record(env, seriesName, &stat);

     if (template != NULL && stat == DRMS_SUCCESS)
     {
	ret = drms_series_intcreatepkeyarray(template, 
					     nPKeys, 
					     kPkeysDRMSExternal, 
					     &stat);
     }

     if (status)
     {
	*status = stat;
     }

     return ret;
}

void drms_series_destroypkeyarray(char ***pkeys, int nElements)
{
     int iElement = 0;
     char **array = *pkeys;

     while (iElement < nElements)
     {
	  if (array[iElement] != NULL)
	  {
	       free(array[iElement]);
	  }

	  iElement++;
     }

     free(array);
     *pkeys = NULL;
}

/* For modules like arithtool */
int drms_series_checkseriescompat(DRMS_Env_t *drmsEnv,
				  const char *series1, 
				  const char *series2, 
				  HContainer_t *matchSegs,
				  int *status)
{
   int ret = 0;
   int nMatch = 0;

   /* Ensure prime keywords match exactly. */   
   if (drms_series_pkeysmatch(drmsEnv, series1, series2, status) && *status == DRMS_SUCCESS)
   {
      /* Create a list of matching segments, if they exist. */
      nMatch = drms_series_creatematchsegs(drmsEnv, series1, series2, matchSegs, status);

      if (nMatch == 0)
      {
	 fprintf(stdout, "Series %s and %s have no matching segments.\n", series1, series2);
      }
   }

   if (*status == DRMS_SUCCESS)
   {
      ret = nMatch > 0;
   }
   
   return ret;
}

/* For modules like regrid */
/* Caller must specify a segment in the series to check. A segment is really specified 
 * by not only a series and segment name, but also by a primaryIndex, so the caller 
 * must specify a set of keyword names. */

/* recTempl contains the segment information that the caller wants to write. It can 
 * be a prototype record or template. */
/* The difference between this function and the previous one is that recTempl may 
 * refer to a record in a series that hasn't been created yet. */
int drms_series_checkrecordcompat(DRMS_Env_t *drmsEnv,
				  const char *series,
				  DRMS_Record_t *recTempl,
				  HContainer_t *matchSegs,
				  int *status)
{
   int ret = 0;
   int nMatch = 0;
   DRMS_Record_t *seriesTempl = NULL;
   int nSeriesPKeys = 0;
   int nPKeys = 0;
   char **seriesPKArray = NULL;
   char **pkArray = NULL;
   
   seriesPKArray = drms_series_createrealpkeyarray(drmsEnv, 
						   series, 
						   &nSeriesPKeys, 
						   status);
   if (*status == DRMS_SUCCESS)
   {
      pkArray = drms_series_intcreatepkeyarray(recTempl,
					       &nPKeys,
					       kPkeysDRMSInternal,
					       status);
      
      if (*status == DRMS_SUCCESS)
      {
	 seriesTempl = drms_template_record(drmsEnv, series, status);

	 if (*status == DRMS_SUCCESS)
	 {
	    if (drms_series_intpkeysmatch(seriesTempl, 
					  seriesPKArray, 
					  nSeriesPKeys, 
					  recTempl,
					  pkArray, 
					  nPKeys))
	    {
	       /* Now check for acceptable segments */
	       nMatch = drms_series_intcreatematchsegs(drmsEnv, 
						       series, 
						       recTempl, 
						       matchSegs, 
						       status);

	       if (nMatch == 0)
	       {
		  fprintf(stdout, 
			  "No series %s segment matches a series %s segment.\n",
			  recTempl->seriesinfo->seriesname,
			  series);  
	       }
	    }
	    else
	    {
	       fprintf(stdout, 
		       "Series %s prime key does not match series %s prime key.\n",
		       recTempl->seriesinfo->seriesname,
		       series);
	    }
	 }
      }
   }

   if (*status == DRMS_SUCCESS)
   {
      ret = nMatch > 0;
   }

   if (seriesPKArray)
   {
      drms_series_destroypkeyarray(&seriesPKArray, nSeriesPKeys);
   }

   if (pkArray)
   {
      drms_series_destroypkeyarray(&pkArray, nPKeys);
   }
   
   return ret;
}

int drms_series_checkkeycompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Keyword_t *keys,
			       int nKeys,
			       int *status)
{
   int ret = 0;
   
   DRMS_Record_t *recTempl = drms_template_record(drmsEnv, series, status);
   if (*status == DRMS_SUCCESS)
   {
      int iKey = 0;
      ret = 1;
      for (; iKey < nKeys; iKey++)
      {
	 DRMS_Keyword_t *oneKey = &(keys[iKey]);
	 DRMS_Keyword_t *sKey = drms_keyword_lookup(recTempl, 
						    oneKey->info->name, 
						    0);

	 if (sKey)
	 {
	    if (!drms_keyword_keysmatch(oneKey, sKey))
	    {
	       ret = 0;
	       break;
	    }
	 }
	 else
	 {
	    ret = 0;
	    break;
	 }
      }
   }
   
   return ret;
}

int drms_series_checksegcompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Segment_t *segs,
			       int nSegs,
			       int *status)
{
   int ret = 0;
   
   DRMS_Record_t *recTempl = drms_template_record(drmsEnv, series, status);
   if (*status == DRMS_SUCCESS)
   {
      int iSeg = 0;
      ret = 1;
      for (; iSeg < nSegs; iSeg++)
      {
	 DRMS_Segment_t *oneSeg = &(segs[iSeg]);
	 DRMS_Segment_t *sSeg = drms_segment_lookup(recTempl, oneSeg->info->name);

	 if (sSeg)
	 {
	    if (!drms_segment_segsmatch(oneSeg, sSeg))
	    {
	       ret = 0;
	       break;
	    }
	 }
	 else
	 {
	    ret = 0;
	    break;
	 }
      }
   }

   return ret;
}

/* Returns true iff si >= v.first && si <= v.last */
int drms_series_isvers(DRMS_SeriesInfo_t *si, DRMS_SeriesVersion_t *v)
{
   long long smajor;
   long long sminor;
   long long vmajor;
   long long vminor;

   int ok = 1;

   if (*(si->version) == '\0')
   {
      ok = 0;
   }
   else if (sscanf(si->version, "%lld.%lld", &smajor, &sminor) == 2)
   {
      if (*(v->first) != '\0')
      {
	 /* Series must be GTE to first */
	 if (sscanf(v->first, "%lld.%lld", &vmajor, &vminor) == 2)
	 {
	    if (smajor < vmajor || (smajor == vmajor && sminor < vminor))
	    {
	       ok = 0;
	    }
	 }
	 else
	 {
	    fprintf(stderr, "Invalid series version '%s'.\n", v->first);
	    ok = 0;
	 }
      }

      if (ok && *(v->last) != '\0')
      {
	 /* Series must be LTE to last */
	 if (sscanf(v->last, "%lld.%lld", &vmajor, &vminor) == 2)
	 {
	    if (smajor > vmajor || (smajor == vmajor && sminor > vminor))
	    {
	       ok = 0;
	    }
	 }
	 else
	 {
	     fprintf(stderr, "Invalid series version '%s'.\n", v->last);
	     ok = 0;
	 }
      }
   }
   else
   {
      fprintf(stderr, "Invalid series version '%s'.\n", si->version);
      ok = 0;
   }

   return ok;
}

static int CanCallDrmsReplicated(DRMS_Env_t *env)
{
   int tabexists = 0;
   int status = DRMS_SUCCESS;

   tabexists = drms_query_tabexists(env->session, "_jsoc", "sl_table", &status);

   if (status == DRMS_ERROR_BADDBQUERY)
   {
      fprintf(stderr, "Unable to check database for the existence of _jsoc.sl_table.\n");
      return 0;
   }

   return (!tabexists || drms_series_hastableprivs(env, "_jsoc", "sl_table", "SELECT"));
}

/* returns:
 *   -2  Can't call drms_replicated(). This does NOT destroy the dbase transaction.
 *   -1  The query failed - when this happens, this aborts the dbase transaction, 
 *       so you need to fail and rollback the dbase.
 *    0  Not replicated
 *    1  Replicated
 */
int drms_series_isreplicated(DRMS_Env_t *env, const char *series)
{
   int ans = 0;
   char query[1024];
   DB_Binary_Result_t *qres = NULL;

   /* First, check for presence of drms_replicated(). If you don't do this and 
    * drms_replicated() doesn't exist and you try to use it, the entire transaction
    * is hosed, and the error message you get from that isn't helpful. */
   sprintf(query,
           "select routine_name from information_schema.routines where routine_name like '%s'",
           DRMS_REPLICATED_SERIES_TABLE);

   
   if ((qres = drms_query_bin(env->session, query)) == NULL)
   {
      printf("Query failed. Statement was: %s\n", query);
      ans = -1;
   }
   else
   {
      if (qres->num_rows == 1)
      {
         /* drms_replicated() exists */
         char *nspace = NULL;
         char *relname = NULL;

         /* Before calling drms_repicated(), check to see if the user has permissions to do so. */
         if (!CanCallDrmsReplicated(env))
         {
            fprintf(stderr, "You do not have permission to call database function '%s'. Please have an administrator grant you permission before proceeding.\n", DRMS_REPLICATED_SERIES_TABLE);
            ans = -2;
         }
         else
         {
            db_free_binary_result(qres);

            get_namespace(series, &nspace, &relname);

            sprintf(query, 
                    "select tab_id from %s() where tab_nspname ~~* '%s' and tab_relname ~~* '%s'",
                    DRMS_REPLICATED_SERIES_TABLE, 
                    nspace,
                    relname);

            if (nspace)
            {
               free(nspace);
            }

            if (relname)
            {
               free(relname);
            }

            if ((qres = drms_query_bin(env->session, query)) == NULL)
            {
               printf("Query failed. Statement was: %s\n", query);
               ans = -1;
            }
            else
            {
               if (qres->num_rows == 1)
               {
                  ans = 1;
               }

               db_free_binary_result(qres);
            }
         }    
      }
      else
      {
         /* drms_replicated() function doesn't exist - not safe to continue. */
         ans = -1;
      }
   }

   return ans;
}

int GetTableOID(DRMS_Env_t *env, const char *ns, const char *table, char **oid)
{
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;
   int err = 0;

   snprintf(query, sizeof(query), "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^(%s)$' AND n.nspname ~ '^(%s)$' ORDER BY 2, 3", table, ns);
   
   if (!oid)
   {
      fprintf(stderr, "Missing required argument 'oid'.\n");
      err = 1;
   }
   else if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = DRMS_ERROR_QUERYFAILED;
   }
   else
   {
      if (qres->num_rows != 1)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = 1;
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

int GetColumnNames(DRMS_Env_t *env, const char *oid, char **colnames)
{
   int err = 0;
   char query[DRMS_MAXQUERYLEN];
   DB_Binary_Result_t *qres = NULL;
   DRMS_Session_t *session = env->session;

   snprintf(query, sizeof(query), "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) as defval, a.attnotnull FROM pg_catalog.pg_attribute a WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", oid);

   if ((qres = drms_query_bin(session, query)) == NULL)
   {
      fprintf(stderr, "Invalid database query: '%s'\n", query);
      err = 1;
   }
   else
   {
      if (qres->num_cols != 4)
      {
         fprintf(stderr, "Unexpected database response to query '%s'\n", query);
         err = 1;
      }
      else
      {
         char *list = NULL;
         int irow;
         size_t strsize = DRMS_MAXQUERYLEN;
         char colname[512];

         if (colnames)
         {
            list = malloc(sizeof(char) * strsize);
            memset(list, 0, sizeof(char) * strsize);

            for (irow = 0; irow < qres->num_rows; irow++)
            {
               if (irow)
               {
                  list = base_strcatalloc(list, ",", &strsize);
               }

               /* row irow + 1, column 1 */
               db_binary_field_getstr(qres, irow, 0, sizeof(colname), colname);
               list = base_strcatalloc(list, colname, &strsize);
            }

            *colnames = list;
            list = NULL;
         }

         db_free_binary_result(qres);
      }
   }
   
   return err;
}

/* The caller has just inserted 'nrows' records into the series table 'series'. Now they are updating the
 * table of counts. 'pkeynames' is an array, with 'ncols' elements, of prime keyword names. 'recnums' is
 * an array, one per row, of the record numbers of the records just inserted into 'series'. */

/* Do not create the shadow table if it does not already exist. Otherwise, it might be the case that 
 * we take a very long time to create a shadow table when a user has inserted some small number of
 * records into a series. Create the shadow table only if the user is doing something that will
 * result in the access of all of the records in the series. */
int drms_series_updatesummaries(DRMS_Env_t *env,
                                const char *series,
                                int nrows,
                                int ncols,
                                char **pkeynames,
                                long long *recnums,
                                int added)
{
    int status = DRMS_SUCCESS;
    int shadowexists = 0;
    size_t lsz = 0;
    char *recnumlist = NULL;
    
#if (defined TOC && TOC)
    int tocexists = 0;
    char stmnt[8192];
#endif
    
    /* In practice, there will not be any prime-key duplicates (no duplicate DRMS records), but of course user error could 
     * result in such duplicates. Given this, there is no need to sort the incoming records - just iterate through each 
     * one in the order they were passed in and check the db to see if the series table already contains such a DRMS record. 
     * If not, then add a new record to the table of counts. If so, then update one row in the table of counts. */
    
    /* If the table of counts does not exist, create it. */
    
#if (defined TOC && TOC)
    tocexists = TocExists(env, &status);
    
    if (status == DRMS_ERROR_BADDBQUERY)
    {
        fprintf(stderr, "Unable to check database for the existence of %s.%s.\n", kTableOfCountsNS, kTableOfCountsTab);
    }
    else
    {
        snprintf(stmnt, sizeof(stmnt), "SET search_path TO %s", kTableOfCountsNS);
        
        if (drms_dms(env->session, NULL, stmnt))
        {
            fprintf(stderr, "Failed: %s\n", stmnt);
            status = DRMS_ERROR_BADDBQUERY;
        }
        else
        {
            if (!tocexists && env->createshadows)
            {
                if ((status = CreateTableOfCounts(env)) == DRMS_SUCCESS)
                {
                    tocexists = 1;
                }
            }
        }
    }
#endif
    
    if (status == DRMS_SUCCESS)
    {
        shadowexists = ShadowExists(env, series, &status);
        
        if (status == DRMS_ERROR_BADDBQUERY)
        {
            fprintf(stderr, "Unable to check database for the existence of the shadow table.\n");
        }    
    }
    
    /* Update the summary tables. If the shadow table exists, then it should be updated, regardless 
     * of the env->createshadow flag. */
    if (status == DRMS_SUCCESS && shadowexists)
    {
        /* Now check for the presence of a row for each record that was inserted into the series table. */
        int irow;
        
#if (defined TOC && TOC)
        int present;
        long long count;
#endif
        
        long long recnum;
        int isnew = -1;
        char recnumstr[64];
        
#if (defined TOC && TOC)
        for (irow = 0; irow < nrows && status == DRMS_SUCCESS && tocexists; irow++)
#else
            for (irow = 0; irow < nrows && status == DRMS_SUCCESS; irow++)
#endif
            {
                recnum = recnums[irow];
                snprintf(recnumstr, sizeof(recnumstr), "%lld", recnum);
                
#if (defined TRACKSHADOWS && TRACKSHADOWS)
                lsz = 128; 
                recnumlist = calloc(lsz, sizeof(char));
                recnumlist = base_strcatalloc(recnumlist, recnumstr, &lsz);
                if (irow < nrows - 1)
                {
                    recnumlist = base_strcatalloc(recnumlist, ",", &lsz);
                }
#endif
                
#if (defined TOC && TOC)
                if (tocexists)
                {
                    /* update the TOC if it exists. It will NOT accurately reflect the rows just added to the series table. */
                    
                    /* check for presence of a row in table of counts for the series into which rows were inserted */
                    present = IsSeriesPresent(env, kTableOfCountsNS, kTableOfCountsTab, series, &status);
                    
                    if (status == DRMS_SUCCESS)
                    {
                        if (!present)
                        {
                            /* Insert a new row after doing the big count (which could take a while). Since we                                   
                             * already added new rows to the series table, the count will reflect these recent                                   
                             * insertions. */
                            count = CountRecordGroups(env, series, &status);
                            if (status == DRMS_SUCCESS)
                            {
                                status = InsertSeries(env, kTableOfCountsNS, kTableOfCountsTab, series, count);
                            }
                        }
                        else
                        {
                            /* A row already exists. Update it. */
                            
                            /* Determine if this row belongs to a new DRMS-record group. If it doesn't, then do nothing -                        
                             * there has been no increase in the number of groups. Otherwise, increment                                          
                             * the existing count by one. */
                            isnew = IsGroupNew(env, recnum, series, pkeynames, ncols, &status);
                            
                            if (status == DRMS_SUCCESS)
                            {
                                if (!isnew)
                                {
                                    status = AdvanceGroupCount(env, kTableOfCountsNS, kTableOfCountsTab, series);
                                }
                            }
                        }
                    }
                } /* TOC update */
#endif
                
                if (status == DRMS_SUCCESS)
                {
                    if (added)
                    {
                        /* We're updating the shadow table because one or more rows was inserted          
                         * into the original series table. */
                        if (isnew == -1)
                        {
                            /* isnew test was not done when the TOC was updated - do it now. */
                            isnew = IsGroupNew(env, recnum, series, pkeynames, ncols, &status);
                        }
                        
                        if (isnew == 0)
                        {
                            /* Need to update an existing record in the shadow table. */
                            status = UpdateShadow(env, series, pkeynames, ncols, recnum, 1);
                        }
                        else if (isnew == 1)
                        {
                            /* Need to add a new group (record) to the shadow table. */
                            /* shadow - pkey1, pkey2, ..., recnum, nrecords                                                                         
                             */
                            status = InsertIntoShadow(env, series, pkeynames, ncols, recnum);
                        }
                        else
                        {
                            /* Unexpected value for isnew. */
                        }
                    }
                    else
                    {
                        /* We're updating the shadow table because one or more rows was deleted           
                         * from the original series table. */
                        int wasdel;
                        
                        wasdel = WasGroupDeleted(env, recnum, series, pkeynames, ncols, &status);
                        if (wasdel)
                        {
                            /* The last DRMS record was deleted - delete corresponding group from          
                             * shadow table. */
                            status = DeleteFromShadow(env, series, pkeynames, ncols, recnum);
                        }
                        else
                        {
                            /* One version of a DRMS record was deleted. May need to update the            
                             * corresponding group's record in the shadow table (update nrecords           
                             * and recnum). If the version deleted was an obsolete version,                
                             * then no change to the recnum in the shadow table is needed. */
                            status = UpdateShadow(env, series, pkeynames, ncols, recnum, 0);
                        }
                    }
                } /* shadow-table update */
            } /* loop recs */
    }
    
#if (defined TRACKSHADOWS && TRACKSHADOWS)
    /* We need the ability to disable this feture, since it does get called every time we insert 
     * records and won't be necessary once we are confident that there isn't old code running around. */
    if (status == DRMS_SUCCESS && shadowexists)
    {
        /* When the series table had records inserted into it, a trigger MAY HAVE fired that caused a record to
         * be inserted into a shadow-tracking table. The trigger might also not exist, in which case this 
         * block of code should be a no-op. */
        char *ns = NULL;
        char *tab = NULL;
        int track = 0;
        char query[2048];
        char *lcseries = NULL;
        int ndel;
        
        if (!get_namespace(kShadowTrackTab, &ns, &tab))
        {
            track = (drms_query_tabexists(env->session, ns, tab, &status) != 0);
            free(ns);
            free(tab);
            
            if (track)
            {
                /* Delete the tracking records for the series records just inserted. 
                 * We need to obtain the number of records deleted. There are two 
                 * acceptable numbers - either 0 (because no tracking records were inserted
                 * to begin with, because there was no trigger on the shadow table), or 
                 * nrows, the total number of records inserted into the original table. */
                lcseries = strdup(series);
                if (!lcseries)
                {
                    status = DRMS_ERROR_OUTOFMEMORY;
                }
                else
                {
                    strtolower(lcseries);
                    snprintf(query, sizeof(query), "DELETE FROM %s WHERE seriesname = '%s' AND recnum IN (%s)", kShadowTrackTab, lcseries, recnumlist);
                    free(lcseries);
                    
                    if (drms_dms(env->session, &ndel, query))
                    {
                        fprintf(stderr, "Failed: %s\n", query);
                        status = DRMS_ERROR_BADDBQUERY;   
                    }
                    else
                    {
                        if (nrows > 0 && ndel != nrows)
                        {
                            fprintf(stderr, "Unexpected number of rows (%d) in the shadow tracker.\n", ndel);
                            status = DRMS_ERROR_SHADOWTAB; 
                        }
                    }
                }
            }
        }
        else
        {
            status = DRMS_ERROR_OUTOFMEMORY;
        }
    }
#endif
    
    return status;
}

#if (defined TOC && TOC)
int drms_series_tocexists(DRMS_Env_t *env, int *status)
{
    return TocExists(env, status);
}

/* This function will not create the shadow table if: 1. it already exists, or
 * 2. the table doesn't exist and the env->createshadows flag is not set. */
int drms_series_createtoc(DRMS_Env_t *env)
{
    if (env->createshadows)
    {
        return CreateTableOfCounts(env);
    }
    else
    {
        return DRMS_ERROR_CANTCREATESHADOW;
    }
}

int drms_series_intoc(DRMS_Env_t *env, const char *series, int *status)
{
    return IsSeriesPresent(env, kTableOfCountsNS, kTableOfCountsTab, series, status);
}

/* Assumes that series is not already in the table of counts. If it is, then the lower-level SQL queries will fail, and                
 * the db will rollback and this function will return an error status. */
int drms_series_insertintotoc(DRMS_Env_t *env, const char *series)
{
    int status = DRMS_SUCCESS;
    int count;
    
    count = CountRecordGroups(env, series, &status);
    if (status == DRMS_SUCCESS)
    {
        status = InsertSeries(env, kTableOfCountsNS, kTableOfCountsTab, series, count);
    }
    
    return status;
}
#endif

/* neither pkwhere nor npkwhere exists. */
char *drms_series_nrecords_querystringA(const char *series, int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int istat = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            
#if (defined TOC && TOC)
            query = base_strcatalloc(query, "SELECT ", &stsz);
            query = base_strcatalloc(query, kTableOfCountsColNRecs, &stsz);
            query = base_strcatalloc(query, " FROM ", &stsz);
            query = base_strcatalloc(query, kTableOfCountsNS, &stsz);
            query = base_strcatalloc(query, ".", &stsz);
            query = base_strcatalloc(query, kTableOfCountsTab, &stsz);
            query = base_strcatalloc(query, " WHERE lower(", &stsz);
            query = base_strcatalloc(query, kTableOfCountsColSeries, &stsz);
            query = base_strcatalloc(query, ") = '", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, "'", &stsz);
#else
            /* No TOC - use shadow table only.*/
            query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, kShadowSuffix, &stsz);
#endif
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere does not exist, but npkwhere does. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT count(*) FROM (SELECT recnum FROM <series> WHERE <npkwhere>) AS T1, <shadow table> AS T2 WHERE T1.recnum = T2.recnum */

/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 *   # Apply the prime-key logic by selecting all records from the shadow table. Even though this 
 *   # seems like a silly thing to do, if you don't do this, then PG will do a sequential scan
 *   # on both the original and shadow tables. 
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table>;
 *
 *   # Select the records in the series whose recnums are in the 
 *   # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT count(*) FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX) AND <npkwhere>;
 *
 * NOTE: If the npkwhere clause results in the selection of many records in the original table, then this
 * query will run a very long time, and there is nothing we can do about this. We definitely want to 
 * create the temporary table that contains the single recnum column. If we select directly from
 * the shadow table in the second query, then the query runs about 10x slower (because PG will do
 * a sequential scan on the original series table).
 *
 * SPEED: Very good, but can be slow if npkwhere selects lots of record.
 */
char *drms_series_nrecords_querystringB(const char *series, const char *npkwhere, int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    int istat = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            if (GetTempTable(tabname, sizeof(tabname)))
            {
                istat = DRMS_ERROR_OVERFLOW;
            }
            else
            {
                /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, ";\n", &stsz);
                
                /* Select the records from the series table. */
                query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T WHERE T.recnum IN (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, ") AND ", &stsz);
                query = base_strcatalloc(query, npkwhere, &stsz);
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere exists, but npkwhere does not. */
/* SELECT count(recnum) FROM <shadow table> WHERE <pkwhere>;
 */
char *drms_series_nrecords_querystringC(const char *series, const char *pkwhere, int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int istat = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            
            query = base_strcatalloc(query, "SELECT count(recnum) FROM ", &stsz);
            query = base_strcatalloc(query, lcseries, &stsz);
            query = base_strcatalloc(query, kShadowSuffix, &stsz);
            query = base_strcatalloc(query, " WHERE ", &stsz);
            query = base_strcatalloc(query, pkwhere, &stsz);
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* both pkwhere and npkwhere exist. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT count(*) FROM (SELECT recnum FROM <series> WHERE <npkwhere>) AS T1, (SELECT recnum FROM <shadow table> WHERE <pkwhere>) AS T2 WHERE T1.recnum = T2.recnum
 */

/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 *   # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table> WHERE pkwhere;
 *
 *   # Select the records in the series whose recnums are in the 
 *   # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT count(*) FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX) AND <npkwhere>;
 *
 * SPEED: Very good, but can be slow if npkwhere selects lots of record.
 */
char *drms_series_nrecords_querystringD(const char *series, const char *pkwhere, const char *npkwhere, int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    int istat = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            if (GetTempTable(tabname, sizeof(tabname)))
            {
                istat = DRMS_ERROR_OVERFLOW;
            }
            else
            {
                /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, " WHERE ", &stsz);
                query = base_strcatalloc(query, pkwhere, &stsz);
                query = base_strcatalloc(query, ";\n", &stsz);
                
                /* Select the records from the series table. */
                query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T WHERE T.recnum IN (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, ") AND ", &stsz);
                query = base_strcatalloc(query, npkwhere, &stsz);
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* 
 *
 SELECT recnum FROM <series>
 WHERE recnum IN
 (SELECT recnum FROM <shadow> AS T4
 WHERE (T4.pkey1, T4.pkey2, ..., T4.pkeyN) IN
 (SELECT T3.pkey1, T3.pkey2, ..., max(T3.pkeyN) AS pkeyN FROM <shadow> T3
 WHERE (T3.pkey1, T3.pkey2, ..., T3.pkey(N-1)) IN
 (SELECT T2.pkey1, T2,pkey2, ..., max(T2.pkey(N-1)) AS pkey(N-1) FROM 
 (SELECT T1.recnum, T1.pkey1, T1.pkey2, ..., T1.pkey(N-1) FROM <shadow> as T1
 WHERE T1.pkey1 = (SELECT max(pkey1) FROM <shadow>)) AS T2 GROUP BY T2.pkey1)
 GROUP BY T3.recnum, T3.pkey1, T3.pkey2))
 *
 * IGNORE the pkwhere clause. It will contain SQL that drms_names.c added to try and select records with the maximum/minimum
 * prime-key value. It would have been better to not have done that in a general way in drms_names.c. It precludes 
 * tailoring the queries to the context.
 */


/*
 
 # 1
 (SELECT recnum FROM su_arta.testshadow_shadow T2
 WHERE (T2.observationtime) IN
 (SELECT T1.observationtime FROM su_arta.testshadow_shadow as T1
 WHERE T1.observationtime = (SELECT MAX(observationtime) FROM su_arta.testshadow_shadow)))
 
 # 2
 (SELECT recnum FROM su_arta.testshadow_shadow T3
  WHERE (T3.observationtime, T3.regionnumber) IN
   (SELECT T2.observationtime, max(T2.regionnumber) FROM su_arta.testshadow_shadow T2  
    WHERE (T2.observationtime) IN
      (SELECT T1.observationtime FROM su_arta.testshadow_shadow as T1
       WHERE T1.observationtime = (SELECT MAX(observationtime) FROM su_arta.testshadow_shadow))
    GROUP BY T2.observationtime))
 
 #3
 (SELECT recnum from su_arta.testshadow T4
 WHERE (T4.observationtime, T4.regionnumber, T4.zurichclass) IN
 (SELECT T3.observationtime, T3.regionnumber, max(T3.zurichclass) FROM su_arta.testshadow T3
 WHERE (T3.observationtime, T3.regionnumber) IN 
 (SELECT T2.observationtime, max(T2.regionnumber) FROM su_arta.testshadow_shadow T2  
 WHERE (T2.observationtime) IN
 (SELECT T1.observationtime FROM su_arta.testshadow_shadow as T1
 WHERE T1.observationtime = (SELECT MAX(observationtime) FROM su_arta.testshadow_shadow))
 GROUP BY T2.observationtime)
 GROUP BY T3.observationtime, T3.regionnumber))
 
 SELECT recnum, zurichclass FROM su_arta.testshadow T4
 WHERE (T4.recnum) IN
 (SELECT recnum FROM su_arta.testshadow_shadow T3 
 WHERE T3.regionnumber IN
 (SELECT max(T2.regionnumber) FROM su_arta.testshadow_shadow T2  
 WHERE (T2.observationtime) IN
 (SELECT T1.observationtime FROM su_arta.testshadow_shadow as T1
 WHERE 1112659234.000000<=T1.ObservationTime AND T1.ObservationTime<( 1112659234.000000 + 864000.000000 ))))
 AND T4.zurichclass != '?'
 
 
 SELECT recnum, observationtime, regionnumber, zurichclass FROM su_arta.testshadow T3
 WHERE (T3.recnum) IN
 (SELECT T2.recnum FROM su_arta.testshadow_shadow T2  
 WHERE (T2.observationtime) IN
 (SELECT T1.observationtime FROM su_arta.testshadow_shadow as T1
 WHERE T1.observationtime = (SELECT MAX(observationtime) FROM su_arta.testshadow_shadow))
 AND T2.regionnumber > 11590) <-- pkwhere
 AND T3.zurichclass != '?' <-- npkwhere
 
 
 # order
 1. process all first/last filters
 2. select recnum from shadow table that satisfies these filters. As part of the where clause, AND the npkwhere clause. End up with a recnum into the shadow table.
 3. select all columns from series table where recnum IN recnums in #1, AND npkwhere clause.
 
 */
static int InnerFLSelect(int npkeys, HContainer_t *firstlast, size_t *stsz, char *pkey[], const char *pkeylist, const char *shadow, HContainer_t *pkwhereNFL, char **query, char **lasttab)
{
    int iloop;
    int init;
    char *pkwhere = NULL;
    int istat = 0;  
    char **ppkwhere = NULL;
    char fl;
    char tmptab[256];
    char prevtmptab[256];
    
    init = 0;
    for (iloop = 0; iloop < npkeys; iloop++)
    {
        if (hcon_member_lower(firstlast, pkey[iloop]))
        {
            /* We have a first/last filter for this element. */
            
            if (GetTempTable(tmptab, sizeof(tmptab)))
            {
                istat = DRMS_ERROR_OVERFLOW;
            }
            else
            {
                if (init != 0)
                {
                   *query = base_strcatalloc(*query, ";", stsz);
                }

                *query = base_strcatalloc(*query, "CREATE TEMPORARY TABLE ", stsz);
                *query = base_strcatalloc(*query, tmptab, stsz);
                *query = base_strcatalloc(*query, " AS ", stsz);
                *query = base_strcatalloc(*query, "SELECT recnum, ", stsz);
                *query = base_strcatalloc(*query, pkeylist, stsz);
                *query = base_strcatalloc(*query, " FROM ", stsz);
                
                if (init == 0)
                {
                    *query = base_strcatalloc(*query, shadow, stsz);
                }
                else
                {
                    *query = base_strcatalloc(*query, prevtmptab, stsz);
                }
                
                *query = base_strcatalloc(*query, " WHERE ", stsz);
                *query = base_strcatalloc(*query, pkey[iloop], stsz);
                *query = base_strcatalloc(*query, " = (SELECT ", stsz);
                fl = *((char *)hcon_lookup_lower(firstlast, pkey[iloop]));
                if (fl == 'F')
                {
                    *query = base_strcatalloc(*query, "min(", stsz);
                }
                else
                {
                    *query = base_strcatalloc(*query, "max(", stsz);
                }
                
                *query = base_strcatalloc(*query, pkey[iloop], stsz);
                *query = base_strcatalloc(*query, ") FROM ", stsz);
                
                if (init == 0)
                {
                    *query = base_strcatalloc(*query, shadow, stsz);
                }
                else
                {
                    *query = base_strcatalloc(*query, prevtmptab, stsz);
                }
                
                *query = base_strcatalloc(*query, ")", stsz);
                
                snprintf(prevtmptab, sizeof(prevtmptab), "%s", tmptab);
                
                init = 1;
            }
        }
        else
        {
            /* The where clause for the current keyword does NOT involve a FIRST/LAST
             * filter. But only if there is a prime-key filter. */
            /* Need to add the where clause for JUST THIS KEY. */
            ppkwhere = (char **)hcon_lookup_lower(pkwhereNFL, pkey[iloop]);
            if (ppkwhere)
            {
                pkwhere = *ppkwhere;
            }
            else
            {
                pkwhere = NULL;
            }
            
            if (pkwhere)
            {   
                if (GetTempTable(tmptab, sizeof(tmptab)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
                else
                {
                    if (init != 0)
                    {
                        *query = base_strcatalloc(*query, ";", stsz);
                    }
                    
                    *query = base_strcatalloc(*query, "CREATE TEMPORARY TABLE ", stsz);
                    *query = base_strcatalloc(*query, tmptab, stsz);
                    *query = base_strcatalloc(*query, " AS ", stsz);
                    *query = base_strcatalloc(*query, "SELECT recnum, ", stsz);
                    *query = base_strcatalloc(*query, pkeylist, stsz);
                    *query = base_strcatalloc(*query, " FROM ", stsz);
                    
                    if (init == 0)
                    {
                        *query = base_strcatalloc(*query, shadow, stsz);
                    }
                    else
                    {
                        *query = base_strcatalloc(*query, prevtmptab, stsz);
                    }
                    
                    if (ppkwhere)
                    {
                        *query = base_strcatalloc(*query, " WHERE ", stsz);
                        *query = base_strcatalloc(*query, pkwhere, stsz);
                    }
                    
                    snprintf(prevtmptab, sizeof(prevtmptab), "%s", tmptab);
                    
                    init = 1;
                }
            }
            else
            {
                /* There is no where clause involving this prime-key, so there is no need to 
                 * make a temporary table, which would otherwise be identical to the original shadow table. 
                 * So, skip this prime-key. The temporary tables created by this function are used 
                 * to restrict the number of records we pull from the shadow table. */
            }
        }
    }
    
    if (lasttab)
    {
        *lasttab = strdup(tmptab);
    }
    
    return istat;
}

/* SELECT count(*) FROM <series> 
 * WHERE recnum IN
 *   (SELECT recnum FROM <lasttab>)
 * AND <npkwhere>
 */
char *drms_series_nrecords_querystringFL(DRMS_Env_t *env, const char *series, const char *npkwhere, HContainer_t *pkwhereNFL, HContainer_t *firstlast, int *status)
{
    char *query = NULL;
    size_t stsz = 2048;
    char *lcseries = NULL;
    int iloop;
    char *pkey[DRMS_MAXPRIMIDX];
    int npkeys;
    char shadow[DRMS_MAXSERIESNAMELEN];
    char *lasttab = NULL;
    char *pkeylist = NULL;
    int istat = DRMS_SUCCESS;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        if (query)
        {
            *query = '\0';
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            pkeylist = CreatePKeyList(env, series, NULL, NULL, pkey, &npkeys, 0, &istat);
            
            if (istat == DRMS_SUCCESS)
            {
                istat = InnerFLSelect(npkeys, firstlast, &stsz, pkey, pkeylist, shadow, pkwhereNFL, &query, &lasttab);
            }
            
            if (istat == DRMS_SUCCESS)
            {
                /* Make sure the CREATE TEMP TABLE statements that precede the SELECT statement    
                 * are separated from the latter by ";\n". When this code is used in a db cursor, 
                 * the CREATE statements are executed first, then a cursor is created on the       
                 * remaining statements that follow the ";\n". */
                query = base_strcatalloc(query, ";\n", &stsz);

                query = base_strcatalloc(query, "SELECT count(*) FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);                
                query = base_strcatalloc(query, " WHERE recnum in (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, lasttab, &stsz);
                query = base_strcatalloc(query, ")", &stsz);
                
                if (npkwhere && *npkwhere)
                {
                    query = base_strcatalloc(query, " AND ", &stsz);
                    query = base_strcatalloc(query, npkwhere, &stsz);
                }
            }
            
            /* free stuff. */
            for (iloop = 0; iloop < npkeys; iloop++)
            {
                if (pkey[iloop])
                {
                    free(pkey[iloop]);
                    pkey[iloop] = NULL;
                }
            }
        }
        else
        { 
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

int drms_series_nrecords(DRMS_Env_t *env, const char *series, int *status)
{
    int count;
    int istat = DRMS_SUCCESS;
    char *query = NULL;
    DB_Text_Result_t *tres = NULL;
    
    count = -1;
    
    query = drms_series_nrecords_querystringA(series, &istat);
    tres = drms_query_txt(env->session, query);
    
    if (tres)
    {
        if (tres->num_rows == 1 && tres->num_cols == 1)
        {
            count = atoll(tres->field[0][0]);
        }
        
        db_free_text_result(tres);
        tres = NULL;
    }
    else
    {
        fprintf(stderr, "Failed: %s\n", query);
        istat = DRMS_ERROR_BADDBQUERY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return count;
}

int drms_series_shadowexists(DRMS_Env_t *env, const char *series, int *status)
{
    return ShadowExists(env, series, status);
}

/* tname is optional - if it is set, then this is the name of the shadow table that will be created. */
int drms_series_createshadow(DRMS_Env_t *env, const char *series, const char *tname)
{
    return CreateShadow(env, series, tname, NULL);
}

int drms_series_dropshadow(DRMS_Env_t *env, const char *series, const char *tname)
{
    return DropShadow(env, series, tname, NULL);
}

void drms_series_setcreateshadows(DRMS_Env_t *env, int *val)
{
    if (val)
    {
        env->createshadows = (*val == 0 ? 0 : 1);
    }
    else
    {
        env->createshadows = 1;
    }
}

void drms_series_unsetcreateshadows(DRMS_Env_t *env)
{
    env->createshadows = 0;
}

/* neither pkwhere nor npkwhere exists. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT <fields> FROM <series> AS T1, (SELECT recnum AS therec FROM <shadow table>) AS T2 WHERE T1.recnum = T2.therec ORDER BY T1.pkey1, T1.pkey2, ... LIMIT <limit> 
 *   Ensure that all column names in <fields> are prepended with "T1."
 */
/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 * # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table>;
 *
 * # Select the records in the series whose recnums are in the 
 * # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX)
 *     ORDER BY T.pkey1, T.pkey2, ... LIMIT <limit>;
 * 
 * Third attempt - Since there are no where clauses, there is no advantage to using a temporary table at all.
 * Instead just JOIN with the shadow table directly. Do NOT use a temporary table. Without a LIMIT statement and 
 * outside of a cursor, a temporary table will result in much better performance. BUT this query will always be
 * run either with a LIMIT statement, or inside a cursor, and in both those cases, it runs much more quickly 
 * without a temp table.
 * 
 * SELECT <fields> FROM <series> as T1 JOIN <shadow> AS T2 ON (T1.recnum = T2.recnum) ORDER BY T2.pkey1, T2.pkey2, ... LIMIT <limit>
 *
 * NOTE - not using any temporary table! 
 *
 * SPEED - The cursor declaration is instantaneous, and the first FETCH takes only a second (on su_arta.hmilev1). This is ideal!
 */
char *drms_series_all_querystringA(DRMS_Env_t *env, const char *series, const char *fields, int limit, int cursor, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char limitstr[32];
    char tabname[256];
    char shadow[DRMS_MAXSERIESNAMELEN];
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            
            if (limit > kLimitCutoff && !cursor)
            {
                /* Use a temp table - if there is no cursor and the limit is too big, the non-temp-table solution 
                 * sucks. */
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
                else
                {
                    qualpkeylist = CreatePKeyList(env, series, "", NULL, NULL, NULL, 0, &istat);
                    
                    if (istat == DRMS_SUCCESS)
                    {
                        /* Create the temporary table. */
                        query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, shadow, &stsz);
                        query = base_strcatalloc(query, ";\n", &stsz);
                        
                        /* Select the records from the series table. */
                        query = base_strcatalloc(query, "SELECT ", &stsz);
                        query = base_strcatalloc(query, fields, &stsz);
                        query = base_strcatalloc(query, " FROM ", &stsz);
                        query = base_strcatalloc(query, lcseries, &stsz);
                        query = base_strcatalloc(query, " WHERE recnum IN (SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                        query = base_strcatalloc(query, qualpkeylist, &stsz);
                        query = base_strcatalloc(query, " LIMIT ", &stsz);
                        query = base_strcatalloc(query, limitstr, &stsz);
                        
                        free(qualpkeylist);
                        qualpkeylist = NULL;
                    }
                }           
            }
            else
            {
                /* Prepend all column names in fields with T1 (just in case the series table has a column named "therec"). */
                qualfields = PrependFields(fields, "T1.", &istat);
                
                /* Create a list of all prime key names. */
                qualpkeylist = CreatePKeyList(env, series, "T2.", NULL, NULL, NULL, 0, &istat);
                
                if (istat == DRMS_SUCCESS)
                {                
                    /* Select the records from the series table. */
                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, qualfields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);
                    query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                    query = base_strcatalloc(query, shadow, &stsz);
                    query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                
                free(qualpkeylist);
                qualpkeylist = NULL;
                free(qualfields);
                qualfields = NULL;
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere does not exist, but npkwhere does. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT <fields> FROM <series> AS T1, (SELECT T2.therec FROM (SELECT recnum AS therec FROM <series> WHERE <npkwhere>) AS T2, <shadow table> AS T3 WHERE T2.therec = T3.recnum) AS T4 WHERE T1.recnum = T4.therec ORDER BY T1.pkey1, T1.pkey2, ... LIMIT <limit>
 *                                                                                                                                     
 *   T4 is a list of recnums (column therec) that satisfy both the prime-key logic and the npkwhere clause.                              
 *                                                                                                                                     
 *   Ensure that all column names in <fields> are prepended with "T1." */

/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 *
 *   # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table>;
 *
 *   # Select the records in the series that satisfy the non-prime-key query AND whose recnums are in the 
 *   # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX) AND <npkwhere>
 *     ORDER BY T.pkey1, T.pkey2, ... LIMIT <limit>;
 *
 * Third attempt - Do NOT use a temporary table. Without a LIMIT statement and outside of a cursor, a temporary table 
 * will result in much better performance. BUT this query will always be run either with a LIMIT statement, or inside
 * a cursor, and in both those cases, it runs much more quickly without a temp table.
 * 
 * SELECT <fields> FROM <series> AS T1 JOIN <shadow> AS T2 ON (T1.recnum = T2.recnum) WHERE <npkwhere> ORDER BY T2.pkey1, T2.pkey2, ... LIMIT <limit>;
 *
 *
 * NOTE: Not using any temporary table!
 *
 * SPEED: This runs too slowly if the npkwhere clause selects too many records. The cursor declaration is quick
 *   but the FETCH is a bit slow. But the performance when there are 7.5 million resulting rows isn't too bad.
 *   the first FETCH takes about 15 seconds to return.
 */
char *drms_series_all_querystringB(DRMS_Env_t *env, const char *series, const char *npkwhere, const char *fields, int limit, int cursor, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *qualnpkwhere = NULL;
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];

    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            
            if (limit > kLimitCutoff && !cursor)
            {
                /* Use a temp table - if there is no cursor and the limit is too big, the non-temp-table solution 
                 * sucks. */
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
                else
                {
                    qualpkeylist = CreatePKeyList(env, series, "", NULL, NULL, NULL, 0, &istat);
                    
                    if (istat == DRMS_SUCCESS)
                    {
                        /* Create the temporary table. */
                        query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, shadow, &stsz);
                        query = base_strcatalloc(query, ";\n", &stsz);
                        
                        /* Select the records from the series table. */
                        query = base_strcatalloc(query, "SELECT ", &stsz);
                        query = base_strcatalloc(query, fields, &stsz);
                        query = base_strcatalloc(query, " FROM ", &stsz);
                        query = base_strcatalloc(query, lcseries, &stsz);
                        query = base_strcatalloc(query, " WHERE recnum IN (SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, ") AND ", &stsz);
                        query = base_strcatalloc(query, npkwhere, &stsz);
                        query = base_strcatalloc(query, " ORDER BY ", &stsz);
                        query = base_strcatalloc(query, qualpkeylist, &stsz);
                        query = base_strcatalloc(query, " LIMIT ", &stsz);
                        query = base_strcatalloc(query, limitstr, &stsz);
                        
                        free(qualpkeylist);
                        qualpkeylist = NULL;
                    }
                }           
            }
            else
            {
                /* Prepend all column names in fields with T1 (just in case the series table has a column named "therec"). */
                qualfields = PrependFields(fields, "T1.", &istat);
                
                if (istat == DRMS_SUCCESS)
                {
                    /* It is possible that npkwhere could be recnum (for the [:#X] notation). If this is
                     * the case, then npkwhere will have recnum in it, and then "recnum" is ambiguous. We must prepend 
                     * with T1. */
                    qualnpkwhere = base_strcasereplace(npkwhere, "recnum", "T1.recnum");
                    if (!qualnpkwhere)
                    {
                        /* recnum did not appear in the npkwhere clause */
                        qualnpkwhere = strdup(npkwhere);
                    }
                }
                
                if (qualnpkwhere)
                {
                    /* Must prepend all prime-key column names in npkwhere with T2., otherwise the query has an ambiguity
                     * since both T1 and T2 have the prime-key columns. */
                    char *tmp = NULL;
                    tmp = PrependWhere(env, qualnpkwhere, series, "T2.", &istat);
                    if (istat == DRMS_SUCCESS)
                    {
                        free(qualnpkwhere);
                        qualnpkwhere = tmp;
                    }
                }
                else
                {
                    istat = DRMS_ERROR_OUTOFMEMORY;
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Create a list of all prime key names. */
                    qualpkeylist = CreatePKeyList(env, series, "T2.", NULL, NULL, NULL, 0, &istat);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Select the records from the series table. */
                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, qualfields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);
                    query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                    query = base_strcatalloc(query, shadow, &stsz);
                    query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) WHERE ", &stsz);
                    query = base_strcatalloc(query, qualnpkwhere, &stsz);
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                
                free(qualnpkwhere);
                qualnpkwhere = NULL;
                free(qualpkeylist);
                qualpkeylist = NULL;
                free(qualfields);
                qualfields = NULL;
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere does exist, but npkwhere does not. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT <fields> FROM <series> AS T1, (SELECT recnum AS therec FROM <shadow table> WHERE <pkwhere>) AS T2 WHERE T1.recnum = T2.therec ORDER BY T1.pkey1, T1.pkey2, ... LIMIT <limit>                                                                                       
 *                                                                                                                                     
 * Ensure that all column names in <fields> are prepended with "T1." */

/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 *
 *   # Apply the prime-key logic by selecting all records from the shadow table that satisfy the phwhere clause.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table>
 *     WHERE <pkwhere>;
 *
 *   # Select the records in the series whose recnums are in the 
 *   # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic AND the pkwhere clause).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX)
 *     ORDER BY T.pkey1, T.pkey2, ... LIMIT <limit>;
 * NOTE: This is going too slow when the where clause selects lots of records (for the non-cursor case).
 *
 * Third attempt - Do NOT use a temporary table. Without a LIMIT statement and 
 * outside of a cursor, a temporary table will result in much better performance. BUT this query will always be
 * run either with a LIMIT statement, or inside a cursor, and in both those cases, it runs much more quickly 
 * without a temp table.
 *
 * SELECT <fields> FROM <series> as T1 JOIN <shadow> AS T2 ON (T1.recnum = T2.recnum) WHERE <pkwhere> ORDER BY T2.pkey1, T2.pkey2, ... LIMIT <limit>
 *
 * NOTE: No temporary table!
 * 
 * SPEED: Excellent!
 */
char *drms_series_all_querystringC(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *fields, int limit, int cursor, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *qualpkwhere = NULL;
    char limitstr[32];
    char tabname[256];
    char shadow[DRMS_MAXSERIESNAMELEN];
    
    lcseries = strdup(series);

    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            
            if (limit > kLimitCutoff && !cursor)
            {
                /* Use a temp table - if there is no cursor and the limit is too big, the non-temp-table solution 
                 * sucks. */
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
                else
                {
                    qualpkeylist = CreatePKeyList(env, series, "", NULL, NULL, NULL, 0, &istat);
                    
                    if (istat == DRMS_SUCCESS)
                    {
                        /* Create the temporary table. */
                        query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, shadow, &stsz);
                        query = base_strcatalloc(query, " WHERE ", &stsz);
                        query = base_strcatalloc(query, pkwhere, &stsz);
                        query = base_strcatalloc(query, ";\n", &stsz);
                        /* Select the records from the series table. */
                        query = base_strcatalloc(query, "SELECT ", &stsz);
                        query = base_strcatalloc(query, fields, &stsz);
                        query = base_strcatalloc(query, " FROM ", &stsz);
                        query = base_strcatalloc(query, lcseries, &stsz);
                        query = base_strcatalloc(query, " WHERE recnum IN (SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                        query = base_strcatalloc(query, qualpkeylist, &stsz);
                        query = base_strcatalloc(query, " LIMIT ", &stsz);
                        query = base_strcatalloc(query, limitstr, &stsz);
                        
                        free(qualpkeylist);
                        qualpkeylist = NULL;
                    }
                }           
            }
            else
            {
                /* Prepend all column names in fields with T1 (just in case the series table has a column named "therec"). */
                qualfields = PrependFields(fields, "T1.", &istat);
                
                /* Create a list of all prime key names. */
                if (istat == DRMS_SUCCESS)
                {
                    qualpkeylist = CreatePKeyList(env, series, "T2.", NULL, NULL, NULL, 0, &istat);
                }
                
                /* Must prepend all prime-key column names with T2., otherwise the query has an ambiguity
                 * since both T1 and T2 have the prime-key columns. Use a function that does a search and 
                 * replace. */
                if (istat == DRMS_SUCCESS)
                {
                    qualpkwhere = PrependWhere(env, pkwhere, series, "T2.", &istat);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Select the records from the series table. */
                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, qualfields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);
                    query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                    query = base_strcatalloc(query, shadow, &stsz);
                    query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) WHERE ", &stsz);                
                    query = base_strcatalloc(query, qualpkwhere, &stsz);
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                
                free(qualpkwhere);
                qualpkwhere = NULL;
                free(qualpkeylist);
                qualpkeylist = NULL;
                free(qualfields);
                qualfields = NULL;
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* both pkwhere and npkwhere exist. */
/* First attempt - this selected the correct records, but it ran too slowly. PG would not always use the index.
 *   SELECT <fields> FROM <series> AS T1, (SELECT T2.therec FROM (SELECT recnum AS therec FROM <series> WHERE <pkwhere> AND <npkwhere>) AS T2, <shadow table> AS T3 WHERE T2.therec = T3.recnum) AS T4 WHERE T1.recnum = T4.therec ORDER BY T1.pkey1, T1.pkey2, ... LIMIT <limit>
 *                                                                                                                                     
 *   T4 is a list of recnums (column therec) that satisfy the prime-key logic, the npkwhere clause, and the pkwhere clause.              
 *                                                                                                                                     
 *   Ensure that all column names in <fields> are prepended with "T1." */

/* Second attempt - use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 *
 *   # Apply the prime-key logic by selecting all records from the shadow table that satisfy the pkwhere clause.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table>
 *     WHERE <pkwhere>;
 *
 *   # Select the records in the series that satisfy the non-prime-key query AND whose recnums are in the
 *   # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic AND the pkwhere clause).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX) AND <npkwhere>
 *     ORDER BY T.pkey1, T.pkey2, ... LIMIT <limit>;
 *
 * Third attempt - Do NOT use a temporary table. Without a LIMIT statement and 
 * outside of a cursor, a temporary table will result in much better performance. BUT this query will always be
 * run either with a LIMIT statement, or inside a cursor, and in both those cases, it runs much more quickly 
 * without a temp table.
 *
 * SELECT <fields> FROM <series> AS T1 JOIN <shadow> AS T2 ON (T1.recnum = T2.recnum) WHERE <npkwhere> and <npkwhere> ORDER BY T2.pkey1, T2.pkey2, ... LIMIT <limit>;
 *
 * NOTE: Not using any temporary table!
 *
 * SPEED: Excellent!
 */
char *drms_series_all_querystringD(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *npkwhere, const char *fields, int limit, int cursor, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *qualpkwhere = NULL;
    char *qualnpkwhere = NULL;
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            
            if (limit > kLimitCutoff && !cursor)
            {
                /* Use a temp table - if there is no cursor and the limit is too big, the non-temp-table solution 
                 * sucks. */
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
                else
                {
                    qualpkeylist = CreatePKeyList(env, series, "", NULL, NULL, NULL, 0, &istat);
                    
                    if (istat == DRMS_SUCCESS)
                    {
                        /* Create the temporary table. */
                        query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, shadow, &stsz);
                        query = base_strcatalloc(query, " WHERE ", &stsz);
                        query = base_strcatalloc(query, pkwhere, &stsz);
                        query = base_strcatalloc(query, ";\n", &stsz);
                        
                        /* Select the records from the series table. */
                        query = base_strcatalloc(query, "SELECT ", &stsz);
                        query = base_strcatalloc(query, fields, &stsz);
                        query = base_strcatalloc(query, " FROM ", &stsz);
                        query = base_strcatalloc(query, lcseries, &stsz);
                        query = base_strcatalloc(query, " WHERE recnum IN (SELECT recnum FROM ", &stsz);
                        query = base_strcatalloc(query, tabname, &stsz);
                        query = base_strcatalloc(query, ") AND ", &stsz);
                        query = base_strcatalloc(query, npkwhere, &stsz);
                        query = base_strcatalloc(query, " ORDER BY ", &stsz);
                        query = base_strcatalloc(query, qualpkeylist, &stsz);
                        query = base_strcatalloc(query, " LIMIT ", &stsz);
                        query = base_strcatalloc(query, limitstr, &stsz);
                        
                        free(qualpkeylist);
                        qualpkeylist = NULL;
                    }
                }           
            }
            else
            {
                
                /* Prepend all column names in fields with T1 (just in case the series table has a column named "therec"). */
                qualfields = PrependFields(fields, "T1.", &istat);
                
                /* Create a list of all prime key names. */
                if (istat == DRMS_SUCCESS)
                {
                    qualpkeylist = CreatePKeyList(env, series, "T2.", NULL, NULL, NULL, 0, &istat);
                }
                
                /* Must prepend all prime-key column names with T2., otherwise the query has an ambiguity
                 * since both T1 and T2 have the prime-key columns. Use a function that does a search and 
                 * replace. */
                if (istat == DRMS_SUCCESS)
                {
                    qualpkwhere = PrependWhere(env, pkwhere, series, "T2.", &istat);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    
                    /* It is possible that npkwhere could be recnum (for the [:#X] notation). If this is
                     * the case, then npkwhere will have recnum in it, and then "recnum" is ambiguous. We must prepend 
                     * with T1. */
                    qualnpkwhere = base_strcasereplace(npkwhere, "recnum", "T1.recnum");
                    if (!qualnpkwhere)
                    {
                        /* recnum did not appear in the npkwhere clause */
                        qualnpkwhere = strdup(npkwhere);
                    }
                    
                    if (qualnpkwhere)
                    {
                        /* Must prepend all prime-key column names in npkwhere with T2., otherwise the query has an ambiguity
                         * since both T1 and T2 have the prime-key columns. */
                        char *tmp = NULL;
                        tmp = PrependWhere(env, qualnpkwhere, series, "T2.", &istat);
                        if (istat == DRMS_SUCCESS)
                        {
                            free(qualnpkwhere);
                            qualnpkwhere = tmp;
                        }
                    }
                    else
                    {
                        istat = DRMS_ERROR_OUTOFMEMORY;
                    }
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Select the records from the series table. */
                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, qualfields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);
                    query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                    query = base_strcatalloc(query, shadow, &stsz);
                    query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) WHERE ", &stsz);
                    query = base_strcatalloc(query, qualpkwhere, &stsz);
                    query = base_strcatalloc(query, " AND ", &stsz);
                    query = base_strcatalloc(query, qualnpkwhere, &stsz);
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                
                free(qualnpkwhere);
                qualnpkwhere = NULL;
                free(qualpkwhere);
                qualpkwhere = NULL;            
                free(qualpkeylist);
                qualpkeylist = NULL;
                free(qualfields);
                qualfields = NULL;
            }
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}


/* SELECT <fields> FROM <series>
 * WHERE recnum IN
 *   (SELECT recnum FROM <lasttab>) 
 * AND <npkwhere>
 * ORDER BY pkey1, pkey2, ...
 * LIMIT <limit>
 */
char *drms_series_all_querystringFL(DRMS_Env_t *env, const char *series, const char *npkwhere, HContainer_t *pkwhereNFL, const char *fields, int limit, HContainer_t *firstlast, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 2048;
    char *lcseries = NULL;
    int npkeys = 0;
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char *pkey[DRMS_MAXPRIMIDX];
    char *pkeylist = NULL;
    char *lasttab = NULL;
    int iloop;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            
            pkeylist = CreatePKeyList(env, series, NULL, NULL, pkey, &npkeys, 0, &istat);
            
            if (istat == DRMS_SUCCESS)
            {
                if (istat == DRMS_SUCCESS)
                {
                    istat = InnerFLSelect(npkeys, firstlast, &stsz, pkey, pkeylist, shadow, pkwhereNFL, &query, &lasttab);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Make sure the CREATE TEMP TABLE statements that precede the SELECT statement 
                     * are separated from the latter by ";\n". When this code is used in a db cursor, 
                     * the CREATE statements are executed first, then a cursor is created on the 
                     * remaining statements that follow the ";\n". */
                    query = base_strcatalloc(query, ";\n", &stsz);

                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, fields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);                
                    query = base_strcatalloc(query, " WHERE recnum in (SELECT recnum FROM ", &stsz);
                    query = base_strcatalloc(query, lasttab, &stsz);
                    query = base_strcatalloc(query, ")", &stsz);
                    
                    if (npkwhere && *npkwhere)
                    {
                        query = base_strcatalloc(query, " AND ", &stsz);
                        query = base_strcatalloc(query, npkwhere, &stsz);
                    }
                    
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, pkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                
                /* free stuff. */
                free(lasttab);
                lasttab = NULL;
                
                free(pkeylist);
                pkeylist = NULL;
                
                for (iloop = 0; iloop < npkeys; iloop++)
                {
                    if (pkey[iloop])
                    {
                        free(pkey[iloop]);
                        pkey[iloop] = NULL;
                    }
                }
            }
            else
            {
                istat = DRMS_ERROR_OUTOFMEMORY;
            }            
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;   
}

/* neither pkwhere nor npkwhere exists. */
/* Use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 * # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table> 
 *     ORDER BY pkey1 [DESC], pkey2 [DESC], ... LIMIT min(abs(<nrecs>), <limit>);
 *
 * # Select the records in the series whose recnums are in the 
 * # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX)
 *     ORDER BY T.pkey1, T.pkey2, ...;
 * 
 *  NOTE: We use LIMIT min(abs(<nrecs>), <limit>) for the first query to ensure that we don't 
 *  have queries that run too long. If <limit> < abs(<nrecs>), then we will truncate EARLIER
 *  (from the top of the sorting) records. It might be more desirable to truncate from the bottom
 *  of the sorted list, but then we cannot really limit queries that select too many records
 *  effectively.
 *
 * SPEED: Excellent!
 */
char *drms_series_n_querystringA(DRMS_Env_t *env, const char *series, const char *fields, int nrecs, int limit, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int desc; /* If 1, then we are sorting the records in descending order. */
    char nrecsstr[32];
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *orderpkeylist = NULL; /* A comma-separated list of primary-key constituents. Each
                               * key name will have " DESC" appeneded if nrecs < 0. */
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    
    desc = nrecs < 0;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            snprintf(nrecsstr, sizeof(nrecsstr), "%d", abs(nrecs));
            
            /* Prepend all column names in fields with T (just in case the series table has a column named "therec"). */
            qualfields = PrependFields(fields, "T.", &istat);
            
            /* Create a list of all prime key names. */
            qualpkeylist = CreatePKeyList(env, series, "T.", NULL, NULL, NULL, 0, &istat);
            
            /* Create the list of prime-key names that will be used for sorting the records 
             * before applying the limit to select the "n" records. */
            orderpkeylist = CreatePKeyList(env, series, NULL, desc ? " DESC" : NULL, NULL, NULL, 0, &istat);
            
            if (istat == DRMS_SUCCESS)
            {
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
            }
            
            if (istat == DRMS_SUCCESS)
            {
                /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, " ORDER BY ", &stsz);
                query = base_strcatalloc(query, orderpkeylist, &stsz);
                query = base_strcatalloc(query, " LIMIT ", &stsz);
                
                if (limit < abs(nrecs))
                {
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                else
                {
                    query = base_strcatalloc(query, nrecsstr, &stsz);
                }
                
                query = base_strcatalloc(query, ";\n", &stsz);
                
                /* Select the records from the series table. */
                query = base_strcatalloc(query, "SELECT ", &stsz);
                query = base_strcatalloc(query, qualfields, &stsz);
                query = base_strcatalloc(query, " FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T WHERE T.recnum IN (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                query = base_strcatalloc(query, qualpkeylist, &stsz);
            }
            
            free(orderpkeylist);
            orderpkeylist = NULL;
            free(qualpkeylist);
            qualpkeylist = NULL;
            free(qualfields);
            qualfields = NULL;
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere does not exist, but npkwhere does. */
/* Use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 *
 * # Select ALL PG records in the original table that satisfy the non-prime-key where
 * # clause.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum from <series> WHERE <npkwhere>;
 * 
 * # From this list of recnums, select the recnums that also appear in the shadow table.
 * # The resulting list of recnums identify the records in the original table that satisfy
 * # both the prime-key logic and the non-prime-key where clause. Then, select the entire
 * # set of columns from the original table where the recnum matches the list of recnums
 * # previously created.
 *   SELECT <fields> FROM <series> AS T2 WHERE T2.recnum IN 
 *     (SELECT recnum from <shadow table> as T1 WHERE T1.recnum IN 
 *       (SELECT recnum from shadowtempXXX) ORDER BY T1.pkey1 [DESC], T1.pkey2 [DESC], ... LIMIT <abs(nrecs)>)
 *     ORDER BY T2.pkey1, T2, pkey2, ... LIMIT <limit>
 *
 * The first attempt produced a query that ran too slowly.
 *
 * # Select the recnums that satisfy the prime-key logic AND the npkwhere clause.
 * CREATE TEMPORARY TABLE shadowtempXXX AS SELECT T1.recnum, T1.pkey1, T1.pkey2, ... FROM <shadow table> AS T1 JOIN <series> AS T2 ON (T1.recnum = T2.recnum) WHERE <npkwhere> ORDER BY T1.pkey1, T1.pkey2, ... LIMIT min(abs(<nrecs>), <limit>);
 *
 * # Select the columns from the original table whose recnum's match those in the temporary table.
 * SELECT <fields> FROM <series> AS T1 WHERE T1.recnum IN (SELECT recnum from shadowtempXXX) ORDER BY T1.pkey1, T1.pkey2, ...;
 *
 *  NOTE: We use LIMIT min(abs(<nrecs>), <limit>) for the first query to ensure that we don't 
 *  have queries that run too long. If <limit> < abs(<nrecs>), then we will truncate EARLIER
 *  (from the top of the sorting) records. It might be more desirable to truncate from the bottom
 *  of the sorted list, but then we cannot really limit queries that select too many records
 *  effectively.
 *
 * SPEED: Excellent!
 */
char *drms_series_n_querystringB(DRMS_Env_t *env, const char *series, const char *npkwhere, const char *fields, int nrecs, int limit, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int desc; /* If 1, then we are sorting the records in descending order. */
    char nrecsstr[32];
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *orderpkeylist = NULL; /* A comma-separated list of primary-key constituents. Each
                                 * key name will have " DESC" appeneded if nrecs < 0. */
    char *qualnpkwhere = NULL;
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    
    desc = nrecs < 0;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            snprintf(nrecsstr, sizeof(nrecsstr), "%d", abs(nrecs));
            
            qualfields = PrependFields(fields, "T1.", &istat);
            
            /* Create a list of all prime key names. */
            qualpkeylist = CreatePKeyList(env, series, "T1.", NULL, NULL, NULL, 0, &istat);
            
            /* Create the list of prime-key names that will be used for sorting the records 
             * before applying the limit to select the "n" records. */
            orderpkeylist = CreatePKeyList(env, series, "T1.", desc ? " DESC" : NULL, NULL, NULL, 0, &istat);
            
            if (istat == DRMS_SUCCESS)
            {
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
            }
            
            if (istat == DRMS_SUCCESS)
            {
                /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT T1.recnum, ", &stsz);
                query = base_strcatalloc(query, qualpkeylist, &stsz);                
                query = base_strcatalloc(query, " FROM ", &stsz);                
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) WHERE ", &stsz);
                
                /* Man, people can put a prime-key where clause in the place where a non-prime-key should be. 
                 * For example, series[][? pkey > 0 ?]. So we need to prefix each keyword in the npkwhere clause. In general,
                 * the keywords in the npkwhere clause will not exist in the shadow table, so prefix each such keyword 
                 * with T2 (the series table). 
                 *
                 * PrependWhere() will prepend pkey names only. That is ok since the only keywords that will 
                 * lead to ambiguity about which table the keyword resides in are prime-key keywords. */
                qualnpkwhere = PrependWhere(env, npkwhere, series, "T1.", &istat);
                
                if (istat == DRMS_SUCCESS)
                {
                    query = base_strcatalloc(query, qualnpkwhere, &stsz);
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, orderpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);
                    
                    if (limit < abs(nrecs))
                    {
                        query = base_strcatalloc(query, limitstr, &stsz);
                    }
                    else
                    {
                        query = base_strcatalloc(query, nrecsstr, &stsz);
                    }
                    
                    query = base_strcatalloc(query, ";\n", &stsz);
                    
                    /* Select the records from the series table. */
                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, qualfields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);
                    query = base_strcatalloc(query, " AS T1 WHERE T1.recnum IN (SELECT recnum FROM ", &stsz);
                    query = base_strcatalloc(query, tabname, &stsz);                
                    query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                }
            }
            
            if (qualnpkwhere)
            {
                free(qualnpkwhere);
                qualnpkwhere = NULL;
            }
            
            free(orderpkeylist);
            orderpkeylist = NULL;
            free(qualpkeylist);
            qualpkeylist = NULL;
            free(qualfields);
            qualfields = NULL;
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* pkwhere does exist, but npkwhere does not. */
/* Use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 * # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table> WHERE <pkwhere>
 *     ORDER BY pkey1 [DESC], pkey2 [DESC], ... LIMIT min(abs(<nrecs>), <limit>);
 *
 * # Select the records in the series whose recnums are in the 
 * # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX)
 *     ORDER BY T.pkey1, T.pkey2, ...;
 *
 * NOTE: We use LIMIT min(abs(<nrecs>), <limit>) for the first query to ensure that we don't 
 *  have queries that run too long. If <limit> < abs(<nrecs>), then we will truncate EARLIER
 *  (from the top of the sorting) records. It might be more desirable to truncate from the bottom
 *  of the sorted list, but then we cannot really limit queries that select too many records
 *  effectively.
 *
 * SPEED: Excellent, unless the prime-key where clause selects lots of records, and so does nrecs. But
 *   even in that case, it doesn't run for more than 15 seconds.
 */
char *drms_series_n_querystringC(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *fields, int nrecs, int limit, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int desc; /* If 1, then we are sorting the records in descending order. */
    char nrecsstr[32];
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *orderpkeylist = NULL; /* A comma-separated list of primary-key constituents. Each
                                 * key name will have " DESC" appeneded if nrecs < 0. */
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    
    desc = nrecs < 0;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            snprintf(nrecsstr, sizeof(nrecsstr), "%d", abs(nrecs));
            
            /* Prepend all column names in fields with T (just in case the series table has a column named "therec"). */
            qualfields = PrependFields(fields, "T.", &istat);
            
            /* Create a list of all prime key names. */
            qualpkeylist = CreatePKeyList(env, series, "T.", NULL, NULL, NULL, 0, &istat);
            
            /* Create the list of prime-key names that will be used for sorting the records 
             * before applying the limit to select the "n" records. */
            orderpkeylist = CreatePKeyList(env, series, NULL, desc ? " DESC" : NULL, NULL, NULL, 0, &istat);
            
            if (istat == DRMS_SUCCESS)
            {
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
            }
            
            if (istat == DRMS_SUCCESS)
            {
                /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, " WHERE ", &stsz);
                query = base_strcatalloc(query, pkwhere, &stsz);                
                query = base_strcatalloc(query, " ORDER BY ", &stsz);
                query = base_strcatalloc(query, orderpkeylist, &stsz);
                query = base_strcatalloc(query, " LIMIT ", &stsz);
                
                if (limit < abs(nrecs))
                {
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                else
                {
                    query = base_strcatalloc(query, nrecsstr, &stsz);
                }
                
                query = base_strcatalloc(query, ";\n", &stsz);
                
                /* Select the records from the series table. */
                query = base_strcatalloc(query, "SELECT ", &stsz);
                query = base_strcatalloc(query, qualfields, &stsz);
                query = base_strcatalloc(query, " FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T WHERE T.recnum IN (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                query = base_strcatalloc(query, qualpkeylist, &stsz);
            }
            
            free(orderpkeylist);
            orderpkeylist = NULL;
            free(qualpkeylist);
            qualpkeylist = NULL;
            free(qualfields);
            qualfields = NULL;
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* both pkwhere and npkwhere exist. */
/* Use temporary tables. These tables will be deleted when the database session ends. Each
 * time one is created, it is given a unique name (shadowtempXXX).
 * 
 * # Apply the prime-key logic by selecting all records from the shadow table.
 *   CREATE TEMPORARY TABLE shadowtempXXX AS SELECT recnum FROM <shadow table> WHERE <pkwhere>
 *     ORDER BY pkey1 [DESC], pkey2 [DESC], ... LIMIT <abs(nrecs)>;
 *
 * # Select the records in the series whose recnums are in the 
 * # list of recnums in shadowtempXXX (those that have satisfied the prime-key logic).
 *   SELECT <fields> FROM <series> AS T WHERE T.recnum IN (SELECT recnum FROM shadowtempXXX) AND <npkwhere>
 *     ORDER BY T.pkey1, T.pkey2, ... [ LIMIT <limit> ];
 *
 * The first query is wrong. We need to apply BOTH the pkwhere and npkwhere clause before 
 * the limit <abs(nrecs)> is applied. Try again:
 *
 * # Select the recnums that satisfy the prime-key logic AND the npkwhere clause.
 * CREATE TEMPORARY TABLE shadowtempXXX AS SELECT T1.recnum, T1.pkey1, T1.pkey2, ... FROM <shadow table> AS T1 JOIN <series> AS T2 ON (T1.recnum = T2.recnum) WHERE <pkwhere> AND <npkwhere> ORDER BY T1.pkey1, T1.pkey2, ... LIMIT min(abs(<nrecs>), <limit>);
 *
 * # Select the columns from the original table whose recnum's match those in the temporary table.
 * SELECT <fields> FROM <series> AS T1 WHERE T1.recnum IN (SELECT recnum from shadowtempXXX) ORDER BY T1.pkey1, T1.pkey2, ...;
 *
 * NOTE: We use LIMIT min(abs(<nrecs>), <limit>) for the first query to ensure that we don't 
 *  have queries that run too long. If <limit> < abs(<nrecs>), then we will truncate EARLIER
 *  (from the top of the sorting) records. It might be more desirable to truncate from the bottom
 *  of the sorted list, but then we cannot really limit queries that select too many records
 *  effectively.
 *
 * SPEED: Excellent, unless the prime-key where clause selects lots of records, and so does nrecs. But
 *   even in that case, it doesn't run for more than 15 seconds.
 */
char *drms_series_n_querystringD(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *npkwhere, const char *fields, int nrecs, int limit, int *status)
{
    char *query = NULL;
    int istat = DRMS_SUCCESS;
    size_t stsz = 8192;
    char *lcseries = NULL;
    int desc; /* If 1, then we are sorting the records in descending order. */
    char nrecsstr[32];
    char *qualfields = NULL;
    char *qualpkeylist = NULL;
    char *orderpkeylist = NULL; /* A comma-separated list of primary-key constituents. Each
                                 * key name will have " DESC" appeneded if nrecs < 0. */
    char *qualpkwhere = NULL;
    char limitstr[32];
    char shadow[DRMS_MAXSERIESNAMELEN];
    char tabname[256];
    
    desc = nrecs < 0;
    
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            snprintf(nrecsstr, sizeof(nrecsstr), "%d", abs(nrecs));
            
            /* Prepend all column names in fields with T (just in case the series table has a column named "therec"). */
            qualfields = PrependFields(fields, "T1.", &istat);
            
            /* Create a list of all prime key names. */
            if (istat == DRMS_SUCCESS)
            {
                qualpkeylist = CreatePKeyList(env, series, "T1.", NULL, NULL, NULL, 0, &istat);
            }
            
            /* Create the list of prime-key names that will be used for sorting the records 
             * before applying the limit to select the "n" records. */
            if (istat == DRMS_SUCCESS)
            {
                orderpkeylist = CreatePKeyList(env, series, "T1.", desc ? " DESC" : NULL, NULL, NULL, 0, &istat);
            }
            
            if (istat == DRMS_SUCCESS)
            {
                qualpkwhere = PrependWhere(env, pkwhere, series, "T1.", &istat);
            }
            
            if (istat == DRMS_SUCCESS)
            {
                if (GetTempTable(tabname, sizeof(tabname)))
                {
                    istat = DRMS_ERROR_OVERFLOW;
                }
            }
            
            if (istat == DRMS_SUCCESS)
            {
               /* Create the temporary table. */
                query = base_strcatalloc(query, "CREATE TEMPORARY TABLE ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, " AS SELECT T1.recnum, ", &stsz);
                query = base_strcatalloc(query, qualpkeylist, &stsz);
                query = base_strcatalloc(query, " FROM ", &stsz);
                query = base_strcatalloc(query, shadow, &stsz);
                query = base_strcatalloc(query, " AS T1 JOIN ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T2 ON (T1.recnum = T2.recnum) WHERE ", &stsz);
                query = base_strcatalloc(query, qualpkwhere, &stsz);
                query = base_strcatalloc(query, " AND ", &stsz);
                query = base_strcatalloc(query, npkwhere, &stsz);
                query = base_strcatalloc(query, " ORDER BY ", &stsz);
                query = base_strcatalloc(query, orderpkeylist, &stsz);
                query = base_strcatalloc(query, " LIMIT ", &stsz);
                
                if (limit < abs(nrecs))
                {
                    query = base_strcatalloc(query, limitstr, &stsz);
                }
                else
                {
                    query = base_strcatalloc(query, nrecsstr, &stsz);
                }
                
                query = base_strcatalloc(query, ";\n", &stsz);
                
                /* Select the records from the series table. */
                query = base_strcatalloc(query, "SELECT ", &stsz);
                query = base_strcatalloc(query, qualfields, &stsz);
                query = base_strcatalloc(query, " FROM ", &stsz);
                query = base_strcatalloc(query, lcseries, &stsz);
                query = base_strcatalloc(query, " AS T1 WHERE T1.recnum IN (SELECT recnum FROM ", &stsz);
                query = base_strcatalloc(query, tabname, &stsz);
                query = base_strcatalloc(query, ") ORDER BY ", &stsz);
                query = base_strcatalloc(query, qualpkeylist, &stsz);
            }
            
            free(qualpkwhere);
            qualpkwhere = NULL;
            free(orderpkeylist);
            orderpkeylist = NULL;
            free(qualpkeylist);
            qualpkeylist = NULL;
            free(qualfields);
            qualfields = NULL;
        }
        else
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }
        
        free(lcseries);
        lcseries = NULL;
    }
    else
    {
        istat = DRMS_ERROR_OUTOFMEMORY;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

/* SELECT <fields> FROM <series> 
 * WHERE recnum IN
 *   (SELECT recnum FROM <lasttab>)
 * AND <npkwhere>
 * ORDER BY pkey1, pkey2, ..., pkeyN
 * LIMIT min(abs(<nrecs>), <limit>)
 */
char *drms_series_n_querystringFL(DRMS_Env_t *env, const char *series, const char *npkwhere, HContainer_t *pkwhereNFL, const char *fields, int nrecs, int limit, HContainer_t *firstlast, int *status)
{
    char *lcseries = NULL;
    char *query = NULL;
    size_t stsz;
    char shadow[DRMS_MAXSERIESNAMELEN];
    char limitstr[32];
    char nrecsstr[32];
    char *pkey[DRMS_MAXPRIMIDX];
    int npkeys = 0;
    int desc;
    char *lasttab = NULL;
    int iloop;
    char *pkeylist = NULL;
    char *qualpkeylist = NULL;
    int istat = DRMS_SUCCESS;
    
    desc = nrecs < 0;
    lcseries = strdup(series);
    
    if (lcseries)
    {
        strtolower(lcseries);
        stsz = 2048;
        query = malloc(stsz);
        
        if (query)
        {
            *query = '\0';
            
            snprintf(shadow, sizeof(shadow), "%s%s", lcseries, kShadowSuffix);
            snprintf(limitstr, sizeof(limitstr), "%d", limit);
            snprintf(nrecsstr, sizeof(nrecsstr), "%d", abs(nrecs));
            
            if (istat == DRMS_SUCCESS)
            {
                pkeylist = CreatePKeyList(env, series, NULL, NULL, pkey, &npkeys, 0, &istat);
                
                if (istat == DRMS_SUCCESS)
                {
                    istat = InnerFLSelect(npkeys, firstlast, &stsz, pkey, pkeylist, shadow, pkwhereNFL, &query, &lasttab);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    qualpkeylist = CreatePKeyList(env, series, NULL, desc ? " DESC" : NULL, NULL, NULL, 0, &istat);
                }
                
                if (istat == DRMS_SUCCESS)
                {
                    /* Make sure the CREATE TEMP TABLE statements that precede the SELECT statement    
                     * are separated from the latter by ";\n". When this code is used in a db cursor, 
                     * the CREATE statements are executed first, then a cursor is created on the       
                     * remaining statements that follow the ";\n". */
                    query = base_strcatalloc(query, ";\n", &stsz);

                    query = base_strcatalloc(query, "SELECT ", &stsz);
                    query = base_strcatalloc(query, fields, &stsz);
                    query = base_strcatalloc(query, " FROM ", &stsz);
                    query = base_strcatalloc(query, lcseries, &stsz);                
                    query = base_strcatalloc(query, " WHERE recnum in (SELECT recnum FROM ", &stsz);
                    query = base_strcatalloc(query, lasttab, &stsz);
                    query = base_strcatalloc(query, ")", &stsz);
                    
                    if (npkwhere && *npkwhere)
                    {
                        query = base_strcatalloc(query, " AND ", &stsz);
                        query = base_strcatalloc(query, npkwhere, &stsz);
                    }
                    
                    query = base_strcatalloc(query, " ORDER BY ", &stsz);
                    query = base_strcatalloc(query, qualpkeylist, &stsz);
                    query = base_strcatalloc(query, " LIMIT ", &stsz);

                    if (limit < abs(nrecs))
                    {
                        query = base_strcatalloc(query, limitstr, &stsz);
                    }
                    else
                    {
                        query = base_strcatalloc(query, nrecsstr, &stsz);
                    }
                }
                
                /* free stuff. */
                free(qualpkeylist);
                qualpkeylist = NULL;
                
                free(lasttab);
                lasttab = NULL;
                
                free(pkeylist);
                pkeylist = NULL;
                
                for (iloop = 0; iloop < npkeys; iloop++)
                {
                    if (pkey[iloop])
                    {
                        free(pkey[iloop]);
                        pkey[iloop] = NULL;
                    }
                }                
            }
        }
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return query;
}

int drms_series_summaryexists(DRMS_Env_t *env, const char *series, int *status)
{
#if (defined TOC && TOC)
   int istat = DRMS_SUCCESS;
   int tocexists = -1;
   int shadowexists = -1;
   int summexists = -1;

   tocexists = TocExists(env, &istat);

   if (istat == DRMS_SUCCESS)
   {
      if (tocexists)
      {
         int present = IsSeriesPresent(env, kTableOfCountsNS, kTableOfCountsTab, series, &istat);
         
         if (istat == DRMS_SUCCESS)
         {
            summexists = present ? 1 : 0;
         }
      }
      else
      {
         summexists = 0;
      }
   }

   if (istat == DRMS_SUCCESS)
   {
      if (!summexists)
      {
         shadowexists = ShadowExists(env, series, &istat);

         if (istat == DRMS_SUCCESS)
         {
            summexists = shadowexists ? 1 : 0;
         }
         else
         {
            summexists = -1; /* since we don't know if it really exists. */
         }
      }
   }

   if (status)
   {
      *status = istat;
   }

   return summexists;
#else

   int istat = DRMS_SUCCESS;
   int shadowexists = -1;
   int summexists = -1;

   shadowexists = ShadowExists(env, series, &istat);

   if (istat == DRMS_SUCCESS)
   {
      summexists = shadowexists ? 1 : 0;
   }
   else
   {
      summexists = -1; /* since we don't know if it really exists. */
   }

   if (status)
   {
      *status = istat;
   }

   return summexists;

#endif
}

int drms_series_canupdatesummaries(DRMS_Env_t *env, const char *series, int *status)
{
   /* This is just a stub that is going into a version of our code tree that cannot 
    * create/update the summary tables. The idea is to get all users to update to this
    * version of lib DRMS that contains this function so that when I later add 
    * code that CAN make the TOC and shadow tables, that we don't accidentally get 
    * users with older code attempting to add records to series that have associated 
    * summary tables (The old code does not update the summary tables, and if we were
    * to allow the old code to add records to a series table that has associated summary tables
    * then the series table would get out of sync with the summary tables). */
    return 1;
}

/* This function determines if query contains a temporary-table-creating query. */
int drms_series_hastemptab(const char *query)
{
    int rv = -1;
    const char *pat = "CREATE[:space:]+TEMPORARY[:space:]+TABLE[:space:]+shadowtemp";
    regex_t regexp;
    
    if (regcomp(&regexp, pat, (REG_EXTENDED | REG_ICASE)) != 0)
    {
        fprintf(stderr, "Bad regular expression '%s'.\n", pat);
    }
    else
    {
        if (regexec(&regexp, query, 0, NULL, 0) == 0)
        {
            /* Match. */
            rv = 1;
        }
        else
        {
            rv = 0;
        }
        
        regfree(&regexp);
    }

    return rv;
}
