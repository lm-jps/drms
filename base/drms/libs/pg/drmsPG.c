/* This concept was a big failure since we couldn't figure out how to call this C function from plpgsql. This
 * project has been supplanted by a plperlu trigger function (updateshadow()). */
/* drmsPG.c - This file contains functions to interface PG functions to DRMS functions. */

/* UpdateShadow can be called from a trigger function, or it can be called from other SQL. */


#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

#define kShadowSuffix "_shadow"

/* The following function will NOT be exported to PG. */
/* Return values:
 *   -1 : An error occurred.
 *    0 : The table does not exist in the database currently connected to.
 *    1 : The table does exist in the database currently connected to.
 *
 * Status values:
 *    0 : Success.
 *    1 : An error occurred.
 */
static int PGCtxTabexists(const char *ns, const char *tab, int *status)
{
    int result = -1;
    int istat = 0;
    char query[2048];
    DB_Text_Result_t *qres = NULL;
    
    snprintf(query, sizeof(query), "SELECT * FROM pg_tables WHERE schemaname ILIKE '%s' AND tablename ILIKE '%s'", ns, tab);
    
    /* Execute this read-only SQL. */
    istat = SPI_execute(query, true, 0);
    
    if (istat == SPI_OK_SELECT)
    {
        if (SPI_processed == 0)
        {
            /* No rows returned. */
            result = 0;
        }
        else if (SPI_processed == 1)
        {
            /* 1 row returned (one table with the the name tab in the namespace ns). */
            result = 1;
        }
        else
        {
            /* Can't happen - more than one table with the same name. */
            istat = 1; 
        }
    }
    else
    {
        /* An error occurred during the evaluation of the query. */
        istat = 1;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return result;
}


/* The following function will NOT be exported to PG. */
/* Returns 0 on success, 1 on failure. */
static int PGCtxShadowExists(const char *ns, const char *tab, int *status)
{
    int istat = 0;
    int tabexists = 0;
    char shadowtable[64];
    
    snprintf(shadowtable, sizeof(shadowtable), "%s%s", tab, kShadowSuffix);
    tabexists = PGCtxTabexists(namespace, shadowtable, &istat);        
    
    if (status)
    {
        *status = istat;
    }
    
    return tabexists;
}

static size_t PGCtxStrlcat(char *dst, const char *src, size_t size)
{
    size_t max = size - strlen(dst) - 1; /* max non-NULL can add */
    size_t start = strlen(dst);
    
    if (max > 0)
    {
        snprintf(dst + start, max + 1, "%s", src); /* add 1 to max for NULL */
    }
    
    return start + strlen(src);
}

static void *PGCtxStrcatalloc(char *dst, const char *src, size_t *sizedst)
{
    size_t srclen = strlen(src);
    size_t dstlen = strlen(dst);
    void *retstr = NULL;
    
    if (srclen > *sizedst - dstlen - 1)
    {
        void *tmp = palloc(*sizedst * 2);
        
        if (tmp)
        {
            snprintf(tmp, *sizedst * 2, "%s", dst);
            *sizedst *= 2;
            retstr = tmp;
        }
    }
    else
    {
        retstr = dst;
    }
    
    if (retstr)
    {
        PGCtxStrlcat(retstr, src, *sizedst);
    }
    
    return retstr;
}

static void PGCtxStrtolower(char *str)
{
    int n;
    int i;
    
    n = strlen(str);
    
    for (i = 0; i < n; i++)
    {
        str[i] = (char)tolower(str[i]);
    }
}


static int PGCtxIsGroupNew(long long recnum,
                           const char *ns, 
                           const char *tab
                           char **pkeynames,
                           int ncols,
                           int *status)
{
    char *query = NULL;
    size_t stsz = 8192;
    char scolnum[8];
    char srecnum[32];
    int ans = -1;
    int icol;
    char *lcseries = NULL;
    int istat = 0;
    
    snprintf(srecnum, sizeof(srecnum), "%lld", recnum);
    
    lcseries = palloc((strlen(ns) + strlen(tab) + 2) * sizeof(char));
    snprintf(lcseries, (strlen(ns) + strlen(tab) + 2) * sizeof(char), "%s.%s", ns, tab);
    
    if (lcseries)
    {
        PGCtxStrtolower(lcseries);
        query = palloc(stsz);
        if (query)
        {
            /* Select a count of records in the original table where the prime-key value
             * matches the prime-key value of the record just inserts (should be
             * at least one record). */
            *query = '\0';
            
            query = PGCtxStrcatalloc(query, "SELECT count(*) FROM ", &stsz);
            query = PGCtxStrcatalloc(query, lcseries, &stsz);
            query = PGCtxStrcatalloc(query, " AS T1, (SELECT ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = PGCtxStrcatalloc(query, pkeynames[icol], &stsz);
                query = PGCtxStrcatalloc(query, " AS p", &stsz);
                query = PGCtxStrcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = PGCtxStrcatalloc(query, ", ", &stsz);
                }
            }
            
            query = PGCtxStrcatalloc(query, " FROM ", &stsz);
            query = PGCtxStrcatalloc(query, lcseries, &stsz);
            query = PGCtxStrcatalloc(query, " WHERE recnum = ", &stsz);
            query = PGCtxStrcatalloc(query, srecnum, &stsz);
            query = PGCtxStrcatalloc(query, ") AS T2 WHERE ", &stsz);
            
            for (icol = 0; icol < ncols; icol++)
            {
                snprintf(scolnum, sizeof(scolnum), "%d", icol);
                query = PGCtxStrcatalloc(query, "T1.", &stsz);
                query = PGCtxStrcatalloc(query, pkeynames[icol], &stsz);
                query = PGCtxStrcatalloc(query, " = T2.p", &stsz);
                query = PGCtxStrcatalloc(query, scolnum, &stsz);
                
                if (icol < ncols - 1)
                {
                    query = PGCtxStrcatalloc(query, " AND ", &stsz);
                }
            }
        }
        else
        {
            istat = 1;
        }        
    }
    else
    {
        istat = 1;
    }
    
    /* Execute the query. */
    if (istat == 0)
    {
        istat = SPI_execute(query, true, 0);
        
        if (istat == SPI_OK_SELECT)
        {
            if (SPI_processed == 0)
            {
                /* No rows returned. */
                /* This implies that the record just added doesn't exist */
                fprintf(stderr, "Unexpected record count %lld; should be at least one.\n", num);
                istat = 1;
            }
            else if (SPI_processed == 1)
            {
                /* There was only one row, so this IS a new group. */
                ans = 1;
            }
            else
            {
                /* At least one record with the prime-key values already existed 
                 * before the insert of this recrd, so this is NOT a new group. */
                ans = 0;
            }
        }
        else
        {
            /* An error occurred during the evaluation of the query. */
            istat = 1;
        }
        
        tres = drms_query_txt(env->session, query);
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return ans;
}


/* Exported functions. */
PG_FUNCTION_INFO_V1(UpdateShadow);

/* This function MUST be called: 
 *    By code that modifies a shadowed DRMS table, but does not update the shadow table 
 *    (e.g., pre-shadow-table DRMS code, SUMS' deletion of DRMS-series records for 
 *    archive == -1).
 *
 * This function should NOT be called:
 *    By code that properly updates a shadowed DRMS table (e.g., shadow-table aware DRMS code,
 *    non-slony code that inserts records into a replicated table - like shadow-table aware
 *    DRMS code).
 
 if it is called from a trigger function installed
 * on a shadowed DRMS table.
 */
Datum UpdateShadow(PG_FUNCTION_ARGS)
{
    /* Have to re-write everything that exists in drms_series.c:drms_series_updatesummaries() because ...
     * 1. The trigger function can pass only record information. It cannot pass the DRMS_Env_t, which 
     * is used extensively by drms_series_updatesummaries().
     * 2. There are many system calls that cannot be made in this PG context - malloc for one!!
     */
    
    /* Arguments to this function:
     *   0 - namespace of the shadowed database table.
     *   1 - table name of the shadowed database table.
     *   2 - the operation on the shadowed database table (insert or delete).
     *   3 - the recnum of the record that got inserted or deleted.
     *   4 - an array of the names of the keywords that compose the prime key.
     **/
    
    int status = 0;
    int shadowexists = 0;
    size_t lsz = 0;
    char recnumstr[64];
    int isnew;
    text *nstext = NULL;
    text *tabletext = NULL;
    int added;
    long long recnum = -1;
    ArrayType *pkeynames = NULL;
    char *ns = NULL;
    char *tab = NULL;
    
    if (SPI_connect() == SPI_OK_CONNECT)
    {
        nstext = PG_GETARG_TEXT_P(0);
        tabletext = PG_GETARG_TEXT_P(1);
        added = (strcmp(PG_GETARG_INT32(2), "INSERT") ? 1 : 0);
        recnum = PG_GETARG_INT64(3);
        pkeynames = PG_GETARG_ARRAYTYPE_P(4);
        
        ns = palloc(nstext->length + 1);
        table = palloc(tabletext->length + 1);
        
        if (!ns || !table)
        {
            PG_RETURN_INT32(kPGCtxErrNoMemory);
        }
        
        memcpy(ns, nstext->data, nstext->length);
        *(ns + nstext->length) = '\0';

        memcpy(table, tabletext->data, tabletext->length);
        *(table + tabletext->length) = '\0';

        get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);
        deconstruct_array(pkeynames, TEXTOID, i_typelen, i_typebyval, i_typealign, &idata, nulls, &num );
        
        shadowexists = PGCtxShadowExists(ns, table, &status);
        
        if (status != 0)
        {
            /* There was a problem checking for shadow-table existence. */
            PG_RETURN_INT32(kPGCtxErrShadowCheck);
        }
        
        if (shadowexists)
        {
            snprintf(recnumstr, sizeof(recnumstr), "%lld", recnum);
            
            if (added)
            {
                /* We're updating the shadow table because one or more rows was inserted          
                 * into the original series table. */
                isnew = IsGroupNew(recnum, ns, tab, pkeynames, ncols, &status);

                if (isnew == 0)
                {
                    /* Need to update an existing record in the shadow table. */
                    status = UpdateShadow(ns, tab, pkeynames, ncols, recnum, 1);
                }
                else if (isnew == 1)
                {
                    /* Need to add a new group (record) to the shadow table. */
                    /* shadow - pkey1, pkey2, ..., recnum, nrecords                                                                         
                     */
                    status = InsertIntoShadow(ns, tab, pkeynames, ncols, recnum);
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
                
                wasdel = WasGroupDeleted(recnum, ns, tab, pkeynames, ncols, &status);
                if (wasdel)
                {
                    /* The last DRMS record was deleted - delete corresponding group from          
                     * shadow table. */
                    status = DeleteFromShadow(ns, tab, pkeynames, ncols, recnum);
                }
                else
                {
                    /* One version of a DRMS record was deleted. May need to update the            
                     * corresponding group's record in the shadow table (update nrecords           
                     * and recnum). If the version deleted was an obsolete version,                
                     * then no change to the recnum in the shadow table is needed. */
                    status = UpdateShadow(ns, tab, pkeynames, ncols, recnum, 0);
                }
            }
            
        }
            

        
        PG_RETURN_INT32(kPGCtxSuccess);
        
        SPI_finish();
    }
}
