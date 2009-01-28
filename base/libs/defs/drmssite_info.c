/*
 *  drms_site_info.c					~rick/netdrms/src
 *
 *  This module contains three functions associated withg extracting DRMS
 *    site information from SUNUM's (SUMS identifiers, which must be in
 *    unique ranges for each registered DRMS site):
 *
 *	drmssite_code_from_sunum
 *	drmssite_info_from_sunum
 *	drmssite_sunum_is_local
 *
 *  drmssite_info_from_sunum - parse SUNUM (SUMID) to obtain DRMS site ID,
 *    and return a struct containing the information about the site that may
 *    be necessary to initiate or authenticate transfer of SUMS storage units,
 *    or to provide information for building or maintaining SUMS or for
 *    back channel communications
 *
 *  Author: Rick Bogart					RBogart@spd.aas.org
 *
 *  The function has two arguments:
 *	unsigned long long sunum	This can be any 64-bit number; it is
 *					assumed to represent a SUNUM, which is
 *					parsed to determine the corresponding
 *					identification (15 bit) code
 *	DRMSSiteInfo_t **info		This is a pointer to a struct containing
 *					all of the fields associated with the
 *					site; all strings except for the
 *					site_code itself. The meaning of the
 *					structure members is documented under
 *					the declaration. The structure is
 *					allocated and filled as applicable
 *					
 *
 *  It returns a short integer which is the parsed site code.  Site codes
 *    in the range 0 - 16383 (0x0000 - 0x3fff) are reserved for sites that
 *    may publicly export data from their SUMS, and must be controlled through
 *    a data series mirrored from a single master. Codes in the range 16384 -
 *    32767 are assumed to apply to importing sites only (such as individual
 *    workstations/laptops); they only require registration for the ssh
 *    client information in the server's public key list; their ID's are
 *    otherwise unimportant. Negative codes are invalid.
 *
 *  Bugs:
 *    Two tables are searched: jsoc.drms_sites and drms.sites, in that order
 *      If the first table is found, the second table is ignored, so it is
 *	possible for a site to be registered in its (locally maintained)
 *	table, but missing from the "authoritative" one.
 *    If neither of the expected tables is found, a warning is printed
 *	to stderr, and the info struct is set to a NULL pointer
 *    Site info struct members corresponding to columns missing from the
 *	table are NULL; this should not happen if the table is complete.
 *	Missing values may be simply blanks ("").
 *    drmssite_code_from_sunum() and drmssite_sunum_is_local() might be better
 *	defined as macros.
 *
 *  Revision history is at the end of the file.
 *
 */

#include "drmssite_info.h"
#include "jsoc.h"
#include "serverdefs.h"

short drmssite_code_from_sunum (unsigned long long sunum) {
  return (sunum & 0xffff000000000000) >> 48;
}

#ifdef DEFS_CLIENT
short drmssite_client_info_from_sunum (unsigned long long sunum,
    int sockfd, DRMSSiteInfo_t **info) {
#else
short drmssite_server_info_from_sunum (unsigned long long sunum,
    DB_Handle_t *dbin, DRMSSiteInfo_t **info) {
#endif
  DB_Text_Result_t *text_res = NULL;
  int current, col;
  char query[DRMSSITE_MAXQUERYLEN];
  char *schema, *series;

  short site_code = -1;
  *info = NULL;
					  /*  parse SUNUM to obtain Site ID  */
  site_code = (sunum & 0xffff000000000000) >> 48;
  if (site_code < 0) return site_code;
			    /*  check for existence of DRMS site info table  */
						 /*  a: check for default 0  */
    schema = strdup (DEFAULT_SITE_TABLE_0);
    series = strchr (schema, '.');
    *series = '\0';
    series++;
    sprintf (query,
	"select * from pg_catalog.pg_tables where schemaname = \'%s\' and tablename = \'%s\'",
	schema, series);
#ifdef DEFS_CLIENT
    text_res = db_client_query_txt (sockfd, query, 0);
#else
    text_res = db_query_txt (dbin, query);
#endif
    if (!text_res->num_rows) {
				   /*  b: if necessary, check for default 1  */
      free (schema);
      schema = strdup (DEFAULT_SITE_TABLE_1);
      series = strchr (schema, '.');
      *series = '\0';
      series++;
      sprintf (query,
	  "select * from pg_catalog.pg_tables where schemaname = \'%s\' and tablename = \'%s\'",
	  schema, series);

      if (text_res) {
         db_free_text_result(text_res);
         text_res = NULL;
      }
#ifdef DEFS_CLIENT
      text_res = db_client_query_txt (sockfd, query, 0);
#else
      text_res = db_query_txt (dbin, query);
#endif
      if (!text_res->num_rows) {
	fprintf (stderr, "Warning: neither default table %s nor %s exists\n",
	    DEFAULT_SITE_TABLE_0, DEFAULT_SITE_TABLE_1);
	return site_code;
      }
    }

  sprintf (query, "select * from %s.%s where SiteCode = %d order by recnum",
      schema, series, site_code);

  if (text_res) {
     db_free_text_result(text_res);
     text_res = NULL;
  }
#ifdef DEFS_CLIENT
      text_res = db_client_query_txt (sockfd, query, 0);
#else
  text_res = db_query_txt (dbin, query);
#endif
  current = text_res->num_rows - 1;
  if (current < 0) return site_code;

  *info = (DRMSSiteInfo_t *)malloc (sizeof (struct DRMSSiteInfo_struct));
  memset(*info, 0, sizeof (struct DRMSSiteInfo_struct));
  (*info)->site_code = site_code;
  
  for (col = 0; col < text_res->num_cols; col++) {
    if (!strcasecmp (text_res->column_name[col], "SiteName"))
      (*info)->site_name = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "Location"))
      (*info)->location = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "Contact"))
      (*info)->contact = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "Notify"))
      (*info)->notify = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "DataURL"))
      (*info)->data_URL = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "QueryURL"))
      (*info)->query_URL = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "ReqstURL"))
      (*info)->request_URL = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "SUMS_URL"))
      (*info)->SUMS_URL = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "Owner"))
      (*info)->owner = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "DBName"))
      (*info)->db_name = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "DBServer"))
      (*info)->db_server = strdup (text_res->field[current][col]);
    if (!strcasecmp (text_res->column_name[col], "NameList"))
      (*info)->namelist = strdup (text_res->field[current][col]);
  }

  if (text_res) {
     db_free_text_result(text_res);
     text_res = NULL;
  }

  free (schema);

  return site_code;
}

#ifdef DEFS_CLIENT
short drmssite_client_getlocalinfo(int sockfd, DRMSSiteInfo_t **info) {
   return drmssite_client_info_from_sunum(DRMS_LOCAL_SITE_CODE, sockfd, info);
}
#else
short drmssite_server_getlocalinfo(DB_Handle_t *dbin, DRMSSiteInfo_t **info) {
   return drmssite_server_info_from_sunum(DRMS_LOCAL_SITE_CODE, dbin, info);
}
#endif

int drmssite_sunum_is_local (unsigned long long sunum) {
  return (drmssite_code_from_sunum (sunum) == DRMS_LOCAL_SITE_CODE);
}

void drmssite_freeinfo(DRMSSiteInfo_t **info) {
   if (info && *info) {
      DRMSSiteInfo_t *iinfo = *info;

      /* free all the fields */
      if (iinfo->site_name) free(iinfo->site_name);
      if (iinfo->location) free(iinfo->location);
      if (iinfo->contact) free(iinfo->contact);
      if (iinfo->notify) free(iinfo->notify);
      if (iinfo->data_URL) free(iinfo->data_URL);
      if (iinfo->query_URL) free(iinfo->query_URL);
      if (iinfo->request_URL) free(iinfo->request_URL);
      if (iinfo->SUMS_URL) free(iinfo->SUMS_URL);
      if (iinfo->owner) free(iinfo->owner);
      if (iinfo->db_name) free(iinfo->db_name);
      if (iinfo->db_server) free(iinfo->db_server);
      if (iinfo->namelist) free(iinfo->namelist);

      free(*info);
      *info = NULL;
   }
}

#ifndef DEFS_CLIENT
int drmssite_server_siteinfo(int sockfd, DB_Handle_t *db_handle)
{
   /* Tnis communicates with the client drmssite_info_from_sunum(). It simply
    * accepts the three db_client_query_txt() calls over the socket,
    * and then returns the three query results back to drmssite_info_from_sunum().
    */
   int err = 0;

   err = db_server_query_txt(sockfd, db_handle);
   err |= db_server_query_txt(sockfd, db_handle);
   err |= db_server_query_txt(sockfd, db_handle);

   return err;
}

int drmssite_server_localsiteinfo(int sockfd, DB_Handle_t *db_handle)
{
   return drmssite_server_siteinfo(sockfd, db_handle);
}
#endif

/*
 *  Revision History
 *
 *  09.01.22    Rick Bogart     created file
 *
 */
