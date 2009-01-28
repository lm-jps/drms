/*
 *  drmssite_info.h - declarations for drmssite_info_from_sunum
 *
 *  Author: Rick Bogart					RBogart@spd.aas.org
 *
 *  Revision history is at the end of the file.
 *
 */

#include "db.h"

typedef struct DRMSSiteInfo_struct {
  unsigned short site_code;
	  /*  The unique 15-bit number assigned to a DRMS/SUMS installation  */
  char *site_name;
	       /*  A short site name string, typically an acronym like JSOC  */
  char *location;
				  /*  A longer descriptive site name string  */
  char *contact;
					/*  The name of a contact person;
			 someone who knows whom to call about what problems  */
  char *notify;
	/*  An email address for automated notifications of problems/status  */
  char *data_URL;
		/*  A base URL at which SUMS data can be publicly accessed,
					      e.g. by http or anonymous ftp  */
  char *query_URL;
		       /*  A public URL at which query scripts are executed  */
  char *request_URL;
	  /*  A public URL at which SUMS data export requests are executed  */
  char *SUMS_URL;
   /*  A pseudo-URL for the service of remote SUMS requests from DRMS sites;
    should include the protocol, server username, hostname, and port number,
			      e.g. scp://jsoc_export@j0.stanford.edu/:55000  */
  char *owner;
	/*  The username of the owner of SUMS processes generating remote
			   SUMS requests; the SUMS manager, e.g. production  */
  char *db_name;
			      /*  The site DRMS database name: JSOC_DBNAME   */
  char *db_server;
			     /*  The site DRMS server hostname: JSOC_DBHOST  */
  char *namelist;
	/*  A comma-separated list of namespaces and/or namespace prefixes
						       reserved by the site  */
} DRMSSiteInfo_t;

#define DEFAULT_SITE_TABLE_0	("jsoc.drms_sites")
#define DEFAULT_SITE_TABLE_1	("drms.sites")
#define DRMSSITE_MAXQUERYLEN    8192

extern short drmssite_code_from_sunum (unsigned long long sunum);

#ifdef DEFS_CLIENT
extern short drmssite_client_info_from_sunum (unsigned long long sunum,
    int sockfd, DRMSSiteInfo_t **info);

extern short drmssite_client_getlocalinfo(int sockfd, DRMSSiteInfo_t **info);
#else
extern short drmssite_server_info_from_sunum (unsigned long long sunum,
    DB_Handle_t *dbin, DRMSSiteInfo_t **info);

extern short drmssite_server_getlocalinfo(DB_Handle_t *dbin, DRMSSiteInfo_t **info);
#endif

extern int drmssite_sunum_is_local (unsigned long long sunum);

extern void drmssite_freeinfo(DRMSSiteInfo_t **info);

#ifndef DEFS_CLIENT
extern int drmssite_server_siteinfo(int sockfd, DB_Handle_t *db_handle);

extern int drmssite_server_localsiteinfo(int sockfd, DB_Handle_t *db_handle);
#endif
/*
 *  Revision History
 *
 *  09.01.24    Rick Bogart     created file
 *
 */
