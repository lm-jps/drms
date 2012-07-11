/**
\file serverdefs.h
\brief Define default values for dbhost, dbname, user, password, etc.
*/

#ifndef __SERVERDEFS_H
#define __SERVERDEFS_H

#ifdef __LOCALIZED_DEFS__
#include "localization.h"
#else
/* Stanford defs */
/** \brief default dbhost */
#define SERVER "hmidb"
/** \brief local postgres admin */
#define DRMS_LOCAL_SITE_CODE 0x0000
/** \brief default user name */
#define USER NULL
/** \brief default passwd */
#define PASSWD NULL
/** \brief default dbname */
#define DBNAME "jsoc"

/** \brief default port on SERVER */
#define DRMSPGPORT      "5432"

#define POSTGRES_ADMIN	"postgres"
#define SUMS_MANAGER	"production"
#define SUMS_MANAGER_UID "388"
#define SUMS_GROUP	"SOI"
#define SUMLOG_BASEDIR	"/usr/local/logs/SUM"
#define SUMBIN_BASEDIR	"/usr/local/bin"
#define SUMSERVER	"j1"
/* Number of sum process sets to spawn (for non 1). # = (NUMSUM*5)+2 */
#define SUM_NUMSUM		3

/* Don't exceed this maximum SUM_NUMSUM (and don't change this max!) */
#define SUM_MAXNUMSUM	8

#define SUMS_TAPE_AVAILABLE (1)

#define SUMS_MULTIPLE_PARTNSETS (1)

#define SUMS_DEBUG 1

/* Do NOT create a NetDRMS configuration parameter for DSDS support. NetDRMS 
 * does not support DSDS, ever. */
#define DSDS_SUPPORT (1)

/* Host:Port containing database () that has a table listing the production db users. */
#define PRODUSER_DBHOST    "hmidb:5432"
#define PRODUSER_DBNAME    "jsoc"
#define PRODUSER_PRODTAB   "su_production.produsers"
#define PRODUSER_COLUSER   "username"

#endif /* _LOCALIZED_DEFS */

/* Override one or more defaults (but not all of them, unlike in the case of localization.h), 
 * if user has requested this in config.local */
#ifdef __CUSTOMIZED_DEFS__
#include "customizeddefs.h"
#endif /* __CUSTOMIZED_DEFS__ */

#endif /* __SERVERDEFS_H */
