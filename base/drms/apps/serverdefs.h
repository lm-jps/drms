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

#define POSTGRES_ADMIN	"postgres"
#define SUMS_MANAGER	"production"
#define SUMS_GROUP	"SOI"
#define SUMLOG_BASEDIR	"/usr/local/logs/SUM"
#define SUMBIN_BASEDIR	"/usr/local/bin"
#define SUMSERVER	"j1.Stanford.EDU"

#define SUMS_TAPE_AVAILABLE (1)

#endif /* _LOCALIZED_DEFS */

#endif /* __SERVERDEFS_H */
