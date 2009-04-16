/**
@file drms_series_priv.h
*/

#ifndef _DRMS_SERIES_PRIV_H
#define _DRMS_SERIES_PRIV_H

/** \brief insert a DRMS series 

Create the database table, sequence, and entries in DRMS master tables
for a new series. The series is created from the information in the
series template record given in the argument \a template.
\param sesssion handle for database connnection
\param update unused
\param template specifies a new series
\param perm unused

\return 1 if success, 0 otherwise.
 */
int drms_insert_series(DRMS_Session_t *session, int update, DRMS_Record_t *template, int perms);


#endif
