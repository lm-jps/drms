/**
\file drms_parser.h
*/
#ifndef __DRMS_PARSER_H
#define __DRMS_PARSER_H

#include "hcontainer.h"

/** \brief Parse jsd and fill in a record template */
DRMS_Record_t *drms_parse_description(DRMS_Env_t *env, char *desc);
/** \brief Parse keywords in jsd and fill in a record template*/
int parse_keywords(char *desc, DRMS_Record_t *template, HContainer_t *cparmkeys, int *keynum);
HContainer_t *drms_parse_keyworddesc(DRMS_Env_t *env, const char *desc, int *status);

/**\brief Print jsd from record */
void drms_jsd_printfromrec(DRMS_Record_t *rec);
/**\brief Print jsd of an existing series */
void drms_jsd_print(DRMS_Env_t *drms_env, const char *seriesname);

/**\brief Same as drms_fprint_rec_query() print to string */
void drms_sprint_rec_query(char *querystring, DRMS_Record_t *rec);
int drms_snprint_rec_spec(char *spec, size_t size, DRMS_Record_t *rec);
/**\brief Same as drms_fprint_rec_query() print to stdout */
void drms_print_rec_query(DRMS_Record_t *rec);
/**\brief Print to \a fp a query that will return the given record */
void drms_fprint_rec_query(FILE *fp, DRMS_Record_t *rec);

#endif
