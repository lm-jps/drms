#ifndef __DRMS_PARSER_H
#define __DRMS_PARSER_H

DRMS_Record_t *drms_parse_description(DRMS_Env_t *env, char *desc);
int parse_keywords(char *desc, DRMS_Record_t *template);

/* print jsd from existing series */
void drms_jsd_printfromrec(DRMS_Record_t *rec);
void drms_jsd_print(DRMS_Env_t *drms_env, const char *seriesname);

/* print a query that will return the given record */
void drms_print_rec_query(DRMS_Record_t *rec);
void drms_fprint_rec_query(FILE *fp, DRMS_Record_t *rec);

#endif
