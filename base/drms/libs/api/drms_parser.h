#ifndef __DRMS_PARSER_H
#define __DRMS_PARSER_H

DRMS_Record_t *drms_parse_description(DRMS_Env_t *env, char *desc);
int parse_keywords(char *desc, DRMS_Record_t *template);

#endif
