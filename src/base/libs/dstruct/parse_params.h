#ifndef PARSE_H_PARAMS_DEF
#define PARSE_H_PARAMS_DEF

#define LINEMAX 256

int XXX_params_parse_arguments(int argc, char *argv[]);
void XXX_params_free(void);
void XXX_params_print(void);
void XXX_params_stat(void);
int XXX_params_isdef(const char *name);
int XXX_params_get_int( const char *name);
float XXX_params_get_float( const char *name);
float XXX_params_get_double( const char *name);
const char *XXX_params_get_string( const char *name);
char XXX_params_get_char( const char *name);

#endif
