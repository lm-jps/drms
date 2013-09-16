#ifndef __ATOINC_H_
#define __ATOINC_H_

extern TIME atoinc (char *str);
TIME atoinc2(char *str);
extern int fprint_inc (FILE *fp, TIME inc);
extern int print_inc (TIME inc);
extern char *sprint_inc (char *str, TIME inc);
#ifdef NEVER
extern char *getinc (TIME a, TIME b, double *dt);
#endif

#endif
