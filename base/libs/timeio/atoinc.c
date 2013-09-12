/* atoinc - converts ascii time increment to number of seconds */

#include <stdio.h>
/*#include <soi.h>*/
#include <soi_str.h>
#include <stdlib.h>

typedef struct
{
  char *nm;
  int code;
} nametable;

nametable tmunit[] = {
  {"hz",	8},
  {"hertz",	8},
  {"rot",	7},
  {"rots",	7},
  {"rotation",	7},
  {"rotations",	7},
  {"deg",	6},
  {"degs",	6},
  {"degree",	6},
  {"degrees",	6},
  {"wk",	5},
  {"wks",	5},
  {"week",	5},
  {"weeks",	5},
  {"w",		5},
  {"d",		4},
  {"day",	4},
  {"days",	4},
  {"h",		3},
  {"hr",	3},
  {"hrs",	3},
  {"hour",	3},
  {"hours",	3},
  {"m",		2},
  {"min",	2},
  {"mins",	2},
  {"minute",	2},
  {"minutes",	2},
  {"secs",	1},
  {"second",	1},
  {"seconds",	1},
  {"s",		1},
  {"",		0},
  {0,		0},
};

long secs[] = {
  1,
  5,
  10,
  15,
  30,
  1*60,
  5*60,
  6*60,
  10*60,
  12*60,
  15*60,
  20*60,
  30*60,
  1*3600,
  2*3600,
  3*3600,
  4*3600,
  6*3600,
  8*3600,
  12*3600,
};

static int lookup(char *n, nametable *t)
{
  while (Strcmp(n,t->nm) && t->code) ++t;
  return(t->code);
}

TIME atoinc(char *str)
{
    double num, mult;
    char *units, *base_units;
    TIME rv;
    
    if (!str) return 0;
    num = strtod (str, &units);
    if (str == units) num = 1;  /* only the units specified, assume 1 */
    if (*units == '_') units++; /* to be backwards compatible with form like 1_minutes */
    
    base_units = mprefix(units, &mult);    /* units may have prefix like giga */
    if (*base_units == '_') base_units++;  /* to handle form like giga_hertz */
    
    switch(lookup(base_units,tmunit))
    {
        case 1: /* seconds	*/
            rv = (mult*num);
            break;
        case 2: /* minutes	*/
            rv = (mult*num*60.0);
            break;
        case 3: /* hours	*/
            rv = (mult*num*3600.0);
            break;
        case 4: /* days		*/
            rv = (mult*num*86400);
            break;
        case 5: /* weeks	*/
            rv = (num*604800);
            break;
        case 6: /* carrington degrees	*/
            rv = (num);
            break;
        case 7: /* carrington rotations	*/
            rv = (num*360);
            break;
        case 8:	/* hertz		*/
            rv = (mult*num);
            break;
        default:
            rv = (0);
            break;
    }
    
    return rv;
}

#define integral(x) ((x)==(long)(x))

char *sprint_inc(char *str, TIME inc)
{
  double ii;

  if (integral(ii = inc/31556952)) sprintf(str,"%.0fyear",ii);
  else if (integral(ii = inc/604800)) sprintf(str,"%.0fweek",ii);
  else if (integral(ii = inc/86400)) sprintf(str,"%.0fday",ii);
  else if (integral(ii = inc/3600)) sprintf(str,"%.0fhour",ii);
  else if (integral(ii = inc/60)) sprintf(str,"%.0fminute",ii);
  else sprintf(str,"%.fsecond",ii=inc);
  if (ii>1 || ii<-1) strcat(str,"s");
  return(str);
}

int fprint_inc(FILE *fp, TIME inc)
{
  char str[64];
  return(fprintf(fp,"%s",sprint_inc(str,inc)));
}

int print_inc(TIME inc)
{
  return(fprint_inc(stdout,inc));
}
