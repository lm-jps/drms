#include <stdlib.h>
#include <stdio.h>
#include <string.h>

main (int argc, char **argv) {
  unsigned long long lval, site_min, site_max;
  short seed;
  char *endptr;

  if (argc < 2) {
    fprintf (stderr, "Error: no site code initialization provided\n");
    fprintf (stderr,
	"  sequence SUM_DS_INDEX_SEQ must be created before SUMS is started\n");
    return 1;
  }
  lval = (unsigned long long)strtoull (argv[1], &endptr, 0);
  if (strlen (endptr) | lval > 32767) {
    fprintf (stderr, "Error: invalid site code provided %s for initialization\n",
	argv[1]);
    fprintf (stderr,
	"  sequence SUM_DS_INDEX_SEQ must be created before SUMS is started\n");
    return 1;
  }
  site_min = lval << 48;
  site_max = site_min + ((long long)1 << 48) - 1;
/*
  printf ("drop sequence SUM_DS_INDEX_SEQ;\n");
*/
  printf ("create sequence SUM_DS_INDEX_SEQ\n");
  printf ("  increment 1\n");
  printf ("  start %ld\n", site_min);
  printf ("  minvalue %ld\n", site_min);
  printf ("  maxvalue %ld\n", site_max);
  printf ("  no cycle\n");
  printf ("  cache 10;\n");
  return 0;
}
