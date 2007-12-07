#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"

static char *prot_string[] = {
  "bin",       "bin.gz",    "fits",   "fitz",     "msi",    "tas",
  "generic",   "dsds",      "local"};
static int prot_type[] = {
  DRMS_BINARY, DRMS_BINZIP, DRMS_FITS, DRMS_FITZ, DRMS_MSI, DRMS_TAS,
  DRMS_GENERIC, DRMS_DSDS, DRMS_LOCAL};

/*  N.B.: If you add types to the above lists you must also add them to
			the enum list of DRMS_Protocol_t in drms_protocol.h  */

DRMS_Protocol_t drms_str2prot (const char *str) {
  int n, prot_count = sizeof prot_type / sizeof (int);
  for (n = 0; n < prot_count; n++)
    if (!strcasecmp (str, prot_string[n])) return (DRMS_Protocol_t)prot_type[n];
  fprintf (stderr, "Unknown DRMS protocol \"%s\"\n", str);
  XASSERT(0);
  return (DRMS_Protocol_t) -1;
}


const char *drms_prot2str (DRMS_Protocol_t prot) {
  int n, prot_count = sizeof prot_type / sizeof (int);
  for (n = 0; n < prot_count; n++)
    if (prot == prot_type[n]) return prot_string[n];
  fprintf (stderr,"Unknown DRMS protocol %d\n", prot);
  XASSERT(0);
  return NULL;
}
