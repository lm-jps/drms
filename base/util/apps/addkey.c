/*
 *  drms_addkey.c						~rick/src/drms/
 *
 *  Modify a DRMS series by adding a column to its table, and the appropriate
 *    row to the {namespace}.drms_keyword table
 *
 *  Usage:
 *    drms_addkey [-cv] ds= name= type= default=
 *
 *  Notes:
 *    The persegment column in {ns}.drms_keywordname appears to increment the
 *	high-order two bytes in the order in which keywords were inserted, so
 *	should
 *	select max(persegment) from {ns}.drms_keyword where seriesname ilike
 *	  '{ds}', and with ffff0000, and increment by 65536 to get next, which
 *	should then be or'd with kKeywordFlag_PerSegment (1) etc, (see
 *	drms_types.h
 *
 *  Bugs:
 *    Fails to check for ownership of table
 *    Refuses to add a column that differs only case-sensitively from an
 *      existing column
 *    The drms_keyword columns unit and description are only set if non-default
 *	values are supplied as arguments (except that a default value of "UT"
 *	is supplied for keys of type time; otherwise they receive their (SQL)
 *	default value of NULL
 *    If a keyword is a link, the name of the link must be provided, even if
 *      there is only one in the series
 *    The drms_keyword column persegment is never set, receiving its default
 *	value (NULL).
 *    There is no way to reorder the keys for reporting; there should be
 *	an optional "after" argument
 *
 *  Revision history is at end of file.
 */

char *module_name = "drms_addkey";
char *version_id = "1.0";

#include <jsoc_main.h>

ModuleArgs_t module_args[] = {
  {ARG_STRING,	"ds",	"", "name of series to be modified"}, 
  {ARG_STRING,	"key",	"", "key name of column to be added"}, 
  {ARG_NUME,	"type",	"", "data type for column to be added",
      "string, short, int, longlong, float, double, time, link"}, 
  {ARG_STRING,	"format", "Not Specified", "format for key value report"}, 
  {ARG_STRING,	"unit", "Not Specified", "units for key value"}, 
  {ARG_STRING,	"default", "Not Specified", "default value"}, 
  {ARG_STRING,	"desc", "Not Specified", "key description"}, 
  {ARG_STRING,	"link", "Not Specified", "link target"}, 
  {ARG_STRING,	"linkey", "Not Specified",
      "link target keyword (default: key)"}, 
  {ARG_FLAG,	"c",	"", "key is constant"}, 
  {ARG_FLAG,	"n",	"", "do not commit, just print SQL commands"}, 
  {ARG_FLAG,	"v",	"", "run verbose"}, 
  {ARG_END}
};

int DoIt (void) {
  CmdParams_t *params = &cmdparams;
  DB_Text_Result_t *qres;
  unsigned int seqnum;
  int rowct;
  char *ns;
  char *sqltyp[] = {"TEXT", "SMALLINT", "INTEGER", "BIGINT", "REAL",
      "DOUBLE PRECISION", "DOUBLE PRECISION", "*link*"};
  char *drmstype[] = {"string", "short", "int", "longlong", "float",
      "double", "time", "int"};
  char *def_fmt[] = {"%s", "%d", "%d", "%lld", "%g", "%lg", "0", "0"};
  char *def_val[] = {"", "-32768", "-2147483648", "-18446744073709551616LL",
      "NaN", "NaN", "NaN", ""};
  char cmd[DRMS_MAXQUERYLEN];
  char warning[128], module_ident[64], defstr[32];

  char *ds = strdup (params_get_str (params, "ds"));
  char *key = strdup (params_get_str (params, "key"));
  char *fmt = strdup (params_get_str (params, "format"));
  char *unit = strdup (params_get_str (params, "unit"));
  char *desc = strdup (params_get_str (params, "desc"));
  char *defval = strdup (params_get_str (params, "default"));
  char *link = strdup (params_get_str (params, "link"));
  char *linkey = strdup (params_get_str (params, "linkey"));
  int typval = params_get_int (params, "type");
  int fmt_provided = strcmp (fmt, "Not Specified");
  int unit_provided = strcmp (unit, "Not Specified");
  int desc_provided = strcmp (desc, "Not Specified");
  int def_provided = strcmp (defval, "Not Specified");
  int link_provided = strcmp (link, "Not Specified");
  int linkey_provided = strcmp (linkey, "Not Specified");
  int isconstant = params_isflagset (params, "c") ? 1 : 0;
  int commit = params_isflagset (params, "n") ? 0 : 1;
  int verbose = params_isflagset (params, "v");
  int islink = (typval == 7);

  snprintf (module_ident, 64, "%s v %s", module_name, version_id);
  if (verbose) printf ("%s:\n", module_ident);
				    /*  get name space from data series name  */
  if (strchr (ds, '.')) {
    ns = strtok (ds, ".");
    ds = strdup (params_get_str (params, "ds"));
  } else {
    fprintf (stderr, "Error: %s is not a valid series name: no namespace\n", ds);
    return 1;
  }
	      /*  make sure series to be altered is not in slony replication  */
  if (drms_series_isreplicated (drms_env, ds)) {
    fprintf (stderr, "Series %s is in slony replication;\n", ds);
    fprintf (stderr, "       its structure must not be altered\n");
    return 1;
  }
			      /*  check for prior existence of key in series  */
  sprintf (cmd, "SELECT keywordname FROM %s.drms_keyword ", ns);
  sprintf (cmd, "%s WHERE seriesname ILIKE \'%s\' ", cmd, ds);
  sprintf (cmd, "%s AND keywordname ILIKE \'%s\'", cmd, key);
  if ((qres = drms_query_txt (drms_env->session, cmd)) == NULL) {
    fprintf (stderr, "Error: can\'t connect to DRMS database\n");
    return 1;
  }
  if (qres->num_rows) {
    printf ("series %s already contains keyword %s\n", ds, qres->field[0][0]);
    return 0;
  }
		     /*  check for highest sequence number of keys in series  */
  sprintf (cmd, "SELECT max(persegment) FROM %s.drms_keyword ", ns);
  sprintf (cmd, "%s WHERE seriesname ILIKE \'%s\' ", cmd, ds);
  if ((qres = drms_query_txt (drms_env->session, cmd)) == NULL) {
    fprintf (stderr,
	"Error: could not get maximum sequence number from %s.drms_keyword\n",
	ns);
    return 1;
  }
  seqnum = atoi (qres->field[0][0]);
						/*  increment upper halfword  */
  seqnum += 0x10000;
						/*  and out per-segment info  */
  seqnum &= 0xffff0000;
				   /*  check for optional/required arguments  */
  if (!fmt_provided && !islink) {
    fprintf (stderr, "Warning: format unspecified; using %s\n", def_fmt[typval]);
    fprintf (stderr, "         alter by updating %s.drms_keyword later\n", ns);
    fmt = def_fmt[typval];
    fmt_provided = 1;
  }
  if (!unit_provided && !islink) {
    fprintf (stderr, "Warning: unit unspecified;");
    if (typval == 6) {
      fprintf (stderr, " using UT;");
      unit = strdup ("UT");
      unit_provided = 1;
    }
    fprintf (stderr, "\n");
    fprintf (stderr, "         alter by updating %s.drms_keyword later\n", ns);
  }
  if (!def_provided && !islink) {
    if (isconstant) {
      fprintf (stderr,
	  "Error: a default value  must be specified for a constant key\n");
      return 1;
    }
    if (typval == 6) {
      sprint_time (defstr, DRMS_MISSING_DOUBLE, unit, atoi (fmt));
      sprintf (warning, "Warning: using default value of %s\n", defstr);
    } else {
      fprintf (stderr, "Warning: default unspecified; using default value of \'%s\'\n", def_val[typval]);
      sprintf (defstr, "%s", def_val[typval]);
    }
    fprintf (stderr, "         alter by updating %s.drms_keyword later\n", ns);
    defval = defstr;
  }
  if (islink) {
    if (isconstant) {
      fprintf (stderr, "Error: a link keyword cannot be constant\n");
      return 1;
    }
	/*  should probably look in ns.drms_link to check wether link target
	 is valid, and for default if none provided and only one available  */
    if (!link_provided) {
      fprintf (stderr, "Error: a link must be provided for a linked keyword\n");
      return 1;
    }
    if (!linkey_provided) {
      fprintf (stderr,
	  "Warning: link key name unspecified; using default value of %s\n",
	  key);
      linkey = strdup (key);
    }
				/*  ignored anyway for links so why bother  */
    unit_provided = 0;
    fmt_provided = 0;
  }

  if (verbose) {
    if (isconstant) {
      printf ("adding constant key: %s (%s) = \n", key, sqltyp[typval]);
    } else printf ("adding key: %s (%s)\n", key, sqltyp[typval]);
  }
  

  sprintf (cmd, "INSERT INTO %s.drms_keyword", ns);
  sprintf (cmd, "%s (seriesname, keywordname, type, islink, isconstant, persegment",
	cmd);
  if (islink) sprintf (cmd, "%s, linkname, targetkeyw", cmd);
  else sprintf (cmd, "%s, defaultval", cmd);
  if (unit_provided) sprintf (cmd, "%s, unit", cmd);
  if (fmt_provided) sprintf (cmd, "%s, format", cmd);
  if (desc_provided) sprintf (cmd, "%s, description", cmd);
  else fprintf (stderr,
      "Warning: description not specified; update %s.drms_keyword later\n", ns);
  sprintf (cmd,
      "%s) VALUES (\'%s\',  \'%s\', \'%s\', %d, %d, %u",
      cmd, ds, key, drmstype[typval], islink, isconstant, seqnum);
  if (islink) sprintf (cmd, "%s, \'%s\', \'%s\'", cmd, link, linkey);
  else sprintf (cmd, "%s, \'%s\'", cmd, defval);
  if (unit_provided) sprintf (cmd, "%s, \'%s\'", cmd, unit); 
  if (fmt_provided) sprintf (cmd, "%s, \'%s\'", cmd, fmt); 
  if (desc_provided) sprintf (cmd, "%s, \'%s\'", cmd, desc);
  sprintf (cmd, "%s)", cmd);
  if (verbose || !commit) printf ("%s\n", cmd);

  if (commit) {
    if (drms_dms (drms_env->session, &rowct, cmd)) {
      fprintf (stderr, "Error: unable to execute SQL command\n       %s\n", cmd);
      return 1;
    }
    if (verbose) printf ("%d row(s) added to %s.drms_keyword\n", rowct, ns);
  }

  if (isconstant) return 0;

  if (islink) return 0;
  sprintf (cmd, "ALTER TABLE %s ADD COLUMN %s %s", ds, key, sqltyp[typval]);
  if (verbose || !commit) printf ("%s\n", cmd);
  

  if (commit) {
    if (drms_dms (drms_env->session, &rowct, cmd)) {
      fprintf (stderr, "Error: unable to execute SQL command\n       %s\n", cmd);
      return 1;
    }
    if (verbose) printf ("column %s added to %s; %d rows updated\n", key, ds, rowct);
  }

  return 0;
}

/*
 *  Revision History
 *
 *	11.03.11	file created by R Bogart
 *  v 0.9 frozen 2011.06.02
 *	12.03.16	added proper setting of the linkname, targetkeyw, and
 *		persegment columns in drms_keyword table; added optional link
 *		and linkey arguments; added option to not commit
 *	12.05.10	fixed verbose reporting of defval value being set;
 *		SQL commands automatically printed when not committing
 *  v 1.0 frozen 2012.05.10
 *
 */
