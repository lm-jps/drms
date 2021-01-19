/******************************************************************************/
/*
 *  drms_update						/home/rick/src/drms
 *
 *  Module description
 *    Updating selected records in a DRMS series by modifying the selected
 *	keyword values
 *
 *  Responsible: Rick Bogart				rick@sun.stanford.edu
 *
 *  Usage:
 *    drms_update [-nhv] ds=... key=... val= ... [log=...]
 *
 *  Arguments: (type	default	description)
 *      ds	str	-	Data series or record set to be modified
 *	key	str	-	Name of keyword to be modified
 *	val	str	-	New value to set for key
 *	log	str	HISTORY	Name of keyword to which change history will be
 *		logged (to avoid logging, enter blank or set -h flag)
 *
 *  Flags
 *	-h	turn off logging of change history; same as blank value for
 *		log
 *	-n	do not do updates, only print SQL commands
 *	-v	run verbose
 *
 *  Possible Improvements:
 *    Option for an existing value to be universally updated
 *
 *  Bugs:
 *    Only keys of type string, short, int, longlong, float,double, and time
 *	are supported; these are the only types used to date in the aia and
 *	hmi namespaces - sha also uses char, but possibly incorrectly
 *    Floating point and time value comparisons are performed as strings of
 *	the values printed with the default format for the key
 *    Unclear what happens if the key to be updated is also the log key;
 *	should be checked
 *    Numeric values may be specified in hexadecimal (0x...) or decimal
 *	format, but not octal
 *    No updates are attempted on slotted keys, although they could be by
 *	also updating the values of the corresponding index keys
 *    No updates are attempted for linked or constant keys, and no further
 *	instructions are given about what to do about them
 *    There is no check for select privilege on the series; in the unlikely
 *	event that the user does not have select privilege on the table,
 *	the module is likely to abort on the zero-population test, but
 *	this has not been tested; in any case it would fail because of the
 *	inability to compare existing key values with proposed updates, or
 *	for that matter to be able to open the dataset.
 *
 *  Revision history is at the end of the file
 */
/******************************************************************************/

#include <jsoc_main.h>

char *module_name = "drms_update";
char *version_id = "0.8";

#define ARG_PRINT_SQL_ONLY "s"

ModuleArgs_t module_args[] = {
  {ARG_DATASET, "ds", "", "output data series or dataset"},
  {ARG_STRING,	"key", "", "name of key whose values are to be set"},
  {ARG_STRING,	"val", "", "value to set for key"},
  {ARG_STRING,	"log", "HISTORY",
  "name of key to which change history will be appended\n    (To avoid logging, enter empty string)"},
  {ARG_FLAG,	  "h", "", "don\'t update records"},
						    /*  fix this explanation  */
  {ARG_FLAG,	  "n", "", "don\'t update records"},
  {ARG_FLAG,     ARG_PRINT_SQL_ONLY, NULL, "print SQL commands, but do not run them - implies records will not be updated"},
  {ARG_FLAG,	  "v",	"", "verbose mode"},
  {}
};

/******************************************************************************/
			/*  module body begins here  */
/******************************************************************************/
int DoIt (void) {
  CmdParams_t *params = &cmdparams;
  DB_Text_Result_t *qres;
  DRMS_RecordSet_t *upds;
  DRMS_Record_t *rec, *tmprec;
  DRMS_Keyword_t *keystruc;
  DRMS_Type_t keytype;
  TIME ntval, otval;
  double ndval, odval;
  float nfval, ofval;
  long int oival, nival;
  int base, chglog, rowct, status;
  int recct, rct;
  char *dbuser, *ns, *series;
  char *endptr;
  char cmd[DRMS_MAXQUERYLEN], ccmd[DRMS_MAXQUERYLEN];
  char recname[DRMS_MAXQUERYLEN];
  char *history_value = NULL;
  size_t history_value_sz = 256;
  char updrec[DRMS_MAXQUERYLEN];
  char nstrval[DRMS_MAXQUERYLEN], ostrval[DRMS_MAXQUERYLEN];
  char keyfmt[DRMS_MAXFORMATLEN], rfmt[DRMS_MAXFORMATLEN];
  char keyunit[DRMS_MAXUNITLEN];
  char module_ident[64], tbuf[64];

  char *ds = strdup (params_get_str (params, "ds"));
  char *key = strdup (params_get_str (params, "key"));
  char *val = strdup (params_get_str (params, "val"));
  char *logkey = strdup (params_get_str (params, "log"));
  int nolog = params_isflagset (params, "h");
  int noupd = params_isflagset (params, "n");
  int print_sql_only = params_isflagset (params, ARG_PRINT_SQL_ONLY);
  int verbose = params_isflagset (params, "v");

  if (print_sql_only)
  {
      noupd = 1;
  }

  snprintf (module_ident, 64, "%s v %s", module_name, version_id);
  if (verbose) printf ("%s:\n", module_ident);
						     /*  get current db role  */
#ifndef DRMS_CLIENT
  dbuser = drms_env->session->db_handle->dbuser;
#else
  drms_send_commandcode (drms_env->session->sockfd, DRMS_GETDBUSER);
  dbuser = receive_string(drms_env->session->sockfd);
#endif
  chglog = (nolog) ? 0 : strlen (logkey);
					   /*  parse dataset name for series  */
  if (strchr (ds, '[')) {
    series = strtok (ds, "[");
    ds = strdup (params_get_str (params, "ds"));
  } else series = ds;
					   /*  check for existence of series  */
  if (!drms_series_exists (drms_env, series, &status)) {
    fprintf (stderr, "Error: data series %s does not exist\n", series);
    return 1;
  }
				     /*  get namespace from data series name  */
  ns = strdup (series);
  ns = strtok (ns, ".");
						    /*  check for population  */

    if (!print_sql_only)
    {
        sprintf (cmd, "SELECT COUNT(*) FROM %s", series);
        if ((qres = drms_query_txt (drms_env->session, cmd)) == NULL) {
          fprintf (stderr, "Error: can\'t connect to DRMS database\n");
          return 1;
        }
        recct = atoi (qres->field[0][0]);
        if (!recct) {
          fprintf (stderr, "Warning: data series %s is unpopulated\n", series);
          return 0;
        }
      						    /*  check for permission  */
        sprintf (cmd, "SELECT has_table_privilege(\'%s\', \'update\')", series);
        if ((qres = drms_query_txt (drms_env->session, cmd)) == NULL) {
          fprintf (stderr, "Error: can\'t connect to DRMS database\n");
          return 1;
        }
        if (strcmp (qres->field[0][0], "t")) {
          fprintf (stderr, "Warning: You do not have update permission on %s\n",
      	series);
          if (!noupd) fprintf (stderr,
      	"         SQL commands will be printed but not executed\n");
          noupd = 1;
        }
    }
		    /*  create a series template record for keyword checking  */
  if (!(tmprec = drms_template_record (drms_env, series, &status))) {
    fprintf (stderr,
	"Error: can't get keyword information for data series %s\n", series);
    return 1;
  }
		       /*  check that key is not link or constant or slotted  */
  if (!(keystruc = drms_keyword_lookup (tmprec, key, 1))) {
    fprintf (stderr, "Error: data series %s doesn\'t contain keyword %s\n",
	series, key);
    return 1;
  }
  if (drms_keyword_isconstant (keystruc)) {
    fprintf (stderr, "Warning: keyword %s is constant\n", key);
    return 0;
  }
  if (drms_keyword_islinked (keystruc)) {
    fprintf (stderr, "Warning: keyword %s is a link\n", key);
    return 0;
  }
  if (drms_keyword_isslotted (keystruc)) {
    fprintf (stderr, "Warning: keyword %s is slotted\n", key);
    return 0;
  }
  strcpy (keyfmt, keystruc->info->format);
  strcpy (keyunit, keystruc->info->unit);
  keytype = keystruc->info->type;
					  /*  check value of proposed update  */
  errno = 0;
  switch (keytype) {
    case DRMS_TYPE_STRING:
			 /*  anything goes - might check for embedded quotes  */
      break;
    case DRMS_TYPE_LONGLONG:
    case DRMS_TYPE_INT:
    case DRMS_TYPE_SHORT:
      base = (strncmp (val, "0x", 2)) ? 10 : 16;
      nival = strtol (val, &endptr, base);
      if (errno || strlen (endptr)) {
	fprintf (stderr, "Error: invalid value \"%s\"\n", val);
	return 1;
      }
      if (keytype == DRMS_TYPE_SHORT &&
	  (nival > 32767 || nival < -32768)) {
	fprintf (stderr, "Error: out-of-range value for update: %lld\n", nival);
	return 1;
      } else if (keytype == DRMS_TYPE_INT &&
	  (nival > 2147483647 || nival < -2147483648)) {
	fprintf (stderr, "Error: out-of-range value for int update: %lld\n", nival);
	return 1;
      }
      break;
    case DRMS_TYPE_FLOAT:
      nfval = strtof (val, &endptr);
      if (errno || strlen (endptr)) {
	fprintf (stderr, "Error: invalid value \"%s\"\n", val);
	return 1;
      }
      sprintf (nstrval, keyfmt, nfval);
      break;
    case DRMS_TYPE_DOUBLE:
      ndval = strtod (val, &endptr);
      if (errno || strlen (endptr)) {
	fprintf (stderr, "Error: invalid value \"%s\"\n", val);
	return 1;
      }
      sprintf (nstrval, keyfmt, ndval);
      break;
    case DRMS_TYPE_TIME:
      ntval = sscan_time (val);
      if (time_is_invalid (ntval)) {
	fprintf (stderr, "Error: invalid value \"%s\"\n", val);
	return 1;
      }
      sprint_time (nstrval, ntval, keyunit, atoi (keyfmt));
      break;
    default:
      fprintf (stderr, "Error: unsupported key type %s for key %s\n",
	  drms_type_names[keytype], key);
      return 1;
  }
		       /*  check that change log key is not link or constant  */
  if (chglog) {
    if (!(keystruc = drms_keyword_lookup (tmprec, logkey, 1))) {
      fprintf (stderr, "Warning: data series %s doesn\'t contain keyword %s\n",
	  series, logkey);
      fprintf (stderr, "         Changes will not be logged to a key\n");
      chglog = 0;
    }
  }
  if (chglog && drms_keyword_isconstant (keystruc)) {
    fprintf (stderr, "Warning: logging keyword %s is constant\n", logkey);
    fprintf (stderr, "         Changes will not be logged to it\n");
    chglog = 0;
  }
  if (chglog && drms_keyword_islinked (keystruc)) {
    fprintf (stderr, "Warning: logging keyword %s is a link\n", logkey);
    fprintf (stderr, "         Changes will not be logged to it\n");
    chglog = 0;
  }
			    /*  Also check that logging key is a string type  */
  if (chglog && (keystruc->info->type != DRMS_TYPE_STRING)) {
    fprintf (stderr, "Warning: logging keyword %s is not of string type\n", logkey);
    fprintf (stderr, "         Changes will not be logged to it\n");
    chglog = 0;
  }
  if (chglog) {
	/*  Also warn if logging key is not a FITS recognized appendable key  */
    if (strcasecmp (logkey, "HISTORY")  && strcasecmp (logkey, "COMMENT"))
      fprintf (stderr,
	  "Warning: logging keyword \'%s\' is not recognized as a FITS Commentary Keyword\n",
	  logkey);
  }
					 /*  now process the dataset records  */
  if (!(upds = drms_open_records (drms_env, ds, &status))) {
    fprintf (stderr, "Error: unable to open data set %s\n", ds);
    return 1;
  }
  if (!(recct = upds->n)) {
    fprintf (stderr, "Warning: No records in data set %s\n", ds);
    fprintf (stderr, "         Nothing to do\n");
    return 0;
  }
  if (verbose) {
    if (noupd) printf ("NOT ");
    printf ("Updating up to %d record(s) in dataset:\n", recct);
    printf ("    %s\n", ds);
  }

    if (keytype == DRMS_TYPE_STRING && chglog)
    {
        /*  logging action to "history" not implemented for this case  */
        fprintf (stderr, "Warning: logging of changes to %s unimplemented for keys of type %s", logkey, drms_type_names[keytype]);
        chglog = 0;
    }

  for (rct = 0; rct < recct; rct++) {
    rec = upds->records[rct];
    drms_sprint_rec_query (recname, rec);
    keystruc = drms_keyword_lookup (rec, key, 0);
    if (!keystruc || status) {
      fprintf (stderr, "Error: unable to get keyword %s\n", key);
      drms_close_records (upds, DRMS_FREE_RECORD);
      return 1;
    }

    if (chglog)
    {
        if (history_value)
        {
            /* free value from previous record iteration */
            free(history_value);
            history_value = NULL;
        }

        history_value = calloc(1, history_value_sz);
        if (!history_value)
        {
            fprintf(stderr, "out of memory\n");
            return 1;
        }
    }

    if (history_value)
    {
        /* initialize history_value with existing history-keyword value, if one exists (an empty string will append nothing to history_value)
         */
        history_value = base_strcatalloc(history_value, drms_getkey_string (rec, logkey, &status), &history_value_sz);
    }

    switch (keytype) {
      case DRMS_TYPE_STRING:
	if (!(strcmp (drms_getkey_string (rec, key, &status), val))) {
	  if (verbose) {
	    sprintf (rfmt, "rec #%%lld unchanged, value already = %s\n",
		keyfmt);
	    printf (rfmt, rec->recnum, val);
	  }
	  continue;
	}
	sprintf (cmd, "UPDATE %s SET %s = \'%s\'", series, key, val);
	if (chglog) {
/*
	  sprint_time (tbuf, CURRENT_SYSTEM_TIME, "Z", -1);
	  if (strlen (history)) strcat (history, "\n");
	  sprintf (rfmt, "%%s : %%s changed from \\\'%s\\\' to \\\'%s\\\'",
	      keyfmt, keyfmt);
printf ("%s\n", rfmt);
	  sprintf (updrec, rfmt, tbuf, key,
	      drms_getkey_string (rec, key, &status), val);
printf ("%s\n", updrec);
printf ("%s : %s changed from \'%s\' to \'%s\'\n", tbuf, key,
drms_getkey_string (rec, key, &status), val);
printf ("\n");
	  strcat (history, updrec);
	  sprintf (ccmd, " SET %s = \'%s\'", logkey, history);
	  strcat (cmd, ccmd);
*/
	}
	break;
      case DRMS_TYPE_LONGLONG:
      case DRMS_TYPE_INT:
      case DRMS_TYPE_SHORT:
	oival = (keytype == DRMS_TYPE_SHORT) ?
	    drms_getkey_short (rec, key, &status) :
	    drms_getkey_int (rec, key, &status);
	if (oival == nival) {
	  if (verbose) {
	    sprintf (rfmt, "rec #%%lld unchanged, value already = %s\n",
		keyfmt);
	    printf (rfmt, rec->recnum, nival);
	  }
	  continue;
	}


	if (chglog) sprintf (cmd, "UPDATE %s SET (%s,%s) = (%lld,",
	    series, key, logkey, nival);
	else sprintf (cmd, "UPDATE %s SET %s = %lld", series, key, nival);
	if (chglog) {
	  sprint_time (tbuf, CURRENT_SYSTEM_TIME, "Z", -1);

    if (*history_value != '\0')
    {
        if (print_sql_only)
        {
            history_value = base_strcatalloc(history_value, "\\n", &history_value_sz);
        }
        else
        {
            history_value = base_strcatalloc(history_value, "\n", &history_value_sz);
        }
    }

	  sprintf (rfmt, "%%s : %%s changed from %s to %s",
	      keyfmt, keyfmt);
	  sprintf (updrec, rfmt, tbuf, key, oival, nival);
    history_value = base_strcatalloc(history_value, updrec, &history_value_sz);
	  sprintf (ccmd, "E\'%s\')", history_value);
	  strcat (cmd, ccmd);
	}
	break;
      case DRMS_TYPE_DOUBLE:
      case DRMS_TYPE_FLOAT:
        if (keytype == DRMS_TYPE_FLOAT) {
	  ofval = drms_getkey_float (rec, key, &status);
	  sprintf (ostrval, keyfmt, ofval);
	} else {
          odval = drms_getkey_float (rec, key, &status);
	  sprintf (ostrval, keyfmt, odval);
	}
	if (!strcmp (ostrval, nstrval)) {
	  if (verbose) {
	    printf ("rec #%lld unchanged, value already = %s\n",
		rec->recnum, nstrval);
	  }
	  continue;
	}
	if (chglog) sprintf (cmd, "UPDATE %s SET (%s,%s) = (%s,",
	    series, key, logkey, nstrval);
	else sprintf (cmd, "UPDATE %s SET %s = %s", series, key, nstrval);
	if (chglog) {
	  sprint_time (tbuf, CURRENT_SYSTEM_TIME, "Z", -1);

    if (*history_value != '\0')
    {
        if (print_sql_only)
        {
            history_value = base_strcatalloc(history_value, "\\n", &history_value_sz);
        }
        else
        {
            history_value = base_strcatalloc(history_value, "\n", &history_value_sz);
        }
    }

	  sprintf (updrec, "%s: %s changed from %s to %s", tbuf, key, ostrval,
	      nstrval);
    history_value = base_strcatalloc(history_value, updrec, &history_value_sz);
	  sprintf (ccmd, "E\'%s\')", history_value);
	  strcat (cmd, ccmd);
	}
	break;
      case DRMS_TYPE_TIME:
        otval = drms_getkey_time (rec, key, &status);
	sprint_time (ostrval, otval, keyunit, atoi (keyfmt));
	if (!strcmp (ostrval, nstrval)) {
	  if (verbose) {
	    printf ("rec #%lld unchanged, value already = %s\n",
		rec->recnum, nstrval);
	  }
	  continue;
	}
	if (chglog) sprintf (cmd, "UPDATE %s SET (%s,%s) = (%23.16e,",
	    series, key, logkey, ntval);
	else sprintf (cmd, "UPDATE %s SET %s = %23.16e", series, key, ntval);
	if (chglog) {
	  sprint_time (tbuf, CURRENT_SYSTEM_TIME, "Z", -1);

    if (*history_value != '\0')
    {
        if (print_sql_only)
        {
            history_value = base_strcatalloc(history_value, "\\n", &history_value_sz);
        }
        else
        {
            history_value = base_strcatalloc(history_value, "\n", &history_value_sz);
        }
    }

	  sprintf (updrec, "%s: %s changed from %s to %s", tbuf, key, ostrval,
	      nstrval);
    history_value = base_strcatalloc(history_value, updrec, &history_value_sz);
	  sprintf (ccmd, "E\'%s\')", history_value);
	  strcat (cmd, ccmd);
	}
	break;
      default:
	fprintf (stderr, "Error: unsupported key type %s for key %s\n",
	    drms_type_names[keystruc->info->type], key);
	drms_close_records (upds, DRMS_FREE_RECORD);
	return 1;
    }
    sprintf (ccmd, " WHERE recnum = %lld", rec->recnum);
    strcat (cmd, ccmd);

    if (noupd)
    {
      if (print_sql_only)
      {
          printf("%s;\n", cmd);
      }
      else
      {
          printf ("       SQL update command not executed:\n%s\n", cmd);
      }

      continue;
    }

    if (drms_dms (drms_env->session, &rowct, cmd)) {
      fprintf (stderr,"Error: unable to execute SQL command\n       %s\n", cmd);
      drms_close_records (upds, DRMS_FREE_RECORD);
      return 1;
    }

    if (verbose) printf ("record %s updated:\n%s\n", recname, cmd);
  } /* end record loop */
/*
  sprintf (cmd, "SELECT COUNT(*) from %s.drms_keyword ", ns);
  sprintf (ccmd, "WHERE seriesname ILIKE \'%s\' ", series);
  strcat (cmd, ccmd);
  sprintf (ccmd, "AND keywordname ILIKE \'%s\' ", key);
  strcat (cmd, ccmd);
  if ((qres = drms_query_txt (drms_env->session, cmd)) == NULL) {
    fprintf (stderr, "Error: can\'t connect to DRMS database\n");
    return 1;
  }
  rct = atoi (qres->field[0][0]);
  if (!rct) {
    fprintf (stderr, "Error: data series %s doesn\'t contain keyword %s\n",
	series, key);
    return 1;
  }
*/

    /* free history_value from last record iteration */
    if (history_value)
    {
        free(history_value);
        history_value = NULL;
    }

		       /*  OK to free records?, they were only used for info  */
  drms_close_records (upds, DRMS_FREE_RECORD);

  return 0;
}
/******************************************************************************/
/*
 *  Revision history
 *  all mods by Rick Bogart unless otherwise noted
 *
 *  2017.02.01	Rick Bogart	created this file
 */
/******************************************************************************/
