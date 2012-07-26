/**
\defgroup create_series create_series - create a new DRMS series from a .jsd file
@ingroup drms_util

\brief Create a new DRMS series from a JSOC Series Descriptor (jsd) file.

\par Synopsis:

\code
create_series [-fGEN_FLAGS] jsdfile
\endcode


Read a <a href="http://jsoc.stanford.edu/jsocwiki/Jsd">JSOC Series
Definition file</a> and create the series table in the DRMS
database. By default, all series are created with public read
permission, but update and insert permission for the owner only.
Upon success, \ref create_series prints out the following message: 
\code
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "xxyx_pkey" for table "xxyx"
\endcode

\par Flags: 
\c -f: replace an existing series definition with the new
one. Since this removes all the metadata for a series - essentially
deleting the series - the user is prompted to confirm. Except for
initial debugging of the .jsd file it is probably an error to use the
<br>
\c -f flag.
<br>

\par GEN_FLAGS: 
Ubiquitous flags present in every module.
\ref jsoc_main

\param name=seriesname optional parameter to override seriesname in JSD file

\param jsdfile JSOC series definition file name

\bug
The permissions parameter has been omitted since it is presently ignored in the code.  It
should be restored with some method to allow marking a series private.

\sa
delete_series describe_series modify_series show_info

*/
#include "drms.h"
#include "jsoc_main.h"

/* Command line parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_END}
};

char *module_name = "create_series";
int DoIt(void) {
  int force, len, perms;
  const char *filename;
  FILE *fp;
  char *buf;
  const char *series = NULL;
  struct stat file_stat;
  DRMS_Record_t *template;
  char yesno[10];
    int usestdin = 0;

  /* Parse command line parameters. */
  if (cmdparams_numargs (&cmdparams) < 1) 
  {
      fprintf(stderr, "Insufficient number of args.\n");
      goto usage;
  }

  force = cmdparams_exists(&cmdparams, "f");
    usestdin = cmdparams_isflagset(&cmdparams, "i");

  /* Get privileges to be granted to the public on the tables belonging to
     the new series. */
  /* Default is a public read-only table. */
  perms = DB_PRIV_SELECT;

  filename = cmdparams_getarg(&cmdparams, 1);

    if (filename)
    {
        /* Read the whole series definition file into memory. */
        if (stat(filename, &file_stat)) 
        {
            printf("Can't stat file %s\n", filename);
            return 1;
        }  
        buf = (char *)malloc( file_stat.st_size+1 );
        XASSERT(buf);
        fp = fopen(filename,"r");
        fread(buf,file_stat.st_size,1,fp);
        buf[file_stat.st_size] = 0;
    }
    else if (usestdin)
    {
        char bufin[80];
        char *bufout = NULL;
        size_t szbuf = 1024;
        
        bufout = malloc(szbuf * sizeof(char));
        *bufout = '\0';
        
        if (!bufout)
        {
            fprintf(stderr, "No memory!\n");
            goto bailout;
        }
        
        while (fgets(bufin, sizeof(bufin), stdin) != NULL)
        {
            bufout = base_strcatalloc(bufout, bufin, &szbuf);    
        }
        
        buf = bufout;
    }
    else
    {
        /* Bail */
        fprintf(stderr, "No jsd provided?\n");
        goto usage;
    }

  /* Parse the description into a template record structure. */
  template = drms_parse_description(drms_env, buf);
  free(buf);
  if (template==NULL)
  {
      if (!usestdin)
      {
          printf("Failed to parse series description in file '%s'.\n",filename);
      }
      else
      {
          printf("Failed to parse series description from stdin.\n");
      }
      fflush(stdout);
      fflush(stderr);
    return 1;
  }
//  if (series = strdup(cmdparams_get_str(&cmdparams, "name", NULL))) {
  if (series = cmdparams_get_str(&cmdparams, "name", NULL)) {
    strcpy(template->seriesinfo->seriesname, series);
  } else {
    if (!strncmp(template->seriesinfo->seriesname, "XXX", 3)) {
      fprintf(stderr, "This JSD requires user supplied series name.\n");
      goto usage;
    }
    series = template->seriesinfo->seriesname;
  }

  if (force) {
    int status;
    /* Remove existing series */
    printf ("You are about to permanently erase all metadata for the series "
	"'%s'.\nAre you sure you want to do this (yes/no)? ",series);
    fgets (yesno, 10, stdin);
    len = strlen (yesno);
    if (yesno[len-1] == '\n') yesno[len-1] = 0;
    if (strcmp(yesno,"yes")) goto bailout;

    printf ("Removing existing series '%s'...\n", series);
    status = drms_delete_series(drms_env, series, 1, 0);
    if (status) {
      if (status != DRMS_ERROR_UNKNOWNSERIES) {
	goto bailout;
      }
    }
  }

  /* Generate SQL from template to create database tables
     and global entries for the new series. */
  printf ("Creating new series '%s'...\n",series);
  if (drms_create_series (template, perms)) {
    printf ("Failed to create series.\n");
    goto bailout;
  }
  drms_free_record_struct (template);
  free (template);

    fflush(stdout);
    fflush(stderr);
  return 0;
 bailout:
    fflush(stdout);
    fflush(stderr);
  return 1;
 usage:
  printf ("Usage: %s [-f] [perm=[s|i|u]] [name=seriesname] (file.jsd | -i)\n",
      cmdparams_getarg (&cmdparams, 0));
    fflush(stdout);
    fflush(stderr);
  return 1;
}
