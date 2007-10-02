/* store_ds_vw_V_06h_lev1_8.c  
 * 	stores mdi vw_V_06h_lev1_8 ds in DRMS/SUMS.
*/
/* Usage:  store_ds_vw_V_06h_lev1_8 series_num=n-m
 *         
 *     Store MDI datasets into DRMS/SUMS.
 * 
 * Queries the mdi dsds to get the location of the given series numbers
 * and copies the dirs into the SUMS and makes the DRMS keywords according
 * to the ds_mdi.vw_V_06h_lev1_8.jsd where:
 *		Index:          series_num
 *
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>

#define NOTSPECIFIED "***NOTSPECIFIED***"
					 /* List of default parameter values */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "series_num", NOTSPECIFIED, "", ""},    /* series #s to store */
  {ARG_FLAG, "h", "0", "", ""},              /* print usage message and quit */
  {ARG_FLAG, "v", "0", "", ""},	       /* verbose flag, normally do not use  */
  {ARG_END}
};
					    /* Module name presented to DRMS */
char *module_name = "store_ds_vw_V_06h_lev1_8";
char *series = "ds_mdi.vw_V_06h_lev1_8";	/* our series name */

/* Some global variables for this module. */
int verbose = 0;

/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro()
  {
  int usage = cmdparams_get_int(&cmdparams, "h", NULL);
  verbose = cmdparams_get_int(&cmdparams, "v", NULL);
  if (usage)
    {
    printf("store_ds_vw_V_06h_lev1_8 {-h} {-v} series_num=n-m\n"
	"  -h: print this message and exit\n"
	"  -v: verbose\n"
	"series_num - series numbers to ingest\n");
    return(1);
    }
  if (verbose)
     cmdparams_printall(&cmdparams);
  return(0);
  }

/* Module main function. */
int DoIt(void)
{
int status = 0;
int j = 0;		/* temp for some debug tests */

FILE *logfp;
char *series_num, *token, *rsp, *wd;
char *create_date, *t_begin;
char path[DRMS_MAXPATHLEN];
char cmd[DRMS_MAXPATHLEN+1024];
char fname[128], logname[128], line[128];
DRMS_Record_t *rec, *template;
int s_start, s_end, pid, i, level_num, ds_index;

/* Series must already exist */
template = drms_template_record(drms_env, series, &status);
if (template==NULL && status == DRMS_ERROR_UNKNOWNSERIES)
  {
    printf("Series '%s' does not exist. Give up.\n", series);
    return(1);
  }
else
  if(status)
    {
    fprintf(stderr, "DRMS problem looking up series.\n");
    return(status);
    }

if (nice_intro())
  return(0);
series_num = cmdparams_get_str(&cmdparams, "series_num", NULL);

if (strcmp(series_num, NOTSPECIFIED) == 0)
  {
  fprintf(stderr, "'series_num' must be specified.  Abort\n");
  return(1);
  }

if((rsp = index(series_num, ','))) {
  fprintf(stderr, "No \",\" allowed in series_num=n-m\n");
  return(1);
}
if((rsp = index(series_num, '-'))) {
  token = (char *)strtok(series_num, "-");
  s_start = atoi(token);
  token = (char *)strtok(NULL, "-");
  s_end = atoi(token);
}
else {
  s_start = atoi(series_num);
  s_end = s_start;
}
if(s_start == 0 || (s_start > s_end)) {
  fprintf(stderr, "series_num error\n");
  return(1);
}
pid = getpid();
sprintf(logname, "/tmp/peqlog_%d.log", pid);

for(i = s_start; i <= s_end; i++) {
  j++;					/* temp for debug tests */
  sprintf(cmd, "peq 'prog:mdi,level:lev1.8,series:vw_V_06h[%d]' 1> %s 2>&1", 
		i, logname);
  /*printf("About to execute:\n   %s\n", cmd);*/
  if(system(cmd)) {
    fprintf(stderr, "Failed: %s\n", cmd);
    return(0);	/* ret 0 status so DRMS will commit what already done */
  }
  if((logfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", logname);
    return(0);
  } 
  while(fgets(line, 128, logfp)) {    /* get a log file line */
    token=(char *)strtok(line, " \t\n");
    if(strstr(line, "in_0_level_sn:")) {
      token=(char *)strtok(NULL, " \t\n");
      level_num=atoi((char *)strtok(NULL, " \t\n")); 
      continue;
    }
    if(strstr(line, "in_0_ds_index:")) {
      token=(char *)strtok(NULL, " \t\n");
      ds_index=atoi((char *)strtok(NULL, " \t\n")); 
      continue;
    }
    if(strstr(line, "in_0_creat_date:")) {
      token=(char *)strtok(NULL, " \t\n");
      rsp=(char *)strtok(NULL, " \t\n"); 
      create_date = strdup(rsp);
      continue;
    }
    if(strstr(line, "t_first:")) {	/* use t_first as t_begin */
      token=(char *)strtok(NULL, " \t\n");
      rsp=(char *)strtok(NULL, " \t\n"); 
      t_begin = strdup(rsp);
      continue;
    }
    if(!strstr(line, "in_0_wd:")) continue;              
    token=(char *)strtok(NULL, " \t\n");
    rsp=(char *)strtok(NULL, " \t\n"); /* this is the wd */ 
    wd = strdup(rsp);
    if(!strcmp(wd, ".")) wd = NULL; 
  }
  fclose(logfp);
  if(wd) {
    /*printf("wd for series# %d: %s\n", i, wd);      /* !!TEMP */ 
    /*printf("level_num=%d  ds_index=%d  series_num=%d  create_date=%s\n",
		level_num, ds_index, i, create_date);
    */
  }
  else {
    printf("No wd for series# %d\n", i);
    continue;
  }
  /* First, check to see that the dir exists */
  if (access(wd, R_OK) != 0)
    {
    printf("The requested dir can not be accessed.  %s\n", wd);
    return(0);		/* 0 = commit what already done */
    }
  /* See if any overview.fits file to get T_START from */
  sprintf(fname, "%s/vw_V_06h.%d.overview.fits", wd, i);
  if((logfp=fopen(fname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file %s\n", fname);
  } 
  else {
    while(fgets(line, 128, logfp)) {    /* get an overview.fits line */
      token=(char *)strtok(line, " \t\n");
      if(strstr(line, "T_START")) {	/* use t_start as t_begin */
        token=(char *)strtok(NULL, " \t\n");
        rsp=(char *)strtok(NULL, " \t\n"); 
        rsp++;				/* elim start tick mark */
        *(rsp + strlen(rsp) -1) = NULL;  /* elim end tick */
        t_begin = strdup(rsp);
        break;
      }
    }
  }
  fclose(logfp);

/* Now ready to make a new record and set keywords */
rec = drms_create_record(drms_env, series, DRMS_PERMANENT, &status);
if (!rec || status)
    {
    printf("drms_create_record failed, series=%s, status=%d.  Aborting.\n", series,status);
    return(status);
    }
if ((status = drms_setkey_string(rec, "T_BEGIN", t_begin)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'T_BEGIN'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'T_BEGIN'\n");
   return(1);
   }
if ((status = drms_setkey_string(rec, "create_date", create_date)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'create_date'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'create_date'\n");
   return(1);
   }
if ((status = drms_setkey_int(rec, "series_num", i)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'series_num'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'series_num'\n");
   return(1);
   }
if ((status = drms_setkey_int(rec, "level_num", level_num))) 
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("WARNING: series %s does not have a keyword named 'level_num'\n", series);
   else
     printf("WARNING: drms_setkey_string failed for 'level_num'\n");
   return(1);
   }
if ((status = drms_setkey_int(rec, "ds_index", ds_index))) 
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("WARNING: series %s does not have a keyword named 'ds_index'\n", series);
   else
     printf("WARNING: drms_setkey_string failed for 'ds_index'\n");
   return(1);
   }

/* a SU should have been allocated since there is a segment in the series defn */
drms_record_directory(rec, path, 1);
if (! *path)
  {
  fprintf(stderr,"no path found\n");
  return(1);
  }
sprintf(cmd, "cp -r %s %s", wd, path);
status = system(cmd);
if (status)
    {
    printf("Copy failed: %s\n", cmd);
    return(status);
    }
else
    printf("%s\n", cmd);

/*if(j == 3) return(1);		/* !!!TEMP debug test */

if ((status = drms_close_record(rec, DRMS_INSERT_RECORD)))
  printf("drms_close_record failed!\n");

}
return(status);
}
