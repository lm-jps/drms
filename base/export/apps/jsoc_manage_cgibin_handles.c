#include <stdio.h>
#include <stdlib.h>
#include "cmdparams.h"

/* 
 * Uses flock on a file (default: /home/jsoc/exports/jsocWebHandles) that contains the active
 * unique handles and PID information.  Locks the file, adds, reports, or removes entries.
 * returns a single line on stdout containing the user provided unique handle and the cgi-bin instance PID.
 *
 * synopsis: jsoc_manage_cgibin_handles -a|-l|-d handle=<handle> {pid=<PID>} {file=<handlefile>}
 *
 *  -a - add new entry, expects both handle and pid arguments.
 *  -l - lookup entry, expects handle and returns pid
 *  -d - delete entry, expects handle 
 *  -e - edit file with vi while locked, 5 minutes allowed.
 *  handle must be unique, up to 99 chars.
 *  pid may be up to 99 chars.
 *  the master file can be specified via "file" parameter, defaults to HANDLE_FILE
 */

#define HANDLE_FILE "/home/jsoc/exports/Web_request_handles"
#define OP_ERR		(0)
#define OP_ADD		(1)
#define OP_LOOKUP	(2)
#define OP_DELETE	(3)
#define NOT_SPECIFIED	"NOT Specified"

// From here to XXXXX comment copy to code that needs one of these functions.
// also include unistd.h

#include <unistd.h>

static FILE *lock_open(const char *filename)
  {
  int sleeps;
  char lockfile[1024];
  strcpy(lockfile, filename);
  strcat(lockfile, ".lck");
  FILE *fp = fopen(lockfile, "w");
  if (!fp)
    {
    fprintf(stderr, "Failed to open file for locking %s.\n", lockfile);
    return(NULL);
    }
  for(sleeps=0; lockf(fileno(fp),F_TLOCK,0); sleeps++)
    {
    if (sleeps >= 300)
      {
      fprintf(stderr,"Lock on %s failed.\n", lockfile);
      return(NULL);
      }
    sleep(1);
    }
  return(fp);
  }

static lock_close(FILE *fp)
  {
  lockf(fileno(fp),F_ULOCK,0);
  fclose(fp);
  }

int jsoc_add_proc_handle(const char *file, const char *handle, const char *pid)
  {
  FILE *fp, *fp_lock;
  if (strcmp(pid, NOT_SPECIFIED) == 0)
    {
    fprintf(stderr, "For adding entry, the pid must be provided.\n");
    return(1);
    }
  fp_lock = lock_open(file);
  if (!fp_lock)
    return(1);
  fp = fopen(file, "a");
  if (!fp)
    {
    fprintf(stderr, "Failed to open %s file.\n", file);
    return(1);
    }
  fprintf(fp, "%s\t%s\n", handle, pid);
  fclose(fp);
  lock_close(fp_lock);
  return(0);
  }

char *jsoc_lookup_proc_handle(const char *file, const char *handle)
  {
  FILE *fp, *fp_lock;
  char PID[100];
  char HANDLE[100];
  char *pid = NULL;
  fp_lock = lock_open(file);
  if (!fp_lock)
    return(NULL);
  fp = fopen(file, "r");
  if (!fp)
    {
    fprintf(stderr, "Failed to open %s file.\n", file);
    return(NULL);
    }
  while (fscanf(fp, "%s\t%s\n", HANDLE, PID) == 2)
    if (strcmp(handle, HANDLE) == 0)
      {
      pid = strdup(PID);
      break;
      }
  fclose(fp);
  lock_close(fp_lock);
  return(pid);
  }

int jsoc_delete_proc_handle(const char *file, const char *handle)
  {
  FILE *fp, *fp_lock;
  char cmd[1024];
  fp_lock = lock_open(file);
  if (!fp_lock)
    return(1);
  sprintf(cmd, "ed -s %s <<END\ng/^%s\t/d\nwq\nEND\n", file, handle);
  system(cmd);
  lock_close(fp_lock);
  return(0);
  }

int jsoc_edit_proc_handles(const char *file )
  {
  FILE *fp, *fp_lock;
  char cmd[1024];
  fp_lock = lock_open(file);
  if (!fp_lock)
    return(1);
  sprintf(cmd, "vi %s\n", file);
  system(cmd);
  lock_close(fp_lock);
  return(0);
  }

// XXXXXX End of functions that could be in library

#include "cmdparams.h"

ModuleArgs_t module_args[] =
{
  {ARG_STRING, "handle", NOT_SPECIFIED, "Unique request handle"},
  {ARG_STRING, "pid", NOT_SPECIFIED, "Running processes ID"},
  {ARG_STRING, "file", HANDLE_FILE, "Active handle log file"},
  {ARG_FLAG, "a", "0", "op = add entry"},
  {ARG_FLAG, "l", "0", "op = lookup entry"},
  {ARG_FLAG, "d", "0", "op = delete entry"},
  {ARG_FLAG, "e", "0", "op = edit file"},
  {ARG_END}
};
ModuleArgs_t *gModArgs = module_args;
/* @} */

CmdParams_t cmdparams;
char *module_name = "jsoc_manage_cgibin_handles";

int main(int argc, char **argv)
{
int status;
const char *handle;
const char *filename;
const char *pid;
int op_add, op_lookup, op_delete, op_edit;

/* Parse command line parameters */
status = cmdparams_parse (&cmdparams, argc, argv);
if (status == CMDPARAMS_QUERYMODE)
  {
  cmdparams_usage (argv[0]);
  return 0;
  }

filename = cmdparams_get_str(&cmdparams, "file", NULL);
pid = cmdparams_get_str(&cmdparams, "pid", NULL);
handle = cmdparams_get_str(&cmdparams, "handle", NULL);
op_add = cmdparams_isflagset(&cmdparams, "a");
op_lookup = cmdparams_isflagset(&cmdparams, "l");
op_delete = cmdparams_isflagset(&cmdparams, "d");
op_edit = cmdparams_isflagset(&cmdparams, "e");

if (!op_edit && (!handle || strcmp(handle, NOT_SPECIFIED) == 0))
  {
  fprintf(stderr, "handle must be provided.\n");
  return(1);
  }

if (op_add)
  return(jsoc_add_proc_handle(filename, handle, pid));

if (op_lookup)
  {
  char *pid = jsoc_lookup_proc_handle(filename, handle);
  if (pid)
    printf("%s\n", pid);
  return(0);
  }

if (op_delete)
  return(jsoc_delete_proc_handle(filename, handle));

if (op_edit)
  return(jsoc_edit_proc_handles(filename));

}
