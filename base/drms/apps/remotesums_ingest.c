/* Called by remotesums_master.pl (which is called by DRMS - drms_storageunit.c) to 
 * ingest one or more storage units directly into SUMS. remotesums_master.pl obtains
 * the paths to the original storage unit and passes those paths to this module. This 
 * module then creates new SUMS directories (via SUM_alloc2), and directly copies the 
 * original storage units into the newly allocated SUMS directories.
 */



/* This will work 
scp -r 'j0:/SUM8/D2214195/D1829901/*' .
*/

#include "jsoc_main.h"
#include "drms.h"

#define kSUNUMS "sunums"
#define kPATHS "paths"
#define kSERIES "series"
#define kAGENTFILE "agentfile"


enum RSINGEST_stat_enum
{
   kRSING_success = 0,
   kRSING_sumallocfailure,
   kRSING_failure
};

typedef enum RSINGEST_stat_enum RSINGEST_stat_t;

char *module_name = "rs_ingest";

ModuleArgs_t module_args[] =
{
  {ARG_STRING, kSUNUMS, NULL, "comma-separated list of SUNUMs to ingest"},
  {ARG_STRING, kPATHS, NULL, "comma-separated list of paths to remote SUs to ingest"},
  {ARG_STRING, kSERIES, NULL, "comma-separated list of series owning the SUs"},
  {ARG_STRING, kAGENTFILE, NULL, "ssh agent configuration file"},
  {ARG_END}
};

int DoIt(void)
{
   RSINGEST_stat_t status = kRSING_success;
   char *listsunums = NULL;
   char *listpaths = NULL;
   char *listseries = NULL;
   char cmd[DRMS_MAXPATHLEN];
   char query[DRMS_MAXQUERYLEN];
   int drmsst;
   DRMS_RecordSet_t *rs = NULL;
   const char *server = NULL;
   char *sudir = NULL;
   char *ansunum;
   char *apath;
   char *aseries;
   char *lsunum = NULL;
   char *lpath = NULL;
   char *lseries = NULL;
   char *agentfile = NULL;
   long long sunum;

   listsunums = cmdparams_get_str(&cmdparams, kSUNUMS, NULL);
   listpaths = cmdparams_get_str(&cmdparams, kPATHS, NULL);
   listseries = cmdparams_get_str(&cmdparams, kSERIES, NULL);
   agentfile = cmdparams_get_str(&cmdparams, kAGENTFILE, NULL);

   /* loop through SUNUMs calling scp for each one */
   for (ansunum = strtok_r(listsunums, ",", &lsunum), 
          apath = strtok_r(listpaths, ",", &lpath), 
          aseries = strtok_r(listseries, ",", &lseries); 
        ansunum && apath && aseries; 
        ansunum = strtok_r(NULL, ",", &lsunum), 
          apath = strtok_r(NULL, ",", &lpath), 
          aseries = strtok_r(NULL, ",", &lseries))
   {
      /* Use aseries and ansunum to ensure that this SUMS knows 
       * about the SU it is about to ingest. */
      snprintf(query, sizeof(query), "%s[?sunum=%s?]", aseries, ansunum);
      rs = drms_open_records(drms_env, query, &drmsst);
      if (!rs || rs->n == 0)
      {
         fprintf(stderr, 
                 "Uknown series '%s' and/or SUNUM '%s'; cannot ingest - skipping.\n",
                 aseries,
                 ansunum);
         continue;
      }

      if (sscanf(ansunum, "%lld", &sunum) != 1)
      {
         fprintf(stderr, "Invalid sunum '%s'; skipping\n", ansunum);
         continue;
      }

      /* The "_sock" version of this module will not build (nor should it) */
      if (!drms_su_alloc2(drms_env, 
                          drms_su_size(drms_env, aseries) + 1000000, 
                          sunum,
                          &sudir, 
                          NULL))
      {
         server = drms_su_getexportserver();

         if (server && sudir)
         {
            /* Create scp cmd - user running this module must have
             * started ssh-agent and added the private key to the
             * agent's list of known keys. Also, the site providing
             * the SUMS files must have put the user's corresponding 
             * public key into its authorized_keys file.
             */
            snprintf(cmd, 
                     sizeof(cmd), 
                     "source %s;scp -r '%s:%s/*' %s", 
                     agentfile,
                     server, 
                     apath, 
                     sudir);
            // system(cmd); /* doesn't return until child terminates */
         }
      }
      else
      {
         status = kRSING_sumallocfailure;
      }

      if (sudir)
      {
         free(sudir);
         sudir = NULL;
      }
   }

   return status;
}
