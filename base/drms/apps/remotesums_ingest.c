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
#define kSEP1 "://"
#define kSEP2 ":"

enum RSINGEST_stat_enum
{
   kRSING_success = 0,
   kRSING_badparma,
   kRSING_sumallocfailure,
   kRSING_sumcommitfailure,
   kRSING_failure
};

typedef enum RSINGEST_stat_enum RSINGEST_stat_t;

char *module_name = "rs_ingest";

ModuleArgs_t module_args[] =
{
  {ARG_STRING, kSUNUMS, NULL, "comma-separated list of SUNUMs to ingest"},
  {ARG_STRING, kPATHS, NULL, "comma-separated list of paths to remote SUs to ingest"},
  {ARG_STRING, kSERIES, NULL, "comma-separated list of series owning the SUs"},
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
   char serverstr[512];
   char *server = NULL;
   char *servermeth = NULL;
   char *serverport = NULL;
   char *sudir = NULL;
   char *ansunum;
   char *apath;
   char *aseries;
   char *lsunum = NULL;
   char *lpath = NULL;
   char *lseries = NULL;
   long long sunum;

   listsunums = cmdparams_get_str(&cmdparams, kSUNUMS, NULL);
   listpaths = cmdparams_get_str(&cmdparams, kPATHS, NULL);
   listseries = cmdparams_get_str(&cmdparams, kSERIES, NULL);

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
      if (!drms_su_allocsu(drms_env, 
                           drms_su_size(drms_env, aseries) + 1000000, 
                           sunum,
                           &sudir, 
                           NULL))
      {
         if (sudir && !drms_su_getexportserver(drms_env, serverstr, sizeof(serverstr)))
         {
            /* Create scp cmd - user running this module must have
             * started ssh-agent and added their private key to the
             * agent's list of known keys. ssh-agent
             * is user-specific - you can't use an ssh-agent started
             * by another user. Also, the site providing
             * the SUMS files must have the user's corresponding 
             * public key in its authorized_keys file. The user must 
             * have also sourced the ssh-agent environment variables
             * so that the local scp can find the user's private
             * key. If remotesums_master.pl is used to launch
             * this program, it will have ensured that ssh-agent
             * is running.
             *
             * The plan is to identify a single "production" user per SUMS site -
             * this user (eg. production_MPI) will have a user
             * account at Stanford through which they can access
             * j0.stanford.edu. Because ANY user can run a command
             * that will execute the remotesums code, the remote site
             * will need to funnel the scp cmd through a process
             * run by this production user. In other
             * words, because user Patrick cannot access j0, he must
             * send his request to the production user, who can in turn
             * pass on the request to j0.
             * 
             * XXX - For now, just do the scp cmd as whoever is running 
             * this module.
             */

            /* parse serverstr -> scp://jsoc_export@j0.stanford.edu/:55000 */
            char *tmp = strdup(serverstr);
            status = kRSING_badparma;

            server = strstr(tmp, kSEP1);
            if (server)
            {
               *server = '\0';
               servermeth = tmp;
               server += 3;
               serverport = strstr(server, kSEP2);

               if (serverport)
               {
                  *serverport = '\0';
                  serverport++;

                  status = kRSING_success;
               }
            }

            if (status == kRSING_success)
            {
               snprintf(cmd, 
                        sizeof(cmd), 
                        "%s -r -P %s '%s:%s/*' %s > /dev/null", 
                        servermeth,
                        serverport,
                        server, 
                        apath, 
                        sudir);
               system(cmd); /* doesn't return until child terminates */

               if (tmp)
               {
                  free(tmp);
               }

               /* commit the newly allocated SU */
               if (drms_su_commitsu(drms_env, aseries, sunum, sudir))
               {
                  status = kRSING_sumcommitfailure;
               }
            }
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

      if (status != kRSING_success)
      {
         /* We skip and continue with the next SU on error. */
         fprintf(stderr, "Error '%d' transfering storage unit %lld; skipping.\n", (int)status, sunum);
         status = kRSING_success;
      }
   } /* su loop */

   if (status == kRSING_success)
   {
      /* Tell DRMS that ingest was successful */
      printf("1\n");
   }
   else
   {
      /* Tell DRMS that ingest was unsuccessful */
      printf("-1\n"); 
   }

   return status;
}
