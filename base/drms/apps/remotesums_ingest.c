/* Called by remotesums_master.pl (which is called by DRMS - drms_storageunit.c) to 
 * ingest one or more storage units directly into SUMS. remotesums_master.pl obtains
 * the paths to the original storage unit and passes those paths to this module. This 
 * module then creates new SUMS directories (via SUM_alloc2), and directly copies the 
 * original storage units into the newly allocated SUMS directories.
 */



/* This will work 
scp -r 'j0:/SUM8/D2214195/D1829901/*' .
*/

#include <dirent.h>
#include "jsoc_main.h"
#include "drms.h"

#define kSUNUMS "sunums"
#define kPATHS "paths"
#define kSERIES "series"
#define kSEP1 "://"
#define kSEP2 ":"
#define kHASHKEYLEN 64
#define kMAXREQ 512

enum RSINGEST_stat_enum
{
   kRSING_success = 0,
   kRSING_badparma,
   kRSING_tracker,
   kRSING_sumallocfailure,
   kRSING_sumcommitfailure,
   kRSING_sumexportfailure,
   kRSING_failure
};

typedef enum RSINGEST_stat_enum RSINGEST_stat_t;

struct RSING_FCopyTracker_struct
{
  char series[DRMS_MAXSERIESNAMELEN];
  char sudir[DRMS_MAXPATHLEN];
};

typedef struct RSING_FCopyTracker_struct RSING_FCopyTracker_t;

struct RSING_ExpTracker_struct
{
  SUMEXP_t sumexpt;
  int maxreq;
};

typedef struct RSING_ExpTracker_struct RSING_ExpTracker_t;



char *module_name = "rs_ingest";

ModuleArgs_t module_args[] =
{
  {ARG_STRING, kSUNUMS, NULL, "comma-separated list of SUNUMs to ingest"},
  {ARG_STRING, kPATHS, NULL, "comma-separated list of paths to remote SUs to ingest"},
  {ARG_STRING, kSERIES, NULL, "comma-separated list of series owning the SUs"},
  {ARG_END}
};

static int rsing_createhashkey(long long sunum, char *buf, int size)
{
   return (snprintf(buf, size, "%lld", sunum) > 0);
}

int DoIt(void)
{
   RSINGEST_stat_t status = kRSING_success;
   char *listsunums = NULL;
   char *listpaths = NULL;
   char *listseries = NULL;
   char query[DRMS_MAXQUERYLEN];
   int drmsst;
   DRMS_RecordSet_t *rs = NULL;
   char serverstr[512];
   char *server = NULL;
   char *servermeth = NULL;
   char *serverport = NULL;
   unsigned int port;
   char *sudir = NULL;
   char *ansunum;
   char *apath;
   char *aseries;
   char *lsunum = NULL;
   char *lpath = NULL;
   char *lseries = NULL;
   long long sunum;
   HContainer_t *sutracker = NULL; /* Ensure that the storage unit really got copied */
   HContainer_t *sumexptracker = NULL;
   char hashkey[kHASHKEYLEN];
   RSING_FCopyTracker_t fcopy;
   RSING_FCopyTracker_t *pfcopy = NULL;
   struct stat stBuf;
   RSING_ExpTracker_t *pexp = NULL;
   HIterator_t *hiter = NULL;
   RSING_ExpTracker_t *pexptracker = NULL;
   SUMEXP_t *psumexpt = NULL;
   const char *phashkey = NULL;
   struct dirent **fileList = NULL;
   int atleastonefilecopied = 0;

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

      drms_close_records(rs, DRMS_FREE_RECORD);

      if (sscanf(ansunum, "%lld", &sunum) != 1)
      {
         fprintf(stderr, "Invalid sunum '%s'; skipping\n", ansunum);
         continue;
      }

      if (!sutracker)
      {
         sutracker = hcon_create(sizeof(RSING_FCopyTracker_t), kHASHKEYLEN, NULL, NULL, NULL, NULL, 0);
      }

      if (!sutracker)
      {
         fprintf(stderr, "Failed to create file-copy tracker.\n");
         status = kRSING_tracker;
         break;
      }

      if (!sumexptracker)
      {
         sumexptracker = hcon_create(sizeof(RSING_ExpTracker_t), kHASHKEYLEN, NULL, NULL, NULL, NULL, 0);
      }

      if (!sumexptracker)
      {
         fprintf(stderr, "Failed to create export-request tracker.\n");
         status = kRSING_tracker;
         break;
      }

      /* The "_sock" version of this module will not build (nor should it) */
      if (!drms_su_allocsu(drms_env, 
                           drms_su_size(drms_env, aseries) + 1000000, 
                           sunum,
                           &sudir, 
                           NULL))
      {
         if (sudir && !drms_su_getexportserver(drms_env, sunum, serverstr, sizeof(serverstr)))
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

                  sscanf(serverport, "%u", &port);

                  status = kRSING_success;
               }
            }

            if (status == kRSING_success)
            {
               if (rsing_createhashkey(sunum, hashkey, sizeof(hashkey)))
               {
                  if (hcon_lookup(sutracker, hashkey))
                  {
                     fprintf(stderr, "Attempt to export a storage unit more than once; skipping\n");
                     free(tmp);
                     tmp = NULL;
                     /* Don't do anything with the alloc'd su - it will get cleaned up eventually */

                     continue;
                  }

                  snprintf(fcopy.series, DRMS_MAXSERIESNAMELEN, "%s", aseries);
                  snprintf(fcopy.sudir, DRMS_MAXPATHLEN, "%s", sudir);
                  hcon_insert(sutracker, hashkey, &fcopy);
               }

               /* Sort requests - combine each server's requests into a single SUMEXP_t */
               if ((pexp = (RSING_ExpTracker_t *)hcon_lookup(sumexptracker, server)) != NULL)
               {
                  /* Use existing SUMEXP_t */
                  if (pexp->sumexpt.reqcnt == pexp->maxreq)
                  {
                     /* time to allocate more space */
                     pexp->sumexpt.src = 
                       (char **)realloc(pexp->sumexpt.src, pexp->maxreq * 2 * sizeof(char *));
                     pexp->sumexpt.dest = 
                       (char **)realloc(pexp->sumexpt.dest, pexp->maxreq * 2 * sizeof(char *));
                  }
               }
               else
               {
                  /* Create new SUMEXP_t */
                  pexp = (RSING_ExpTracker_t *)hcon_allocslot(sumexptracker, server);
                  pexp->sumexpt.host = strdup(server); /* <user>@<server> */
                  pexp->sumexpt.port = port;
                  pexp->sumexpt.src = (char **)calloc(kMAXREQ, sizeof(char *));
                  pexp->sumexpt.dest = (char **)calloc(kMAXREQ, sizeof(char *));
                  pexp->sumexpt.reqcnt = 0;
                  pexp->maxreq = kMAXREQ;
               }

               (pexp->sumexpt.src)[pexp->sumexpt.reqcnt] = strdup(apath);
               (pexp->sumexpt.dest)[pexp->sumexpt.reqcnt] = strdup(sudir);
               pexp->sumexpt.reqcnt++;                             
            }
            else
            {
               /* print warning; but will continue */
               fprintf(stderr, "Invalid server string '%s'.\n", serverstr);
            }
            
            if (tmp)
            {
               free(tmp);
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

   /* Call SUM_export() for each SUMEXP_t */
   hiter = hiter_create(sumexptracker);
   while ((pexptracker = hiter_getnext(hiter)) != NULL)
   {
      psumexpt = &(pexptracker->sumexpt);

      /* Will block for each request (one request per scp server) */
      if (drms_su_sumexport(drms_env, psumexpt))
      {
         status = kRSING_sumexportfailure;
         fprintf(stderr, "Error '%d' SUM_export().\n", (int)status);
         /* but continue */
         status = kRSING_success;
      }

      /* free */
      if (psumexpt->host)
      {
         free(psumexpt->host);
      }

      while (psumexpt->reqcnt >= 0)
      {
         if (psumexpt->src && psumexpt->src[psumexpt->reqcnt])
         {
            free(psumexpt->src[psumexpt->reqcnt]);
         }
         if (psumexpt->dest && psumexpt->dest[psumexpt->reqcnt])
         {
            free(psumexpt->dest[psumexpt->reqcnt]);
         }

         psumexpt->reqcnt--;
      }

      if (psumexpt->src)
      {
         free(psumexpt->src);
      }
      if (psumexpt->dest)
      {
         free(psumexpt->dest);
      }
   }

   hiter_destroy(&hiter);
   hcon_destroy(&sumexptracker);

   /* Test to see if copies succeeded - do a stat on each file */
   hiter = hiter_create(sutracker);
   while ((pfcopy = hiter_extgetnext(hiter, &phashkey)) != NULL)
   {
      /* If the file exists, then go ahead and call SUM_put() */
      if (!stat(pfcopy->sudir, &stBuf) && 
          S_ISDIR(stBuf.st_mode) && 
          scandir(pfcopy->sudir, &fileList, NULL, NULL) > 2) /* don't count . and ..*/
      {
         /* File exists and it has a non-zero size - commit */
         sscanf(phashkey, "%lld", &sunum);

         if (drms_su_commitsu(drms_env, pfcopy->series, sunum, pfcopy->sudir))
         {
            /* print warning, but execution will continue */
            fprintf(stderr, "Error committing SU '%s'.\n", phashkey);
         }
         else
         {
            atleastonefilecopied = 1;
         }
      }
      else
      {
         fprintf(stderr, "Error copying to storage unit '%s'.\n", pfcopy->sudir);
      }
      
   }

   hiter_destroy(&hiter);
   hcon_destroy(&sutracker);

   if (status == kRSING_success && atleastonefilecopied)
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
