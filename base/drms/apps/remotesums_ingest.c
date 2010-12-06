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
#define kSEP2 "@"
#define kSEP3 ":"
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

/* Maps the sums_url field of jsoc.drms_sites into the cmd, host, and port fields used by 
 * sum_export_svc. Relies upon macros defined in localization.h */
static RSINGEST_stat_t SumsURLMap(const char *instr, char **cmdout, char **hostout, unsigned int *portout)
{
   char *tmp = strdup(instr);
   char *user = NULL;
   char *meth = NULL;
   char *host = NULL;
   char *port = NULL;
   char *domain = NULL;
   char methmapped[1024];
   char usermapped[1024];
   char hostmapped[1024];
   char portmapped[1024];
   int size;

   RSINGEST_stat_t status;
   
   status = kRSING_success;
 
   if (tmp)
   {
      meth = tmp;
      user = strstr(tmp, kSEP1);

      if (user)
      {
         *user = '\0';
         user += 3;
         host = strstr(user, kSEP2);

         if (host)
         {
            *host = '\0';
            host++;
            domain = strchr(host, '.');

            if (domain)
            {
               *domain = '\0';
               domain++;
               port = strstr(domain, kSEP3);

               /* port is optional, defaults to 22 */
               if (port)
               {
                  *port = '\0';
                  port++;
               }
            }
            else
            {
               /* domain is optional */
               port = strstr(host, kSEP3);

               /* port is optional, defaults to 22 */
               if (port)
               {
                  *port = '\0';
                  port++;
               }
            }
         }
         else
         {
            status = kRSING_badparma;
         }
      }
      else
      {
         status = kRSING_badparma;
      }

      /* Check for localization */
#ifdef LOC_SUMEXP_METHFMT
      snprintf(methmapped, sizeof(methmapped), LOC_SUMEXP_METHFMT);
#else
      snprintf(methmapped, sizeof(methmapped), "%s", meth);
#endif

#ifdef LOC_SUMEXP_USERFMT
      snprintf(usermapped, sizeof(usermapped), LOC_SUMEXP_USERFMT);
#else
      snprintf(usermapped, sizeof(usermapped), "%s", user);
#endif

#ifdef LOC_SUMEXP_HOSTFMT
      snprintf(hostmapped, sizeof(hostmapped), LOC_SUMEXP_HOSTFMT);
#else
      snprintf(hostmapped, sizeof(hostmapped), "%s.%s", host, domain);
#endif

      *portmapped = '\0';
      if (!port)
      {
         port = "0";
      }
#ifdef LOC_SUMEXP_PORTFMT
      snprintf(portmapped, sizeof(portmapped), LOC_SUMEXP_PORTFMT);
#else
      snprintf(portmapped, sizeof(portmapped), "%s", port);
#endif

      if (cmdout)
      {
         *cmdout = strdup(methmapped);
      }

      if (hostout)
      {
         size = strlen(usermapped) + strlen(hostmapped) + 128;
         *hostout = malloc(size);
         snprintf(*hostout, size, "%s@%s", usermapped, hostmapped);
      }

      if (portout)
      {
         if (sscanf(portmapped, "%u", portout) != 1)
         {
            status = kRSING_badparma;
         }
      }

      free(tmp);
   }
   else
   {
      status = kRSING_badparma;
   }

   return status;
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
   DRMS_Record_t *template = NULL;
   int drmsstat = DRMS_SUCCESS;

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
      template = drms_template_record(drms_env, aseries, &drmsstat);
      XASSERT(template); /* This should succeed because there was a test on this series above. */
      if (!drms_su_allocsu(drms_env, 
                           drms_su_size(drms_env, aseries) + 1000000, 
                           sunum,
                           &sudir, 
                           &template->seriesinfo->tapegroup,
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
            status = SumsURLMap(serverstr, &servermeth, &server, &port);

            if (status == kRSING_success)
            {
               if (rsing_createhashkey(sunum, hashkey, sizeof(hashkey)))
               {
                  if (hcon_lookup(sutracker, hashkey))
                  {
                     fprintf(stderr, "Attempt to export a storage unit more than once; skipping\n");
                     
                     if (servermeth)
                     {
                        free(servermeth);
                        servermeth = NULL;
                     }

                     if (server)
                     {
                        free(server);
                        server = NULL;
                     }

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
                  pexp->sumexpt.cmd = servermeth;
                  pexp->sumexpt.host = server; /* <user>@<server> */
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
            
            if (servermeth)
            {
               servermeth = NULL;
            }

            if (server)
            {
               server = NULL;
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
