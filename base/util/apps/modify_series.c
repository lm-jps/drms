/**
\defgroup modify_series modify_series - for a series, change the archive flag and/or add keywords as described in a .jsd file
@ingroup drms_util
\brief Modify a DRMS series structure.

\par Synopsis:

\code
modify_series [-GEN_FLAGS] series=<seriesname> [archive={0|1}] [jsdfile]
\endcode

Modify a series definition. It does one of the following:
 - Change archive flag
 - Add keywords as specified in jsdfile

\par GEN_FLAGS: 
Ubiquitous flags present in every module.
\ref jsoc_main

\param seriesname
\param jsdfile JSOC series definition file name. This file contains keywords to add.
\param archive 0: not to achive, 1: to archive.

\par Limitations:
 - \ref modify_series does not deal with duplications in keyword definition
 - \ref modify_series does not fill in defaultval for the newly added keywords.

\sa
create_series delete_series describe_series show_info

*/
#include "drms.h"
#include "jsoc_main.h"
#include "util.h"

/* Command line parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_END}
};

char *module_name = "modify_series";
typedef enum {ARCHIVE, ADD_KEYWORD} action_t;

int DoIt(void) {
  const int bufsz = 2048;
  action_t action = ADD_KEYWORD;
  int status = DRMS_NO_ERROR;
  int drmsstatus = DRMS_SUCCESS;
  int archive;
  const char *series;
  char *series_short, *namespace, *series_lower;
  char stmt [DRMS_MAXQUERYLEN];
  //char stmt[2048];
  DB_Text_Result_t *qres = NULL;
  const char *filename;
  FILE *fp;
  struct stat file_stat;
  char *buf = NULL;
  DRMS_Record_t *template = NULL;
  DRMS_Record_t *recproto = NULL;
  int keynum;
  HIterator_t hit;
  DRMS_Keyword_t *key = NULL;
  DRMS_Keyword_t *lastkey = NULL;
  size_t bufsize = 0;
  char *p = NULL;

  /* Parse command line parameters. */
  if (cmdparams_numargs (&cmdparams) < 2) goto usage;


  /* Get series name. */
  series = cmdparams_get_str(&cmdparams, "series", NULL);
  p = strchr(series, '.');  
  if (!p) {
    printf("Invalid series name\n");
    return 1;
  }
  namespace = ns(series);
  series_short  = strdup(p+1);
  series_lower = strdup(series);

  archive = cmdparams_get_int(&cmdparams, "archive", &status);
  if (!status) {
    action = ARCHIVE;
  }

  /*See if series exists before going through any other checks */
  if (!drms_series_exists(drms_env, series_lower, &drmsstatus))
  { printf("***** The series %s does not exist, and cannot be removed. *****\n Please try again with a valid series name\n", series);
      return 1;
  }
  /*See if it's in replication before going any further */
  if (drms_series_isreplicated(drms_env, series_lower))
     { printf("***** The series %s is in replication and cannot be modified *****\nProgram exiting\n", series);
       return 1;
     }


  if (action == ADD_KEYWORD) {
    filename = cmdparams_getarg(&cmdparams, 1);
    /* Read the whole series definition file into memory. */
    if (stat(filename, &file_stat)) 
      {
	printf("Can't stat file %s\n", filename);
	return 1;
      }  
    XASSERT((buf = (char *)malloc( file_stat.st_size+1 )));
    fp = fopen(filename,"r");
    fread(buf,file_stat.st_size,1,fp);
    buf[file_stat.st_size] = 0;
  }

  switch (action) {
  case ARCHIVE:
    snprintf(stmt, bufsz, "select has_table_privilege('%s', '%s.drms_series', 'update')", drms_env->session->db_handle->dbuser, namespace);
    if ( (qres = drms_query_txt(drms_env->session, stmt)) != NULL) {
      if (qres->num_rows > 0) {
	if (qres->field[0][0][0] == 'f') {
	  fprintf(stderr, "dbuser %s does not have create privilege in namespace %s.\n",
		  drms_env->session->db_handle->dbuser, namespace);
	  goto bailout;
	}
      } else {
	fprintf(stderr, "Failed: %s\n", stmt);
	goto bailout;
      }
    } else {
      fprintf(stderr, "Failed: %s\n", stmt);
      goto bailout;
    }      

    snprintf(stmt, bufsz, "update %s.drms_series set archive = %d where seriesname='%s'", namespace, archive, series);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }

    printf("Archive flag has been set to %d for series %s\n", archive, series);
    printf("Note this does not have any effect on existing records of this series.\n");
    break;
     case ADD_KEYWORD:
     {
        // assume the new keywords do not conflict with any existing
        // ones. otherwise all hell will break loose. 

        /* Here's how this works:
         * 1. recproto contains the content parsed from the "new" jsd - the one containing the new keywords.
         * 2. Remove the old template record from series_cache, but don't delete any parts of the old template record.
         *    This is achieved by temporarily setting the deep_free function to NULL, and calling drms_delete_series().
         *    cascade == 0 in the call to ensure that the original series table is not dropped. The old entries in 
         *    the drms_series, drms_keywords, etc. tables will also be deleted.
         * 3. Restore the deep_free function.
         * 4. Rename the old series table and sequence to *_tmp.
         * 5. Create the series based on the new template. This will make appropriate entries in drms_series, drms_keywords, 
         *    etc.
         * 6. Drop the newly created sequence and replace it with the *_tmp sequence saved in in step 4.
         * 7. Copy the series table data saved in step 4 to the newly created series table.
         */
 
        template = drms_template_record(drms_env, series, &status);

        if (template == NULL) {
           printf("Series '%s' does not exist. drms_template_record returned "
                  "status=%d\n",series,status);
           goto bailout;
        }

        /* must not rely upon template - drms_delete_series() will delete the template record when
         * hcon_remove is called. */
        // !!!!!!!!!
        // drms_create_recproto does NOT currently modify the pidx and dbidx fields in the series info to 
        // point to the newly created keywords in recproto
        // !!!!!!!!!
        recproto = drms_create_recproto(template, &status);
        drms_link_getpidx(recproto); 

        // save the original column names
        char *field_list = drms_field_list(recproto, NULL);
   
        // allocate for a record template

        /* Find rank of highest-ranked keyword */
        hiter_new_sort(&hit, &recproto->keywords, drms_keyword_ranksort);
        while ((key = hiter_getnext(&hit)) != NULL)
        {
           lastkey = key;
        }

        keynum = lastkey->info->rank + 1;

        // You can't call parse_keywords on recproto - recproto already has keywords in it!
        // You will get collisions with existing keywords. Must provide an empty record to 
        // parse_keywords, they copy over keywords from the new record to recproto.
        // status = parse_keywords(buf, recproto, NULL, &keynum);
        DRMS_Record_t newkws;
        HIterator_t kwiter;
        DRMS_Keyword_t *kw = NULL;
        DRMS_Keyword_t *keytempl = NULL;

        // Ugh - can't just pass in any old empty record. parse_keywords needs to use
        // the DRMS object hcontainers.

        /* Initialize container structure. */
        hcon_init(&newkws.segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
                  (void (*)(const void *)) drms_free_segment_struct, 
                  (void (*)(const void *, const void *)) drms_copy_segment_struct);
        /* Initialize container structures for links. */
        hcon_init(&newkws.links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
                  (void (*)(const void *)) drms_free_link_struct, 
                  (void (*)(const void *, const void *)) drms_copy_link_struct);
        /* Initialize container structure. */
        hcon_init(&newkws.keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
                  (void (*)(const void *)) drms_free_keyword_struct, 
                  (void (*)(const void *, const void *)) drms_copy_keyword_struct);

        status = parse_keywords(buf, &newkws, NULL, &keynum);

        free(buf);
        buf = NULL;

        if (status)
        {
           fprintf(stderr, "Invalid jsd file '%s'.\n", filename);
           goto bailout;
        }

        // Now copy keywords from newkws to recproto, overwriting if necessary (if a keyword exists both
        // in the new jsd and in the old series, overwrite)
        hiter_new_sort(&kwiter, &(newkws.keywords), drms_keyword_ranksort);
        while ((kw = hiter_getnext(&kwiter)) != NULL)
        {
           if ((keytempl = hcon_lookup_lower(&recproto->keywords, kw->info->name)) != NULL)
           {
              char *tmpst = strdup(kw->info->name);
              strtolower(tmpst);

              // hcon_remove will call drms_free_keyword_struct, which will free the string
              // value of string-type keywords
              hcon_remove(&recproto->keywords, tmpst);

              // but it won't free the info structure keytempl->info
              if (keytempl->info)
              {
                 free(keytempl->info);
              }

              free(tmpst);
           }

           // Now insert the keyword from newkws to recproto
           XASSERT(keytempl = hcon_allocslot_lower(&recproto->keywords, kw->info->name));
           memset(keytempl, 0, sizeof(DRMS_Keyword_t));
           drms_copy_keyword_struct(keytempl, kw);

           // Arg! Need to set the keyword record ptr to point to recproto
           keytempl->record = recproto;
        }

        // We removed, then added the keywords - must re-do the pidx and dbidx pointers in the
        // series info
        /* The series info must refer to The template */
        for (int i = 0; i < recproto->seriesinfo->pidx_num; i++) 
        {
           recproto->seriesinfo->pidx_keywords[i] = 
             drms_keyword_lookup(recproto, template->seriesinfo->pidx_keywords[i]->info->name, 0);
        }

        for (int i = 0; i < recproto->seriesinfo->dbidx_num; i++) 
        {
           recproto->seriesinfo->dbidx_keywords[i] = 
             drms_keyword_lookup(recproto, template->seriesinfo->dbidx_keywords[i]->info->name, 0);
        }

        // Clean up all the DRMS objects in newkws
        hcon_free(&newkws.segments);
        hcon_free(&newkws.links);
        hcon_free(&newkws.keywords);

        // Delete current series metadata without dropping the series table and
        // sequence. Take care not to free the template. Use cascade=0 in drms_delete_series to avoid dropping table.
        void (*pFn)(const void *);
        pFn = (drms_env->series_cache).deep_free; /* temp var to hold fn. */
        (drms_env->series_cache).deep_free = NULL;
        if (drms_delete_series(drms_env, series, 0, 1)) {
           fprintf(stderr, "Failed to remove previous definitions\n");
           status = 1;
           goto bailout;
        }

        // rename series table and sequence
        char tmptable[DRMS_MAXSERIESNAMELEN];

        snprintf(tmptable, sizeof(tmptable), "%s_tmp", series_short);
        snprintf(stmt,  bufsz, "alter table %s rename to %s_tmp; alter table %s_seq rename to %s_tmp_seq", series, series_short, series, series_short);
        //if ((qres = drms_query_txt(drms_env->session, stmt)) && qres->num_rows>0) {
        if(drms_dms(drms_env->session, NULL, stmt)) {
           fprintf(stderr, "Failed: %s\n", stmt);
           status = 1;
           goto bailout;
        }

#if 1
        // It turns out that you can't drop the index on recnum because the tmp table
        // has a constraint on that column - so onto plan B. Simply rename the indexes
        // which will later be dropped when the tmp table is dropped.

        // delete the indexes associated with the table - must do this, otherwise
        // drms_insert_series() will try to create indexes that already exist.
        // Ack, must find name of index - crap
        char *oid = NULL;
        char query[DRMS_MAXQUERYLEN];
        char indexname[128];
        int irow;
        char *indexst = NULL;
        DB_Binary_Result_t *bres = NULL;

        if (GetTableOID(drms_env, namespace, tmptable, &oid) || !oid)
        {
           fprintf(stderr, "Unable to get OID for '%s'\n", series);
           status = 1;
           goto bailout;
        }

        // First, need to remove the prime key constraint on the tmp table, because we are going to delete the 
        // index which serves as a constraint.
        snprintf(query, sizeof(query), "SELECT conname FROM pg_constraint WHERE conrelid = %s", oid);
        if ((bres = drms_query_bin(drms_env->session, query)) == NULL)
        {
           fprintf(stderr, "Invalid database query: '%s'\n", query);
           status = 1;
           goto bailout;
        }
        else
        {
           char conname[128];

           for (irow = 0; irow < bres->num_rows; irow++)
           {
              // The name of the constraint is in column 0
              db_binary_field_getstr(bres, irow, 0, sizeof(conname), conname); 
              snprintf(stmt, sizeof(stmt), "ALTER TABLE %s.%s DROP CONSTRAINT %s", namespace, tmptable, conname);
              if (drms_dms(drms_env->session, NULL, stmt))
              {
                 fprintf(stderr, "Unable to drop constraints on temporary table.\n");
                 status = 1;
                 goto bailout;
              }
           }           

           db_free_binary_result(bres);
        }

        // Get the names of the indexes on the temporary table and drop them.
        snprintf(query, sizeof(query), "SELECT c2.relname FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i WHERE c.oid = '%s' AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND i.indisvalid = 't' ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname", oid);

        if ((bres = drms_query_bin(drms_env->session, query)) == NULL)
        {
           fprintf(stderr, "Invalid database query: '%s'\n", query);
           status = 1;
           goto bailout;
        }
        else
        {
           if (bres->num_cols != 1)
           {
              fprintf(stderr, "Unexpected database response to query '%s'\n", query);
              status = 1;
              goto bailout;
           }
           else
           {
              bufsize = bufsz;
              indexst = (char *)calloc(bufsize, 1);

              for (irow = 0; irow < bres->num_rows; irow++)
              {
                 /* The name of the index is in column 0 (relname) */
                 db_binary_field_getstr(bres, irow, 0, sizeof(indexname), indexname); 

                 if (irow != 0)
                 {
                    base_strcatalloc(indexst, ",", &bufsize);
                 }

                 base_strcatalloc(indexst, namespace, &bufsize);
                 base_strcatalloc(indexst, ".", &bufsize);
                 base_strcatalloc(indexst, indexname, &bufsize);
              }
           }

           db_free_binary_result(bres);
        }

        snprintf(stmt, bufsz, "DROP INDEX IF EXISTS %s", indexst);

        if (drms_dms(drms_env->session, NULL, stmt))
        {
           fprintf(stderr, "Unable to drop indexes '%s'\n", indexst);
           if (indexst)
           {
              free(indexst);
           }

           if (oid)
           {
              free(oid);
           }

           status = 1;
           goto bailout;
        }

        if (indexst)
        {
           free(indexst);
        }

        if (oid)
        {
           free(oid);
        }
#else
        // Rename the indexes
        "ALTER INDEX %s rename to %s_tmp";
#endif

        // restore deep_free function
        (drms_env->series_cache).deep_free = pFn;

        // retain the spot in the series_cache. this is a hack to undo
        // hcon_remove()
        // Enclosed the hcon_remove() in an 'if (cascade)' stmt, so can remove this hack.
        /* Adding to the series cache is superfluous because nobody is going to consult the cache later
         * in this module - but it won't hurt either. */
        // hcon_allocslot_lower(&drms_env->series_cache, series);
        drms_print_record(recproto);

        // create a new series
        //        if (drms_insert_series(drms_env->session, 0, recproto, 0)) {
        if (drms_create_series_fromprototype(&recproto, series, 0))
        {
           fprintf(stderr, "Failed to create new definitions\n");
           status = 1;
           goto bailout;
        }

        // drop newly created series sequence
        snprintf(stmt, bufsz, "drop sequence %s_seq; alter table %s_tmp_seq rename to %s_seq", series, series, series_short);
        if(drms_dms(drms_env->session, NULL, stmt)) {
           fprintf(stderr, "Failed: %s\n", stmt);
           status = 1;
           goto bailout;
        }

        // move back contents of series
        //snprintf(stmt, bufsz, "insert into %s (%s) select %s from %s_tmp", series, field_list, field_list, series);
        //buffer size could be huge, above statment doesn't have enough space.  Need to build with base_strcatalloc instead.
        char *stmt2 = malloc(DRMS_MAXQUERYLEN);
        bufsize = DRMS_MAXQUERYLEN;
        memset (stmt2, 0, bufsize);
        base_strcatalloc(stmt2, "insert into ", &bufsize);
        base_strcatalloc(stmt2, series, &bufsize);
        base_strcatalloc(stmt2, " (", &bufsize);
        base_strcatalloc(stmt2, field_list, &bufsize);
        base_strcatalloc(stmt2, ") ", &bufsize);
        base_strcatalloc(stmt2, "select ", &bufsize);
        base_strcatalloc(stmt2, field_list, &bufsize);
        base_strcatalloc(stmt2, " from ", &bufsize);
        base_strcatalloc(stmt2, series, &bufsize);
        base_strcatalloc(stmt2, "_tmp", &bufsize);

        if(drms_dms(drms_env->session, NULL, stmt2)) {
           fprintf(stderr, "Failed: %s\n", stmt2);
           status = 1;
           goto bailout;
        }
        free(field_list);
        free(stmt2);

        srand((unsigned int) time(NULL)+getpid());
        snprintf(stmt, bufsz, "create table backup.%s_%s_%d as select * from %s_tmp;drop table %s_tmp", namespace, series_short, rand(), series, series);
        if(drms_dms(drms_env->session, NULL, stmt)) {
           fprintf(stderr, "Failed: %s\n", stmt);
           status = 1;
           goto bailout;
        }
     }
    break;
  default:
    printf("Don't know what to do, give up...\n");
    status = 1;
  }

 bailout:
  free(series_short);
  free(namespace);
  if (qres) 
    db_free_text_result(qres);
  if (buf)
  {
     free(buf);
  }

  return status;

 usage:
  printf("Usage: %s archive=[1|0] series=seriesname\n", cmdparams_getarg (&cmdparams, 0));
  printf("       %s series=seriesname filename\n", cmdparams_getarg (&cmdparams, 0));
  return 1;
}
    
