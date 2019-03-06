#include "jsoc.h"
#include "exputil.h"
#include "drms_keyword.h"

/* File name generator used for exports from DRMS.
 * Takes a segment pointer and filename template.
 * Returns generated filename.
 *
 * Template rules:
 * Any char not enclosed in '{}' is used explicitly.
 * The first word, up to an optional ':' must be one of the
 * special proxy names or a keyword from the record associated
 * with the provided segment.
 * For keywords, #, and recnum, a display format may be provided after a ':'.
 * The special proxy keyword names are:
 *   seriesname which returns the seriesname
 *   segment which returns the segment filename
 *   recnum which returns the record number.
 *   # which returns the ordinal number of the filename generated within this run of the calling program.
 * The optional display format is a 'printf' style format.
 * The default format for recnum is '%lld'
 * The default format for # is '%05d'
 * The default format for all keywords is the JSD display format for that keyword.
 * Times allow both precision and zone specifiers in the display format.
 * The two allowed quantities, precision and zone are separated with a comma, ','
 * Both time quantities are optional with precision defaulting to 0 and zone to the JSD value.
 * The precision is an integer following the rules for sprint_time.
 * Thus a positive precision is the number of digits of fractional seconds.
 * and a negative precision denotes how many fields to omit from the right.
 * A -1 omits seconds, -2 minutes, etc.
 * A special modification letter may preceed the precision.
 * At present the only options are 'A' for 'alternate' format
 * and 'D' for directory format.  'A' casues the time component separators '.' and ':' to be omitted.
 * 'D' does the same but surrounds the 3 date fields with '@' chars.
 * The names made with the 'D' can be easily scripted to mode the export files
 * into date based directory trees.
 * Thus a time keyword of T_REC for mdi.fd_M of {T_REC:A-1} would make
 * a filename component of '19960624_1230_TAI'
 *
 */

/* For series with linked records, always use the source record for keyword names, segment names, etc., 
 * and use the target record for keyword values, etc. To achieve this, 'rec' is the source-series record, 
 * and 'segname' is the name of the segment in the sourc series. Obtain the target record by 
 * "following links" when using functions to look-up keywords, for example. */
ExpUtlStat_t exputl_mk_expfilename(DRMS_Segment_t *srcseg,
                                   DRMS_Segment_t *tgtseg, /* If the segment is a linked segment, then tgtseg is the 
                                                            * segment in the target series. */
                                   const char *filenamefmt, 
                                   char *filename)
{
   static int namesMade = 0;
   ExpUtlStat_t ret = kExpUtlStat_Success;
   char filenameWorking[128] = {0};
   char *fn = filenameWorking;
   char format[1024];
   char *fmt;
    
   if (filenamefmt)
     snprintf(format, sizeof(format), "%s", filenamefmt);
   else
     snprintf(format, sizeof(format), "{seriesname}.{recnum:%%lld}.{segment}");

   fmt = format;
   *fn = '\0';
   while (*fmt)
      {
      char *last;
      if (*fmt == '{')
         {
         char *val = NULL;
         char *p;
         char *keyname;
         char *layout;
         last = index(fmt, '}');
         if (!last)
            {
            ret = kExpUtlStat_InvalidFmt;
            break;
            }

         keyname = ++fmt;
         layout = NULL;
         *last = '\0';
         for (p=keyname; p<last; p++)
            {
            if (*p == ':')
               {
               *p++ = '\0';
               layout = p;
               }
            }
         if (*keyname)
            {
            char valstr[128];
            char *tmpstr = NULL;
            char tmpstr2[128];

            if (strcmp(keyname, "#") == 0) // insert record count within current program run
               {
               snprintf(valstr, sizeof(valstr), (layout ? layout : "%05d"), namesMade++);
               val = valstr;
               }
            else if (strcmp(keyname,"seriesname")==0)
               val = srcseg->record->seriesinfo->seriesname;
            else if (strcmp(keyname,"recnum")==0)
               {
               snprintf(valstr, sizeof(valstr), (layout ? layout : "%lld"), 
                        srcseg->record->recnum); 
               val = valstr;
               }
            else if (strcmp(keyname,"segment")==0)
            {
               if (srcseg->info->islink)
               {
                  val = tgtseg->filename;
               }
               else
               {
                  val = srcseg->filename;
               }
                
                if (!val || !*val)
                {
                    /* Use the segment name, just like drms_segment_filename() does. */
                    val = srcseg->info->name;
                }
            }
            // At this point the keyname is a normal keyword name.
            else if (layout) // use user provided format to print keyword.  User must be careful.
              {
                 /* drms_keyword_lookup will properly follow a link to the target series to
                  * obtain the target key and keyword value. */
              DRMS_Keyword_t *key = drms_keyword_lookup(srcseg->record,keyname,1);

              if (!key)
              {
                  ret = kExpUtlStat_UnknownKey;
                  val = "ERROR";
              }
              else if (key->info->type == DRMS_TYPE_TIME)
                { // do special time formats here 
                char formatwas[DRMS_MAXFORMATLEN], unitwas[DRMS_MAXUNITLEN];
                int precision = 0;
                char Mod = ' ';
                strncpy(formatwas, key->info->format, DRMS_MAXFORMATLEN);
                strncpy(unitwas, key->info->unit, DRMS_MAXUNITLEN);
                if (isalpha(*layout)) Mod = *layout++;
                if (isdigit(*layout) || *layout == '-')
                  precision = strtol(layout, &layout, 10);
                snprintf(key->info->format, DRMS_MAXFORMATLEN, "%d", precision);
                if (*layout == ',' && *(layout+1))
                  strncpy(key->info->unit,layout+1,DRMS_MAXUNITLEN);

                /* avoid leaks */
                if (srcseg->info->islink)
                {
                   tmpstr = drms_getkey_string(tgtseg->record, keyname, NULL);
                }
                else
                {
                   tmpstr = drms_getkey_string(srcseg->record, keyname, NULL);
                }

                snprintf(tmpstr2, sizeof(tmpstr2), "%s", tmpstr);
                free(tmpstr);
                val = tmpstr2;

                strncpy(key->info->format, formatwas, DRMS_MAXFORMATLEN);
                strncpy(key->info->unit, unitwas, DRMS_MAXUNITLEN);
                if (Mod != ' ')
                   { 
                   if (Mod == 'A')
                     {
                     int i;
                     char *cp;
                     for (i=0, cp=val; *cp; cp++)
                       if (*cp != '.' && *cp != ':')
                         valstr[i++] = *cp;
                     valstr[i] = '\0';
                     val = valstr;
                     }
                   else if (Mod == 'D')
                     {
                     int i;
                     char *cp;
                     valstr[0] = '@';
                     for (i=1, cp=val; *cp; cp++)
                       {
                       if (*cp == ':') continue;
                       if (*cp == '.' || i == 11)
                         valstr[i++] = '@';
                       else
                         valstr[i++] = *cp;
                       }
                     valstr[i] = '\0';
                     val = valstr;
                     }
                   // else ignore unrecognized Mod char
                   }
                else
                  {
                  strncpy(valstr, val, 128);
                  // free(val);
                  val = valstr;
                  }
                }
              else
                {
                   /* non-time keyword types */
                char formatwas[DRMS_MAXFORMATLEN];
                DRMS_Keyword_t *followedKey = NULL;
                char *pCh = NULL;
                char *pVal = NULL;

                strncpy(formatwas, key->info->format, DRMS_MAXFORMATLEN);
                strncpy(key->info->format,layout,DRMS_MAXFORMATLEN);
                followedKey = drms_keyword_lookup(srcseg->record, keyname, 1);
                    
                if (followedKey)
                    {
                        *tmpstr2 = '\0';
                        drms_keyword_snprintfval(followedKey, tmpstr2, sizeof(tmpstr2));
                        
                        /* Strip-out whitespace. */
                        for (pCh = tmpstr2, pVal = valstr; *pCh && pVal < valstr + sizeof(valstr) - 1; pCh++)
                        {
                            if (!isspace(*pCh))
                            {
                                *pVal = *pCh;
                                pVal++;
                            }
                        }
                        
                        *pVal = '\0';
                        
                        val = valstr; // To be consistent with other cases above.
                    }
                    
                    strncpy(key->info->format, formatwas, DRMS_MAXFORMATLEN);
                }
              }
            else // No user provided layout string
            {
               /* drms_keyword_lookup will properly follow the keyword to the target series. */
               DRMS_Keyword_t *key = drms_keyword_lookup(srcseg->record, keyname, 1);

               if (key)
               {
                  drms_keyword_snprintfval(key, tmpstr2, sizeof(tmpstr2));
                  val = tmpstr2;
               }
               else
               {
                  ret = kExpUtlStat_UnknownKey;
                  val = "ERROR";
               }
            }

            /* At this point, val should have a valid string in it, and val might 
             * actually be pointing to valstr which contains the valid string, but 
             * it might not. */
            if (!val)
               {
               ret = kExpUtlStat_InvalidFmt;
               val = "ERROR";
               }
#if 0
            /* Why copy back to valstr when the valid string is in 
             * val already? Plus this is causing memory corruption - you can't copy from val
             * to valstr if both pointers point to the same address. */
            else
               {
               strncpy(valstr, val, 128);
               // free(val);
               val = valstr;
               }
#endif
                if (strlen(filenameWorking) + strlen(val) < sizeof(filenameWorking))
                {
                    for (p=val; *p; )
                    {
                        *fn++ = *p++;
                    }
                    *fn = '\0';
                }
                else
                {
                    /* Return some kind of overflow error. */
                    ret = kExpUtlStat_InvalidFmt;
                }
            }
         fmt = last+1;

         val = NULL;
         }
      else
          {
              if (strlen(filenameWorking) + strlen(fmt) < sizeof(filenameWorking))
              {
                  *fn++ = *fmt++;
              }
              else
              {
                  /* Return some kind of overflow error. */
                  ret = kExpUtlStat_InvalidFmt;
              }
          }
      }
   *fn = '\0';
    
    if (filename)
    {
        /* Just gotta hope this doesn't overrun the output buffer. */
        /* Replace remaining whitespace with underscores. */
        char *pIn = NULL;
        char *pOut = NULL;
        
        for (pIn = filenameWorking, pOut = filename; *pIn ; pIn++)
        {
            if (!isspace(*pIn))
            {
                *pOut = *pIn;
                pOut++;
            }
        }
        
        *pOut = '\0';
        
        
        snprintf(filename, sizeof(filenameWorking), "%s", filenameWorking);
    }

   return ret;
}

// Code from jsoc_manage_cgibin_handles. It is not easy to choose x86_64 vs. avx in a CGI context, and jsoc_info had assumed everything was x86_64.
// Instead, port the code here (there wasn't that much of it) so that jsoc_info has access to the needed functions built for the correct
// architecture.
#define HANDLE_FILE_BASE "Web_request_handles"

static FILE *lock_open(const char *filename)
{
    int sleeps;
    char lockfile[1024];

    snprintf(lockfile, sizeof(lockfile), "%s.lck", filename);
    
    FILE *fp = fopen(lockfile, "w");
    if (!fp)
    {
        fprintf(stderr, "Failed to open file for locking %s.\n", lockfile);
        return NULL;
    }
    
    for (sleeps=0; lockf(fileno(fp), F_TLOCK, 0); sleeps++)
    {
        if (sleeps >= 300)
        {
            fprintf(stderr,"Lock on %s failed.\n", lockfile);
            return NULL;
        }
        sleep(1);
    }
    
    return(fp);
}

static lock_close(FILE *fp)
{
    lockf(fileno(fp), F_ULOCK, 0);
    fclose(fp);
}

ExpUtlStat_t exputl_manage_cgibin_handles_add_proc_handle(const char *file, const char *handle, pid_t pid)
{
    FILE *fp = NULL;
    FILE *fp_lock = NULL;
    
    fp_lock = lock_open(file);
    if (!fp_lock)
    {
        return kExpUtlStat_ManageHandles;
    }
    
    fp = fopen(file, "a");
    if (!fp)
    {
        fprintf(stderr, "Failed to open %s file.\n", file);
        return kExpUtlStat_ManageHandles;
    }
    
    fprintf(fp, "%s\t%d\n", handle, pid);
    fclose(fp);
    lock_close(fp_lock);
    return kExpUtlStat_Success;
}

char *exputl_manage_cgibin_handles_lookup_proc_handle(const char *file, const char *handle)
{
    FILE *fp = NULL;
    FILE *fp_lock = NULL;
    char PID[128];
    char HANDLE[128];
    char *pid = NULL;
    
    fp_lock = lock_open(file);
    if (!fp_lock)
    {
        return NULL;
    }
    
    fp = fopen(file, "r");
    if (!fp)
    {
        fprintf(stderr, "Failed to open %s file.\n", file);
        return NULL;
    }
    
    while (fscanf(fp, "%s\t%s\n", HANDLE, PID) == 2)
    {
        if (strcmp(handle, HANDLE) == 0)
        {
            pid = strdup(PID);
            break;
        }
    }
    
    fclose(fp);
    lock_close(fp_lock);
    return pid;
}

ExpUtlStat_t exputl_manage_cgibin_handles_delete_proc_handle(const char *file, const char *handle)
{
    FILE *fp = NULL;
    FILE *fp_lock = NULL;
    char cmd[1024];
    
    fp_lock = lock_open(file);
    if (!fp_lock)
    {
        return kExpUtlStat_ManageHandles;
    }
    
    sprintf(cmd, "ed -s %s <<END\ng/^%s\t/d\nwq\nEND\n", file, handle);
    system(cmd);
    lock_close(fp_lock);
    return kExpUtlStat_Success;
}

ExpUtlStat_t exputl_manage_cgibin_handles_edit_proc_handles(const char *file)
{
    FILE *fp = NULL;
    FILE *fp_lock = NULL;
    char cmd[1024];
    
    fp_lock = lock_open(file);
    if (!fp_lock)
    {
        return kExpUtlStat_ManageHandles;
    }
    
    sprintf(cmd, "vi %s\n", file);
    system(cmd);
    lock_close(fp_lock);
    return kExpUtlStat_Success;
}

ExpUtlStat_t exputl_manage_cgibin_handles(const char *op, const char *handle, pid_t pid, const char *file)
{
    int op_add = 0;
    int op_delete = 0;
    int op_edit = 0;
    int op_lookup = 0;

    if (!op)
    {
        fprintf(stderr, "No operation provided.\n");
        return kExpUtlStat_ManageHandles;
    }
    else if (strlen(op) != 1)
    {
        fprintf(stderr, "Invalid operation: %s\n", op);
        return kExpUtlStat_ManageHandles;
    }
    else
    {
        if (!file || *file == '\0')
        {
           char *lockfile = calloc(1, PATH_MAX);

           base_strlcat(lockfile, DRMS_LOCK_DIR, PATH_MAX);

           if (lockfile[strlen(lockfile) - 1] != '/')
           {
              base_strlcat(lockfile, "/", PATH_MAX);
           }

           base_strlcat(lockfile, HANDLE_FILE_BASE, PATH_MAX);
           file = lockfile;
        }
        
        op_add = (*op == 'a');
        op_delete = (*op == 'd');
        op_edit = (*op == 'e');
        op_lookup = (*op == 'l');
        
        if (!op_edit && !handle)
        {
            fprintf(stderr, "handle must be provided.\n");
            return kExpUtlStat_ManageHandles;
        }
        
        if (op_add)
        {
            return exputl_manage_cgibin_handles_add_proc_handle(file, handle, pid);
        }
        
        if (op_lookup)
        {
            char *pid = exputl_manage_cgibin_handles_lookup_proc_handle(file, handle);
            
            if (pid)
            {
                printf("%s\n", pid);
            }
            
            fflush(stdout);
            return kExpUtlStat_Success;
        }
        
        if (op_delete)
        {
            return exputl_manage_cgibin_handles_delete_proc_handle(file, handle);
        }
        
        if (op_edit)
        {
            return exputl_manage_cgibin_handles_edit_proc_handles(file);
        }
    }
    
    return kExpUtlStat_Success;
}
