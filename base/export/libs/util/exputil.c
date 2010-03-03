#include "jsoc.h"
#include "exputil.h"
#include "drms_keyword.h"

ExpUtlStat_t exputl_mk_expfilename(DRMS_Segment_t *seg, 
                                   const char *filenamefmt, 
                                   char *filename)
{
   ExpUtlStat_t ret = kExpUtlStat_Success;
   char *fn = filename;
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
         char *val;
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
            if (strcmp(keyname,"seriesname")==0)
              val = seg->record->seriesinfo->seriesname;
            else if (strcmp(keyname,"recnum")==0)
            {
               snprintf(valstr, sizeof(valstr), (layout ? layout : "%lld"), 
                        seg->record->recnum); 
               val = valstr;
            }
            else if (strcmp(keyname,"segment")==0)
              val = seg->filename;
            else
              val = drms_getkey_string(seg->record,keyname,NULL);
            if (!val)
            {
               ret = kExpUtlStat_InvalidFmt;
               val = "ERROR";
            }
            for (p=val; *p; )
            {
               *fn++ = *p++;
            }
            *fn = '\0';
         }
         fmt = last+1;
      }
      else
        *fn++ = *fmt++;
   }
   *fn = '\0';

   return ret;
}
