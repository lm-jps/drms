#include "jsoc_main.h"
#include "drms_types.h"
#include "drms_storageunit.h"
#include "exputil.h"
#include "fitsexport.h"

#include "defs.h"
REGISTERSTRINGSPREFIX
#include "data/export.defs"
REGISTERSTRINGSSUFFIX

/**
@defgroup expfits jsoc_export_as_fits - Export internally stored data to a FITS file
@ingroup su_export

@brief This module exports DRMS keywords and data segments into one or more FITS files. The resulting files are completely self-contained. DRMS keywords have been converted to FITS keywords. The caller can specify the compression parameters to use when creating the FITS file.

@par Synopsis:
@code
stand-alone mode:
jsoc_export_as_fits rsquery=<recset query> n=<limit> reqid=<export request id> expversion=<version>
     method=<exp method> protocol=<output-file protocol> path=<output path&gt
     { ffmt=<filename format> } { kmclass=<keymap class> } { kmfile=<keymap file> }
     { cparms=<compression string list> }

or

export-series mode:
jsoc_export_as_fits reqid=<export request id> expversion=<version> method=<exp method>
     protocol=<output-file protocol> { expseries=<exp series> } { kmclass=<keymap class> }
     { kmfile=<keymap file> } { cparms=<compression string list> }
@endcode

For each segment for each record specified by a record-set query, this module creates a FITS file. The FITS
file contains a copy of the image data that was contained in the original internal SUMS file. It also
contains FITS keywords derived via conversion from the original DRMS keywords. The conversion process
is described in detail in ::drms_keyword_getmappedextname. Succinctly, if @a kmfile is provided,
the argument identifies a file which contains a keyword map which is used to map the DRMS keywords
to FITS keywords. If @a kmfile is not provided, then if @a kmclass is provided,
this argument identifies a keyword-map class, which is a name that identifies a predefined
keyword map which is then used to map the DRMS keywords to FITS keywords. Otherwise, if the
DRMS keyword being converted has a description field that contains
a string of the form [X{:Y}], then if X is a valid FITS keyword, it is used. Otherwise, if the
DRMS keyword name is a valid FITS keyword name, it is used. Otherwise, an algorithm is used to automatically
generate a FITS keyword name.

The name of the output file is determined from a filename format string. The format contains a series
of substrings, each of which starts with '{', is followed by either "seriesname", "segment", "recnum",
or "recnum:<format specifier>;", and ends with '}'. Each of these strings may appear
as many times and in any order in the overall format string.
Each of these strings serves as a placeholder that will be replaced by
an appropriate string value. For example, if the file being exported originates from a series named
"su_arta.testfits", then "{seriesname}" will be evaluated to "su_arta.testfits". "{segment}"
evaluates to the name of the segment being exported, and "{recnum}" evaluates to the
record number of the record being exported. The optional &lt;format specifier&gt; in
"{recnum:<format specifier>}" is the printf format specifier used to convert the record number into
a string (by default "%lld" is used if the format specifier is ommitted). As an example, to
generate a filename like mdi.fd_M_96m_lev18.1111.data.fits, the format string
"{seriesname}.{recnum:%d}.{segment}" would suffice.

This module provides two ways to export data. When run in "stand-alone" mode, the exported FITS
files are written to a directory specified by the @a path argument, or to the working directory
if no @a path argument is present. To use this mode, the @a rsquery argument must be present. This
query identifies the DRMS records and segments to be exported. Under this mode, the module does not assume
the existence of an export series (see "export-series" mode). It simply exports to a
specified directory. As such, the
@a expseries argument is ignored if present. An optional @a ffmt argument contains the
filename format string. If this optional argument is not present, a default
format string of "{seriesname}.{recnum:%lld}.{segment}" is assumed.

When the module is run in "export-series" mode,
the record-set query that identifies records to export
originates from an "export series", not from the command-line. An export series is a
DRMS series established by an administrator. It is used to track exports, and in fact the
SUNUM values of this series' records point to directories where the exported FITS files
temporarily reside. A dbase query for the
request ID (every export request is assigned a unique request ID) in the export series
returns a record which contains the
record-set query and the output filename format string (at the very least, the keywords
rsquery - the record-set query - and ffmt - the filename format string -
must exist in the export series). This export-series record is cloned, and the
output files are written to the clone's SUDIR.
To use the export-series mode, the @a rsquery argument must NOT be present. The export series
is identified by the @a expseries argument if it is present, otherwise it defaults to jsoc.export.
Please see ???? for more information about the general export process.

@a cparms is a comma-separated list of strings. Each string is either
a CFITSIO compression string (please see tile-compression arguments in
http://jsoc.stanford.edu/jsocwiki/Jsd for more information
about this string), or the string "**NONE**". "**NONE**" indicates that the
output file(s) should not be compressed. Each string in this list is relevant to
one of the segments being exported, the order of the strings matches the order of the
segments as determined by the original .jsd used to create the segment.

This module writes, into the export directory, a "packing list"
which is a file that contains metadata about the export. This
information includes the request ID of the export, the version of the export code,
the export method, and the export protocol. These data are passed into the module via the
@a reqid, @a expversion, @a method, and @a protocol arguments, which are required regardless
of the mode under which the module is run. The packing list also contains five other
metadata which are caculated/determined during the module run: a count of the total
number of files exports, the total size in bytes of the exported files, the time of
export completion, the directory to which the files were exported (provided by the @a path
argument to the module), and the status of the export (0 implies success). Finally,
the packing list contains a two-column table of output files. The first column
is a record-set query, and the second column is a single exported file. The
record-set query uniquely identifies the output file.

@par Flags:
This module has no flags.

@par GEN_FLAGS:
Ubiquitous flags present in every module.
@ref jsoc_main

@param rsquery A DRMS record-set query that identifies a set of records and segments
to export.
@param reqid A string that uniquely identifies an export request. This string appears
in the output packing list.
@param expversion The version of the export code calling this module. This string appears
in the output packing list.
@param method The method of export, such as url or url_quick. This string appears
in the output packing list.
@param n Record count limit.
@param protocol The type of output file to produce. This must be FITS. This string appears
in the output packing list.
@param path The directory to which output files are to be written. If present, this string appears
in the output packing list.
@param ffmt The filename format string which defines the name of the output file(s).
@param kmclass Identifies a keyword-map class, which is a name that identifies a predefined
keyword map which is then used to map the DRMS keywords to FITS keywords.
@param kmfile  Identifies a file which contains a keyword map which is used to map the DRMS keywords
to FITS keywords.
@param cparms A comma-separated list of strings. Each string is either
a CFITSIO compression string or the string "**NONE**".

@par Exit_Status:
@c 0 success<br>
@c 1 cannot register definition file<br>
@c 2 missing required argument<br>
@c 3 invalid request ID<br>
@c 4 invalid record-set query<br>
@c 5 invalid filname format string<br>
@c 6 failure to write export file<br>
@c 7 cannot open packing list file<br>
@c 8 failure writing packing list<br>
@c 9 unsupported packing list element type

@par Example:
Running in the stand-alone mode:
@code
jsoc_export_as_fits reqid=JSOC_20090514_001 expversion=0.5 rsquery='su_phil.fd_M_daily[1996.07.4]'
     path=/tmp/jsocexptest ffmt='{seriesname}.{recnum:%d}.{segment}' method=url_quick
     protocol=FITS cparms="**NONE**"
@endcode
*/

char *module_name = "export";

typedef enum
{
   kMymodErr_Success = 0,
   kMymodErr_CantRegisterDefs,
   kMymodErr_MissingArg,
   kMymodErr_BadRequestID,
   kMymodErr_BadRecSetQuery,
   kMymodErr_BadFilenameFmt,
   kMymodErr_ExportFailed,
   kMymodErr_CantOpenPackfile,
   kMymodErr_PackfileFailure,
   kMymodErr_UnsupportedPLRecType,
   kMymodErr_DRMS,
   kMymodErr_MissingSegFile
} MymodError_t;

typedef enum
{
   kPL_metadata,
   kPL_content
} PLRecType_t;

#define kNotSpecified    "NOT SPECIFIED"
#define kArg_reqid       "reqid"
#define kArg_version     "expversion"
#define kArg_method      "method"
#define kArg_protocol    "protocol"
#define kArg_rsquery     "rsquery"
#define kArg_n      	 "n"
#define kArg_expSeries   "expseries"
#define kArg_ffmt        "ffmt"
#define kArg_path        "path"
#define kArg_clname      "kmclass"
#define kArg_kmfile      "kmfile"
#define kArg_cparms      "cparms"


#define kDef_expSeries   "jsoc.export"

#define kPWD             "PWD"

#define kNoCompression   "**NONE**"

#define kMB              (1048576)

/* If rsquery is provided as a cmd-line argument, then jsoc_export does not
 * save the output data files to an export series.  Instead the caller
 * MUST provide a filename format string (which is a template that
 * describes how to create the output file names).  The caller may also
 * provide the path argument to specify the directory into which the
 * output files are to be written.  If the path argument is not specified,
 * then the output files are written to the current working directory.
 * If requery is omitted from the cmd-line, then an export series
 * must either be provided on the cmd-line, or the default series is used.
 * The query that determines what files are to be exported is found
 * in the export series by searching for the record with a matching
 * reqid.
 */
ModuleArgs_t module_args[] =
{
     {ARG_STRING, kArg_version, kNotSpecified, "jsoc export version."},
     {ARG_STRING, kArg_reqid, kNotSpecified, "Export series primary key value that identifies the output record."},
     {ARG_STRING, kArg_method, kNotSpecified, "jsoc export method (eg, url or ftp)."},
     {ARG_STRING, kArg_protocol, kNotSpecified, "file conversion method (eg, convert to fits)."},
     {ARG_STRING, kArg_rsquery, kNotSpecified, "Record-set query that specifies data to be exported."},
     {ARG_STRING, kArg_expSeries, kDef_expSeries, "Series to which exported data are saved."},
     {ARG_STRING, kArg_ffmt, kNotSpecified, "Export filename template."},
     {ARG_STRING, kArg_path, kNotSpecified, "Path to which fits files are output."},
     {ARG_STRING, kArg_clname, kNotSpecified, "Export key map class."},
     {ARG_STRING, kArg_kmfile, kNotSpecified, "Export key map file."},
     {ARG_STRING, kArg_cparms, kNotSpecified, "FITS-standard compression string used to compress exported image."},
     {ARG_INT, kArg_n, "0", "Record count limit."},
     {ARG_END}
};

char gDefBuf[PATH_MAX] = {0};

static void JEAFPrintLocalTime(FILE *stm, const char *msg)
{
    char tbuf[64];
    time_t sounnecessarilycomplicated;
    struct tm *ltime = NULL;

    time(&sounnecessarilycomplicated);
    ltime = localtime(&sounnecessarilycomplicated);

    *tbuf = '\0';
    if (ltime)
    {
        snprintf(tbuf, sizeof(tbuf), "%s", asctime(ltime));
        fprintf(stm, "%s - %s\n", tbuf, msg);
    }
}

/* Convert number of bytes to number of MB. If number of bytes < 1MB, then return 1. */
static long long ToMB(long long nbytes)
{
   if (nbytes <= kMB && nbytes > 0)
   {
      return 1;
   }

   return (long long)((nbytes + 0.5 * kMB) / kMB);
}

MymodError_t WritePListRecord(PLRecType_t rectype, FILE *pkfile, const char *f1, const char *f2)
{
   MymodError_t err = kMymodErr_Success;

   switch (rectype)
   {
      case kPL_metadata:
        fprintf(pkfile, "%s=%s\n", f1, f2);
        break;
      case kPL_content:
        fprintf(pkfile, "%s\t%s\n", f1, f2);
        break;
      default:
        fprintf(stderr, "Unsupported packing-list record type '%d'.\n", (int)rectype);
        err = kMymodErr_UnsupportedPLRecType;
   }

   return err;
}

/* Assumes tcount is zero on the first call.  This function adds
 * the number of files exported to tcount on each call. */
static unsigned long long MapexportRecordToDir(DRMS_Record_t *recin, const char *ffmt, const char *outpath, FILE *pklist, const char *classname, const char *mapfile, int *tcount, const char **cparms, MymodError_t *status, char **errmsg)
{
   int drmsstat = DRMS_SUCCESS;
   MymodError_t modstat = kMymodErr_Success;
   DRMS_Segment_t *segin = NULL;
   DRMS_Segment_t *tgtseg = NULL; /* If segin is a linked segment, then tgtset is the segment in the target series. */
   unsigned long long tsize = 0;
    unsigned long long expsize = 0;
    char *actualfname = NULL;
   char dir[DRMS_MAXPATHLEN];
   char fmtname[DRMS_MAXPATHLEN];
   char fullfname[DRMS_MAXPATHLEN];
   char query[DRMS_MAXQUERYLEN];
   HIterator_t *last = NULL;
   int iseg;
   int lastcparms;
   int count;
    char buf[256];
   ExpUtlStat_t expfn = kExpUtlStat_Success;

   //    JEAFPrintLocalTime(stdout, "Calling drms_record_directory() from MapexportRecordToDir().");
   drms_record_directory(recin, dir, 1); /* This fetches the input data from SUMS. */
   //    JEAFPrintLocalTime(stdout, "Done calling drms_record_directory() from MapexportRecordToDir().");

   /* Must create query from series name and prime keyword values */
   drms_sprint_rec_query(query, recin);

   /* The input rs query can specify a subset of all the series' segments -
    * this is encapsulated in recin. */

   iseg = 0;
   lastcparms = 0;
   count = 0;

   while ((segin = drms_record_nextseg(recin, &last, 0)) != NULL)
   {
      if (segin->info->islink)
      {
         if ((tgtseg = drms_segment_lookup(recin, segin->info->name)) == NULL)
         {
            fprintf(stderr, "Unable to locate target segment %s.\n", segin->info->name);
            iseg++;
            continue;
         }
      }
      else
      {
         tgtseg = segin;
      }

      if ((expfn = exputl_mk_expfilename(segin, tgtseg, ffmt, fmtname)) == kExpUtlStat_Success)
      {
         snprintf(fullfname, sizeof(fullfname), "%s/%s", outpath, fmtname);
      }
      else
      {
         if (expfn == kExpUtlStat_InvalidFmt)
         {
            fprintf(stderr, "Invalid file-name format template '%s'.\n", ffmt);
         }
         else if (expfn == kExpUtlStat_UnknownKey)
         {
            fprintf(stderr, "One or more keywords in the file-name format template '%s' do not exist in series '%s'.\n", ffmt, recin->seriesinfo->seriesname);
         }

         modstat = kMymodErr_BadFilenameFmt;
         break;
      }

      if (!cparms || !cparms[iseg])
      {
         lastcparms = 1;
      }

      /* Must pass source segment if the segment is a linked segment. */
      /* if we end up not having to perform an export, because the file to be exported has an up-to-date header, then
       * actualfname will be a link to the original up-to-date internal FITS file
       */
      drmsstat = fitsexport_mapexport_tofile(segin, !lastcparms ? cparms[iseg] : NULL, classname, mapfile, fullfname, &actualfname, &expsize);
      //       JEAFPrintLocalTime(stdout, "Done calling fitsexport_mapexport_tofile() from MapexportRecordToDir().");
      if (drmsstat == DRMS_ERROR_INVALIDFILE)
      {
         /* No input segment file. */
         fprintf(stderr, "Requested input file %s is missing or invalid.\n", segin->filename);
      }
      else if (drmsstat != DRMS_SUCCESS)
      {
         /* There was an input segment file, but for some reason the export failed. */
         modstat = kMymodErr_ExportFailed;
         fprintf(stderr, "Failure exporting segment '%s'.\n", segin->info->name);

         if (errmsg)
         {
            if (drmsstat == DRMS_ERROR_CANTCOMPRESSFLOAT)
            {
               *errmsg = strdup("Cannot export Rice-compressed floating-point images.\n");
            }
         }
         break;
      }
      else
      {
         count++;
         tsize += expsize;
         WritePListRecord(kPL_content, pklist, query, actualfname);
      }

      iseg++;
   }

   /* If NO file exported, this is an error. But if there is more than one segment, it is okay
    * for some input segment files to be missing. */
   if (count == 0)
   {
      modstat = kMymodErr_ExportFailed;
   }

   if (tcount)
   {
      *tcount = count;
   }

   if (last)
   {
      hiter_destroy(&last);
   }

    if (actualfname)
    {
        free(actualfname);
    }

   if (status)
   {
      *status = modstat;
   }

   return tsize;
}

static DRMS_Segment_t *linked_segment(DRMS_Env_t *env, DRMS_Segment_t *template_segment)
{
    DRMS_Segment_t *child_template_segment = NULL;
    DRMS_Link_t *link = NULL;
    DRMS_Record_t *child_template_record = NULL;

    if (template_segment->info->islink)
    {
        link = (DRMS_Link_t *)hcon_lookup_lower(&template_segment->record->links, template_segment->info->linkname);
        if (link)
        {
            child_template_record = (DRMS_Record_t *)hcon_lookup_lower(&env->series_cache, link->info->target_series);
        }

        if (child_template_record)
        {
            child_template_segment = (DRMS_Segment_t *)hcon_lookup_lower(&child_template_record->segments, template_segment->info->target_seg);
        }
    }

    return child_template_segment;
}

static int parse_specification(DRMS_Env_t *env, const char *specification, DRMS_Record_t **template_record_out, LinkedList_t **segments_out)
{
    char *clean_specification = NULL;
    char *all_versions = NULL;
    char **sets = NULL;
    DRMS_RecordSetType_t *set_types = NULL;
    char **series = NULL;
    char **filters = NULL;
    char **segments = NULL;
    int number_sets = 0;
    DRMS_RecQueryInfo_t info;
    char *segment = NULL;
    char *saver = NULL;
    char *clean_segments = NULL;
    char segment_buffer[DRMS_MAXSEGNAMELEN] = {0};
    int drms_status = DRMS_SUCCESS;

    int error = 0;
    DRMS_Record_t *template_record = NULL;
    DRMS_Record_t *template_segment = NULL;
    LinkedList_t *segment_list = NULL;

    base_strip_whitespace(specification, &clean_specification);

    if (drms_record_parserecsetspec_plussegs(clean_specification, &all_versions, &sets, &set_types, &series, &filters, &segments, &number_sets, &info) != DRMS_SUCCESS)
    {
        fprintf(stderr, "invalid specification `%s`\n", specification);
        error = 1;
    }
    else if (number_sets != 1)
    {
        fprintf(stderr, "jsoc_export_as_fits does not support subsetted record-set specifications\n");
        error = 1;
    }

    if (!error)
    {
        if (template_record_out)
        {
            template_record = drms_template_record(env, series[0], &drms_status);

            if (!template_record || drms_status != DRMS_SUCCESS)
            {
                fprintf(stderr, "unknown series `%s`\n", series[0]);
                error = 1;
            }
            else
            {
                *template_record_out = template_record;

                if (segments_out)
                {
                    segment_list = list_llcreate(DRMS_MAXSEGNAMELEN, NULL);

                    if (segment_list)
                    {
                        /* segments[0] contains "{}" if segment filter exists; NULL otherwise */
                        if (segments[0] && *segments[0] != '\0')
                        {
                            clean_segments = strdup(segments[0] + 1);
                            clean_segments[strlen(clean_segments) - 1] = '\0';
                        }

                        if (clean_segments && *clean_segments != '\0')
                        {
                            for (segment = strtok_r(clean_segments, ",", &saver); segment; segment = strtok_r(NULL, ",", &saver))
                            {
                                /* don't follow links */
                                template_segment = hcon_lookup_lower(&(template_record->segments), segment);

                                if (!template_segment)
                                {
                                    fprintf(stderr, "unknown segment `%s`\n", segment);
                                    error = 1;
                                }
                                else
                                {
                                    snprintf(segment_buffer, sizeof(segment_buffer), "%s", segment);
                                    list_llinserttail(segment_list, segment_buffer);
                                }
                            }
                        }

                        *segments_out = segment_list;
                    }
                }
            }
        }
    }

    if (clean_segments)
    {
        free(clean_segments);
        clean_segments = NULL;
    }

    if (clean_specification)
    {
        free(clean_specification);
        clean_specification = NULL;
    }

    drms_record_freerecsetspecarr_plussegs(&all_versions, &sets, &set_types, &series, &filters, &segments, number_sets);

    return error;
}

static int fetch_linked_segments(DRMS_Env_t *env, DRMS_Record_t *template_record, LinkedList_t *segments)
{
    DRMS_Segment_t *template_segment = NULL;
    int number_segments = -1;
    ListNode_t *list_node = NULL;
    char *segment = NULL;
    HIterator_t *current_segment_hit = NULL;
    int b_fetch_linked_segments = -1;

    number_segments = list_llgetnitems(segments);

    b_fetch_linked_segments = 0;
    if (segments && number_segments > 0 && number_segments != hcon_size(&template_record->segments))
    {
        /* a subset of segments is being requested */
        list_llreset(segments);
        while (!b_fetch_linked_segments && (list_node = list_llnext(segments)) != NULL)
        {
            segment = (char *)list_node->data;
            template_segment = (DRMS_Segment_t *)hcon_lookup_lower(&template_record->segments, segment);

            while (template_segment)
            {
                template_segment = linked_segment(env, template_segment);

                if (template_segment)
                {
                    if (!template_segment->info->islink)
                    {
                        b_fetch_linked_segments = 1;
                        break;
                    }
                }
            }
        }
    }
    else
    {
        /* all segments are being requested */
        while (!b_fetch_linked_segments && (template_segment = drms_record_nextseg(template_record, &current_segment_hit, 0)) != NULL)
        {
            while (template_segment)
            {
                template_segment = linked_segment(env, template_segment);

                if (template_segment)
                {
                    if (!template_segment->info->islink)
                    {
                        b_fetch_linked_segments = 1;
                        break;
                    }
                }
            }
        }

        hiter_destroy(&current_segment_hit);
    }

    return b_fetch_linked_segments;
}

static unsigned long long MapexportToDir(DRMS_Env_t *env, const char *rsinquery, const char *ffmt, const char *outpath, FILE *pklist, const char *classname, const char *mapfile, int *tcount, TIME *exptime, const char **cparms, MymodError_t *status)
{
    int stat = DRMS_SUCCESS;
    MymodError_t modstat = kMymodErr_Success;
    DRMS_RecordSet_t *rsin = NULL;
    DRMS_Record_t *recin = NULL;
    int iRec = 0;
    int nSets = 0;
    int iSet = 0;
    int nRecs = 0;
    int RecordLimit = *tcount;
    unsigned long long tsize = 0;
    int errorCount = 0;
    int okayCount = 0;
    int count;
    int itcount;
    DRMS_Record_t *template_record = NULL;
    LinkedList_t *segments = NULL;
    int b_fetch_linked_segments = -1;

    itcount = 0;

    /* parse specification */
    if (parse_specification(env, rsinquery, &template_record, &segments))
    {
        modstat = kMymodErr_BadRecSetQuery;
    }

    if (modstat == kMymodErr_Success)
    {
        b_fetch_linked_segments = fetch_linked_segments(env, template_record, segments);
    }

    list_llfree(&segments);

    if (modstat == kMymodErr_Success)
    {
        /* temporarily reverting to drms_open_records until I can fix the problem with
         * not passing a segment-list to drms_open_recordset()
         */
        rsin = drms_open_records2(env, rsinquery, NULL, 0, RecordLimit, 1, &stat);
    }

    if (rsin)
    {
        /* stage records to reduce number of calls to SUMS. */
        if (b_fetch_linked_segments)
        {
            drms_stage_records(rsin, 1, 0);
        }
        else
        {
            drms_stage_records_dontretrievelinks(rsin, 1);
        }

        nSets = rsin->ss_n;

        for (iSet = 0; iSet < nSets; iSet++)
        {
            nRecs = drms_recordset_getssnrecs(rsin, iSet, &stat);

            if (stat != DRMS_SUCCESS)
            {
                fprintf(stderr, "Failure calling drms_recordset_getssnrecs(), skipping subset '%d'.\n", iSet);
            }
            else
            {
                for (iRec = 0; iRec < nRecs; iRec++)
                {
                    recin = drms_recordset_fetchnext(env, rsin, &stat, NULL, NULL);

                    if (!recin)
                    {
                        /* Exit rec loop - last record was fetched last time. */
                        break;
                    }

                    count = 0;
                    tsize += MapexportRecordToDir(recin, ffmt, outpath, pklist, classname, mapfile, &count, cparms, &modstat, NULL);

                    if (modstat == kMymodErr_Success)
                    {
                        okayCount++;
                        itcount += count;
                    }
                    else
                    {
                        errorCount++;
                    }
                }
            }
        }

        modstat = kMymodErr_Success; /* Could have been set to BAD in loop above, but okayCount and errorCount
                              * account for fatal errors in the loop, not modstat. */
        if (errorCount > 0)
        {
            fprintf(stderr,"Export failed for %d segments of %d attempted.\n", errorCount, errorCount + okayCount);
        }

        if (exptime)
        {
            *exptime = CURRENT_SYSTEM_TIME;
        }

        if (tcount)
        {
            *tcount = itcount;
        }
    }
    else
    {
        fprintf(stderr, "Record-set query '%s' is not valid.\n", rsinquery);
        modstat = kMymodErr_BadRecSetQuery;
    }

    if (rsin)
    {
        drms_close_records(rsin, DRMS_FREE_RECORD);
    }

    if (status)
    {
        *status = modstat;
    }

    return tsize;
}

static char *GenErrMsg(const char *fmt, ...)
{
   char *msgout = NULL;
   char errmsg[4096];

   va_list ap;
   va_start(ap, fmt);
   vsnprintf(errmsg, sizeof(errmsg), fmt, ap);

   msgout = strdup(errmsg);
   fprintf(stderr, errmsg);

   va_end (ap);

   return msgout;
}

static MymodError_t AppendContent(FILE *dst, FILE *src)
{
   MymodError_t err = kMymodErr_Success;
   char buf[32768];
   int nread = 0;
   int dstfd;
   int srcfd;

   if (dst && src)
   {
      dstfd = fileno(dst);
      srcfd = fileno(src);

      while(1)
      {
         nread = read(srcfd, buf, sizeof(buf));
         if (nread > 0)
         {
            if (write(dstfd, buf, nread) == -1)
            {
               err = kMymodErr_PackfileFailure;
               fprintf(stderr, "Failure writing packing-list file.\n");
               break;
            }
         }
         else
         {
             if (nread == -1)
             {
                err = kMymodErr_PackfileFailure;
                fprintf(stderr, "Failure reading packing-list file.\n");
             }

             break;
         }
      }
   }

   return err;
}

//#undef DEFS_MKPATH
//#define FRED(X, Y) X##Y
//#define DEFS_MKPATH(Y) FRED(CDIR, Y)
//#define DEFS_MKPATH(Y) CDIR##Y

int DoIt(void)
{
    MymodError_t err = kMymodErr_Success;
    int drmsstat = DRMS_SUCCESS;
    long long tsize = 0; /* total size of export payload in bytes */
    long long tsizeMB = 0; /* total size of export payload in Mbytes */
    int tcount = 0;
    TIME exptime = DRMS_MISSING_TIME;
    FILE *pklist = NULL;
    FILE *pklistTMP = NULL;
    char pklistfname[PATH_MAX];
    char pklistfnameTMP[PATH_MAX];
    char pklistpath[PATH_MAX];
    char pklistpathTMP[PATH_MAX];

    const char *version = NULL;
    const char *reqid = NULL;
    const char *method = NULL;
    const char *protocol = NULL;
    const char *rsquery = NULL;
    const char *clname = NULL;
    const char *mapfile = NULL;
    const char *cparmsarg = NULL;
    const char **cparms = NULL;
    int RecordLimit = 0;

    /* "packing list" header/metadata */
    char *md_version = NULL;
    char *md_reqid = NULL;
    char *md_method = NULL;
    char *md_protocol = NULL;
    char *md_count = NULL; /* number of files exported */
    char *md_size = NULL; /* total Mbytes exported */
    char *md_exptime = NULL;
    char *md_dir = NULL; /* same as outpath, the /SUMX path that contains the exported files
                         * before they are downloaded by the user */
    char *md_status = NULL;
    char *md_error = NULL;

    RecordLimit = cmdparams_get_int(&cmdparams, kArg_n, &drmsstat);

    /* This will add strings to the global string container. */
    defs_init();

    snprintf(pklistfname, sizeof(pklistfname), "%s", drms_defs_getval("kPackListFileName"));
    snprintf(pklistfnameTMP, sizeof(pklistfnameTMP), "%s.tmp", pklistfname);

    version = cmdparams_get_str(&cmdparams, kArg_version, &drmsstat);
    reqid = cmdparams_get_str(&cmdparams, kArg_reqid, &drmsstat);
    method = cmdparams_get_str(&cmdparams, kArg_method, &drmsstat);
    protocol = cmdparams_get_str(&cmdparams, kArg_protocol, &drmsstat);

    rsquery = cmdparams_get_str(&cmdparams, kArg_rsquery, &drmsstat);


    clname = cmdparams_get_str(&cmdparams, kArg_clname, &drmsstat);
    if (drmsstat != DRMS_SUCCESS || !strcmp(clname, kNotSpecified))
    {
        clname = NULL;
    }

    mapfile = cmdparams_get_str(&cmdparams, kArg_kmfile, &drmsstat);
    if (drmsstat != DRMS_SUCCESS || !strcmp(mapfile, kNotSpecified))
    {
        mapfile = NULL;
    }

    cparmsarg = cmdparams_get_str(&cmdparams, kArg_cparms, &drmsstat);
    if (strcmp(cparmsarg, kNotSpecified))
    {
        char *dup = strdup(cparmsarg);
        char *pc = NULL;
        char *pend = NULL;
        int nstr;
        int istr;

        /* count number of compression strings (one for each segment being exported) */
        pc = dup;
        nstr = 1;
        while ((pc = strchr(pc, ',')) != NULL)
        {
            pc++;
            ++nstr;
        }

        cparms = (const char **)malloc((nstr + 1) * sizeof(char *));

        pc = dup;
        for (istr = 0; istr < nstr; istr++)
        {
            pend = strchr(pc, ',');
            if (pend)
            {
                *pend = '\0';
            }
            cparms[istr] = (strcmp(pc, kNoCompression) == 0) ? strdup("") : strdup(pc);
            if (pend)
            {
                pc = pend + 1;
            }
        }

        /* Empty string to indicate end */
        cparms[nstr] = NULL;

        if (dup)
        {
            free(dup);
        }
    }

    md_version = strdup(version);   /* Could be "NOT SPECIFIED". */
    md_reqid = strdup(reqid);       /* Could be "NOT SPECIFIED". */
    md_method = strdup(method);     /* Could be "NOT SPECIFIED". */
    md_protocol = strdup(protocol); /* Could be "NOT SPECIFIED". */

    /* The export system uses this branch of code - the record-set specification comes
    * from the cmd-line, not from the export DRMS record. */

    /* Packing list items that come from the cmd-line arguments. */
    const char *outpath = NULL;

    /* Filename Format comes from cmd-line argument */
    const char *ffmt = NULL;

    outpath = cmdparams_get_str(&cmdparams, kArg_path, &drmsstat);
    if (strcmp(outpath, kNotSpecified) == 0)
    {
        /* Use current working directory by default */
        outpath = getenv(kPWD);
    }

    ffmt = cmdparams_get_str(&cmdparams, kArg_ffmt, &drmsstat);
    if (strcmp(ffmt, kNotSpecified) == 0 || *ffmt == '\0')
    {
        /* Assume the user wants the default filename format - set to NULL */
        ffmt = NULL;
    }

    /* Open tmp packing-list file */
    snprintf(pklistpathTMP, sizeof(pklistpathTMP), "%s/%s", outpath, pklistfnameTMP);

    pklistTMP = fopen(pklistpathTMP, "w+");
    if (pklistTMP)
    {
        /* Call export code, filling in tsize, tcount, and exptime */
        tcount = RecordLimit;
        tsize = MapexportToDir(drms_env, rsquery, ffmt, outpath, pklistTMP, clname, mapfile, &tcount, &exptime, cparms, &err);
    }
    else
    {
        err = kMymodErr_CantOpenPackfile;
        md_error = GenErrMsg("Couldn't open temporary packing-list file '%s'.\n",  pklistpathTMP);
    }

    if (err != kMymodErr_Success)
    {
        md_error = GenErrMsg("Failure occurred while processing export Request ID '%s'.\n", reqid);
        err = kMymodErr_ExportFailed;
    }
    else
    {
        /* Set packing list info */
        md_dir = strdup(outpath);
    }

    tsizeMB = ToMB(tsize);

    if (err == kMymodErr_Success)
    {
        char tstr[64];
        int strsize = 0;
        sprint_time(tstr, exptime, "UT", 0);

        /* Set packing list info not already set */
        strsize = 64;
        md_size = malloc(strsize);
        snprintf(md_size, strsize, "%lld", tsizeMB);
        md_count = malloc(strsize);
        snprintf(md_count, strsize, "%d", tcount);
        md_exptime = strdup(tstr);
    }

    fprintf(stdout, "%lld megabytes exported.\n", tsizeMB);

    /* open 'real' pack list */
    if (strcmp(reqid, kNotSpecified) != 0)
    {
        if (md_dir)
        {
            snprintf(pklistpath, sizeof(pklistpath), "%s/%s", md_dir, pklistfname);
            pklist = fopen(pklistpath, "w+");

            if (pklist)
            {
                if (fseek(pklistTMP, 0, SEEK_SET))
                {
                    md_error = GenErrMsg("Failure accessing packing-list file '%s'.\n", pklistfnameTMP);
                    err = kMymodErr_PackfileFailure;
                }
            }
            else
            {
                md_error = GenErrMsg("Failure opening packing-list file '%s'.\n", pklistfname);
                err = kMymodErr_PackfileFailure;
            }
        }

        /* For now, there is just success/failure in the packing-list file */
        if (err == kMymodErr_Success)
        {
            md_status = strdup(drms_defs_getval("kMDStatus_Good"));
        }
        else
        {
            md_status = strdup(drms_defs_getval("kMDStatus_Bad"));
        }

        if (pklist)
        {
            char procSteps[PATH_MAX];
            FILE *fpProcSteps = NULL;
            struct stat stBuf;
            char *lineBuf = NULL;
            size_t lineLen = 0;
            ssize_t nRead = 0;

            /* write packing list */
            fprintf(pklist, "# JSOC \n");
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Version"), md_version);
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_RequestID"), md_reqid);
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Method"), md_method);
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Protocol"), md_protocol);
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Count"), md_count ? md_count : "-1");
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Size"), md_size ? md_size : "-1");
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_ExpTime"), md_exptime ? md_exptime : "");
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Dir"), md_dir ? md_dir : "");
            WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Status"), md_status);

            /* If the proc-steps.txt file exists, put the contents in the packing list. */
            snprintf(procSteps, sizeof(procSteps), "%s/proc-steps.txt", md_dir);
            if (stat(procSteps, &stBuf) == 0)
            {
                fprintf(pklist, "# PROCESSING \n");

                fpProcSteps = fopen(procSteps, "r");
                if (fpProcSteps)
                {
                    while ((nRead = getline(&lineBuf, &lineLen, fpProcSteps)) != -1)
                    {
                        /* Put these in comments, otherwise jsoc_export_make_index will have issues. */
                        fprintf(pklist, lineBuf);
                    }

                    if (lineBuf)
                    {
                        free(lineBuf);
                    }
                }
            }

            fflush(pklist);

            if (err == kMymodErr_Success)
            {
                /* Copy the content from the temporary pklist into the real pklist */
                fprintf(pklist, "# DATA \n");
                fflush(pklist);
                if (tsizeMB > 0)
                {
                    err = AppendContent(pklist, pklistTMP);
                }
                else
                {
                    WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Warning"), "No FITS files were exported. The requested FITS files no longer exist.");
                }
            }

            if (err != kMymodErr_Success)
            {
                fflush(pklist);

                /* If there is an error, write out the error message. */
                WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Error"), md_error ? md_error : "");
            }
        }
    }

    if (pklistTMP)
    {
        /* close tmp packing-list file */
        fclose(pklistTMP);
        pklistTMP = NULL;

        /* delete temporary file */
        unlink(pklistpathTMP);
    }

    if (pklist)
    {
        /* close actual packing-list file */
        fclose(pklist);
        pklist = NULL;
    }

    if (md_version)
    {
        free(md_version);
    }

    if (md_reqid)
    {
        free(md_reqid);
    }

    if (md_method)
    {
        free(md_method);
    }

    if (md_protocol)
    {
        free(md_protocol);
    }

    if (md_count)
    {
        free(md_count);
    }

    if (md_size)
    {
        free(md_size);
    }

    if (md_exptime)
    {
        free(md_exptime);
    }

    if (md_dir)
    {
        free(md_dir);
    }

    if (md_status)
    {
        free(md_status);
    }

    if (md_error)
    {
        free(md_error);
    }

    if (cparms)
    {
        int iseg = 0;
        while (cparms[iseg])
        {
            free((void *)cparms[iseg]);
            iseg++;
        }

        free(cparms);
    }

    return err;
}
