#include "jsoc_main.h"
#include "drms_types.h"
#include "drms_storageunit.h"
#include "exputil.h"
#include "fitsexport.h"

//ISS fly-tar START
#include "fitsio.h"
#include "cfitsio.h"
#if defined(JMD_IS_INSTALLED) && JMD_IS_INSTALLED
#include "libtar.h"
#endif
#define  MAXTARSIZE 2147483648
#define  MAXFILETARSIZE 21474836480
//#define  MAXTARSIZE 59910560
#ifndef __export_callback_func_t__
#define __export_callback_func_t__
typedef int (*export_callback_func_t)(char *, ...); //ISS fly-tar
#endif
typedef struct RequestAttr_struct
{
    int  rscount;
    const char **cparms;
    const char *outpath;
    const char *method;
    char  scid[512];
    const char *stagestatusfile;
    char **rsffmt;
    char **rsquery;
    FILE       *fstatus;
    TAR        *tar;//=NULL
    long       reccount;//=0
    int        arraypointer;//=0
    tartype_t  tartype;//= {open,close,read,write};
    int        terminate;
    char       *infobuffer;//     = (char *) NULL;
    char       *errorbuffer;//    = (char *) NULL;
    int        print_header;//    = 0

} RequestAttr;

#define QUED "QUED"
#define NQUE "NQUE"
#define FAIL "FAIL"
#define DONE "DONE"
#define   URL_CGI       "url_cgi"
#define   STAGE_CGI     "stage_cgi"

void ExportHandle(char *callbackState, ...);
void printRequestAttr(RequestAttr *reqattr);
void freeRequestAttr(RequestAttr *reqattr);
int  addQueryToReqattr(RequestAttr *reqattr, const char * rsquery, const char * ffmt);
int  populatereqattr(RequestAttr *reqattr, const char * rsqueryfile);

int  flytar_append_file_header(TAR *t, char * filename, int size);
int  flytar_append_file_tail(TAR *t, int size);
int  flytar_append_file(TAR *t, FILE *ftar, char * fullname);
int  file_set_stats(TAR *t, int size, char * filename);
int  block_rest(int block_size, int file_size);
char * flattenstring(char *str1, const char *str2, int n);
int  match_query_segments(DRMS_Segment_t *seg);
char ** parse_query_for_segment_names(char * query);
int  count_segment_names(char **segments);
int  testRecordCount(DRMS_RecordSet_t* rsin, int* stat, int querySegCount);
void CallbackErrorBuffer( RequestAttr *lreqattr, char* filename, char*query, int errorCode);
void CallbackClose(RequestAttr *lreqattr, FILE *ftar);
void CallbackInfo(RequestAttr *lreqattr, char * filename);
void generateStageTarName(const char * stagedir, const char * f_scid, int stagetarcount, char* newname);
int  checkdir(const char *dirname);
int  checkfile(const char *filename);
FILE * openstatefile(const char *sfile, const char *reqid, int *cstatus);
int  stgprintf(FILE *file, const char *format,...);
void setstagestate(const char * path, const char * scid, const char * state);


char sackfile[4000];
char starfile[4000];
char gcgi_request[200];
char **segments       =    (char **) NULL; //work-around

//ISS fly-tar END

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
   kMymodErr_MissingSegFile,
   kMymodErr_QuedFileExists,
   kMymodErr_MaxURLSizeReached,
   kMymodErr_Rename2QuedFail
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
//ISS fly-tar START
#define kArg_ackfile     "ackfile"
#define kArg_tarfile     "tarfile"
#define kArg_cgi_request      "cgi_request"
#define kArg_segments    "segments"
//ISS fly-tar END
#define kArg_protocol    "protocol"
#define kArg_rsqueryfile "rsqueryfile"
#define kArg_rsquery     "rsquery"
#define kArg_n      	 "n"
#define kArg_expSeries   "expseries"
#define kArg_ffmt        "ffmt"
#define kArg_path        "path"
#define kArg_clname      "kmclass"
#define kArg_kmfile      "kmfile"
#define kArg_cparms      "cparms"
#define kArg_stagestatusfile   "stagestatusfile"


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
     {ARG_STRING, kArg_version, "", "jsoc export version."},
     {ARG_STRING, kArg_reqid, "",
        "Export series primary key value that identifies the output record."},
     {ARG_STRING, kArg_method, "", "jsoc export method (eg, url or ftp)."},
     //ISS fly-tar START
     {ARG_STRING, kArg_ackfile, kNotSpecified, "Acknowledgement File"},
     {ARG_STRING, kArg_tarfile, kNotSpecified, "Tar File Name"},
     {ARG_STRING, kArg_cgi_request, kNotSpecified, "CGI request HEAD,GET,POST"},
     {ARG_STRING, kArg_segments, kNotSpecified, "Export segment name."},
     //ISS fly-tar END
     {ARG_STRING, kArg_protocol, "", "file conversion method (eg, convert to fits)."},
     {ARG_STRING, kArg_rsquery, kNotSpecified,
        "Record-set query that specifies data to be exported."},
     {ARG_STRING, kArg_rsqueryfile, kNotSpecified,
        "Record-set query file list that specifies data to be exported."},
     {ARG_STRING, kArg_expSeries, kDef_expSeries, "Series to which exported data are saved."},
     {ARG_STRING, kArg_ffmt, kNotSpecified, "Export filename template."},
     {ARG_STRING, kArg_path, kNotSpecified, "Path to which fits files are output."},
     {ARG_STRING, kArg_clname, kNotSpecified, "Export key map class."},
     {ARG_STRING, kArg_kmfile, kNotSpecified, "Export key map file."},
     {ARG_STRING, kArg_stagestatusfile, kNotSpecified, "Status file in Stage Status directory."},
     {ARG_STRING, kArg_cparms, kNotSpecified, "FITS-stanford compression string used to compress exported image."},
     {ARG_INT, kArg_n, "0", "Record count limit."},
     {ARG_END}
};

char gDefBuf[PATH_MAX] = {0};

/* Convert number of bytes to number of MB. If number of bytes < 1MB, then return 1. */
static long long ToMB(long long nbytes)
{
   if (nbytes <= kMB)
   {
      return 1;
   }

   return nbytes / kMB;
}

MymodError_t WritePListRecord(PLRecType_t rectype, FILE *pkfile, const char *f1, const char *f2)
{
   MymodError_t err = kMymodErr_Success;
   return err; //TODO
#if 0
   // calm compiler down
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
#endif
}

/* Assumes tcount is zero on the first call.  This function adds
 * the number of files exported to tcount on each call. */
static int MapexportRecordToDir(DRMS_Record_t *recin,
                                const char *ffmt,
                                const char *outpath, //Set in reqattr
                                FILE *pklist,
                                const char *classname,
                                const char *mapfile,
                                int *tcount,
                                const char **cparms,
                                MymodError_t *status,
                                export_callback_func_t callback) //ISS fly-tar
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
   struct stat filestat;
   HIterator_t *last = NULL;
   int iseg;
   int lastcparms;
   int gotone;

   drms_record_directory(recin, dir, 1); /* This fetches the input data from SUMS. */

   /* Must create query from series name and prime keyword values */
   drms_sprint_rec_query(query, recin);

   /* The input rs query can specify a subset of all the series' segments -
    * this is encapsulated in recin. */

   iseg = 0;
   lastcparms = 0;
   gotone = 0;

   while ((segin = drms_record_nextseg(recin, &last, 1)) != NULL)
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

      if (exputl_mk_expfilename(segin, tgtseg, ffmt, fmtname) == kExpUtlStat_Success)
      {
//		 if ( NULL == outpath) {
         	snprintf(fullfname, sizeof(fullfname), "%s", fmtname);
//		 } else {
//        	snprintf(fullfname, sizeof(fullfname), "%s/%s", outpath, fmtname);
//		 }
      }
      else
      {
         modstat = kMymodErr_BadFilenameFmt;
         break;
      }

      if (!cparms || !cparms[iseg])
      {
         lastcparms = 1;
      }

      drmsstat = fitsexport_mapexport_tofile2( segin,
                                               !lastcparms ? cparms[iseg] : NULL,
                                               classname,
                                               mapfile,
                                               fullfname,
                                               &actualfname,
                                               &expsize,
                                               callback); //ISS fly-tar

      //ISS fly-tar : modify test on file only if callback NULL
      if ( drmsstat != DRMS_SUCCESS && callback != NULL) {  //ISS fly-tar
         fprintf(stderr, "(1) Failure exporting segment '%s'.\n", segin->info->name);
         (*callback)("error",query,fullfname);  //ISS fly-tar
      }
      else if (drmsstat == DRMS_ERROR_INVALIDFILE)
      {
         /* No input segment file. */
      }
      else if ((drmsstat != DRMS_SUCCESS ||
               ( stat(fullfname, &filestat) ) && (callback == NULL)) ) //ISS fly-tar
      {
         /* There was an input segment file, but for some reason the export failed. */
         modstat = kMymodErr_ExportFailed;
         fprintf(stderr, "(2) Failure exporting segment '%s'.\n", segin->info->name);
         break;
      }
      else
      {
         //ISS fly-tar
         if (callback != NULL) {
           (*callback)("info",fullfname);
         } else {

           gotone = 1;

           if (tcount)
           {
              ++*tcount;
           }

           tsize += filestat.st_size;
           WritePListRecord(kPL_content, pklist, query, fmtname);
         } //ISS fly-tar
      }

      iseg++;
   }

   /* If NO file exported, this is an error. But if there is more than one segments, it is okay
    * for some input segment files to be missing. */
   if (!gotone && callback == NULL)
   {
      modstat = kMymodErr_ExportFailed;
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



static long long  VSOExportData (DRMS_Env_t *env,
                                 RequestAttr *reqattr,
                                 int *tcount,
                                 const char **cparms,
                                 MymodError_t *status)
{
   int stat = DRMS_SUCCESS;
   MymodError_t modstat = kMymodErr_Success;
   DRMS_RecordSet_t *rsin = NULL;
   DRMS_Record_t *recin = NULL;
   int iRec = 0;
   int nSets = 0;
   int iSet = 0;
   int nRecs = 0;
   unsigned long long tsize = 0;
   int errorCount = 0;
   int okayCount = 0;

   export_callback_func_t callback = (export_callback_func_t) ExportHandle;

   int segmentCount=0;

   //init callback / ExportHandle.
   // This also initializes the below for loop index
   (*callback)("init",reqattr);

//TODO
//   .- For the tar file the directory path should be ./scid/tarname
//   .- See if I can workout why the fits files dissapears from the directory
//   .- Add State function to this.
//          If stagestatus dir present:
//             if status file present
//                IF SUCCESFUL move it to DONE
//                IF ERROR to FAIL
//                IF start processing to pid.QUED
//
   for (; reqattr->arraypointer<reqattr->rscount; reqattr->arraypointer++) {

      char *rsinquery=reqattr->rsquery[reqattr->arraypointer];
      rsin = drms_open_records(env, rsinquery, &stat);

      if (rsin)
      {
         reqattr->reccount+=testRecordCount(rsin,&stat,segmentCount) - 1;
         fprintf(stderr,"drms_export_cgi::Record Count [%ld]\n",reqattr->reccount);

         /* stage records to reduce number of calls to SUMS. */
         drms_stage_records(rsin, 1, 0);
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
                   recin = rsin->records[(rsin->ss_starts)[iSet] + iRec];
                   fprintf(stderr, "nRecs [%d] rsin[%d]\n",nRecs,(rsin->ss_starts)[iSet] + iRec);
                   MapexportRecordToDir(recin,
                                        reqattr->rsffmt[reqattr->arraypointer],
                                        NULL,//reqattr->outpath, let ExportHandle handle this.
                                        NULL,//pklist,
                                        NULL,//classname,
                                        NULL,//mapfile,
                                        tcount,
                                        cparms,
                                        &modstat,
                                        callback);  //ISS fly-tar
                   fprintf(stderr, "out of MapexportRecordToDir\n");
                   if (modstat == kMymodErr_Success)
                   {
                      okayCount++;
                   }
                   else
                   {
                      errorCount++;
                   }

                   if (reqattr->terminate) {
                      break;
                   }
                }

                if (reqattr->terminate)
                  break;
            }

            if (reqattr->terminate)
              break;
         }


         modstat = kMymodErr_Success; /* Could have been set to BAD in loop above, but okayCount and errorCount
                                       * account for fatal errors in the loop, not modstat. */

         if (errorCount > 0)
         {
            fprintf(stderr,"Export Failed for %d segments of %d attempted.\n", errorCount, errorCount + okayCount);
            if (reqattr->fstatus != NULL)
              stgprintf(reqattr->fstatus,"EXPORT_MSG=Export Failed for %d segments of %d attempted.\n", errorCount, errorCount + okayCount);
            if (okayCount == 0)
              modstat = kMymodErr_ExportFailed;
         }

         //ISS fly-tar END
      }
      else
      {
         fprintf(stderr, "Record-set query '%s' is not valid.\n", rsinquery);
         if (reqattr->fstatus != NULL)
           stgprintf(reqattr->fstatus, "EXPORT_MSG=Record-set query '%s' is not valid.\n", rsinquery);
         modstat = kMymodErr_BadRecSetQuery;
      }

      if (rsin)
      {
         fprintf(stderr, "drms_close_records\n");
         drms_close_records(rsin, DRMS_FREE_RECORD);
         rsin=NULL;
      }

   } //end of for loop

   fprintf(stderr, "calling callback close in MapexportRecordToDir loop\n");
   (*callback)("close");

   if (status)
   {
      *status = modstat;
   }

   (*callback)("getsize",&tsize);
   (*callback)("getcount",tcount);
   return tsize;
}

int DoIt(void)
{
   MymodError_t err = kMymodErr_Success;
   int drmsstat = DRMS_SUCCESS;
   long long tsize = 0; /* total size of export payload in bytes */
   long long tsizeMB = 0; /* total size of export payload in Mbytes */
   int tcount = 0;

   const char *reqid = NULL;
   const char *method = NULL;
   //ISS fly-tar START
   const char *ackfile= NULL;
   const char *tarfile= NULL;
   const char *cgi_request= NULL;
   //ISS fly-tar END
   const char *rsquery = NULL;
   const char *rsqueryfile = NULL;
   const char *stagestatusfile= NULL;
   const char *clname = NULL;
   const char *mapfile = NULL;
   const char *cparmsarg = NULL;
   const char **cparms = NULL;
   int RecordLimit = 0;

   /* "packing list" header/metadata */
   char *md_error = NULL;

   RecordLimit = cmdparams_get_int(&cmdparams, kArg_n, &drmsstat);

//   snprintf(pklistfname, sizeof(pklistfname), "%s", drms_defs_getval("kPackListFileName"));
//   snprintf(pklistfnameTMP, sizeof(pklistfnameTMP), "%s.tmp", pklistfname);


   reqid = cmdparams_get_str(&cmdparams, kArg_reqid, &drmsstat);
   method = cmdparams_get_str(&cmdparams, kArg_method, &drmsstat);

   //ISS fly-tar START copy the argument to a static variable.
   ackfile= cmdparams_get_str(&cmdparams, kArg_ackfile, &drmsstat);
   if (strcmp(ackfile,kNotSpecified) && access(ackfile, F_OK) == 0 ) {
//     fprintf(stderr, "Including ACK file [%s]\n",ackfile);
     strcpy(sackfile,ackfile);
   }

   tarfile=cmdparams_get_str(&cmdparams, kArg_tarfile, &drmsstat);
   if (strcmp(tarfile,kNotSpecified)) {
//     fprintf(stderr, "Including tar file name [%s]\n",tarfile);
     strcpy(starfile,tarfile);
   }

   cgi_request=cmdparams_get_str(&cmdparams, kArg_cgi_request, &drmsstat);
   if (strcmp(cgi_request,kNotSpecified)) {
//     fprintf(stderr, "Including CGI REQUEST [%s]\n", cgi_request);
     strcpy(gcgi_request,cgi_request);
   }

   //ISS fly-tar END copy the argument to a static variable.

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

   rsqueryfile = cmdparams_get_str(&cmdparams, kArg_rsqueryfile, &drmsstat);
   if (drmsstat != DRMS_SUCCESS || !strcmp(rsqueryfile, kNotSpecified))
   {
      rsqueryfile = NULL;
   }

   stagestatusfile = cmdparams_get_str(&cmdparams, kArg_stagestatusfile, &drmsstat);
   if (drmsstat != DRMS_SUCCESS || !strcmp(stagestatusfile, kNotSpecified))
   {
      stagestatusfile = NULL;
   }


   /* Packing list items that come from the cmd-line arguments. */
   const char *outpath = NULL;

   /* Filename Format comes from cmd-line argument */
   const char *ffmt = NULL;

   outpath = cmdparams_get_str(&cmdparams, kArg_path, &drmsstat);
   //ISS fly-tar only get in if it is not cgi
   if (strcmp(outpath, kNotSpecified) == 0 && (strcmp(method, URL_CGI) || strcmp(method,STAGE_CGI)))
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

   /* Call export code, filling in tsize, tcount, and exptime */
   tcount = RecordLimit;

   //Setting RequestAttr structure
   RequestAttr    reqattr;
   //structure needs to be memset otherwise pointers not NULL
   memset(&reqattr,'\0',sizeof(RequestAttr));
   reqattr.outpath= outpath;
   reqattr.method = method;

   //make sure the id only have alphanumeric characters and replace everything else with "_"
   const char * reqid_ptr = reqid;
   flattenstring(reqattr.scid,reqid_ptr,0);

   // With method STAGE it is a must to also pass a path.
   if (!strcmp(method,STAGE_CGI)) {
     if (checkdir(outpath) ) {
       fprintf(stderr,"Error:: Dir [%s] can't be accessed\n",outpath);
       return 1;
       //error
       //return
     } else {
       fprintf(stderr, "Stagedatadir [%s]\n",reqattr.outpath);
       //If a stagestatusfile is passed, that means
       // we want to keep a state via file name flag
       // E.g.
       //  <reqid>.<pid>.QUED
       //  <reqid>.FAIL
       //  <regid>.DONE
       if(checkfile(stagestatusfile) == 0 ) {
         reqattr.stagestatusfile=stagestatusfile;
         // if <scid>.NQUE exists it moves the state/file to
         // <scid>.<pid>.QUED
         int cstatus = kMymodErr_Success;
         reqattr.fstatus = openstatefile(stagestatusfile,reqattr.scid,&cstatus);
         if (cstatus != kMymodErr_Success) {
           return cstatus;
         }
       } else { // if stage status dir is not present then
                // set it with outpath
         reqattr.stagestatusfile=NULL;
         reqattr.fstatus = NULL;
       }
     }
   }

   if (rsqueryfile!=NULL) {
     populatereqattr(&reqattr,rsqueryfile);
   } else if (rsquery!=NULL) {
     addQueryToReqattr(&reqattr, rsquery, ffmt);
   } else {
     //TODO deal with error
     //return
   }

//   fprintf(stderr, "Print Request Attr\n");
//   printRequestAttr(&reqattr);

   tsize = VSOExportData(drms_env,
                         &reqattr,
                         &tcount,
                         cparms,
                         &err);

   tsizeMB = ToMB(tsize);

   stgprintf(reqattr.fstatus, "EXPORT_MB=%lld\n", tsizeMB);
   stgprintf(reqattr.fstatus, "EXPORT_FILES=%d\n", tcount);

   if (err != kMymodErr_Success) {
     stgprintf(reqattr.fstatus,"EXPORT_MSG=Failure occurred while processing export Request ID '%s'.\n", reqattr.scid);
     if (reqattr.fstatus != NULL) {
       fprintf(reqattr.fstatus,"EXPORT_STATE=FAIL\n");
       fclose(reqattr.fstatus);
//       setstagestate(reqattr.stagestatusdir,reqattr.scid,FAIL);
     }
   } else {
     if (reqattr.fstatus != NULL) {
       fprintf(reqattr.fstatus,"EXPORT_STATE=DONE\n");
       fclose(reqattr.fstatus);
//       setstagestate(reqattr.stagestatusdir,reqattr.scid,DONE);
     }
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

//ISS fly-tar START
int flytar_append_file_header(TAR *t, char * filename, int size) {
   //setting header block
   memset(&(t->th_buf), 0, sizeof(struct tar_header));
   file_set_stats(t, size, filename);
   fflush(NULL);
   th_write(t);
   fflush(NULL);
   return 0;
}

int flytar_append_file_tail(TAR *t, int size) {
   char block[512];

   memset(block, 0, 512);
//   fprintf(stderr,"block size to add [%d]\n", block_rest(512,size));
   fflush(NULL);
   if (-1==(*((t)->type->writefunc))((t)->fd, block, block_rest(512,size))) {
     return -1;
   }
   fflush(NULL);
   return 0;
}

int flytar_append_file(TAR *t, FILE *ftar, char * fullname) {
  struct stat   s;
  int           ACK        =  -1;
  int           tsize      =   0;
  if (lstat(fullname,&s) != 0)
    return 1;

  char *basename = strrchr(fullname, '/');
  basename = basename? basename + 1: (char *) fullname;
//  fprintf(stderr, "Adding ACK file [%s] ; basename [%s] size [%d]\n", fullname, basename, s.st_size);
  flytar_append_file_header(t, basename, s.st_size);


  if ( (ACK=open(fullname,O_RDONLY))>-1  ) {
    char buffer[81];
    int  rest=0;
    while (( rest = read(ACK,buffer, 80)) == 80) {
      tsize += 80;
      buffer[80]='\0';
      //fprintf(stdout,"%s",buffer);
      fprintf(ftar,"%s",buffer);
    }
    if (rest > 0 ) {
      tsize += rest;
      read(ACK,buffer,rest);
      buffer[rest]='\0';
    }

    //fprintf(stdout,"%s",buffer);
    fprintf(ftar,"%s",buffer);

    close(ACK);

  } else {
    return 1;
  }

//  fprintf(stderr, "Before padding ACK file size [%d] and read size [%d]\n", s.st_size, tsize);
  flytar_append_file_tail(t,s.st_size);

  return 0;
}

int block_rest(int block_size, int file_size) {
  int mod = 0;
  if ( file_size < block_size) {
   return block_size - file_size;
  } else {
   mod = file_size%block_size;
   if (mod > 0) {
     return 512 - mod;
   } else {
     return mod;
   }
  }
}

int file_set_stats(TAR *t, int size, char * filename) {
    t->th_buf.typeflag = REGTYPE;

    //setting user info
    strcpy(t->th_buf.uname, "production");
	int_to_oct(670, t->th_buf.uid, 8);

    //setting group info
    strcpy(t->th_buf.gname, "SUMS");
	int_to_oct(669, t->th_buf.gid, 8);

    //setting file mode
    int_to_oct(33204,t->th_buf.mode,8);

    //setting time
    int_to_oct(time(NULL),t->th_buf.mtime,12);

    //setting size
    int_to_oct(size,t->th_buf.size,12);

    //setting pathname
    th_set_path(t,filename);

    // ART - calm compiler down.
    return 0;
}

//leaves only alphanumeric characters and those that are not a flattened to "_"
//str1 MUST be preallocated!!
char * flattenstring(char *str1, const char *str2, int n) {
  char c = '\0';
  char *str1_ptr=str1;
  int  initn=n;
  while ( (c = *str2++)!= '\0' && (0 == initn || n-- >= 1)) {
    if ( (c>=48 && c<=57) || (c>=65 && c<= 90) || (c>=97 && c<=122)) {
      *str1++=c;
    } else {
      *str1++='_';
    }
  }

  return str1_ptr;
}


int match_query_segments(DRMS_Segment_t *seg)
{
  int j=0;
  if (segments!=NULL) {
    for (j=0; segments[j][0] !='\0'; j++) {
/*
      fprintf(stderr,"segment name [%s]\n",seg->info->name);
      fprintf(stderr,"segment counter [%d]\n",j);
      fprintf(stderr,"segment pass name [%s]\n",segments[j]);
*/
      if (strcmp(segments[j],seg->info->name) == 0) {
        return 0;
      }
    }
    return 1;
  } else {
    return 0;
  }
}
int count_segment_names(char **segments)
{
  int j=0;
  while(segments!=NULL && segments[++j][0]!='\0');
  return j;
}

char ** parse_query_for_segment_names(char * query)
{
  char **segments = (char **) NULL;

  char *cquery = (char *) NULL;

  char *ptr1, *ptr2;
  //duplicate query
  cquery=strdup(query);

  //find begin and end brackets
  ptr1 = strchr( cquery, '{');
  ptr2 = strrchr(cquery, '}');
  if (ptr1 != NULL && ptr2 != NULL) {
    *ptr2='\0';  //overwrite end bracket with str teminator
    ++ptr1;      //Position pointer in first char after first bracket

    char *segstr=ptr1;  // This is the full segment list string

    //count commas
    int commaCount=0;
    while ((ptr2 = strchr( ptr1, ',')) != NULL) {
      ptr1=ptr2+1;
      commaCount++;
    }

    //the segment array has always commaCount + 2 elements.
    //where the last element is always null;
    int arrCount = commaCount+2;
    segments = malloc(arrCount*sizeof(char *));

    //add last element in array; always an empty string.
    segments[arrCount-1] = malloc(sizeof(char));
    segments[arrCount-1][0]='\0';

    //reuse ptr1
    ptr1 = segstr;
    int i = 0;
    int segNameLen = 0;
    //find the segment names and populate array
    while((ptr2 = strchr( ptr1, ',')) != NULL) {
       segNameLen= ptr2 - ptr1;
       segments[i]=malloc(segNameLen*sizeof(char)+1);
       strncpy(segments[i],ptr1,segNameLen);
       segments[i][segNameLen]='\0';
       ptr1=ptr2+1;
       i++;
    }

    segNameLen=strlen(ptr1);
    segments[i]=malloc(segNameLen*sizeof(char)+1);
    strcpy(segments[i],ptr1);
    segments[i][segNameLen]='\0';

    //test the array
/*
    int j=0;
    while(segments !=NULL && segments[j++][0]!='\0') {
      fprintf(stderr,"in parser, segment name [%s]\n", segments[j-1]);
    }
*/
    free(cquery);
  }
  return segments;
}

int testRecordCount(DRMS_RecordSet_t* rsin, int* stat, int querySegCount)
{
   MymodError_t modstat = kMymodErr_Success;
   DRMS_Record_t *recin = NULL;
   int iSet =0, nSets = 0, nRecs = 0, recCount = 0, iRec = 0, segNum = 0;

   nSets = rsin->ss_n; //number of Sets?

   //test for multiple records (fly-tar mode)
   for (iSet = 0;
           *stat == DRMS_SUCCESS && modstat == kMymodErr_Success && iSet < nSets;
           iSet++)
      {
         nRecs = drms_recordset_getssnrecs(rsin, iSet, stat);
         //fprintf(stderr, "testRecordCount nRecs[%d]",nRecs);
         for (iRec = 0; modstat == kMymodErr_Success && iRec < nRecs; iRec++)
         {
           recin =rsin->records[(rsin->ss_starts)[iSet] + iRec];
           //fprintf(stderr, "testRecordCount recCount[%d]",recCount);
           if ( querySegCount != 1) {
             if ( (segNum = drms_record_numsegments(recin)) > 1) {
               recCount=segNum;
             }
           }
           recCount ++;
         }
      }

   return recCount;
}

void CallbackErrorBuffer( RequestAttr *lreqattr, char* filename, char*query, int errorCode) {
  char line[4096];
  char error[4096];

  if ( 2 == errorCode) {
    strcpy(error,"###Tar file too big. Please reduced your query range. Max tar file 2GB. Contact igor@noao.edu or oneiros@grace.nascom.nasa.gov if you need help #\n");
  } else {
    strcpy(error,"###The data requested could not be served. Please retry again in a few minutes. If the problem persists please contact: igor@noao.edu or oneiros@grace.nascom.nasa.gov  #\n");
  }

  snprintf(line, 4095, "%s; Query=[%s]\n",filename,query);

  int linelen = strlen(line);
  char *ptr = lreqattr->errorbuffer;

  if (lreqattr->errorbuffer == (char *) NULL) {

    int      errorbufferlen     = strlen(error) + linelen + 1;  // 1 is for EndOfString char \0


    lreqattr->errorbuffer =  malloc(sizeof(char)*errorbufferlen);
    // ART - change %x to %p to calm compiler down.
    fprintf(stderr,"Allocating errorbuffer [%p]\n",lreqattr->errorbuffer);

    memset(lreqattr->errorbuffer,0,sizeof(char)*errorbufferlen);

    sprintf(lreqattr->errorbuffer,"%s%s",error,line);
  } else {
    int currentbufferlen = strlen(lreqattr->errorbuffer) + 1; // 1 is for EndOfString char \0

    // ART - change %x to %p to calm compiler down.
    fprintf(stderr,"Reallocating errorbuffer [%p]; current buffer length [%d], adding additional [%d] and [%d]\n",lreqattr->errorbuffer, currentbufferlen,linelen, (int)strlen(error));
    lreqattr->errorbuffer = (char *) realloc(lreqattr->errorbuffer, sizeof(char)*(currentbufferlen + linelen + strlen(error)));

    //repointing errorbuffer to unwritten space
    ptr= lreqattr->errorbuffer + strlen(lreqattr->errorbuffer)*sizeof(char);

    //memset newly allocated memory.
    memset(ptr,0,sizeof(char)*(linelen+strlen(error)));
    sprintf(ptr,"%s%s",error,line);
  }

}

void CallbackClose(RequestAttr *lreqattr, FILE *ftar) {
  const char *method  = lreqattr->method;
  int  recCount       = lreqattr->reccount;
  TAR  *tar           = lreqattr->tar;
  const char *rsquery = lreqattr->rsquery[lreqattr->arraypointer];


  if (NULL == ftar)
    return;

//  fprintf(stderr, "closing tar, records [%d] tar ptr [%x]\n",recCount,tar);
  if (recCount > 1 && tar != (TAR *) NULL) {
//    fprintf(stderr,"Closing tar file\n");
    if (!lreqattr->print_header && !strcmp(URL_CGI,method)) {
      char tarfilename[300];
      char starfile[300];
      printf("Content-type: application/octet-stream\n");
      printf("Content-Disposition: attachment; filename=\"%s.tar\"\n",strlen(starfile)==0?flattenstring(tarfilename,rsquery,300):starfile);
      printf("Content-transfer-encoding: binary\n\n");
      lreqattr->print_header = 1;
    }

    fflush(NULL);
    if (lreqattr->infobuffer !=NULL) {
      if (flytar_append_file_header(tar, "jsoc/file_list.txt", strlen(lreqattr->infobuffer))){
        fprintf(stderr, "Error in flytar_append_file\n");
      } else {
        //fprintf(stderr,"lreqattr->infobuffer [%s]\n",lreqattr->infobuffer);
        //fprintf(stdout,lreqattr->infobuffer);
        fprintf(ftar,lreqattr->infobuffer);
        if (flytar_append_file_tail(tar, strlen(lreqattr->infobuffer))) {
          fprintf(stderr, "Error in flytar_append_tail\n");
        }
        free(lreqattr->infobuffer);
        lreqattr->infobuffer=NULL;
      }
    }

    fflush(NULL);
    if ((lreqattr->errorbuffer != (char *) NULL)) {
      if (flytar_append_file_header(tar, "jsoc/error_list.txt", strlen(lreqattr->errorbuffer))){
        fprintf(stderr, "Error in flytar_append_file\n");
      } else {
        //fprintf(stdout,lreqattr->errorbuffer);
        fprintf(ftar,lreqattr->errorbuffer);
        if (flytar_append_file_tail(tar, strlen(lreqattr->errorbuffer))) {
          fprintf(stderr, "Error in flytar_append_tail\n");
        }
        free(lreqattr->errorbuffer);
        lreqattr->errorbuffer=NULL;
      }
    }

    //fprintf(stderr,"before appending ACK file\n");
    fflush(NULL);
    flytar_append_file(tar,ftar,sackfile);
    fprintf(stderr,"tar closing ... \n");
    tar_append_eof(tar);
    tar_close(tar);
    lreqattr->tar = NULL;
//    fprintf(stderr,"tar pointer [%x]\n",tar);
    fflush(NULL);
  } else {
//TODO see what to do in this case
// probably tie the error to a error based file in a status directory
    if (lreqattr->errorbuffer != NULL) {
        //char *basename = strrchr(fitsfilename, '/');
        //basename = basename? basename + 1: (char *) fitsfilename;
        if (!lreqattr->print_header && !strcmp(URL_CGI,method)) {
          printf("Content-type: text/html\n\n");
          lreqattr->print_header = 1;
        }
        printf("<html><base>\n<h1>Retrieval for the below data failed</h1>\n");
        //printf("<p>DRMS query:<font color=\"blue\">[%s]</font></p>\n",rsquery);
        printf("<p><i><font color=\"blue\">%s</font></i></p>\n",lreqattr->errorbuffer);
        printf("<p>If a retry does not fix the problem, it could be the data is missing. In which case, as there are no data regeneration capabilities at JSOC at the moment, it should be considered as missing data and not retrievable at the present time.</p>\n<p>We are currently working on showing the detailed reason for the export failure.</p>\n");
        printf("<p>Thanks for your patience</p>\n<p>Please contact <a href=\"mailto:igor@noao.edu\">igor@noao.edu</a> or  <a href=\"mailto:oneiros@grace.nascom.nasa.gov\">oneiros@grace.nascom.nasa.gov</a> for further questions.</p>\n");
        printf("</base></html>");
    }
  }
}

void CallbackInfo(RequestAttr *lreqattr, char * filename) {

  char line[400];

  sprintf(line,"%s\n",filename);

  int linelen = strlen(line);
  char *ptr = lreqattr->infobuffer;


  if (lreqattr->infobuffer == (char *) NULL) {

    char    *infohead          = "# List of files in Tar file #\n";
    int      infobufferlen     = strlen(infohead) + linelen + 1;  // 1 is for EndOfString char \0

    lreqattr->infobuffer =  malloc(sizeof(char)*infobufferlen);

    memset(lreqattr->infobuffer,0,sizeof(char)*infobufferlen);

    sprintf(lreqattr->infobuffer,"%s",infohead);
    //repointing infobuffer to unwritten space
    ptr = lreqattr->infobuffer + strlen(infohead)*(sizeof(char));

  } else {
    int currentbufferlen = strlen(lreqattr->infobuffer) + 1; // 1 is for EndOfString char \0

    lreqattr->infobuffer = (char *) realloc(lreqattr->infobuffer, sizeof(char)*(currentbufferlen + linelen));

    //repointing infobuffer to unwritten space
    ptr= lreqattr->infobuffer + strlen(lreqattr->infobuffer)*sizeof(char);

    //memset newly allocated memory.
    memset(ptr,0,sizeof(char)*linelen);
  }

  strcpy(ptr,line);

}

void generateStageTarName(const char * stagedir, const char * scid, int stagetarcount, char* newname) {
  sprintf(newname,"%s/%s_%04d.tar",stagedir, scid,stagetarcount);
}

int addQueryToReqattr(RequestAttr *reqattr,const char * query,const char * ffmt) {
  int allocbatch=5;

  if (reqattr->rscount > 0 ) {
  }

  if (reqattr->rsquery == NULL || (reqattr->rscount%allocbatch==0) ) {
    //allocates pointers in batches
//fprintf(stderr, "realloc pointers of array [%d]\n", alloccount);
    int alloccount = (reqattr->rscount/5) + 1;
    reqattr->rsquery = (char **) realloc(reqattr->rsquery,sizeof(char *)*allocbatch*alloccount);
    reqattr->rsffmt = (char **) realloc(reqattr->rsffmt,sizeof(char *)*allocbatch*alloccount);
  }

  //note that the index in reality is reqattr->rscount -1, but here
  // should be pointing to the next element.
  reqattr->rsffmt[reqattr->rscount] = (char *) calloc(strlen(ffmt)+1,sizeof(char));
  reqattr->rsquery[reqattr->rscount] = (char *) calloc(strlen(query)+1,sizeof(char));

  strcpy(reqattr->rsffmt[reqattr->rscount],ffmt);
  strcpy(reqattr->rsquery[reqattr->rscount],query);
  reqattr->rscount++;

  return 0;
}
int populatereqattr(RequestAttr *reqattr, const char * rsqueryfile) {
  FILE *fqry = NULL;
  fqry = fopen(rsqueryfile, "r");

  if (fqry != NULL) {
    fprintf(stderr,"File [%s] does exist.\n",rsqueryfile);

//need to check for the following
// 1.- comment line: A line that starts with # and ends with \n
//    E.g.
//      ^#
// 2.- info line: look for the sequence <|!|> and the end of line \n
//

    char c=0;

    int allocbatch=5;
    int j=0; //rscount
    int alloccount=1;

    while ( (c = (char)getc(fqry) ) != EOF) {
      if (c == '#') {
         while ( !((c = (char)getc(fqry) ) == '\n' || c == EOF));
      } else if (c != '\n')  {
          int i=0;
          char line[4096];
          memset(line, 0, 4096);

          line[i++]=c;
          while ( !((c = (char)getc(fqry) ) == '\n' || c == EOF)) {
            line[i++]=c;
          }
//          printf("line is [%s]\n",line);
          char *saveptr=NULL;
          char *ffmt = NULL;
          char *query= NULL;

          ffmt = strtok_r(line,"<|!|>",&saveptr);
          if (ffmt != NULL) {
            query = strtok_r(NULL,"<|!|>",&saveptr);
            if (query != NULL) {
              fprintf(stderr,"ffmt--> [%s], query--> [%s]\n",ffmt,query);
              fprintf(stderr,"line--> [%s]\n",line);
              int qrysize=0;
              int ffmtsize=0;
              qrysize=strlen(query)+1;
              ffmtsize=strlen(ffmt)+1;

              if (reqattr->rsquery == NULL || (j%allocbatch==0) ) {
                //allocates pointers in batches
fprintf(stderr, "realloc pointers of array [%d]\n", alloccount);
                reqattr->rsquery = (char **) realloc(reqattr->rsquery,sizeof(char *)*allocbatch*alloccount);
                reqattr->rsffmt = (char **) realloc(reqattr->rsffmt,sizeof(char *)*allocbatch*alloccount);
                alloccount++;
              }

              reqattr->rsffmt[j] = (char *) calloc(ffmtsize,sizeof(char));
              reqattr->rsquery[j] = (char *) calloc(qrysize,sizeof(char));

              strcpy(reqattr->rsffmt[j],ffmt);
              strcpy(reqattr->rsquery[j],query);
              j++;
             }
          }

      }
    }
    fclose(fqry);
    //printf("\n\n\n\n\n\n");

    reqattr->rscount=j;

    return  0;
  } else {
//    fprintf(stderr, "ERROR::File %s does not exist.\n",rsqueryfile);
    return 1;
  }
}

void printRequestAttr(RequestAttr *reqattr) {
  int i =0;
  fprintf(stderr,"Query Count [%d]\n",reqattr->rscount);
  fprintf(stderr,"Stage Data Dir [%s]\n",reqattr->outpath);
  fprintf(stderr,"Stage Status Dir [%s]\n",reqattr->stagestatusfile);
  for (; i<reqattr->rscount; i++) {
    fprintf(stderr,"ffmt:[%s]\n",reqattr->rsffmt[i]);
    fprintf(stderr,"query:[%s]\n",reqattr->rsquery[i]);
  }
}

void freeRequestAttr(RequestAttr *reqattr) {
  int i =0;
  for (; i<reqattr->rscount; i++) {
    free(reqattr->rsquery[i]);
    free(reqattr->rsffmt[i]);
  }

  free(reqattr->rsquery);
  free(reqattr->rsffmt);
}

int checkfile(const char *filename) {
  struct stat buf;
  if (!stat(filename,&buf)) {
    if(S_ISREG(buf.st_mode)) {
      return access(filename,R_OK|W_OK);
    }
  }
  return 1;
}
int checkdir(const char *dirname) {
  struct stat buf;
  if (!stat(dirname,&buf)) {
    if(S_ISDIR(buf.st_mode)) {
      return access(dirname,R_OK|W_OK);
    }
  }
  return 1;
}

/* fTarGlobal is modified in ExportHandle() and in the openfunc callbacks that are passed to tar_open(). */
static FILE *fTarGlobal = NULL;          //file pointer to the tar file. Note: it's different from TAR*
                                         // which is the TAR file pointer structure for the tar library.


int tarURLOpen (const char *path, int flags, ... ) 
{
   fflush(NULL);
   fTarGlobal = stdout;
   return fileno(fTarGlobal); //return STDOUT file descriptor == (1)
}

int tarStageOpen (const char *path, int flags, ... ) 
{
   fTarGlobal = fopen(path, "w");
   return fileno(fTarGlobal); 
}

void ExportHandle(char *callbackState, ...) {

   //TODO explain the diferent methods

   va_list arguments;
   va_start(arguments, callbackState);
   static DRMS_Array_t *arrout = NULL; //internal pointer to the DRMS_Array structure
                                       //arrout points to the actual image and needs to
                                       // be freed from ExportHandle.
   static long long l_maxtarsize   = MAXTARSIZE;
   static long long  totalsize   = 0;       //Size of total export
   static int   totalcount  = 0;       //Number of total exports
   static long  currentsize = 0;       //current size of current file

   static int   stagetarcount=0;       //Number of tar files created
   static char  currentfileitem[1024]; //name of the current file being exported
   static int   first = 1; //TODO revise see if it can be removed.
                          // Normally we need to know the difference between
                          //  first time into create "method" and subsequence calls to the method

   static RequestAttr *lreqattr=NULL;  //RequestAttr_struct. This is declare in the DoIt method.

   if (strcmp(callbackState,"create")==0) {

     fitsfile **fptr           = (fitsfile **)  va_arg(arguments, fitsfile **);
     char      *filein         = (char *)       va_arg(arguments, char *);
     char      *cparms         = (char *)       va_arg(arguments, char *);
     int       *cfiostat       = (int *)        va_arg(arguments, int  *);
     int       *retVal         = (int *)        va_arg(arguments, int  *);

     char      tempfilein[1024];
     char      filename[252];

     strcpy(tempfilein,"mem://");
     totalcount++;
     //fprintf(stderr, "filein [%s]\n",filein);

     l_maxtarsize = !strcmp(lreqattr->method,URL_CGI)?MAXTARSIZE:MAXFILETARSIZE;

//#### If first time, only one record and "on the fly"
     if (first && (1 == lreqattr->reccount) && !strcmp(lreqattr->method,URL_CGI)) {
       //sprintf(currentfileitem,"%s/%s", "jsoc", filein);
       sprintf(currentfileitem,"%s", filein);
     }
//#### If first time, multiple records (<==> to tar file) and it's
//#### an "on the fly" request then build html headers for a
//#### tar streaming. Also create a dummy tar file stdout) that will be used
//#### internally in place of a real tar file.
     else if ((1 < lreqattr->reccount) && !strcmp(lreqattr->method,URL_CGI)) {
       char tarfilename[300];
       if (first) {
         lreqattr->print_header=1;
         printf("Content-type: application/octet-stream\n");
         printf("Content-Disposition: attachment; filename=\"%s.tar\"\n",strlen(starfile)==0?flattenstring(tarfilename,lreqattr->rsquery[lreqattr->arraypointer],300):starfile);
         printf("Content-transfer-encoding: binary\n\n");


         fprintf(stderr, "cgi_request(1)=%s, pid=%d; ppid=%d\n",gcgi_request,getpid(),getppid());
         if (!strcmp(gcgi_request,"HEAD")) {
           fflush(stdout);
           lreqattr->terminate=1;
         }
         //Create dummy tar file for straight STDOUT streaming
         lreqattr->tartype.openfunc = &tarURLOpen; //Set our own open function

         //create tar
         if (tar_open(&lreqattr->tar, "some_generic_name.tar", &lreqattr->tartype,
                O_WRONLY | O_CREAT, 0644, TAR_GNU) == -1 ) {
           return;
         }
         first = 0;
       }
       fflush(NULL);
//       strcpy(currentfileitem, filein);
       sprintf(currentfileitem,"%s/%s", "jsoc", filein);
       fprintf(stderr, "first, URL_CGI and multiple record\n");
     }
//#### Only one record to export and it's a stage request
//#### then no memory writing step, rather direct writting to
//#### final destination.
     else if (first && (1 == lreqattr->reccount) && !strcmp(lreqattr->method,STAGE_CGI)) {
       strcpy(tempfilein,"");
       sprintf(tempfilein,"%s/%s",lreqattr->outpath,filein);
       first = 0;
       fprintf(stderr, "first, STAGE_CGI and only one record\n");
     }
//#### If first time or tar file size limit reached and multiple records (<==> to
//#### tar file) and it's a stage request then create a real tar file on disk
//TODO should only get in to create a new tar file. that's all
     //else if ( (first || currentsize > MAXTARSIZE) && (1 < lreqattr->reccount) && !strcmp(lreqattr->method,STAGE_CGI)) {
     else if ( (first || currentsize > l_maxtarsize) && (1 < lreqattr->reccount) && !strcmp(lreqattr->method,STAGE_CGI)) {
       lreqattr->tartype.openfunc = &tarStageOpen;  //set our own open function

       // If the tar files needs to be broken up because of its size,
       // we need to close the current one and create a new tarfile.
       //if (currentsize > MAXTARSIZE) {
       if (currentsize > l_maxtarsize) {
          // TODO check that the list file and error file get save
          // and initialized for a next round.
          ExportHandle("close");

          currentsize=0; //reset current tar file size.
          stagetarcount++;
       } else {
         first = 0;
       }


       char tarfilestagename[1024];
       fprintf(stderr,"b4 setting stage file scid [%s], stagetarcount [%04d], stagedir [%s], currentfileitem[%s]\n", lreqattr->scid, stagetarcount, lreqattr->outpath, currentfileitem);
       generateStageTarName(lreqattr->outpath, lreqattr->scid, stagetarcount, (char *) &tarfilestagename);
       fprintf(stderr,"create new tar file [%s]\n",tarfilestagename);
       if (tar_open(&lreqattr->tar, tarfilestagename, &lreqattr->tartype,
              O_WRONLY | O_CREAT, 0644, TAR_GNU) == -1 ) {
         return;
       }
       fflush(NULL);
       fprintf(stderr, "interation [%d], STAGE_CGI and multiple records\n",stagetarcount);
     }

     // keep copying the next current file into current file item
     if ((1 < lreqattr->reccount) && !strcmp(lreqattr->method,STAGE_CGI)) {
       sprintf(currentfileitem,"%s/%s", "jsoc", filein);
     }

     //fprintf(stderr, "after reading params filein=[%s]\n", filein);
     //save file element locally for second call of the callBack function when a tar file exists.

     if (cparms && *cparms) {
       snprintf(filename, sizeof(filename), "%s[%s]", tempfilein, cparms);
     } else {
       //still if more than one file just compress
       if ( 1 < lreqattr->reccount) {
         snprintf(filename, sizeof(filename), "%s[compress Rice]", tempfilein);
         //snprintf(filename, sizeof(filename), "%s", tempfilein);
       }
       else {
         snprintf(filename, sizeof(filename), "%s", tempfilein);
       }
     }

     fprintf(stderr, "before fits_create_file [%s]\n", filename);
     *retVal=fits_create_file(fptr,filename, cfiostat);

   }
//##############################
//### CallBack "stdout" logic
   else if (strcmp(callbackState,"stdout")==0) {
     fitsfile *fptr    = (fitsfile *) va_arg(arguments, fitsfile *);
     int *retVal       = (int *)      va_arg(arguments, int *);
     int mystatus      = 0;
     int cpstatus=0;

     if ((1 == lreqattr->reccount) && !strcmp(lreqattr->method,STAGE_CGI)) {
       fits_flush_file(fptr, &cpstatus); //flush and exit
       return; //it copies the data directly, no mem copy so no
               // need to copy from mem to other file descriptor
     }
     // TODO review this for the stage case
//#### If first time around and only one record and it is a URL_CGI request
//#### then print appropriate headers
     fprintf(stderr,"recCount is [%ld]\n", lreqattr->reccount);
     if (first && (1 == lreqattr->reccount) && !strcmp(lreqattr->method,URL_CGI)) {

//       char *basename = strrchr(currentfitsfilename, '/');
//       basename = basename? basename + 1: (char *) currentfitsfilename;
       lreqattr->print_header=1;
       printf("Content-type: application/octet-stream\n");
       //printf("Content-Disposition: attachment; filename=\"%s\"\n",basename);
       printf("Content-Disposition: attachment; filename=\"%s\"\n",currentfileitem);
       printf("Content-Length: %lld\n", fptr->Fptr->logfilesize);
       printf("Content-transfer-encoding: binary\n\n");

       fprintf(stderr, "cgi_request(2)=%s, pid=%d; ppid=%d\n",gcgi_request,getpid(),getppid());
       if (!strcmp(gcgi_request,"HEAD")) {
         fflush(stdout);
         lreqattr->terminate=1;
       }

       fTarGlobal = stdout;
       first=0;
       fits_flush_file(fptr, &cpstatus);
     }

     fprintf(stderr, "\nfile [%s], file size [%lld]\n", currentfileitem, fptr->Fptr->logfilesize);
     //if (totalsize <= MAXTARSIZE || !strcmp(lreqattr->method,STAGE_CGI)) {
     if (totalsize <= l_maxtarsize|| !strcmp(lreqattr->method,STAGE_CGI)) {

       if (lreqattr->reccount > 1) {
         fflush(NULL);
         totalsize += fptr->Fptr->logfilesize;
         currentsize += fptr->Fptr->logfilesize;

         if (flytar_append_file_header(lreqattr->tar, currentfileitem, fptr->Fptr->logfilesize)){
           *retVal = -1;
         }
       }

       //free some memory
       if (arrout->data != NULL) {
         free(arrout->data);
         arrout->data=NULL;
         arrout=NULL;
       }

       fflush(NULL);

       //Dump fits mem file to stdout
       int hdunum=0;
       fits_flush_file(fptr, &cpstatus);
       fits_get_num_hdus(fptr, &hdunum, &cpstatus);
       if (hdunum > 0) {
         for (int i=1; i<=hdunum; i++) {
           //fprintf(stderr,"before writing hdu [%d]\n",i);
           int hdutype=0;
           fits_movabs_hdu(fptr, i, &hdutype, &cpstatus);
           fits_write_hdu(fptr, fTarGlobal, &cpstatus);
           fflush(fTarGlobal);
         }
       }

       fflush(NULL);
       if (lreqattr->reccount > 1) {
         if (flytar_append_file_tail(lreqattr->tar, fptr->Fptr->logfilesize)) {
           stgprintf(lreqattr->fstatus,"EXPORT_MSG=Error in flytar_append_tail\n");
           *retVal = -1;
         }
       }
       fits_close_file(fptr, &mystatus);
     } else {
       lreqattr->terminate =1;   //want to signal termination.
       fits_close_file(fptr, &mystatus);
       CallbackErrorBuffer( lreqattr, currentfileitem, "Size limitation in tar file.User rice compression or/and make a smaller query", 2);
     }
     fprintf(stderr,"ExportHandle --stdout is [%d]\n",lreqattr->terminate);
   } else if (strcmp(callbackState,"info")==0) {
     char      *filename         = (char *)       va_arg(arguments, char *);
     fprintf(stderr,"ExportHandle --info is [%d]\n",lreqattr->terminate);
     CallbackInfo(lreqattr,filename);

   } else if (strcmp(callbackState,"error")==0) {
     char      *query            = (char *)       va_arg(arguments, char *);
     char      *filename         = (char *)       va_arg(arguments, char *);
     fprintf(stderr,"ExportHandle -- error is [%d]\n",lreqattr->terminate);
     CallbackErrorBuffer( lreqattr, filename, query, 0);
   } else if (strcmp(callbackState,"close")==0) {
     fprintf(stderr,"ExportHandle --close is [%d]\n",lreqattr->terminate);
     CallbackClose(lreqattr, fTarGlobal);
     fTarGlobal = NULL; //at this point the TAR file should be closed.
     lreqattr->tar=NULL;
   } else if (strcmp(callbackState,"setarrout")==0) {
     fprintf(stderr,"ExportHandle setting arrout [%d]\n", lreqattr->terminate);
     arrout    = (DRMS_Array_t *) va_arg(arguments, DRMS_Array_t *);
   } else if (strcmp(callbackState,"init")==0) {
     RequestAttr      *_reqattr = (RequestAttr *)       va_arg(arguments, RequestAttr *);
     fprintf(stderr,"ExportHandle -- init done\n");
     lreqattr=_reqattr;
     lreqattr->arraypointer=0;
     // reqattr->recCount should be initialized with the number of query items.
     // then each query item might add to the total tally by having several segments
     // a tally is kept below.
     lreqattr->reccount=lreqattr->rscount;

     lreqattr->tartype.openfunc = open;
     lreqattr->tartype.closefunc = close;
     lreqattr->tartype.writefunc = write;
     lreqattr->tartype.readfunc = read;

     lreqattr->terminate=0;  //Currently terminate only gets enable if
                             // MAXTARSIZE is reached under URL_CGI method
     lreqattr->print_header=0; //Keeps track of if a HTML header has been issued.
   } else if (strcmp(callbackState,"getsize")==0) {
     int       *tsize          = (int *)        va_arg(arguments, int  *);
     *tsize=totalsize;
   } else if (strcmp(callbackState,"getcount")==0) {
     int       *tcount          = (int *)        va_arg(arguments, int  *);
     *tcount=totalcount;
   }


   va_end(arguments);
}

FILE * openstatefileold(const char *path, const char *reqid, int *cstatus) {
  char oldfilename[512];
  char newfilename[512];
  const char *oldfilename_ptr=oldfilename;
  const char *newfilename_ptr=newfilename;
  FILE * fqued=NULL;

  pid_t  pid = getpid();
//  sprintf(newfilename,"%s/%s.%d.%s",path,reqid,pid,QUED);
  sprintf(newfilename,"%s/%s.%s",path,reqid,QUED);
  sprintf(oldfilename,"%s/%s.%s",path,reqid,NQUE);

  if (!checkfile(newfilename)) {
    *cstatus=kMymodErr_QuedFileExists;
    return NULL;
  }
  if (!checkfile(oldfilename)) {
    if (rename(oldfilename_ptr,newfilename_ptr)) {
      fprintf(stderr, "Error renaming [%s] to [%s] with errno [%d]\n",oldfilename_ptr,newfilename_ptr,errno);
      *cstatus=kMymodErr_Rename2QuedFail;
      return NULL;
    }
  }

  fqued=fopen(newfilename_ptr,"a");
  if (fqued != NULL) {
    fprintf(stderr,"Current pid [%d]\n",pid);
  }
  return fqued;
}

FILE * openstatefile(const char *sfile, const char *reqid, int *cstatus) {
  FILE * fstatus=NULL;

  fstatus=fopen(sfile,"a");
  if (fstatus != NULL) {
    pid_t  pid = getpid();
    fprintf(stderr,"Current pid [%d]\n",pid);
  }
  return fstatus;
}

int stgprintf(FILE *file, const char * format, ...) {
  int ret=0;
  va_list args;
  va_start(args,format);

  if (NULL == file) {
    ret=vfprintf(stderr,format,args);
  } else {
    ret=vfprintf(file,format,args);
  }

  va_end(args);

  return ret;
}

void setstagestate(const char * path, const char * reqid, const char * state) {
  char oldfilename[512];
  char newfilename[512];
  const char *oldfilename_ptr=oldfilename;
  const char *newfilename_ptr=newfilename;

  sprintf(oldfilename,"%s/%s.%s",path,reqid,QUED);
  sprintf(newfilename,"%s/%s.%s",path,reqid,state);

  if (!checkfile(oldfilename)) {
    rename(oldfilename_ptr,newfilename_ptr);
  } else {
    fprintf(stderr,"ERROR, could not find [%s]. Touching one.\n",oldfilename);
    FILE *file =  fopen(newfilename_ptr,"a");
    fclose(file);
  }
}
//ISS fly-tar END


