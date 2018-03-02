/*
    This module always sends FITS file to its stdout stream. The actual file stream that the FITS-file bytes are sent 
    can be controlled by redirecting stdout. If using the Python subprocess module, the caller can supply the stdout
    argument to various subprocess functions to redirect child-process output to a different stream (such as a disk file
    or a pipe). 
 */
 
/* The caller will take care of the HTML headers, if this module is run in a cgi or web context. The header looks like:
 *   Content-type: application/octet-stream
 *   Content-Disposition: attachment; filename="tarfilename.tar"
 *   Content-transfer-encoding: binary
 */
#include <pwd.h>
#include <grp.h>
#include "jsoc_main.h"
#include "exputil.h"
#include "fitsexport.h"

/* third-party stuff */
#include "fitsio.h"
#include "libtar.h"

char *module_name = "export-to-stdout";

#define ARG_RS_SPEC "spec"
#define ARG_KEYMAP_CLASS "mapclass"
#define ARG_KEYMAP_FILE "mapfile"
#define ARG_FILE_TEMPLATE "ffmt"
#define ARG_CPARMS_STRING "cparms"

#define MSGLEN_NUMBYTES 8
#define MAX_MSG_BUFSIZE 4096

#define FILE_LIST_PATH "jsoc/file_list.txt"
#define ERROR_PATH "jsoc/error_list.txt"

#define MAX_CGI_TAR_SIZE 2147483648 /* 2 GB*/
#define MAX_TAR_SIZE 53687091200 /* 50 GB */
#define TAR_BLOCK_SIZE 512

/* return statuses */
#define RV_BAD_RECORDSET "bad-recordset"
#define RV_DONE "done"

/* status codes */
enum __ExpToStdoutStatus_enum__
{
    ExpToStdoutStatus_Success = 0,
    ExpToStdoutStatus_InvalidArgs = 1,
    ExpToStdoutStatus_Dump = 2,
    ExpToStdoutStatus_OutOfMemory = 3,
    ExpToStdoutStatus_DumpPadding = 4,
    ExpToStdoutStatus_GetUser = 5,
    ExpToStdoutStatus_GetGroup = 6,
    ExpToStdoutStatus_IO = 7,
    ExpToStdoutStatus_BadFilenameTemplate = 8,
    ExpToStdoutStatus_DRMS = 9,
    ExpToStdoutStatus_Stdout = 10
};

typedef enum __ExpToStdoutStatus_enum__ ExpToStdoutStatus_t;

/* compression strings */
#define COMPRESSION_NONE "none"
#define COMPRESSION_RICE "rice"
#define COMPRESSION_GZIP1 "gzip1"
#define COMPRESSION_GZIP2 "gzip2"
#define COMPRESSION_PLIO "plio"
#define COMPRESSION_HCOMP "hcompress"

/* compression enum */
enum __ExpToStdout_Compression_enum__
{
    ExpToStdout_Compression_NONE = 0,
    ExpToStdout_Compression_RICE = RICE_1,
    ExpToStdout_Compression_GZIP1 = GZIP_1,
    ExpToStdout_Compression_GZIP2 = GZIP_2,
    ExpToStdout_Compression_PLIO = PLIO_1,
    ExpToStdout_Compression_HCOMP = HCOMPRESS_1
};

typedef enum __ExpToStdout_Compression_enum__ ExpToStdout_Compression_t;

ModuleArgs_t module_args[] =
{
    { ARG_STRING, ARG_RS_SPEC, NULL, "record-set query that specifies data to be exported" },
    { ARG_STRING, ARG_KEYMAP_CLASS, " ", "export key-map class" },
    { ARG_STRING, ARG_KEYMAP_FILE, " ", "export key-map file" },
    { ARG_STRING, ARG_FILE_TEMPLATE, " ", "export filename template" },
    { ARG_STRINGS, ARG_CPARMS_STRING, " ", "a list of FITSIO compression types (none, rice, gzip, plio, hcompress), one for each segment" },
    { ARG_END }
};

static void *GetOptionValue(ModuleArgs_Type_t type, const char *key)
{
    void *rv = NULL;

    switch (type)
    {
        case ARG_STRING:
        {
            const char *innards = NULL;
            
            innards = params_get_str(&cmdparams, (char *)key); /* stupid cmdparams does not make this parameter const */
            if (strcmp(innards, " "))
            {
                rv = (char *)innards;
            }
            break;
        }
        case ARG_STRINGS:
        {
            char **strings = NULL;
            int nElems = 0;
            LinkedList_t *list = NULL;
            int iElem;
            
            nElems = cmdparams_get_strarr(&cmdparams, (char *)key, &strings, NULL); /* stupid cmdparams does not make this parameter const */
            
            if (nElems != 0 && (nElems != 1 || strcmp(strings[0], " ")))
            {
                list = list_llcreate(sizeof(char *), NULL);
                if (list)
                {
                    for (iElem = 0; iElem < nElems; iElem++)
                    {
                        list_llinserttail(list, strings[iElem]);
                    }

                    rv = list;                
                }
            }
            break;
        }
    }
    
    return rv;
}

static int sendMsg(FILE *stream, const char *msg, uint32_t msgLen)
{
    size_t bytesSentTotal;
    size_t bytesSent;
    char numBytesMesssage[MSGLEN_NUMBYTES + 1];
    const char *pBuf = NULL;
    int err = 0;
    
    if (msg && *msg)
    {
        /* first send the length of the message, msgLen */
        snprintf(numBytesMesssage, sizeof(numBytesMesssage), "%08x", msgLen);
        bytesSent = fwrite(numBytesMesssage, 1, MSGLEN_NUMBYTES, stream);
    
        if (bytesSent != MSGLEN_NUMBYTES)
        {
            fprintf(stderr, "unable to send message length\n");
            err = 1;
        }
        else
        {
            /* then send the message */
            bytesSent = fwrite(msg, 1, msgLen, stream);
            if (bytesSent != msgLen)
            {
                fprintf(stderr, "unable to send message\n");
                err = 1;
            }
        }
    }
    else
    {
        fprintf(stderr, "invalid arguments to sendMsg()\n");
    }
    
    fflush(stream);
    
    return err;
}

/* outBuf is dynamically allocated; outSize is the current size of allocation */
static ExpToStdoutStatus_t Dump(FILE *stream, const char *buf, size_t numBytes)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;

    if (stream)
    {    
        if (fwrite(buf, 1, numBytes, stream) != numBytes)
        {
            fprintf(stderr, "unable to dump to stream\n");
            expStatus = ExpToStdoutStatus_Dump;
        }
    }
    else
    {
        fprintf(stderr, "Dump(): invalid arguments\n");
    }
    
    return expStatus;
}

static ExpToStdoutStatus_t DumpPadding(FILE *stream, size_t existing, size_t total)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    
    if (total > existing)
    {
        char *buf = NULL;
        
        buf = calloc(total - existing, 1);
        if (buf)
        {
            expStatus = Dump(stream, buf, total - existing);
            free(buf);
        }
        else
        {
            expStatus = ExpToStdoutStatus_OutOfMemory;
        }
    }
    else if (total < existing)
    {
        fprintf(stderr, "cannot pad to %lu bytes when %lu bytes have already been written\n", total, existing);
        expStatus = ExpToStdoutStatus_DumpPadding;
    }
    
    return expStatus;
}

static ExpToStdoutStatus_t DumpAndPad(FILE *stream, const char *buf, size_t numBytes, size_t total)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;

    if (total >= numBytes)
    {
        expStatus = Dump(stream, buf, numBytes);
        if (expStatus == ExpToStdoutStatus_Success)
        {
            expStatus = DumpPadding(stream, numBytes, total);
        }
    }
    
    return expStatus;
}

static ExpToStdoutStatus_t DumpOctal(FILE *stream, long long value, size_t fieldWidth)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    
    char *field = NULL;
    
    field = calloc(1, fieldWidth + 1);
    if (field)
    {
        snprintf(field, sizeof(field), "%07llo\0", value); /* trailing NUL ('\0') char */
        expStatus = Dump(stream, field, fieldWidth);
    }
    
    return expStatus;
}

/* header must consist of all ascii chars */
static ExpToStdoutStatus_t DumpFileObjectHeader(FILE *stream, const char *fileName, size_t fileSize)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    
    struct passwd pwd;
    struct passwd *resultPwd = NULL;
    struct group grp;
    struct group *resultGrp = NULL;
    uid_t uid;
    gid_t gid;
    char *idBuf = NULL;
    size_t idBufSize;
    int pipefds[2];  /* stupid checksum - we cannot dump bytes on stdout in one pass */
    char header[512];
    FILE *writeStream = NULL;
    FILE *readStream = NULL;
    unsigned long long chksum = 0;
    
    if (pipe(pipefds))
    {
        expStatus = ExpToStdoutStatus_IO;
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* open write end */
        writeStream = fdopen(pipefds[1], "w");
        if (!writeStream)
        {
            expStatus = ExpToStdoutStatus_IO;
        }
        else
        {        
            /* open read end */
            readStream = fdopen(pipefds[0], "r");
            if (!readStream)
            {
                expStatus = ExpToStdoutStatus_IO;
            }
        }
    }
        
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* file name - left-justified string will all trailing '\0' bytes */
        expStatus = DumpAndPad(writeStream, fileName, strlen(fileName), 100);
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* file mode - octal */
        expStatus = DumpOctal(writeStream, 436, 8); /* O0664 */
    }    

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* uid - octal */
        idBufSize = (size_t)sysconf(_SC_GETPW_R_SIZE_MAX);
    
        if (idBufSize == -1)
        {
            idBufSize = 16384;
        }

        idBuf = malloc(idBufSize);
        if (!idBuf )
        {
            expStatus = ExpToStdoutStatus_OutOfMemory;
        }
        else
        {
            uid = getuid();
            getpwuid_r(uid, &pwd, idBuf, idBufSize, &resultPwd);
        
            if (!resultPwd)
            {
                /* not found or failure */
                fprintf(stderr, "user id %u not found\n", uid);
                expStatus = ExpToStdoutStatus_GetUser;
            }
        }

        expStatus = DumpOctal(writeStream, pwd.pw_uid, 8);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* gid - octal */
        gid = pwd.pw_gid;
        expStatus = DumpOctal(writeStream, gid, 8);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* size - octal */    
        expStatus = DumpOctal(writeStream, fileSize, 12);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* mtime - octal */
        expStatus = DumpOctal(writeStream, time(NULL), 12);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* chksum - sum of unsigned byte values of all other fields in header */
        
        /* for now, pretend that the checksum is 8 spaces, because the spec says
         * to set this field to all spaces when calculating the checksum
         */
         expStatus = Dump(writeStream, "        ", 8);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* file type (regular) */
        expStatus = DumpOctal(writeStream, 0, 1);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* linkname - string (unused) */
        expStatus = DumpPadding(writeStream, 0, 100);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* magic - "ustar" */
        expStatus = DumpAndPad(writeStream, "ustar", 5, 6);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* version - "00" with NO terminating NUL char */
        expStatus = DumpAndPad(writeStream, "00", 2, 2);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* uname */
        expStatus = DumpAndPad(writeStream, pwd.pw_name, strlen(pwd.pw_name), 32);
        
        /* no longer need pwd - we need gid, but we copied that already */
        if (idBuf)
        {
            free(idBuf);
            idBuf = NULL;
        }
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* gname */

        /* sigh */
        idBufSize = (size_t)sysconf(_SC_GETGR_R_SIZE_MAX);
    
        if (idBufSize == -1)
        {
            idBufSize = 16384;
        }

        idBuf = malloc(idBufSize);
        if (!idBuf)
        {
            expStatus = ExpToStdoutStatus_OutOfMemory;
        }
        else
        {
            getgrgid_r(gid, &grp, idBuf, idBufSize, &resultGrp);
            
            if (!resultGrp)
            {
                /* not found or failure */
                fprintf(stderr, "group id %u not found\n", gid);
                expStatus = ExpToStdoutStatus_GetGroup;
            }
            else
            {
                expStatus = DumpAndPad(writeStream, grp.gr_name, strlen(grp.gr_name), 32);
            }
        }

        /* no longer need grp */
        if (idBuf)
        {
            free(idBuf);
            idBuf = NULL;
        }
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* device major */
        expStatus = DumpPadding(writeStream, 0, 8);
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* device minor */
        expStatus = DumpPadding(writeStream, 0, 8);
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* prefix - path to file (just a base name) */
        expStatus = DumpPadding(writeStream, 0, 155);
    }
    
    /* now we have to read back the header */
    if (expStatus == ExpToStdoutStatus_Success)
    {
        char *ptr = NULL;
        size_t num = 0;
        
        /* flush, close the write-end of the pipe */
        fflush(writeStream);
        fclose(writeStream); 
        close(pipefds[1]); /* necessary so that reading from the read end does not block */
        
        /* read the read end of the pipe */
        memset(header, 0, sizeof(header));
        ptr = header;
        while (1)
        {
            num = fread(header, sizeof(char), 512, readStream);
            if (num <= 0)
            {
                break;
            }
            else
            {
                ptr += num;
            }
        }
        
        fclose(readStream);
        close(pipefds[0]);
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        char *ptr;
        
        /* calculate the checksum */
        ptr = header;
        while (ptr < header + sizeof(header))
        {
            chksum += (int)*ptr;
            ptr++;
        }        
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* now dump all the bytes before the checksum */
        expStatus = Dump(stream, header, 148);
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* dump the checksum */
        expStatus = DumpOctal(stream, chksum, 8);
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* finally, dump the rest */
        expStatus = Dump(stream, &header[156], 357);
    }

    /* on error, this might not have gotten cleaned up */
    if (idBuf)
    {
        free(idBuf);
        idBuf = NULL;
    }

    return expStatus;
}

static ExpToStdoutStatus_t FillBlock(FILE *stream, int blockSize, int writeSize)
{  
    return DumpPadding(stream, 0, blockSize - writeSize % blockSize);
}

/* filePath - path of the file to be stored in the TAR file 
 * buffer - file data
 * size - number of bytes of file data
 */
static ExpToStdoutStatus_t WriteFileBuffer(FILE *stream, const char *filePath, const char *buffer, size_t size)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    
    /* dump TAR header */
    expStatus = DumpFileObjectHeader(stream, filePath, size);
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* dump buffer */
        fprintf(stream, buffer);
    
        /* pad - fill up last 512 block with zeroes */
        expStatus = FillBlock(stream, 512, size);
    }
    
    fflush(stream);
    
    return expStatus;
}

/* loop over segments */
static ExpToStdoutStatus_t ExportRecordToStdout(DRMS_Record_t *expRec, const char *ffmt, ExpToStdout_Compression_t *segCompression, const char *classname, const char *mapfile, size_t *bytesExported, size_t *numFilesExported, char **infoBuf, size_t *szInfoBuf, char **errorBuf, size_t *szErrorBuf)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    int drmsStatus = DRMS_SUCCESS;
    int fiostat = 0;
    char recordSpec[DRMS_MAXQUERYLEN];
    DRMS_Segment_t *segIn = NULL;
    int iSeg;
    HIterator_t *last = NULL;
    DRMS_Segment_t *segTgt = NULL; /* If segin is a linked segment, then tgtset is the segment in the target series. */
    char formattedFitsName[DRMS_MAXPATHLEN];
    ExpUtlStat_t expUStat = kExpUtlStat_Success;
    fitsfile *fitsPtr = NULL;
    long long numBytesFitsFile; /* the actual FITSIO type is LONGLONG */
    size_t totalBytes = 0;
    size_t totalFiles = 0;
    char msg[128];
    char errMsg[256];
       
    drms_sprint_rec_query(recordSpec, expRec);

    iSeg = 0;
    while ((segIn = drms_record_nextseg(expRec, &last, 0)) != NULL)
    {
        if (segIn->info->islink)
        {
            if ((segTgt = drms_segment_lookup(expRec, segIn->info->name)) == NULL)
            {
                fprintf(stderr, "unable to locate linked segment file %s\n", segIn->info->name);
                iSeg++;
                continue;
            }
        }
        else
        {
            segTgt = segIn;
        }

        if ((expUStat = exputl_mk_expfilename(segIn, segTgt, ffmt, formattedFitsName)) != kExpUtlStat_Success)
        {
            if (expUStat == kExpUtlStat_InvalidFmt)
            {
                fprintf(stderr, "invalid file-name format template %s\n", ffmt);
            }
            else if (expUStat == kExpUtlStat_UnknownKey)
            {
                fprintf(stderr, "one or more keywords in the file-name-format template %s do not exist in series %s\n", ffmt, expRec->seriesinfo->seriesname);
            }

            expStatus = ExpToStdoutStatus_BadFilenameTemplate;
            break;
        }
        
        fiostat = 0;
        if (fits_create_file(&fitsPtr, "-", &fiostat))
        {
            fprintf(stderr, "FITSIO error: %d\n", fiostat);
            fits_report_error(stderr, fiostat);
            snprintf(msg, sizeof(msg), "cannot create FITS file\n");
            if (errorBuf)
            {
                snprintf(errMsg, sizeof(errMsg), "record = %s, file = %s, message = %s\n", recordSpec, formattedFitsName, msg);
                *errorBuf = base_strcatalloc(*errorBuf, errMsg, szErrorBuf);
            }
            
            iSeg++;
            continue; /* we've logged an error message now go onto the next segment; do not set status to error */
        }
        
        /* set compression, if requested */
        if (segCompression[iSeg] != ExpToStdout_Compression_NONE)
        {
            fits_set_compression_type(fitsPtr, segCompression[iSeg], &fiostat);
            if (fiostat)
            {
                fits_report_error(stderr, fiostat);
                iSeg++;
                continue;
            }
        }

        /* writes FITS file to write end of pipe (by re-directing stdout to the pipe) */
        drmsStatus = fitsexport_mapexport_tostdout(fitsPtr, segIn, classname, mapfile);

        if (drmsStatus == DRMS_ERROR_INVALIDFILE)
        {
            /* no input segment file, so no error - there is nothing to export because the segment file was never created */
            fprintf(stderr, "no segment file (segment %s) for this record\n", segIn->info->name);
        }
        else if (drmsStatus != DRMS_SUCCESS)
        {
            if (drmsStatus == DRMS_ERROR_CANTCOMPRESSFLOAT)
            {
                snprintf(msg, sizeof(msg), "cannot export Rice-compressed floating-point images\n");
            }
            else
            {
                /* there was an input segment file, but for some reason the export failed */
                snprintf(msg, sizeof(msg), "failure exporting segment %s\n", segIn->info->name);
            }

            fprintf(stderr, msg);

            if (errorBuf)
            {
                snprintf(errMsg, sizeof(errMsg), "record = %s, file = %s, message = %s\n", recordSpec, formattedFitsName, msg);
                *errorBuf = base_strcatalloc(*errorBuf, errMsg, szErrorBuf); 
            }
            
            /* stupid FITSIO has no way of dumping its internal buffers without writing data to stdout, but if we encountered
             * an error on export, we do not want to dump buffer contents, whatever they may be, on stdout; so, use a pipe
             * to redirect stdout, then read back from the pipe and drop the data on the floor; there might not actually be 
             * any data in the FITSIO internal buffers, so check fitsPtr->Fptr->logfilesize first
             */
            numBytesFitsFile = fitsPtr->Fptr->logfilesize;
            if (numBytesFitsFile > 0)
            {
                int pipefds[2];
                int savedStdout = -1;
                FILE *writeStream = NULL;
                FILE *readStream = NULL;
                char fileBuf[4096];
                int num;
                
                if (pipe(pipefds))
                {
                    expStatus = ExpToStdoutStatus_IO;
                    break;
                }

                if (expStatus == ExpToStdoutStatus_Success)
                {
                    savedStdout = dup(STDOUT_FILENO);
                    if (savedStdout != -1)
                    {
                        if (dup2(pipefds[1], STDOUT_FILENO) != -1)
                        {
                            fiostat = 0;
                            fits_close_file(fitsPtr, &fiostat);
                            if (fiostat)
                            {
                                fprintf(stderr, "unable to close and send FITS file\n");
                            }
                        
                            fflush(stdout);
                            close(pipefds[1]);

                            while (1)
                            {
                                num = read(pipefds[0], fileBuf, sizeof(fileBuf));
                                if (num <= 0)
                                {
                                    break;
                                }
                            
                                /* drop the data */
                            }

                            close(pipefds[0]);
                        }
                        else
                        {
                            /* can't flush FITSIO internal buffers */
                            fprintf(stderr, "unable to flush FITSIO internal buffers following error\n");
                        }
                        
                        /* restore stdout */
                        dup2(savedStdout, STDOUT_FILENO);
                    }
                    else
                    {
                        /* can't flush FITSIO internal buffers */
                        fprintf(stderr, "unable to flush FITSIO internal buffers following error\n");
                    }
                }
            }
        }
        else
        {
            /* at this point, the entire FITS file is in memory; it does not get flushed to stdout until the 
             * FITS file is closed; send message size to caller, then send FITS data 
             */
            numBytesFitsFile = fitsPtr->Fptr->logfilesize;
            if (numBytesFitsFile > 0)
            {
                /* dump FITS file to stdout (0-pads header block) */
                DumpFileObjectHeader(stdout, formattedFitsName, numBytesFitsFile);
    
                /* dump FITS-file data */
                fiostat = 0;
                fits_close_file(fitsPtr, &fiostat);
                if (fiostat)
                {
                    fprintf(stderr, "unable to close and send FITS file\n");
                }
                
                fflush(stdout);
        
                /* pad last TAR data block */
                expStatus = FillBlock(stdout, TAR_BLOCK_SIZE, numBytesFitsFile);
    
                totalBytes += numBytesFitsFile;
                totalFiles++;
                
                if (infoBuf && *infoBuf)
                {
                    snprintf(errMsg, sizeof(errMsg), "record = %s, file = %s, message = successful export\n", recordSpec, formattedFitsName);
                    *infoBuf = base_strcatalloc(*infoBuf, errMsg, szInfoBuf); 
                }
            }
            else
            {
                if (errorBuf && *errorBuf)
                {
                    snprintf(errMsg, sizeof(errMsg), "record = %s, file = %s, message = no data in segment, so no FITS file was produced\n", recordSpec, formattedFitsName);
                    *errorBuf = base_strcatalloc(*errorBuf, errMsg, szErrorBuf); 
                }
            }
        }
    } /* seg loop */

    if (bytesExported)
    {
        *bytesExported = totalBytes;
    }
    
    if (numFilesExported)
    {
        *numFilesExported = totalFiles;
    }
    
    /* error only if bad file name template or problems dumping FITSIO buffers - if 0 segments were exported, there is no error */
    return expStatus;
}

/* loop over records */
static ExpToStdoutStatus_t ExportRecordSetToStdout(DRMS_Env_t *env, DRMS_RecordSet_t *expRS, const char *ffmt, ExpToStdout_Compression_t *segCompression, const char *classname, const char *mapfile, size_t *bytesExported, size_t *numFilesExported, char **infoBuf, size_t *szInfoBuf, char **errorBuf, size_t *szErrorBuf)
{
    int drmsStatus = DRMS_SUCCESS;
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    
    int iSet;
    int iRec;
    int nRecs;
    DRMS_Record_t *expRecord = NULL;
    const char *msg = NULL;
    int fiostat = 0;
    char numBytesMesssage[MSGLEN_NUMBYTES + 1];
    long long numBytesFitsFile; /* the actual FITSIO type is LONGLONG */
    size_t bytesSent;
    int terminate = 0;
    
    int recsExported = 0;
    int recsAttempted = 0;

    for (iSet = 0; iSet < expRS->ss_n; iSet++)
    {
        nRecs = drms_recordset_getssnrecs(expRS, iSet, &drmsStatus);

        if (drmsStatus != DRMS_SUCCESS)
        {
            fprintf(stderr, "failure calling drms_recordset_getssnrecs(), skipping subset '%d'\n", iSet);
            expStatus = ExpToStdoutStatus_DRMS;
        }
        else
        {
            for (iRec = 0; iRec < nRecs; iRec++)
            {
                expRecord = drms_recordset_fetchnext(env, expRS, &drmsStatus, NULL, NULL);

                if (!expRecord || drmsStatus != DRMS_SUCCESS)
                {
                    /* exit rec loop - last record was fetched last time */
                    break;
                }
                
                recsAttempted++;

                /* export each segment file in this record */
                expStatus = ExportRecordToStdout(expRecord, ffmt, segCompression, classname, mapfile, bytesExported, numFilesExported, infoBuf, szInfoBuf, errorBuf, szErrorBuf);

                /* IF the internal FITSIO buffers have data, there is no way to NOT flush output to stdout;
                 * hopefully this error will not happen often; if the pipe to the parent process breaks, 
                 * then we could get into this situation, in which case it does not matter if we spill the 
                 * FITSIO buffers onto stdout; if we are here because ExportRecordToStdout() failed for 
                 * some other reason, the FITSIO inner buffers are likely empty
                 */
                 
                
                /* if expStatus != ExpToStdoutStatus_Success, this is not because we could not send the 
                 * client a message; it has something to do with the export process itself or the 
                 * use of the FITSIO library and it affects this particular DRMS record only; we
                 * already successfully sent the client a status-bad message, so they know to
                 * ignore all data till the next record's data gets returned; in 
                 * this case, we want to go on to the next DRMS record, resetting expStatus to 
                 * ExpToStdoutStatus_Success */
                 
                 /* */
                 expStatus = ExpToStdoutStatus_Success;
            }              
        }
    }
    
    //if rec okcount == 0 && rec error count > 0 ==> error
    
    return expStatus;
}


int DoIt(void)
{
    ExpToStdoutStatus_t expStatus = ExpToStdoutStatus_Success;
    ExpToStdoutStatus_t intStatus = ExpToStdoutStatus_Success;
    int drmsStatus = DRMS_SUCCESS;
    int fiostat = 0;
    fitsfile *fitsPtr = NULL;
    long long tsize = 0; /* total size of export payload in bytes */
    long long tsizeMB = 0; /* total size of export payload in Mbytes */
    void *misspix = NULL;
    const char *rsSpec = NULL;
    const char *mapClass = NULL;
    const char *mapFile = NULL;
    const char *fileTemplate = NULL;
    LinkedList_t *cparmsStrings = NULL;
    ListNode_t *cparmNode = NULL;
    ExpToStdout_Compression_t *segCompression = NULL;
    int iComp;
    DRMS_RecordSet_t *expRS = NULL;
    char *msg = NULL;
    char *infoBuf = NULL;
    size_t szInfoBuf = 0;
    char *errorBuf = NULL;
    size_t szErrorBuf = 0;
    
    size_t bytesExported = 0;
    size_t numFilesExported = 0;
    
    /* read and process arguments */
    rsSpec = params_get_str(&cmdparams, ARG_RS_SPEC);
    fileTemplate = (const char *)GetOptionValue(ARG_STRING, ARG_FILE_TEMPLATE);
    cparmsStrings = (LinkedList_t *)GetOptionValue(ARG_STRINGS, ARG_CPARMS_STRING);
    mapClass = (const char *)GetOptionValue(ARG_STRING, ARG_KEYMAP_CLASS);
    mapFile = (const char *)GetOptionValue(ARG_STRING, ARG_KEYMAP_FILE);
    
    /* map cparms strings to an enum */
    if (cparmsStrings)
    {
        char *cparmStr = NULL;
        
        segCompression = calloc(1, sizeof(ExpToStdout_Compression_t));
        
        if (segCompression)
        {
            list_llreset(cparmsStrings);
            iComp = 0;
            while ((cparmNode = list_llnext(cparmsStrings)) != NULL)
            {
                cparmStr = *(char **)cparmNode;
            
                if (strcasecmp(cparmStr, COMPRESSION_NONE) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_NONE;
                }
                else if (strcasecmp(cparmStr, COMPRESSION_RICE) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_RICE;
                }
                else if (strcasecmp(cparmStr, COMPRESSION_GZIP1) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_GZIP1;
                }
                else if (strcasecmp(cparmStr, COMPRESSION_GZIP2) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_GZIP2;
                }
                else if (strcasecmp(cparmStr, COMPRESSION_PLIO) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_PLIO;
                }
                else if (strcasecmp(cparmStr, COMPRESSION_HCOMP) == 0)
                {
                    segCompression[iComp] = ExpToStdout_Compression_HCOMP;
                }
                else
                {
                    expStatus = ExpToStdoutStatus_InvalidArgs;
                }
                
                iComp++;
            }
        }
        else
        {
            expStatus = ExpToStdoutStatus_OutOfMemory;
        }
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        expRS = drms_open_records(drms_env, rsSpec, &drmsStatus);
        if (!expRS || drmsStatus != DRMS_SUCCESS)
        {
            fprintf(stderr, "unable to open records for specification %s\n", rsSpec);
        
            msg = RV_BAD_RECORDSET;
            if (sendMsg(stdout, msg, strlen(msg)))
            {
                fprintf(stderr, "unable to send %s status\n", RV_BAD_RECORDSET);
            }
    
            expStatus = ExpToStdoutStatus_DRMS;
        }
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* stage records to reduce number of calls to SUMS */
        if (drms_stage_records(expRS, 1, 0) != DRMS_SUCCESS)
        {
            fprintf(stderr, "unable to stage records for specification %s\n", rsSpec);
            expStatus = ExpToStdoutStatus_DRMS;
        }
    }
    
    if (expStatus == ExpToStdoutStatus_Success)
    {
        szInfoBuf = 512;
        infoBuf = calloc(1, szInfoBuf);
        if (infoBuf)
        {
            szErrorBuf = 512;
            errorBuf = calloc(1, szErrorBuf);
            if (!errorBuf)
            {
                expStatus == ExpToStdoutStatus_OutOfMemory;
            }
        }
        else
        {
            expStatus == ExpToStdoutStatus_OutOfMemory;
        }
    }

    if (expStatus == ExpToStdoutStatus_Success)
    {
        /* create a TAR file object header block plus data blocks for each FITS file that is being exported */
        expStatus = ExportRecordSetToStdout(drms_env, expRS, fileTemplate, segCompression, mapClass, mapFile, &bytesExported, &numFilesExported, &infoBuf, &szInfoBuf, &errorBuf, &szErrorBuf);
    }
    
    /* regardless of error, we dump all info/error (ASCII) messages into the TAR file */
    if (infoBuf && *infoBuf)
    {
        /* 0-pads to TAR block size */
        intStatus = WriteFileBuffer(stdout, FILE_LIST_PATH, infoBuf, strlen(infoBuf));
        if (intStatus != ExpToStdoutStatus_Success && expStatus == ExpToStdoutStatus_Success)
        {
            expStatus = intStatus;
        }
        
        free(infoBuf);
        infoBuf = NULL;
    }

    if (errorBuf && *errorBuf)
    {
        /* 0-pads to TAR block size */
        intStatus = WriteFileBuffer(stdout, ERROR_PATH, errorBuf, strlen(errorBuf));
        if (intStatus != ExpToStdoutStatus_Success && expStatus == ExpToStdoutStatus_Success)
        {
            expStatus = intStatus;
        }
        
        free(errorBuf);
        errorBuf = NULL;
    }

    /* write the end-of-archive marker (1024 zero bytes) */
    /* pad - fill up last 512 block with zeroes */
    intStatus = DumpPadding(stdout, 0, 1024);
    if (intStatus != ExpToStdoutStatus_Success && expStatus == ExpToStdoutStatus_Success)
    {
        expStatus = intStatus;
    }
    
    if (cparmsStrings)
    {
        list_llfree(&cparmsStrings);
        cparmsStrings = NULL;
    }
    
    return expStatus;
}
