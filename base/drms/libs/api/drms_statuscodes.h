#ifndef _DRMS_STATUSCODES_H
#define _DRMS_STATUSCODES_H


#define CHECKNULL(ptr)  if (!(ptr)) return DRMS_ERROR_NULLPOINTER
#define CHECKNULL_STAT(ptr,stat)   do {  \
                                      if (!(ptr)) { \
                                        if ((stat)) \
                                          *(stat) = DRMS_ERROR_NULLPOINTER; \
                                          fprintf(stderr,"ERROR at %s, line %d: "#ptr" = NULL.\n",__FILE__,__LINE__); \
                                        return NULL; \
                                      } \
                                   } while(0)
#define CHECKSNPRINTF(code, len) do {\
  if ((code) >= (len)) { \
    fprintf(stderr, "WARNING: string is truncated in %s, line %d\n",__FILE__,__LINE__); \
  }\
} while (0)


/* DRMS status codes */
/* Success. */
#define DRMS_NO_ERROR                   (0)
#define DRMS_SUCCESS                    (0)

/* Status codes for type conversion. */
#define DRMS_VALUE_MISSING     (-3)  /* Returned by getkey for unknown keyword. */
#define DRMS_BADSTRING   (-2)
#define DRMS_RANGE       (-1)
#define DRMS_EXACT        (0)  /* == DRMS_SUCCESS */
#define DRMS_INEXACT      (1)


/* Error codes. */
#define DRMS_ERROR_BADSEQUENCE      (-10001)
#define DRMS_ERROR_BADTEMPLATE      (-10002)
#define DRMS_ERROR_UNKNOWNSERIES    (-10003)
#define DRMS_ERROR_UNKNOWNRECORD    (-10004)
#define DRMS_ERROR_UNKNOWNLINK      (-10005)
#define DRMS_ERROR_UNKNOWNKEYWORD   (-10006)
#define DRMS_ERROR_UNKNOWNSEGMENT   (-10007)
#define DRMS_ERROR_BADFIELDCOUNT    (-10008) 
#define DRMS_ERROR_INVALIDLINKTYPE  (-10009)
#define DRMS_ERROR_BADLINK          (-10010)
#define DRMS_ERROR_UNKNOWNUNIT      (-10011)
#define DRMS_ERROR_QUERYFAILED      (-10012)
#define DRMS_ERROR_BADQUERYRESULT   (-10013)
#define DRMS_ERROR_UNKNOWNSU        (-10014)
#define DRMS_ERROR_RECORDREADONLY   (-10015)
#define DRMS_ERROR_KEYWORDREADONLY  (-10016)
#define DRMS_ERROR_NOTIMPLEMENTED   (-10017)
#define DRMS_ERROR_UNKNOWNPROTOCOL  (-10018)
#define DRMS_ERROR_NULLPOINTER      (-10019)
#define DRMS_ERROR_INVALIDTYPE      (-10020)
#define DRMS_ERROR_INVALIDDIMS      (-10021)
#define DRMS_ERROR_INVALIDACTION    (-10022)
#define DRMS_ERROR_COMMITREADONLY   (-10023)
#define DRMS_ERROR_SYNTAXERROR      (-10024)
#define DRMS_ERROR_BADRECORDCOUNT   (-10025)
#define DRMS_ERROR_NULLENV          (-10026)
#define DRMS_ERROR_OUTOFMEMORY      (-10027)
#define DRMS_ERROR_UNKNOWNCOMPMETH  (-10028)
#define DRMS_ERROR_COMPRESSFAILED   (-10029)
#define DRMS_ERROR_INVALIDRANK      (-10030)
#define DRMS_ERROR_MKDIRFAILED      (-10031)
#define DRMS_ERROR_UNLINKFAILED     (-10032)
#define DRMS_ERROR_STATFAILED       (-10033)
#define DRMS_ERROR_SUMOPEN          (-10034)
#define DRMS_ERROR_SUMPUT           (-10035)
#define DRMS_ERROR_SUMGET           (-10036)
#define DRMS_ERROR_SUMALLOC         (-10037)
#define DRMS_ERROR_SUMWAIT          (-10038)
#define DRMS_ERROR_SUMBADOPCODE     (-10039)
#define DRMS_ERROR_INVALIDFILE      (-10040)
#define DRMS_ERROR_IOERROR          (-10041)
#define DRMS_ERROR_LINKNOTSET       (-10042)
#define DRMS_ERROR_BADJSD           (-10043)
#define DRMS_ERROR_INVALIDRECORD    (-10044)
#define DRMS_ERROR_INVALIDKEYWORD   (-10045)
#define DRMS_ERROR_INVALIDSEGMENT   (-10046)
#define DRMS_ERROR_INVALIDLINK      (-10047)
#define DRMS_ERROR_INVALIDDATA      (-10048) /* Bad parameters to a drms function call. */
#define DRMS_ERROR_NODSDSSUPPORT    (-10049)
#define DRMS_ERROR_LIBDSDS          (-10050)
#define DRMS_ERROR_ABORT            (-10051)

/* Warnings */
#define DRMS_WARNING_BADBLANK         (10000)
#define DRMS_QUERY_TRUNCATED          (10001)
#endif


