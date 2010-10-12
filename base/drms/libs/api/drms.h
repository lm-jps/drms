#ifndef _DRMS_H
#define _DRMS_H


/********************************************************************
DRMS.H: This file defines the types and interfaces used by the Data
Record Management System (DRMS). The DRMS is responsible for
presenting a uniform API to the host language (C, Fortran, IDL or
Matlab) for creating and manipulating data classes and objects in the
data model. This includes

* Perform queries by submitting appropriate SQL queries to the database
  to retrieve data objects, cache the atribute values of those objects in
  local memory and quickly satisfy subsequent queries on attribute values
  of those objects ("keywords") by accessing the cached values.

* Converting atribute values between 
    a) external representation (in the host language)
    b) internal representation (in the DRMS cache in local memory)
    c) database representation (PostgreSQL column types)
    e) export representation (VOTables and FITS headers)

********************************************************************/
 
/* System includes. */
#include "jsoc.h"
#include <arpa/inet.h> 
#include <libgen.h>
#include <netdb.h> 
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <termios.h>
#include <zlib.h>

#include "timeio.h"

/* Lowest-level includes */
#include "foundation.h"

/* Util includes */
#include "db.h"
#include "hcontainer.h"
#include "xassert.h"
#include "util.h"
#include "byteswap.h"
#include "timer.h"
#include "ndim.h"

/* SUMS includes */
#include "SUM.h"
#include "sum_rpc.h"


/* DRMS includes. */
#include "drms_types.h"
#include "drms_defs.h"
#include "drms_env.h"
#include "drms_series.h"
#include "drms_keyword.h"
#include "drms_link.h"
#include "drms_record.h"
#include "drms_segment.h"
#include "drms_array.h"
#include "drms_protocol.h"
#include "drms_statuscodes.h"
#include "drms_network.h"
#include "drms_storageunit.h"
#include "drms_server.h"
#include "drms_binfile.h"
#include "drms_parser.h"
#include "drms_names.h"
#include "drms_array.h"
#include "drms_cmdparams.h"


/* Copy the string "<name>_<num>" into hashkey. */   
static inline void drms_make_hashkey(char *hashkey, const char *name, 
				     long long num)
{
  sprintf(hashkey, "%s_%020lld",name,num);
}

static inline void drms_term()
{
   base_term();
   drms_keyword_term();
   drms_protocol_term();
   drms_defs_term();
   drms_time_term();
}
#endif
