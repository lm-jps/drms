#include "cfortran.h"
#include <drms.h>
#include <drms_types.h>
#include "drms_record.h"
#include "drms_names.h"
#include "drms_fits.h"
#include "drms_segment.h"
#include <drms_array.h>
#include <sum_rpc.h>
#include <drms_compress.h>
#include <drms_tasfile.h>
#include <xassert.h>
#include <drms_network.h>
#include <drms_keyword.h>
#include <drms_link.h>
#include <drms_parser.h>
#include <drms_series.h>
#include <drms_storageunit.h>

//#define VF_EXISTING  0x03000001
#define VF_EXISTING  0x00000001
#define VF_SECTPTR   0x00000002
#define VF_OWNMEMORY 0x00000004

#define DEFBOUND     0x10000000

typedef struct
{
   int lbound;
   int ubound;
   int stride;
} triplet;

//const triplet ALL = {DEFBOUND, DEFBOUND, DEFBOUND};

//#define ELEM(X,i) \
   (X).m_pBase[(X).m_Dim[0].lBound - 1 + i * (X).m_Dim[0].iStride/sizeof(T)]

//#define ELEM2(X,i,j) \
   (X).m_pBase[(X).m_Dim[0].lBound - 1 + i * (X).m_Dim[0].iStride/sizeof(T)  + \
               (X).m_Dim[1].lBound - 1 + j * (X).m_Dim[1].iStride/sizeof(T)]

typedef struct VFDimension
{
   int iExtent;
   int iStride;
   int lBound;
} VFDimension;

typedef struct Fort_Alloc_struct
{
   void *         m_pBase;
   size_t         m_sizeof;
   int            m_iOffset;

   unsigned int   m_iFlags;
   unsigned int   m_iRank;
#ifdef INTEL_FORTRAN
   unsigned int   m_iWhatever;
#endif
   VFDimension    m_Dim[1];

} Fort_Alloc_t;

#ifndef __FORTRAN_DRMS_SETKEY
#define __FORTRAN_DRMS_SETKEY

/* drms_setkey macros */
#define int_ARG()        *value
#define short_ARG()      *value
#define long_ARG()       *value
#define float_ARG()      *value
#define double_ARG()     *value
#define char_ARG()       *value
#define longlong_ARG()   *value
#define string_ARG()      value

#define int_TARG()        int       *value
#define short_TARG()      short     *value
#define long_TARG()       long      *value
#define float_TARG()      float     *value
#define double_TARG()     double    *value
#define char_TARG()       char      *value
#define longlong_TARG()   long long *value
#define string_TARG()     char      *value

#define int_RET()        int
#define short_RET()      short
#define long_RET()       long
#define float_RET()      float
#define double_RET()     double
#define char_RET()       char
#define longlong_RET()   long long

#define  FDRMS_SETKEY(TYPE) \
int fdrms_setkey_ ##TYPE ##_ (DRMS_Record_hdl *rhdl, const char * keyname,TYPE ##_TARG()) { \
  DRMS_Record_t *rec = (DRMS_Record_t *) *rhdl; \
  return drms_setkey_ ##TYPE (rec, keyname, TYPE ##_ARG()); \
}

#define  FDRMS_GETKEY(TYPE) \
TYPE ##_RET() fdrms_getkey_ ##TYPE ##_ (DRMS_Record_hdl *rhdl, const char * keyname, int *status) { \
  DRMS_Record_t *rec = (DRMS_Record_t *) *rhdl; \
  return drms_getkey_ ##TYPE (rec, keyname, status); \
}
#endif

#define FHANDLEKEYSIZE 257 //max lenght of hash key

void fort_alloc(Fort_Alloc_t *, void *, int, int, int);
void free_fort_alloc ( Fort_Alloc_t *);
char * _pointer2handle(void *, char *, char *);
void dealloc_C_string_array(char **, int);
void * _convert_handle(char *);
void convert_C2F_string_array(Fort_Alloc_t *, char **, int, int);
void convert_F2C_string_array(Fort_Alloc_t *, char ***, int, int);
void create_fortran_handleH(void);
void insert2hash(char *, void *);
int  sizeof_drms_type ( DRMS_Type_t );

