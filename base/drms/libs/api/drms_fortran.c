// XXX need to restrict this to non-internal functions XXX

//#define DEBUG
//#define FHASH
#include "drms.h"

#define EXPANDAPI 0          /* 0 - Exclude functions that are not part of the module API. */
                             /* 1 - Include functions that are not part of the module API. */
#define USEINTERNALHANDLES 0 /* 0 - Use handle-scheme defined in jsoc_main_f.c. */
                             /* 1 - Use handle-scheme defined in this file. */
#include "drms_fortran.h"
#include "inthandles.h"

#if !USEINTERNALHANDLES
static char gRethandle[kMaxFHandleKey]; /* The cfortran.h wrappers provide access to 
					* what should be a constant string, but
					* the wrappers declare the variables 
					* pointing to these const strings as
					* not const.  Use this global as a work-around. */
#endif /* USEINTERNALHANDLES */

#if USEINTERNALHANDLES
static char ** handles = (char **) NULL;
static int handle_max_index =0, handle_index =0;
#endif /* USEINTERNALHANDLES */

/* Our modules CAN be multi-threaded.  Also, the current code does not free handles, 
 * except on a call to drms_free_records().  But there are cases where
 * we free objects (like DRMS_Array_t's) and when we do that we need to free 
 * and destroy the handles pointing to those objects.   This should happen outside of
 * a call for drms_free_records().  The code should also validate handles before
 * using them and print error messages if a user tries to use an invalid
 * handle.  
 *
 * For these reasons, using a hash table to track handles.  The USEINTERNALHANDLES, when
 * set to 1, will revert to the original behavior.
 * 
 * --Art Amezcua
 *
 */

/* create a hash table to store pointer of JSOC structures.
   ******
   I won't turn on the hash stuff right now as I don't see it's advantage.
   Although it is implemented and tested.
   The modules in principle are single threaded. Unless the user decides to
   multi-thread it. Plus the string keys would have to be dealocated anyway.
   So it's more efficient and convinient to have a static array of string pointers.
   The only reason to have a HContainer is if it is thread safe! and
   modules can be multi-threaded. 
   However I'll keep the code in case there is a use for it.
*/
#if USEINTERNALHANDLES
static HContainer_t *fHandleH = NULL;

void create_fortran_handleH(void) {
  if (!fHandleH) {
    fHandleH = hcon_create(sizeof(void *),
                           FHANDLEKEYSIZE,
                           NULL, /* cmdparams is static - don't free */
                           NULL,
                           NULL,
                           NULL,
                           0);
  }
  XASSERT(fHandleH);
}

void insert2hash(char * key, void * value) {
  if (!fHandleH) create_fortran_handleH();
  hcon_insert(fHandleH, key, value);
}
#endif /* USEINTERNALHANDLES */

/* allocates a one dimensional Fortran ALLOCATABLE array */
/* void fort_alloc(Fort_Alloc_t * , void * , int , int , int ); */

/* routine that sets the Fort_Alloc_struct structure. The structure is being passed down
   from the fortran side into the C part.
   Arguments:
   Fort_Alloc_t * farr        ; pointer to FORTRAN pointer definition structure
   void         * storage     ; C allocated memory
   int            nElem       ; number of elements allocated (see (*) char exception)
   int            sizeof_type ; size of the type allocated 
   int            mult        ; times a type is repeated (see (*) char exception)
   
   (*) char exception
     In fortran the CHARACTER type seems to be treated differently from C when it comes to 
     memory allocation. In fortran a one dimension array will be defined by the number of rows 
     with a fixed length.
      E.g. 
           CHARACTER(len=80) ALLOCATABLE header(:)
           ALLOCATE(header(100)) 

           this will allocate 100 rows of 80 characters.
           In order to achieve the same in C via our fort_alloc function we would have to pass
               nElem       = 100
               sizeof_type = sizeof(char)
               mult        = 80

           for any other types "mult" should be 1.
*/

void fort_alloc(Fort_Alloc_t * farr, void * storage, int nElem, int sizeof_type, int mult)
{
  int iRank=1; 
#ifdef DEBUG
  printf("Fort Alloc [%p]\n", farr);
#endif
  farr->m_pBase   = storage;
  farr->m_sizeof  = mult * sizeof_type;
  farr->m_iOffset = - (mult * sizeof_type);
  farr->m_iFlags  = VF_EXISTING | VF_OWNMEMORY;
  farr->m_iRank   = iRank;

  farr->m_Dim[0].iExtent = nElem;
  farr->m_Dim[0].iStride = mult*sizeof_type;
  farr->m_Dim[0].lBound  = 1;
#ifdef DEBUG
  printf("Array sizeof [%d]; iOffset [%d]; iExtent [%d]; iStride [%d]\n", farr->m_sizeof, farr->m_iOffset, farr->m_Dim[0].iExtent, farr->m_Dim[0].iStride);
#endif
  return;
}

/* Frees Fortran allocatable structure */
void free_fort_alloc ( Fort_Alloc_t * farr) {
  free(farr->m_pBase);
  farr->m_sizeof = 0;
  farr->m_iOffset = 0;
  farr->m_iFlags = 0;
  farr->m_Dim[0].iExtent = 0;
  farr->m_Dim[0].iStride = 0;
}

#if !USEINTERNALHANDLES
/* given a pointer and function name it generates a handle */
const char *__pointer2handle(void * ptr, char * type, char * func)
{
   const char *ret_str = NULL;

   char buf[kMaxFHandleKey];
   snprintf(buf, sizeof(buf), "%p;%s;%s", ptr, type, func);
   InsertJSOCStructure(buf, (void *)ptr, kFHandleTypeDRMS, &ret_str);
   return ret_str;
}
#endif /* !USEINTERNALHANDLES */

char * _pointer2handle(void * ptr, char * type, char * func) {
#if USEINTERNALHANDLES
  char *ret_str;
   
  int incr = 10, mult=1;
  ret_str = malloc(sizeof(char) * FHANDLEKEYSIZE);
  snprintf(ret_str, FHANDLEKEYSIZE, "%p;%s;%s",ptr, type, func);

  if ( handle_max_index - handle_index == 0 ) {
    mult = (handle_max_index ==0) ? mult: 1 + ( handle_max_index + 1)/ incr;

    handles = (char **) realloc((void *) handles, sizeof(char *) * incr * mult );

    handle_max_index = incr * mult -1;

    printf("handle_max_index _pointer2handle [%d]\n", handle_max_index);
  }

  handles[handle_index] = ret_str;

  handle_index++;

/* only if Hashes (HContainer) is enable */
#ifdef FHASH
  insert2hash(ret_str, (void *) &ptr);
#endif
#else
  const char *intstr = NULL;
#endif /* USEINTERNALHANDLES */

#ifdef DEBUG
  printf("Generating pointer [%s]\n",ret_str);
#endif

#if USEINTERNALHANDLES
  return ret_str;
#else
  intstr = __pointer2handle(ptr, type, func);
  snprintf(gRethandle, sizeof(gRethandle), "%s", intstr);

  return gRethandle;
#endif /* USEINTERNALHANDLES */
}

void _freehandle(char **handle)
{
#if !USEINTERNALHANDLES
   if (handle && *handle && !RemoveJSOCStructure(*handle, kFHandleTypeDRMS))
   {
      (*handle)[0] = '\0';
   }
#endif /* !USEINTERNALHANDLES */
}

void * _convert_handle(char *handle) {
#if USEINTERNALHANDLES
  int ptr_len;
  char ptr_str[250];
  void * ptr;

#ifdef DEBUG
  printf("Converting pointer [%s]\n",handle);
#endif

#ifdef FHASH
  return *(void **) hcon_lookup(fHandleH, handle);
#else
  ptr_len = strcspn(handle, ";");

  strncpy(ptr_str, handle, ptr_len);

  ptr_str[ptr_len]='\0';

  sscanf(ptr_str,"%p",&ptr);
  return ptr;
#endif
#else
  return GetJSOCStructure((void *)handle, kFHandleTypeDRMS);
#endif /* !USEINTERNALHANDLES */
}
/* converts a C string array to fortran character vector */
void convert_C2F_string_array(Fort_Alloc_t *f_alloc, char ** c_arr, int no_of_rows, int row_len) {
  char *alloc_arr = (char *) NULL;
  int i;
  // convert the C char two dimensional array  to a Fortran 1 dimensional one
  XASSERT(alloc_arr = malloc(no_of_rows*row_len));

  for (i=0; i< no_of_rows; i++) {
    strncpy(alloc_arr + row_len*i, c_arr[i], row_len);
  }
  /* deallocate memory */
  dealloc_C_string_array(c_arr, no_of_rows);

  //sets the fortran pointer structure
  fort_alloc(f_alloc, alloc_arr, no_of_rows, row_len, 1);

  return;

}

/* opossite to the previous function ...
   converts a fortran character vector into a C string vector
*/ 
void convert_F2C_string_array(Fort_Alloc_t *f_alloc, char *** c_arr, int no_of_rows, int row_len) {
  char *alloc_arr = (char *) NULL;
  char **_c_arr = (char **) NULL;
  int i;

  alloc_arr = f_alloc->m_pBase;

  // convert the C char two dimensional array  to a Fortran 1 dimensional one
  XASSERT(_c_arr = malloc(no_of_rows*sizeof(char *)));

  for (i=0; i< no_of_rows; i++) {
    XASSERT(_c_arr[i] = malloc(row_len*sizeof(char)));
    strncpy(_c_arr[i], alloc_arr + row_len*i , row_len);
  }

  *c_arr = _c_arr;

  return;
} 

void dealloc_C_string_array(char ** c_arr, int no_of_rows) {
  int i=0;
  for (i = 0; i<no_of_rows;i++) {
    free(c_arr[i]);
  } 
  free(c_arr);
  return;
}

// sizeof DRMS_Types in terms of bytes
int sizeof_drms_type ( DRMS_Type_t type) {
  switch (type) {
    case DRMS_TYPE_CHAR      : return sizeof(char);
    case  DRMS_TYPE_SHORT    : return sizeof(short);
    case  DRMS_TYPE_INT      : return sizeof(int);
    case  DRMS_TYPE_LONGLONG : return sizeof(long long);
    case  DRMS_TYPE_FLOAT    : return sizeof(float);
    case  DRMS_TYPE_DOUBLE   : return sizeof(double);
    case  DRMS_TYPE_TIME     : return sizeof(double);
    case  DRMS_TYPE_STRING   : return sizeof(char);
    case  DRMS_TYPE_RAW      : return sizeof(int);
    default: return sizeof(int);
  }
}
/* Art */
/* Macros defined by cfortran.h that magically handle parameter passing between 
 * Fortran and C.  These are actually function definitions, not protocols - so 
 * don't put them in cmdparams.h.  They define the functions named by
 * their third parameter plus an underscore (e.g., cpgethandle_). */
#ifdef ICCCOMP
#pragma warning (disable : 279)
#pragma warning (disable : 188)
#pragma warning (disable : 981)
#pragma warning (disable : 1418)
#endif
/* The use of cfortran.h for creating C wrapper functions called by Fortran code will 
 * cause this warning to display.  cfortran.h does  not provide a way to prototype 
 * these functions, it only provides a way to define them. */

int f_isnull(char * hdl) {
  void * ptr = _convert_handle(hdl);
  return ptr == (void *) 0;
}
FCALLSCFUN1(LOGICAL,f_isnull,F_ISNULL,f_isnull,STRING)


// ##################################
// ##### Get DRMS_Env            ####
// ##################################

/* get handle to the drms enviroment */

char * f_drms_env_handle(void)
{
  extern DRMS_Env_t *drms_env;
  return _pointer2handle((void *) drms_env, "DRMS_Env_t", "f_drms_env_handle");
}

FCALLSCFUN0(STRING, f_drms_env_handle, F_DRMS_ENV_HANDLE, f_drms_env_handle)



// ##################################
// ##### FILE:: drms_array.c ########
// ####          START           ####
// ##################################
char * f_drms_array_create(DRMS_Type_t type, int naxis, int *axis, void *data, int *status) {
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_array_create(type, naxis, axis, data, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_array_create");
}
FCALLSCFUN5(STRING, f_drms_array_create, F_DRMS_ARRAY_CREATE, f_drms_array_create, INT, INT, PINT, PVOID, PINT)

char * f_drms_array_create_empty(DRMS_Type_t type, int naxis, int *axis, int *status) {
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_array_create(type, naxis, axis, NULL, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_array_create");
}
FCALLSCFUN4(STRING, f_drms_array_create_empty, F_DRMS_ARRAY_CREATE_EMPTY, f_drms_array_create_empty, INT, INT, PINT, PINT)

void f_drms_free_array(char * arr_hdl, int size) {
  DRMS_Array_t  * arr = (DRMS_Array_t  *)  _convert_handle(arr_hdl);
  _freehandle(&arr_hdl);
  drms_free_array(arr);
}
FCALLSCSUB1(f_drms_free_array, F_DRMS_FREE_ARRAY, f_drms_free_array, PSTRING)


void f_drms_array_convert_inplace(DRMS_Type_t newtype, double bzero, double bscale, char * src_hdl) {
  DRMS_Array_t  * src = (DRMS_Array_t  *)  _convert_handle(src_hdl);

  return drms_array_convert_inplace(newtype, bzero, bscale, src);
}
FCALLSCSUB4(f_drms_array_convert_inplace, F_DRMS_ARRAY_CONVERT_INPLACE, f_drms_array_convert_inplace, INT, DOUBLE, DOUBLE, STRING)


char * f_drms_array_convert(DRMS_Type_t dsttype, double bzero, double bscale, char * src_hdl) {
  DRMS_Array_t  * src = (DRMS_Array_t  *)  _convert_handle(src_hdl);
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_array_convert(dsttype, bzero, bscale, src);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_array_convert");
}
FCALLSCFUN4(STRING, f_drms_array_convert, F_DRMS_ARRAY_CONVERT, f_drms_array_convert, INT, DOUBLE, DOUBLE, STRING)


char * f_drms_array_slice(int *start, int *end, char * src_hdl) {
  DRMS_Array_t  * src = (DRMS_Array_t  *)  _convert_handle(src_hdl);
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_array_slice(start, end, src);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_array_slice");
}
FCALLSCFUN3(STRING, f_drms_array_slice, F_DRMS_ARRAY_SLICE, f_drms_array_slice, PINT, PINT, STRING)


char * f_drms_array_permute(char * src_hdl, int *perm, int *status) {
  DRMS_Array_t  * src = (DRMS_Array_t  *)  _convert_handle(src_hdl);
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_array_permute(src, perm, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_array_permute");
}
FCALLSCFUN3(STRING, f_drms_array_permute, F_DRMS_ARRAY_PERMUTE, f_drms_array_permute, STRING, PINT, PINT)


/*
int f_drms_array_rawconvert(int n, DRMS_Type_t dsttype, double bzero, double bscale, void *dst, DRMS_Type_t srctype, void *src)
*/
FCALLSCFUN7(INT, drms_array_rawconvert, F_DRMS_ARRAY_RAWCONVERT, f_drms_array_rawconvert, INT, INT, DOUBLE, DOUBLE, PVOID, INT, PVOID)


void f_drms_array2missing(char * arr_hdl) {
  DRMS_Array_t  * arr = (DRMS_Array_t  *)  _convert_handle(arr_hdl);

  return drms_array2missing(arr);
}
FCALLSCSUB1(f_drms_array2missing, F_DRMS_ARRAY2MISSING, f_drms_array2missing, STRING)


void f_drms_array_print(char * arr_hdl, const char *colsep, const char *rowsep) {
  DRMS_Array_t  * arr = (DRMS_Array_t  *)  _convert_handle(arr_hdl);

  return drms_array_print(arr, colsep, rowsep);
}
FCALLSCSUB3(f_drms_array_print, F_DRMS_ARRAY_PRINT, f_drms_array_print, STRING, STRING, STRING)


/*
int f_drms_array2char(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, char *dst)
*/
FCALLSCFUN6(INT, drms_array2char, F_DRMS_ARRAY2CHAR, f_drms_array2char, INT, INT, DOUBLE, DOUBLE, PVOID, STRING)


/*
int f_drms_array2short(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, short *dst)
*/
FCALLSCFUN6(INT, drms_array2short, F_DRMS_ARRAY2SHORT, f_drms_array2short, INT, INT, DOUBLE, DOUBLE, PVOID, PSHORT)


/*
int f_drms_array2int(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, int *dst)
*/
FCALLSCFUN6(INT, drms_array2int, F_DRMS_ARRAY2INT, f_drms_array2int, INT, INT, DOUBLE, DOUBLE, PVOID, PINT)


/*
int f_drms_array2longlong(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, long long *dst)
*/
FCALLSCFUN6(INT, drms_array2longlong, F_DRMS_ARRAY2LONGLONG, f_drms_array2longlong, INT, INT, DOUBLE, DOUBLE, PVOID, PLONGLONG)


/*
int drms_array2float(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, float *dst)
*/
FCALLSCFUN6(INT, drms_array2float, F_DRMS_ARRAY2FLOAT, f_drms_array2float, INT, INT, DOUBLE, DOUBLE, PVOID, PFLOAT)


/*
int drms_array2double(int, DRMS_Type_t, double, double, void *, double *)
*/
FCALLSCFUN6(INT, drms_array2double, F_DRMS_ARRAY2DOUBLE, f_drms_array2double, INT, INT, DOUBLE, DOUBLE, PVOID, PDOUBLE)


/*
int drms_array2time(int, DRMS_Type_t, double, double, void *, double *)
*/
FCALLSCFUN6(INT, drms_array2time, F_DRMS_ARRAY2TIME, f_drms_array2time, INT, INT, DOUBLE, DOUBLE, PVOID, PDOUBLE)


/* NEW INTERFACE */
int f_drms_array2string(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, void *char_dst) {
  int stat, a2s_len = DRMS_ARRAY2STRING_LEN;
  char **_dst        = (char **) NULL;        /* array of allocated C strings */
  char *alloc_dst    = (char *) NULL;         /* Fortran like buffer on converted string array */
  int i=0;

  Fort_Alloc_t  *dst = (Fort_Alloc_t *) char_dst;  /* Fortran pointer structure */
 
  stat = drms_array2string(n, src_type, bzero, bscale, src, _dst);

  // convert the C char two dimensional array  to a Fortran 1 dimensional one
  XASSERT(alloc_dst = malloc(a2s_len*n));

  if (stat != DRMS_ERROR_INVALIDTYPE) {
    for (i=0; i<n; i++) {
      strncpy(alloc_dst + a2s_len*i, _dst[i], a2s_len);
    }
    /* deallocate memory */
    for (i=0; i<n; i++) {
      free(_dst[i]);
    }
    free(_dst);
  }

  //sets the fortran pointer structure
  fort_alloc(dst, alloc_dst, n, a2s_len, 1);

  return stat;
}

FCALLSCFUN6(INT, f_drms_array2string, F_DRMS_ARRAY2STRING, f_drms_array2string, INT, INT, DOUBLE, DOUBLE, PVOID, PVOID)


// ##################################
// ##### FILE:: drms_array.c ########
// ####          END             ####
// ##################################


// ##################################
// ##### FILE:: drms_client.c #######
// ####          START           ####
// ##################################
char * f_drms_connect(char *host) {
  DRMS_Session_t  * _ret_var;

  _ret_var = drms_connect(host);
  return _pointer2handle((void *)_ret_var, "DRMS_Session_t", "drms_connect");
}
FCALLSCFUN1(STRING, f_drms_connect, F_DRMS_CONNECT, f_drms_connect, STRING)

#ifdef DRMS_CLIENT
void f_drms_disconnect(char * env_hdl, int abort) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_disconnect(env, abort);
}
FCALLSCSUB2(f_drms_disconnect, F_DRMS_DISCONNECT, f_drms_disconnect, STRING, INT)
#endif

int f_drms_commit(char * env_hdl) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_commit(env);
}
FCALLSCFUN1(INT, f_drms_commit, F_DRMS_COMMIT, f_drms_commit, STRING)


int f_drms_rollback(char * session_hdl) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_rollback(session);
}
FCALLSCFUN1(INT, f_drms_rollback, F_DRMS_ROLLBACK, f_drms_rollback, STRING)


char * f_drms_query_txt(char * session_hdl, char *query) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);
  DB_Text_Result_t  * _ret_var;

  _ret_var = drms_query_txt(session, query);
  return _pointer2handle((void *)_ret_var, "DB_Text_Result_t", "drms_query_txt");
}
FCALLSCFUN2(STRING, f_drms_query_txt, F_DRMS_QUERY_TXT, f_drms_query_txt, STRING, STRING)


char * f_drms_query_bin(char * session_hdl, char *query) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);
  DB_Binary_Result_t  * _ret_var;

  _ret_var = drms_query_bin(session, query);
  return _pointer2handle((void *)_ret_var, "DB_Binary_Result_t", "drms_query_bin");
}
FCALLSCFUN2(STRING, f_drms_query_bin, F_DRMS_QUERY_BIN, f_drms_query_bin, STRING, STRING)


/* DON'T KNOW -- how to handle [...] in fortran */
/*
char * f_drms_query_binv(char * session_hdl, char *query, ...) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);
  DB_Binary_Result_t  * _ret_var;

  _ret_var = drms_query_binv(session, query, arg2);
  return _pointer2handle((void *)_ret_var, "DB_Binary_Result_t", "drms_query_binv");
}
## [DB_Binary_Result_t *drms_query_binv(DRMS_Session_t *session, char *query, ...)]
FCALLSCFUN3(STRING, f_drms_query_binv, F_DRMS_QUERY_BINV, f_drms_query_binv, STRING, STRING)
*/


/*####can't find match for [void**] full_text [void **argin]*/
/* DON'T KNOW -- how to handle [void **] in fortran */
/*char * f_drms_query_bin_array(char * session_hdl, char *query, int n_args, DB_Type_t *intype, void **argin) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);
  DB_Binary_Result_t  * _ret_var;

  _ret_var = drms_query_bin_array(session, query, n_args, intype, argin);
  return _pointer2handle((void *)_ret_var, "DB_Binary_Result_t", "drms_query_bin_array");
}
FCALLSCFUN5(STRING, f_drms_query_bin_array, F_DRMS_QUERY_BIN_ARRAY, f_drms_query_bin_array, STRING, STRING, INT, PINT)*/


int f_drms_dms(char * session_hdl, int *row_count, char *query) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_dms(session, row_count, query);
}
FCALLSCFUN3(INT, f_drms_dms, F_DRMS_DMS, f_drms_dms, STRING, PINT, STRING)


/*####can't find match for [] full_text [...]*/
/* DON'T KNOW -- how to handle [...] in fortran */
/*
int f_drms_dmsv(char * session_hdl, int *row_count, char *query, int n_rows, ...) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_dmsv(session, row_count, query, n_rows, arg4);
}
** int drms_dmsv(DRMS_Session_t *session, int *row_count, char *query, int n_rows, ...)
FCALLSCFUN5(INT, f_drms_dmsv, F_DRMS_DMSV, f_drms_dmsv, STRING, PINT, STRING, INT)
*/


/*####can't find match for [void**] full_text [void **argin]*/
/* DON'T KNOW -- how to handle [void **] in fortran */
/*int f_drms_dms_array(char * session_hdl, int *row_count, char *query, int n_rows, int n_args, DB_Type_t *intype, void **argin) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_dms_array(session, row_count, query, n_rows, n_args, intype, argin);
}
FCALLSCFUN7(INT, f_drms_dms_array, F_DRMS_DMS_ARRAY, f_drms_dms_array, STRING, PINT, STRING, INT, INT, PINT)
*/

/*####can't find match for [] full_text [...]*/
/* DON'T KNOW -- how to handle [...] in fortran */
/*
int f_drms_bulk_insertv(char * session_hdl, char *table, int n_rows, int n_cols, ...) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_bulk_insertv(session, table, n_rows, n_cols, arg4);
}
FCALLSCFUN5(INT, f_drms_bulk_insertv, F_DRMS_BULK_INSERTV, f_drms_bulk_insertv, STRING, STRING, INT, INT)
*/


/*####can't find match for [void**] full_text [void **argin]*/
/* DON'T KNOW -- how to handle [void **] in fortran */
/*
int f_drms_bulk_insert_array(char * session_hdl, char *table, int n_rows, int n_args, DB_Type_t *intype, void **argin) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_bulk_insert_array(session, table, n_rows, n_args, intype, argin);
}
FCALLSCFUN6(INT, f_drms_bulk_insert_array, F_DRMS_BULK_INSERT_ARRAY, f_drms_bulk_insert_array, STRING, STRING, INT, INT, PINT)
*/

#if EXPANDAPI
     /* Functions having to do with DRMS sequences are internal to DRMS, and should not be
      * available to to module wriers.
      *
      * --Art Amezcua 8/13/2007 
      */

int f_drms_sequence_drop(char * session_hdl, char *table) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_sequence_drop(session, table);
}
FCALLSCFUN2(INT, f_drms_sequence_drop, F_DRMS_SEQUENCE_DROP, f_drms_sequence_drop, STRING, STRING)


int f_drms_sequence_create(char * session_hdl, char *table) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_sequence_create(session, table);
}
FCALLSCFUN2(INT, f_drms_sequence_create, F_DRMS_SEQUENCE_CREATE, f_drms_sequence_create, STRING, STRING)


/* CHANGE SIGNATURE f_drms_sequence_getnext*/
void f_drms_sequence_getnext(long long * seq, char * session_hdl, char *table, int n) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  seq = drms_sequence_getnext(session, table, n);
  return;
}
FCALLSCSUB4(f_drms_sequence_getnext, F_DRMS_SEQUENCE_GETNEXT, f_drms_sequence_getnext,PLONGLONG, STRING, STRING, INT)

long long f_drms_sequence_getcurrent(char * session_hdl, char *table) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_sequence_getcurrent(session, table);
}
FCALLSCFUN2(LONGLONG, f_drms_sequence_getcurrent, F_DRMS_SEQUENCE_GETCURRENT, f_drms_sequence_getcurrent, STRING, STRING)

long long f_drms_sequence_getlast(char * session_hdl, char *table) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  return drms_sequence_getlast(session, table);
}
FCALLSCFUN2(LONGLONG, f_drms_sequence_getlast, F_DRMS_SEQUENCE_GETLAST, f_drms_sequence_getlast, STRING, STRING)


/* CHANGE SIGNATURE f_drms_alloc_recnum*/
void f_drms_alloc_recnum(long long * recnum, char * session_hdl, char *series, DRMS_RecLifetime_t lifetime, int n) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);

  recnum = drms_alloc_recnum(session, series, lifetime, n);

  return;
}
FCALLSCSUB5(f_drms_alloc_recnum, F_DRMS_ALLOC_RECNUM, f_drms_alloc_recnum, PLONGLONG, STRING, STRING, INT, INT)


char * f_drms_getunit(char * env_hdl, char *series, long long sunum, int retrieve, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  * _ret_var;

  _ret_var = drms_getunit(env, series, sunum, retrieve, status);
  return _pointer2handle((void *)_ret_var, "DRMS_StorageUnit_t", "drms_getunit");
}
FCALLSCFUN5(STRING, f_drms_getunit, F_DRMS_GETUNIT, f_drms_getunit, STRING, STRING, LONG, INT, PINT)

int f_drms_newslots(char * env_hdl, int n, char *series, long long *recnum, DRMS_RecLifetime_t lifetime, int *slotnum, char * su_hdl, int size) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  ** su = (DRMS_StorageUnit_t  **)  _convert_handle(su_hdl);

  return drms_newslots(env, n, series, recnum, lifetime, slotnum, su);
}
FCALLSCFUN7(INT, f_drms_newslots, F_DRMS_NEWSLOTS, f_drms_newslots, STRING, INT, STRING, PLONGLONG, INT, PINT, PSTRING)


int f_drms_slot_setstate(char * env_hdl, char *series, long long sunum, int slotnum, int state) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_slot_setstate(env, series, sunum, slotnum, state);
}
FCALLSCFUN5(INT, f_drms_slot_setstate, F_DRMS_SLOT_SETSTATE, f_drms_slot_setstate, STRING, STRING, LONG, INT, INT)
#endif /* EXPANDAPI */


int f_drms_create_series(char * rec_hdl, int perms) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_create_series(rec, perms);
}
FCALLSCFUN2(INT, f_drms_create_series, F_DRMS_CREATE_SERIES, f_drms_create_series, STRING, INT)


int f_drms_update_series(char * rec_hdl, int perms) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_update_series(rec, perms);
}
FCALLSCFUN2(INT, f_drms_update_series, F_DRMS_UPDATE_SERIES, f_drms_update_series, STRING, INT)

// ##################################
// ##### FILE:: drms_client.c #######
// ####          END             ####
// ##################################


// ##################################
// ##### FILE:: drms_keyword.c ######
// ####          START           ####
// ##################################

void f_drms_free_template_keyword_struct(char * key_hdl) {
  DRMS_Keyword_t  * key = (DRMS_Keyword_t  *)  _convert_handle(key_hdl);

  return drms_free_template_keyword_struct(key);
}
FCALLSCSUB1(f_drms_free_template_keyword_struct, F_DRMS_FREE_TEMPLATE_KEYWORD_STRUCT, f_drms_free_template_keyword_struct, STRING)


void f_drms_free_keyword_struct(char * key_hdl) {
  DRMS_Keyword_t  * key = (DRMS_Keyword_t  *)  _convert_handle(key_hdl);

  return drms_free_keyword_struct(key);
}
FCALLSCSUB1(f_drms_free_keyword_struct, F_DRMS_FREE_KEYWORD_STRUCT, f_drms_free_keyword_struct, STRING)


void f_drms_copy_keyword_struct(char * dst_hdl, char * src_hdl) {
  DRMS_Keyword_t  * dst = (DRMS_Keyword_t  *)  _convert_handle(dst_hdl);
  DRMS_Keyword_t  * src = (DRMS_Keyword_t  *)  _convert_handle(src_hdl);

  return drms_copy_keyword_struct(dst, src);
}
FCALLSCSUB2(f_drms_copy_keyword_struct, F_DRMS_COPY_KEYWORD_STRUCT, f_drms_copy_keyword_struct, STRING, STRING)

void f_drms_keyword_print(char * key_hdl) {
  DRMS_Keyword_t  * key = (DRMS_Keyword_t  *)  _convert_handle(key_hdl);

  return drms_keyword_print(key);
}
FCALLSCSUB1(f_drms_keyword_print, F_DRMS_KEYWORD_PRINT, f_drms_keyword_print, STRING)


void f_drms_keyword_printval(char * key_hdl) {
  DRMS_Keyword_t  * key = (DRMS_Keyword_t  *)  _convert_handle(key_hdl);

  return drms_keyword_printval(key);
}
FCALLSCSUB1(f_drms_keyword_printval, F_DRMS_KEYWORD_PRINTVAL, f_drms_keyword_printval, STRING)


int f_drms_template_keywords(char * template_hdl) {
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);

  return drms_template_keywords(template);
}
FCALLSCFUN1(INT, f_drms_template_keywords, F_DRMS_TEMPLATE_KEYWORDS, f_drms_template_keywords, STRING)


char * f_drms_keyword_lookup(char * rec_hdl, const char *key, int followlink) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_Keyword_t  * _ret_var;

  _ret_var = drms_keyword_lookup(rec, key, followlink);
  return _pointer2handle((void *)_ret_var, "DRMS_Keyword_t", "drms_keyword_lookup");
}
FCALLSCFUN3(STRING, f_drms_keyword_lookup, F_DRMS_KEYWORD_LOOKUP, f_drms_keyword_lookup, STRING, STRING, INT)


DRMS_Type_t f_drms_keyword_type(char * key_hdl) {
  DRMS_Keyword_t  * key = (DRMS_Keyword_t  *)  _convert_handle(key_hdl);

  return drms_keyword_type(key);
}
FCALLSCFUN1(INT, f_drms_keyword_type, F_DRMS_KEYWORD_TYPE, f_drms_keyword_type, STRING)

char f_drms_getkey_char(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_char(rec, key, status);
}
FCALLSCFUN3(SHORT, f_drms_getkey_char, F_DRMS_GETKEY_CHAR, f_drms_getkey_char, STRING, STRING, PINT)


short f_drms_getkey_short(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_short(rec, key, status);
}
FCALLSCFUN3(SHORT, f_drms_getkey_short, F_DRMS_GETKEY_SHORT, f_drms_getkey_short, STRING, STRING, PINT)


int f_drms_getkey_int(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_int(rec, key, status);
}
FCALLSCFUN3(INT, f_drms_getkey_int, F_DRMS_GETKEY_INT, f_drms_getkey_int, STRING, STRING, PINT)

long long f_drms_getkey_longlong(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_longlong(rec, key, status);
}
FCALLSCFUN3(LONGLONG, f_drms_getkey_longlong, F_DRMS_GETKEY_LONGLONG, f_drms_getkey_longlong, STRING, STRING, PINT)


float f_drms_getkey_float(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_float(rec, key, status);
}
FCALLSCFUN3(FLOAT, f_drms_getkey_float, F_DRMS_GETKEY_FLOAT, f_drms_getkey_float, STRING, STRING, PINT)


double f_drms_getkey_double(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_double(rec, key, status);
}
FCALLSCFUN3(DOUBLE, f_drms_getkey_double, F_DRMS_GETKEY_DOUBLE, f_drms_getkey_double, STRING, STRING, PINT)


char * f_drms_getkey_string(char * rec_hdl, const char *key, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey_string(rec, key, status);
}
FCALLSCFUN3(STRING, f_drms_getkey_string, F_DRMS_GETKEY_STRING, f_drms_getkey_string, STRING, STRING, PINT)


/*#### DRMS_Type_Value_t is a union #####*/
/* DON'T KNOW -- how to handle unions! in fortran */
/* Use instead the f_drms_getkey_<type> functions */
/* Use instead the f_drms_setkey_<type> functions */
/* DRMS_Type_Value_t drms_getkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t *type, int *status)]*/
/* int drms_setkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t type, DRMS_Type_Value_t *value)]*/
/*
DRMS_Type_Value_t f_drms_getkey(char * rec_hdl, const char *key, DRMS_Type_t *type, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_getkey(rec, key, type, status);
}
int f_drms_setkey(char * rec_hdl, const char *key, DRMS_Type_t type, DRMS_Type_Value_t *value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey(rec, key, type, value);
}
*/

int f_drms_setkey_char(char * rec_hdl, const char *key, char value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_char(rec, key, (char) value);
}
FCALLSCFUN3(INT, f_drms_setkey_char, F_DRMS_SETKEY_CHAR, f_drms_setkey_char, STRING, STRING, BYTE)


int f_drms_setkey_short(char * rec_hdl, const char *key, short value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_short(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_short, F_DRMS_SETKEY_SHORT, f_drms_setkey_short, STRING, STRING, SHORT)


int f_drms_setkey_int(char * rec_hdl, const char *key, int value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_int(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_int, F_DRMS_SETKEY_INT, f_drms_setkey_int, STRING, STRING, INT)


int f_drms_setkey_longlong(char * rec_hdl, const char *key, long long value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_longlong(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_longlong, F_DRMS_SETKEY_LONGLONG, f_drms_setkey_longlong, STRING, STRING, LONG)


int f_drms_setkey_float(char * rec_hdl, const char *key, float value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_float(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_float, F_DRMS_SETKEY_FLOAT, f_drms_setkey_float, STRING, STRING, FLOAT)


int f_drms_setkey_double(char * rec_hdl, const char *key, double value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_double(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_double, F_DRMS_SETKEY_DOUBLE, f_drms_setkey_double, STRING, STRING, DOUBLE)


int f_drms_setkey_string(char * rec_hdl, const char *key, char *value) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setkey_string(rec, key, value);
}
FCALLSCFUN3(INT, f_drms_setkey_string, F_DRMS_SETKEY_STRING, f_drms_setkey_string, STRING, STRING, STRING)


// ##################################
// ##### FILE:: drms_keyword.c ######
// ####          END             ####
// ##################################


// ##################################
// ##### FILE:: drms_link.c #########
// ####          START           ####
// ##################################

void f_drms_free_template_link_struct(char * link_hdl) {
  DRMS_Link_t  * link = (DRMS_Link_t  *)  _convert_handle(link_hdl);

  return drms_free_template_link_struct(link);
}
FCALLSCSUB1(f_drms_free_template_link_struct, F_DRMS_FREE_TEMPLATE_LINK_STRUCT, f_drms_free_template_link_struct, STRING)


void f_drms_free_link_struct(char * link_hdl) {
  DRMS_Link_t  * link = (DRMS_Link_t  *)  _convert_handle(link_hdl);

  return drms_free_link_struct(link);
}
FCALLSCSUB1(f_drms_free_link_struct, F_DRMS_FREE_LINK_STRUCT, f_drms_free_link_struct, STRING)


void f_drms_copy_link_struct(char * dst_hdl, char * src_hdl) {
  DRMS_Link_t  * dst = (DRMS_Link_t  *)  _convert_handle(dst_hdl);
  DRMS_Link_t  * src = (DRMS_Link_t  *)  _convert_handle(src_hdl);

  return drms_copy_link_struct(dst, src);
}
FCALLSCSUB2(f_drms_copy_link_struct, F_DRMS_COPY_LINK_STRUCT, f_drms_copy_link_struct, STRING, STRING)

int f_drms_setlink_static(char * rec_hdl, const char *linkname, long long recnum) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setlink_static(rec, linkname, recnum);
}
FCALLSCFUN3(INT, f_drms_setlink_static, F_DRMS_SETLINK_STATIC, f_drms_setlink_static, STRING, STRING, LONG)


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *values]*/
/* DON'T KNOW how to handle unions yet */
/*
int f_drms_setlink_dynamic(char * rec_hdl, const char *linkname, DRMS_Type_t *types, DRMS_Type_Value_t *values) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_setlink_dynamic(rec, linkname, types, values);
}
FCALLSCFUN4(INT, f_drms_setlink_dynamic, F_DRMS_SETLINK_DYNAMIC, f_drms_setlink_dynamic, STRING, STRING, PINT)
*/


char * f_drms_link_follow(char * rec_hdl, const char *linkname, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_link_follow(rec, linkname, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_link_follow");
}
FCALLSCFUN3(STRING, f_drms_link_follow, F_DRMS_LINK_FOLLOW, f_drms_link_follow, STRING, STRING, PINT)


char * f_drms_link_followall(char * rec_hdl, const char *linkname, int *status) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_RecordSet_t  * _ret_var;

  _ret_var = drms_link_followall(rec, linkname, status);
  return _pointer2handle((void *)_ret_var, "DRMS_RecordSet_t", "drms_link_followall");
}
FCALLSCFUN3(STRING, f_drms_link_followall, F_DRMS_LINK_FOLLOWALL, f_drms_link_followall, STRING, STRING, PINT)


void f_drms_link_print(char * link_hdl) {
  DRMS_Link_t  * link = (DRMS_Link_t  *)  _convert_handle(link_hdl);

  return drms_link_print(link);
}
FCALLSCSUB1(f_drms_link_print, F_DRMS_LINK_PRINT, f_drms_link_print, STRING)


int f_drms_template_links(char * template_hdl) {
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);

  return drms_template_links(template);
}
FCALLSCFUN1(INT, f_drms_template_links, F_DRMS_TEMPLATE_LINKS, f_drms_template_links, STRING)


void f_drms_link_getpidx(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_link_getpidx(rec);
}
FCALLSCSUB1(f_drms_link_getpidx, F_DRMS_LINK_GETPIDX, f_drms_link_getpidx, STRING)


// ##################################
// ##### FILE:: drms_link.c #########
// ####          END             ####
// ##################################



// ##################################
// ##### FILE:: drms_names.c ########
// ####          START           ####
// ##################################

void f_free_record_set(char * rs_hdl) {
  RecordSet_t  * rs = (RecordSet_t  *)  _convert_handle(rs_hdl);

  return free_record_set(rs);
}
FCALLSCSUB1(f_free_record_set, F_FREE_RECORD_SET, f_free_record_set, STRING)

#if EXPANDAPI
/* NEW INTERFACE drms_recordset_query */
/*int f_drms_recordset_query(char * env_hdl, char *recordsetname, char **query, char **seriesname, int *filter) {*/
int f_drms_recordset_query(char * env_hdl, 
			   char *recordsetname, 
			   void *fquery, 
			   char *fseriesname, 
			   int size, 
			   int *filter, 
			   int * mix) {
  Fort_Alloc_t *query = (Fort_Alloc_t *) fquery;
  char *seriesname=fseriesname;
  char *query_alloc = (char *) NULL;
  int ret;
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  ret = drms_recordset_query(env, recordsetname, &query_alloc, &seriesname, filter, mix);

  fort_alloc(query, query_alloc, 1, DRMS_MAXQUERYLEN, 1);
  return ret;
}
FCALLSCFUN6(INT, f_drms_recordset_query, F_DRMS_RECORDSET_QUERY, f_drms_recordset_query, STRING, STRING, PVOID, PSTRING, PINT, PINT)
#endif /* EXPANDAPI */

// ##################################
// ##### FILE:: drms_names.c ########
// ####          END             ####
// ##################################




// ##################################
// ##### FILE:: drms_parser.c #######
// ####          START           ####
// ##################################
// NO FUNCTIONS IN FORTRAN API
// ##################################
// ##### FILE:: drms_parser.c #######
// ####          END             ####
// ##################################



// ##################################
// ##### FILE:: drms_record.c #######
// ####          START           ####
// ##################################

// fortran helper functions 
//
// ##########################################################
// HContainer and HIterator handlers
char * new_hiterator(char * hc_hdl) {
  HContainer_t * hc = (HContainer_t *)  _convert_handle(hc_hdl);
  HIterator_t *hit= (HIterator_t *) NULL;
  hit = malloc(sizeof(HIterator_t));
  hiter_new(hit,hc);
  return _pointer2handle((void *) hit, "HIterator_t", "get_hiterator");
}
FCALLSCFUN1(STRING, new_hiterator, F_NEW_HITERATOR, f_new_hiterator, STRING)

void hiterator_rewind(char * hit_hdl) {
  HIterator_t * hit = (HIterator_t *)  _convert_handle(hit_hdl);
  hiter_rewind(hit); 
}
FCALLSCSUB1(hiterator_rewind, F_HITERATOR_REWIND, f_hiterator_rewind, STRING)

char * hiterator_getnext(char * hit_hdl) {
  HIterator_t * hit = (HIterator_t *)  _convert_handle(hit_hdl);
  void * obj;
  obj = hiter_getnext(hit); 
  return _pointer2handle((void *) obj, "undef_type", "hiterator_getnext");
}
FCALLSCFUN1(STRING,hiterator_getnext, F_HITERATOR_GETNEXT, f_hiterator_getnext, STRING)

void destroy_hiterator( char * hit_hdl) {
  HIterator_t * hit = (HIterator_t *)  _convert_handle(hit_hdl);
  free(hit);
  return;
}
FCALLSCSUB1(destroy_hiterator, F_DESTROY_HITERATOR, f_destroy_hiterator, STRING)
// END of HContainer Iterator handlers
// ##########################################################

// ##########################################################
// Start of Array structure getors
int get_array_type (char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
// DRMS_Type_t type;            /* Datatype of the data elements. */
  return a->type;
}
FCALLSCFUN1(INT, get_array_type, F_GET_ARRAY_TYPE, f_get_array_type, STRING)

int get_array_naxis (char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
//  int naxis;                     /* Number of dimensions. */
  return a->naxis; 
}
FCALLSCFUN1(INT, get_array_naxis, F_GET_ARRAY_NAXIS, f_get_array_naxis, STRING)

void get_array_axis(int * axis, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  int i=0;
//  int axis[DRMS_MAXRANK];        /* Size of each dimension. */
  for (i=0; i< DRMS_MAXRANK; i++)
    axis[i]=a->axis[i];

  return;
}
FCALLSCSUB2(get_array_axis, F_GET_ARRAY_AXIS, f_get_array_axis, INTV, STRING)


void get_array_data(void * data, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  Fort_Alloc_t *data_alloc = (Fort_Alloc_t *) data;
  int i=0, size=1;
  
  for (i =0 ; i<a->naxis; i++)
    size *= a->axis[i];
  fort_alloc(data_alloc, a->data, size, drms_sizeof(a->type), 1);
  return;
}

/*void get_array_data2(void * data, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  Fort_Alloc_t *data_alloc = (Fort_Alloc_t *) data;
  Fort_Alloc_t *farr;
  farr = data_alloc;
  short int *V;
  int size=703921;

#ifdef DEBUG
  printf("##### data array [%p]\n", data);
  printf("array data size [%d]\n", size);
  printf("type size size [%d]\n", drms_sizeof(a->type));
#endif
  V = malloc(sizeof(short int) * 703921);
  fort_alloc(data_alloc, V, size, sizeof(short int), 1);
  printf("Array sizeof [%d]; iOffset [%d]; iExtent [%d]; iStride [%d]; flags [%d]; rank [%d]; lBound [%d] whatever [%]\n", farr->m_sizeof, farr->m_iOffset, farr->m_Dim[0].iExtent, farr->m_Dim[0].iStride, farr->m_iFlags, farr->m_iRank, farr->m_Dim[0].lBound, farr->m_iWhatever);

  V[0]=75;
  V[1]=99;
  V[2]=291;
  V[2096]=1234;
  
  return;
}*/
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA, f_get_array_data, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_BYTE, f_get_array_data_byte, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_INTEGER, f_get_array_data_integer, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_INTEGER8, f_get_array_data_integer8, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_REAL, f_get_array_data_real, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_REAL8, f_get_array_data_real8, PVOID, STRING)
FCALLSCSUB2(get_array_data, F_GET_ARRAY_DATA_DOUBLE, f_get_array_data_double, PVOID, STRING)


/* Fields relating to scaling and slicing. */
int get_array_israw(char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
//  int israw;                 /* Is this read in with type=DRMS_TYPE_RAW? 
//	          		If israw==0 then shift and scaling have been 
//			        applied to the data and they represent the 
//				"true" values. If israw==1 then no shift
//                              and scaling have been applied to the data. */
  return a->israw;
}
FCALLSCFUN1(INT, get_array_israw, F_GET_ARRAY_ISRAW, f_get_array_israw, STRING)


double get_array_bzero(char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
//  double bzero;              /* Zero point for parent->child mapping. */
  return a->bzero;
}
FCALLSCFUN1(DOUBLE, get_array_bzero, F_GET_ARRAY_BZERO, f_get_array_bzero, STRING)

double get_array_bscale(char* a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
//  double bscale;             /* Slope for parent->child. */
  return a->bscale;
}
FCALLSCFUN1(DOUBLE, get_array_bscale, F_GET_ARRAY_BSCALE, f_get_array_bscale, STRING)

void get_array_start(int * start, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  int i=0;
//  int start[DRMS_MAXRANK];   /* Start offset of slice in parent. */
  for (i=0; i< DRMS_MAXRANK; i++)
    start[i]=a->start[i];

  return;
}
FCALLSCSUB2(get_array_start, F_GET_ARRAY_START, f_get_array_start, INTV, STRING)

  /* Private fields used for array index calculation etc. */
void get_array_dope( int * dope, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  int i=0;
//  int dope[DRMS_MAXRANK]; /* Dimension offset multipliers. */
  for (i=0; i< DRMS_MAXRANK; i++)
    dope[i]=a->dope[i];

  return;
}
FCALLSCSUB2(get_array_dope, F_GET_ARRAY_DOPE, f_get_array_dope, INTV, STRING)


/* Private fields used for packed string arrays. */
/* I'm not sure this is the right way to handle this */
//  char *strbuf; /* String buffer used for packed string arrays. */
//array->buflen is bigger than size_t
/*
void get_array_strbuf(char * strbuf, char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
  strncpy(strbuf,a->strbuf,a->buflen);
  return;
}
FCALLSCSUB2(get_array_strbuf, F_GET_ARRAY_STRBUF, get_array_strbuf, PSTRING, STRING)
*/

long long get_array_buflen(char * a_hdl) {
  DRMS_Array_t *a = (DRMS_Array_t *)  _convert_handle(a_hdl);
//  long long buflen;              /* Size of string buffer. */
  return a->buflen;
}
FCALLSCFUN1(LONGLONG, get_array_buflen, F_GET_ARRAY_BUFLEN, f_get_array_buflen, STRING)

// END of Array structure getors
// ##########################################################


// ##########################################################
// START of getor functions for the DRMS_RecordSet_t structure

//  Get number of records in record set
int f_get_rs_num(char * rs_hdl) {
   DRMS_RecordSet_t  * rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);
   return drms_recordset_getnrecs(rs);
}
FCALLSCFUN1(INT, f_get_rs_num, F_GET_RS_NUM, f_get_rs_num, STRING)

//Gets a record from a record set
char * f_get_rs_record(char * rs_hdl, int index) {
  DRMS_RecordSet_t  * rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);
  return _pointer2handle((void *) rs->records[index], "DRMS_Record_t", "f_get_RS_record");
}
FCALLSCFUN2(STRING, f_get_rs_record, F_GET_RS_RECORD, f_get_rs_record, STRING, INT)
// END of getor functions for the DRMS_RecordSet_t structure
// ##########################################################

// ##########################################################
// START of getor functions for the DRMS_SeriesInfo_struct
//  Get the series name
//  char seriesname[DRMS_MAXNAMELEN];      
char * get_si_seriesname(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->seriesname;
}
FCALLSCFUN1(STRING, get_si_seriesname, F_GET_SI_SERIESNAME, f_get_si_seriesname, STRING)

//  Get the series description 
//  char description[DRMS_MAXCOMMENTLEN];
char * get_si_description(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->description;
}
FCALLSCFUN1(STRING, get_si_description, F_GET_SI_DESCRIPTION, f_get_si_description, STRING)

//  Get the series owner 
//  char owner[DRMS_MAXNAMELEN];
char * get_si_owner(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->owner;
}
FCALLSCFUN1(STRING, get_si_owner, F_GET_SI_OWNER, f_get_si_owner, STRING)

//  Get the series unitsize 
//  int unitsize
int get_si_unitsize(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->unitsize;
}
FCALLSCFUN1(INT, get_si_unitsize, F_GET_SI_UNITSIZE, f_get_si_unitsize, STRING)

//  Get the series archive 
//  int archive;    /* Should this series be archived? */
int get_si_archive(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->archive;
}
FCALLSCFUN1(INT, get_si_archive, F_GET_SI_ARCHIVE, f_get_si_archive, STRING)

//  Get the series retention 
//  int retention;  /* Default retention time in seconds. */
int get_si_retention(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->retention;
}
FCALLSCFUN1(INT, get_si_retention, F_GET_SI_RETENTION, f_get_si_retention, STRING)

//  Get the series tapegroup 
//  int tapegroup;  /* Tapegroup of the series. */
int get_si_tapegroup(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->tapegroup;
}
FCALLSCFUN1(INT, get_si_tapegroup, F_GET_SI_TAPEGROUP, f_get_si_tapegroup, STRING)

//  Get the series pidx_num 
//  int pidx_num;   /* Number of keywords in primary index. */
int get_si_pidx_num(char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  return si->pidx_num;
}
FCALLSCFUN1(INT, get_si_pidx_num, F_GET_SI_PIDX_NUM, f_get_si_pidx_num, STRING)

//  Get the series keyword_handle 
//  int keyword_handle
#define f_get_si_keyword_handlev_STRV_A1 TERM_CHARS(DRMS_MAXPRIMIDX,-1)
void get_si_keyword_handleV(char * strV,  char * si_hdl) {
  DRMS_SeriesInfo_t  * si = (DRMS_SeriesInfo_t *)  _convert_handle(si_hdl);
  char * key_hdl[DRMS_MAXPRIMIDX];
  int i=0;
  for (i=0; i<DRMS_MAXPRIMIDX; i++) {
    key_hdl[i]=_pointer2handle((void *) si->pidx_keywords[i], "DRMS_Keywords_t", "get_si_keyword_handle");
    strncpy(strV +FHANDLEKEYSIZE*i, key_hdl[i], FHANDLEKEYSIZE);
  }

  return;
}
FCALLSCSUB2(get_si_keyword_handleV, F_GET_SI_KEYWORD_HANDLEV, f_get_si_keyword_handlev, PSTRINGV, STRING)
/* Pointers to keyword structs for keywords that make up the primary key.*/
//  struct DRMS_Keyword_struct *pidx_keywords[DRMS_MAXPRIMIDX]; 
                
// END of getor functions for the DRMS_SeriesInfo_struct
// ##########################################################

// ##########################################################
// START of getor functions for the Segment structure

char * get_segment_record(char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t *)  _convert_handle(seg_hdl);
//  struct DRMS_Record_struct *record; /*  The record this segment belongs to. */
  return _pointer2handle((void *) seg->record, "DRMS_Record_t", "get_segment_record");
}
FCALLSCFUN1(STRING, get_segment_record, F_GET_SEGMENT_RECORD, f_get_segment_record, STRING)

char * get_segment_sinfo(char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t *)  _convert_handle(seg_hdl);
//  DRMS_SegmentInfo_t *info;				      /*  see above  */
  return _pointer2handle((void *) seg->info, "DRMS_SegmentInfo_t", "get_segment_sinfo");
}
FCALLSCFUN1(STRING, get_segment_sinfo, F_GET_SEGMENT_SINFO, f_get_segment_sinfo, STRING)

char * get_segment_filename(char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t *)  _convert_handle(seg_hdl);
//  char filename[DRMS_MAXSEGFILENAME];		      /*  Storage file name  */
  return seg->filename;
}
FCALLSCFUN1(STRING, get_segment_filename, F_GET_SEGMENT_FILENAME, f_get_segment_filename, STRING)

void get_segment_axis(int * axis, char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t *)  _convert_handle(seg_hdl);
//  int axis[DRMS_MAXRANK];      			 /*  Size of each dimension. */
  int i=0;
  for (i=0; i< DRMS_MAXRANK; i++)
    axis[i]=seg->axis[i];

  return;
}
FCALLSCSUB2(get_segment_axis, F_GET_SEGMENT_AXIS, f_get_segment_axis, INTV, STRING)

void get_segment_blocksize(int * blocksize, char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t *)  _convert_handle(seg_hdl);
  int i=0;
//  int blocksize[DRMS_MAXRANK];		/*  block sizes for tiled/blocked
  for (i=0; i< DRMS_MAXRANK; i++)
    blocksize[i]=seg->blocksize[i];
}
FCALLSCSUB2(get_segment_blocksize, F_GET_SEGMENT_BLOCKSIZE, f_get_segment_blocksize, INTV, STRING)

// END of getor functions for the Segment structure
// ##########################################################

// ##########################################################
// START of getor functions for the record structure

// Given a record returns parameters such us:
//  *env;               /* Pointer to global DRMS environment. */
char * get_record_env(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_Env_t *env = rec->env;
  return _pointer2handle((void *) env, "DRMS_Env_t", "get_record_env");
}
FCALLSCFUN1(STRING, get_record_env, F_GET_RECORD_ENV, f_get_record_env, STRING)

//  recnum;             /*** Unique record identifier. ***/

long long get_record_recnum(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->recnum;
}
FCALLSCFUN1(LONGLONG, get_record_recnum, F_GET_RECORD_RECNUM, f_get_record_recnum, STRING)

//  sunum;              /* Unique index of the storage unit associated 
//			with this record. */

long long get_record_sunum(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->sunum;
}
FCALLSCFUN1(LONGLONG, get_record_sunum, F_GET_RECORD_SUNUM, f_get_record_sunum, STRING)

//  init;               /* Flag used internally by the series cache. */
int get_record_init(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->init;
}
FCALLSCFUN1(INT, get_record_init, F_GET_RECORD_INIT, f_get_record_init, STRING)
//  readonly;           /* Flag indicating if record is read-only. */
int get_record_readonly(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->readonly;
}
FCALLSCFUN1(INT, get_record_readonly, F_GET_RECORD_READONLY, f_get_record_readonly, STRING)

//  lifetime;           /* Flag indicating if record is session- 
//				  temporary or permanent. */
int get_record_lifetime(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->lifetime;
}
FCALLSCFUN1(INT, get_record_lifetime, F_GET_RECORD_LIFETIME, f_get_record_lifetime, STRING)
//  su;                 /* Holds sudir (Storage unit directory).
//				  	 Until the storage unit has been 
//				 	 requested from SUMS this pointer is 
//					 NULL. */
char * get_record_su(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_StorageUnit_t *su = rec->su;
  return _pointer2handle((void *) su, "DRMS_StorageUnit_t", "get_record_su");
}
FCALLSCFUN1(STRING, get_record_su, F_GET_RECORD_SU, f_get_record_su, STRING)
//  slotnum;            /* Number of the slot assigned within storage unit. */
int get_record_slotnum(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->slotnum;
}
FCALLSCFUN1(INT, get_record_slotnum, F_GET_RECORD_SLOTNUM, f_get_record_slotnum, STRING)
//  sessionid;          /* ID of the session that created this record. */

long long get_record_sessionid(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->sessionid;
}
FCALLSCFUN1(LONGLONG, get_record_sessionid, F_GET_RECORD_SESSIONID, f_get_record_sessionid, STRING)

//  sessionns;          /* namespace of the session that created this record. */
char * get_record_sessionns(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return rec->sessionns;
}
FCALLSCFUN1(STRING, get_record_sessionns, F_GET_RECORD_SESSIONNS, f_get_record_sessionns, STRING)
//  seriesinfo;         /* Series to which this record belongs. */
char * get_record_seriesinfo(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_SeriesInfo_t *seriesinfo = rec->seriesinfo;
  return _pointer2handle((void *) seriesinfo, "DRMS_SeriesInfo_t", "get_record_seriesinfo");
}
FCALLSCFUN1(STRING, get_record_seriesinfo, F_GET_RECORD_SERIESINFO, f_get_record_seriesinfo, STRING)
//  keywords;           /* Container of named keywords. */
char * get_record_keywords(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return _pointer2handle((void *) &rec->keywords, "HContainer_t", "get_record_keywords");
}
FCALLSCFUN1(STRING, get_record_keywords, F_GET_RECORD_KEYWORDS, f_get_record_keywords, STRING)
//  links;              /* Container of named links. */
char * get_record_links(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return _pointer2handle((void *) &rec->links, "HContainer_t", "get_record_links");
}
FCALLSCFUN1(STRING, get_record_links, F_GET_RECORD_LINKS, f_get_record_links, STRING)
//  segments;           /* Container of named data segments. */
char * get_record_segments(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  return _pointer2handle((void *) &rec->segments, "HContainer_t", "get_record_segments");
}
FCALLSCFUN1(STRING, get_record_segments, F_GET_RECORD_SEGMENTS, f_get_record_segments, STRING)
// END of getor functions for the record structure
// ###################################################

char * f_drms_open_records(char * env_hdl, char *recordsetname, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_RecordSet_t  * _ret_var;

  _ret_var = drms_open_records(env, recordsetname, status);
  return _pointer2handle((void *)_ret_var, "DRMS_RecordSet_t", "drms_open_records");
}
FCALLSCFUN3(STRING, f_drms_open_records, F_DRMS_OPEN_RECORDS, f_drms_open_records, STRING, STRING, PINT)


char * f_drms_create_records(char * env_hdl, int n, char *series, DRMS_RecLifetime_t lifetime, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_RecordSet_t  * _ret_var;

  _ret_var = drms_create_records(env, n, series, lifetime, status);
  return _pointer2handle((void *)_ret_var, "DRMS_RecordSet_t", "drms_create_records");
}
FCALLSCFUN5(STRING, f_drms_create_records, F_DRMS_CREATE_RECORDS, f_drms_create_records, STRING, INT, STRING, INT, PINT)


char * f_drms_create_records2(char * env_hdl, int n, char * template_hdl, DRMS_RecLifetime_t lifetime, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);
  DRMS_RecordSet_t  * _ret_var;

  _ret_var = drms_create_records_fromtemplate(env, n, template, lifetime, status);
  return _pointer2handle((void *)_ret_var, "DRMS_RecordSet_t", "drms_create_records2");
}
FCALLSCFUN5(STRING, f_drms_create_records2, F_DRMS_CREATE_RECORDS2, f_drms_create_records2, STRING, INT, STRING, INT, PINT)

char * f_drms_clone_records(char * rs_in_hdl, DRMS_RecLifetime_t lifetime, DRMS_CloneAction_t mode, int *status) {
  DRMS_RecordSet_t  * rs_in = (DRMS_RecordSet_t  *)  _convert_handle(rs_in_hdl);
  DRMS_RecordSet_t  * _ret_var;

  _ret_var = drms_clone_records(rs_in, lifetime, mode, status);
  return _pointer2handle((void *)_ret_var, "DRMS_RecordSet_t", "drms_clone_records");
}
FCALLSCFUN4(STRING, f_drms_clone_records, F_DRMS_CLONE_RECORDS, f_drms_clone_records, STRING, INT, INT, PINT)


int f_drms_close_records(char * rs_hdl, int action) {
  DRMS_RecordSet_t  * rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);

  return drms_close_records(rs, action);
}
FCALLSCFUN2(INT, f_drms_close_records, F_DRMS_CLOSE_RECORDS, f_drms_close_records, STRING, INT)

#if USEINTERNALHANDLES
void free_handles(void) {
  int i;
  for (i =0 ; i< handle_index; i ++) {
    free(handles[i]);
  }

  free(handles);
  handle_max_index = handle_index = 0;
}
#endif  /* USEINTERNALHANDLES */

void f_drms_free_records(char * rs_hdl, int size) {
  DRMS_RecordSet_t  * rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);
  strncpy(rs_hdl, "NULL", size);

  /* XXX Need to free rs_hdl handle; strcpy(rs_hdl, "NULL") causes the handle to leak. */

#if USEINTERNALHANDLES
  free_handles();
#endif /* USEINTERNALHANDLES */
  return drms_free_records(rs);
}
FCALLSCSUB1(f_drms_free_records, F_DRMS_FREE_RECORDS, f_drms_free_records, PSTRING)


char * f_drms_create_record(char * env_hdl, char *series, DRMS_RecLifetime_t lifetime, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_create_record(env, series, lifetime, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_create_record");
}
FCALLSCFUN4(STRING, f_drms_create_record, F_DRMS_CREATE_RECORD, f_drms_create_record, STRING, STRING, INT, PINT)


char * f_drms_clone_record(char * oldrec_hdl, DRMS_RecLifetime_t lifetime, DRMS_CloneAction_t mode, int *status) {
  DRMS_Record_t  * oldrec = (DRMS_Record_t  *)  _convert_handle(oldrec_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_clone_record(oldrec, lifetime, mode, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_clone_record");
}
FCALLSCFUN4(STRING, f_drms_clone_record, F_DRMS_CLONE_RECORD, f_drms_clone_record, STRING, INT, INT, PINT)


int f_drms_close_record(char * rec_hdl, int action) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_close_record(rec, action);
}
FCALLSCFUN2(INT, f_drms_close_record, F_DRMS_CLOSE_RECORD, f_drms_close_record, STRING, INT)


int f_drms_closeall_records(char * env_hdl, int action) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_closeall_records(env, action);
}
FCALLSCFUN2(INT, f_drms_closeall_records, F_DRMS_CLOSEALL_RECORDS, f_drms_closeall_records, STRING, INT)


void f_drms_free_record(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_free_record(rec);
}
FCALLSCSUB1(f_drms_free_record, F_DRMS_FREE_RECORD, f_drms_free_record, STRING)

char * f_drms_alloc_record2(char * template_hdl, long long recnum, int *status) {
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_alloc_record2(template, recnum, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_alloc_record2");
}
FCALLSCFUN3(STRING, f_drms_alloc_record2, F_DRMS_ALLOC_RECORD2, f_drms_alloc_record2, STRING, LONG, PINT)


char * f_drms_alloc_record(char * env_hdl, const char *series, long long recnum, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_alloc_record(env, series, recnum, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_alloc_record");
}
FCALLSCFUN4(STRING, f_drms_alloc_record, F_DRMS_ALLOC_RECORD, f_drms_alloc_record, STRING, STRING, LONG, PINT)


char * f_drms_template_record(char * env_hdl, const char *seriesname, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_Record_t  * _ret_var;

  _ret_var = drms_template_record(env, seriesname, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_template_record");
}
FCALLSCFUN3(STRING, f_drms_template_record, F_DRMS_TEMPLATE_RECORD, f_drms_template_record, STRING, STRING, PINT)


void f_drms_free_template_record_struct(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_free_template_record_struct(rec);
}
FCALLSCSUB1(f_drms_free_template_record_struct, F_DRMS_FREE_TEMPLATE_RECORD_STRUCT, f_drms_free_template_record_struct, STRING)


void f_drms_free_record_struct(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_free_record_struct(rec);
}
FCALLSCSUB1(f_drms_free_record_struct, F_DRMS_FREE_RECORD_STRUCT, f_drms_free_record_struct, STRING)


void f_drms_copy_record_struct(char * dst_hdl, char * src_hdl) {
  DRMS_Record_t  * dst = (DRMS_Record_t  *)  _convert_handle(dst_hdl);
  DRMS_Record_t  * src = (DRMS_Record_t  *)  _convert_handle(src_hdl);

  return drms_copy_record_struct(dst, src);
}
FCALLSCSUB2(f_drms_copy_record_struct, F_DRMS_COPY_RECORD_STRUCT, f_drms_copy_record_struct, STRING, STRING)


int f_drms_populate_record(char * rec_hdl, long long recnum) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_populate_record(rec, recnum);
}
FCALLSCFUN2(INT, f_drms_populate_record, F_DRMS_POPULATE_RECORD, f_drms_populate_record, STRING, LONG)


int f_drms_populate_records(char * rs_hdl, char * qres_hdl) {
  DRMS_RecordSet_t  * rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);
  DB_Binary_Result_t  * qres = (DB_Binary_Result_t  *)  _convert_handle(qres_hdl);

  return drms_populate_records(rs, qres);
}
FCALLSCFUN2(INT, f_drms_populate_records, F_DRMS_POPULATE_RECORDS, f_drms_populate_records, STRING, STRING)


char * f_drms_field_list(char * rec_hdl, int *num_cols) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_field_list(rec, num_cols);
}
FCALLSCFUN2(STRING, f_drms_field_list, F_DRMS_FIELD_LIST, f_drms_field_list, STRING, PINT)


int f_drms_insert_records(char * recset_hdl) {
  DRMS_RecordSet_t  * recset = (DRMS_RecordSet_t  *)  _convert_handle(recset_hdl);
  return drms_insert_records(recset);
}
FCALLSCFUN1(INT, f_drms_insert_records, F_DRMS_INSERT_RECORDS, f_drms_insert_records, STRING)

long long f_drms_record_size(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_size(rec);
}
FCALLSCFUN1(LONGLONG, f_drms_record_size, F_DRMS_RECORD_SIZE, f_drms_record_size, STRING)


void f_drms_print_record(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_print_record(rec);
}
FCALLSCSUB1(f_drms_print_record, F_DRMS_PRINT_RECORD, f_drms_print_record, STRING)


int f_drms_record_numkeywords(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_numkeywords(rec);
}
FCALLSCFUN1(INT, f_drms_record_numkeywords, F_DRMS_RECORD_NUMKEYWORDS, f_drms_record_numkeywords, STRING)


int f_drms_record_numlinks(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_numlinks(rec);
}
FCALLSCFUN1(INT, f_drms_record_numlinks, F_DRMS_RECORD_NUMLINKS, f_drms_record_numlinks, STRING)


int f_drms_record_numsegments(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_numsegments(rec);
}
FCALLSCFUN1(INT, f_drms_record_numsegments, F_DRMS_RECORD_NUMSEGMENTS, f_drms_record_numsegments, STRING)


int f_drms_record_num_nonlink_segments(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_num_nonlink_segments(rec);
}
FCALLSCFUN1(INT, f_drms_record_num_nonlink_segments, F_DRMS_RECORD_NUM_NONLINK_SEGMENTS, f_drms_record_num_nonlink_segments, STRING)


void f_drms_record_directory(char * rec_hdl, char *dirname, int size, int retrieve) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  char buf[DRMS_MAXPATHLEN] = {0};

  drms_record_directory(rec, buf, retrieve);

  if (*buf)
  {
     strncpy(dirname, buf, size);
  }

  return;
}
FCALLSCSUB3(f_drms_record_directory, F_DRMS_RECORD_DIRECTORY, f_drms_record_directory, STRING, PSTRING, INT)


char * f_drms_record_fopen(char * rec_hdl, char *filename, const char *mode) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  FILE  * _ret_var;

  _ret_var = drms_record_fopen(rec, filename, mode);
  return _pointer2handle((void *)_ret_var, "FILE", "drms_record_fopen");
}
FCALLSCFUN3(STRING, f_drms_record_fopen, F_DRMS_RECORD_FOPEN, f_drms_record_fopen, STRING, STRING, STRING)


long long f_drms_record_memsize(char * rec_hdl) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_record_memsize(rec);
}
FCALLSCFUN1(LONGLONG, f_drms_record_memsize, F_DRMS_RECORD_MEMSIZE, f_drms_record_memsize, STRING)


int f_drms_recproto_setseriesinfo(char * rec_hdl, int *unitSize, int *bArchive, int *nDaysRetention, int *tapeGroup, const char *description) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);

  return drms_recproto_setseriesinfo(rec, unitSize, bArchive, nDaysRetention, tapeGroup, description);
}
FCALLSCFUN6(INT, f_drms_recproto_setseriesinfo, F_DRMS_RECORD_SETSERIESINFO, f_drms_recproto_setseriesinfo, STRING, PINT, PINT, PINT, PINT, STRING)


// ##################################
// ##### FILE:: drms_record.c #######
// ####          END             ####
// ##################################


// ##################################
// ##### FILE:: drms_segment.c ######
// ####          START           ####
// ##################################

int f_drms_segment_write_from_file(char *segment_hdl, char *infile) {
  DRMS_Segment_t  * segment = (DRMS_Segment_t  *)  _convert_handle(segment_hdl);
  return drms_segment_write_from_file(segment, infile);
}
FCALLSCFUN2(INT, f_drms_segment_write_from_file, F_DRMS_SEGMENT_WRITE_FROM_FILE, f_drms_segment_write_from_file, STRING, STRING)

void f_drms_free_template_segment_struct(char * segment_hdl) {
  DRMS_Segment_t  * segment = (DRMS_Segment_t  *)  _convert_handle(segment_hdl);

  return drms_free_template_segment_struct(segment);
}
FCALLSCSUB1(f_drms_free_template_segment_struct, F_DRMS_FREE_TEMPLATE_SEGMENT_STRUCT, f_drms_free_template_segment_struct, STRING)


void f_drms_free_segment_struct(char * segment_hdl) {
  DRMS_Segment_t  * segment = (DRMS_Segment_t  *)  _convert_handle(segment_hdl);

  return drms_free_segment_struct(segment);
}
FCALLSCSUB1(f_drms_free_segment_struct, F_DRMS_FREE_SEGMENT_STRUCT, f_drms_free_segment_struct, STRING)


void f_drms_copy_segment_struct(char * dst_hdl, char * src_hdl) {
  DRMS_Segment_t  * dst = (DRMS_Segment_t  *)  _convert_handle(dst_hdl);
  DRMS_Segment_t  * src = (DRMS_Segment_t  *)  _convert_handle(src_hdl);

  return drms_copy_segment_struct(dst, src);
}
FCALLSCSUB2(f_drms_copy_segment_struct, F_DRMS_COPY_SEGMENT_STRUCT, f_drms_copy_segment_struct, STRING, STRING)

int f_drms_template_segments(char * template_hdl) {
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);

  return drms_template_segments(template);
}
FCALLSCFUN1(INT, f_drms_template_segments, F_DRMS_TEMPLATE_SEGMENTS, f_drms_template_segments, STRING)


void f_drms_segment_print(char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_print(seg);
}
FCALLSCSUB1(f_drms_segment_print, F_DRMS_SEGMENT_PRINT, f_drms_segment_print, STRING)

long long f_drms_segment_size(char * seg_hdl, int *status) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_size(seg, status);
}
FCALLSCFUN2(LONGLONG, f_drms_segment_size, F_DRMS_SEGMENT_SIZE, f_drms_segment_size, STRING, PINT)

int f_drms_segment_setdims(char * dims_hdl, char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  DRMS_SegmentDimInfo_t  * dims = (DRMS_SegmentDimInfo_t  *)  _convert_handle(dims_hdl);

  return drms_segment_setdims(seg, dims);
}
FCALLSCFUN2(INT, f_drms_segment_setdims, F_DRMS_SEGMENT_SETDIMS, f_drms_segment_setdims, STRING, STRING)

#if EXPANDAPI
     // XXX Fix!
/* NEW INTERFACE drms_segment_getdims */
void f_drms_segment_getdims(int *dims, char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  drms_segment_getdims(seg, dims);

  return;
}
FCALLSCSUB2(f_drms_segment_getdims, F_DRMS_SEGMENT_GETDIMS, f_drms_segment_getdims, PINT, STRING)
#endif /* EXPANDAPI */

void f_drms_segment_filename(char * seg_hdl, char *filename, int size) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  char buf[DRMS_MAXPATHLEN] = {0};

  drms_segment_filename(seg, buf);

  if (*buf)
  {
     strncpy(filename, buf, size);
  }

  return;
}

FCALLSCSUB2(f_drms_segment_filename, F_DRMS_SEGMENT_FILENAME, f_drms_segment_filename, STRING, PSTRING)


int f_drms_delete_segmentfile(char * seg_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_delete_segmentfile(seg);
}
FCALLSCFUN1(INT, f_drms_delete_segmentfile, F_DRMS_DELETE_SEGMENTFILE, f_drms_delete_segmentfile, STRING)

#if EXPANDAPI
int f_drms_segment_setscaling(char * seg_hdl, double bzero, double bscale) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_setscaling(seg, bzero, bscale);
}
FCALLSCFUN3(INT, f_drms_segment_setscaling, F_DRMS_SEGMENT_SETSCALING, f_drms_segment_setscaling, STRING, DOUBLE, DOUBLE)


int f_drms_segment_getscaling(char * seg_hdl, double *bzero, double *bscale) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_getscaling(seg, bzero, bscale);
}
FCALLSCFUN3(INT, f_drms_segment_getscaling, F_DRMS_SEGMENT_GETSCALING, f_drms_segment_getscaling, STRING, PDOUBLE, PDOUBLE)
#endif

char * f_drms_segment_lookup(char * rec_hdl, const char *segname) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_Segment_t  * _ret_var;

  _ret_var = drms_segment_lookup(rec, segname);
  return _pointer2handle((void *)_ret_var, "DRMS_Segment_t", "drms_segment_lookup");
}
FCALLSCFUN2(STRING, f_drms_segment_lookup, F_DRMS_SEGMENT_LOOKUP, f_drms_segment_lookup, STRING, STRING)


char * f_drms_segment_lookupnum(char * rec_hdl, int segnum) {
  DRMS_Record_t  * rec = (DRMS_Record_t  *)  _convert_handle(rec_hdl);
  DRMS_Segment_t  * _ret_var;

  _ret_var = drms_segment_lookupnum(rec, segnum);
  return _pointer2handle((void *)_ret_var, "DRMS_Segment_t", "drms_segment_lookupnum");
}
FCALLSCFUN2(STRING, f_drms_segment_lookupnum, F_DRMS_SEGMENT_LOOKUPNUM, f_drms_segment_lookupnum, STRING, INT)


char * f_drms_segment_read(char * seg_hdl, DRMS_Type_t type, int *status) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_segment_read(seg, type, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_segment_read");
}
FCALLSCFUN3(STRING, f_drms_segment_read, F_DRMS_SEGMENT_READ, f_drms_segment_read, STRING, INT, PINT)


char * f_drms_segment_readslice(char * seg_hdl, DRMS_Type_t type, int *start, int *end, int *status) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  DRMS_Array_t  * _ret_var;

  _ret_var = drms_segment_readslice(seg, type, start, end, status);
  return _pointer2handle((void *)_ret_var, "DRMS_Array_t", "drms_segment_readslice");
}
FCALLSCFUN5(STRING, f_drms_segment_readslice, F_DRMS_SEGMENT_READSLICE, f_drms_segment_readslice, STRING, INT, PINT, PINT, PINT)


int f_drms_segment_write(char * seg_hdl, char * arr_hdl, int autoscale) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  DRMS_Array_t  * arr = (DRMS_Array_t  *)  _convert_handle(arr_hdl);

  return drms_segment_write(seg, arr, autoscale);
}
FCALLSCFUN3(INT, f_drms_segment_write, F_DRMS_SEGMENT_WRITE, f_drms_segment_write, STRING, STRING, INT)


void f_drms_segment_setblocksize(char * seg_hdl, int *blksz) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_setblocksize(seg, blksz);
}
FCALLSCSUB2(f_drms_segment_setblocksize, F_DRMS_SEGMENT_SETBLOCKSIZE, f_drms_segment_setblocksize, STRING, PINT)


void f_drms_segment_getblocksize(char * seg_hdl, int *blksz) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_getblocksize(seg, blksz);
}
FCALLSCSUB2(f_drms_segment_getblocksize, F_DRMS_SEGMENT_GETBLOCKSIZE, f_drms_segment_getblocksize, STRING, PINT)


#if EXPANDAPI
void f_drms_segment_autoscale(char * seg_hdl, char * arr_hdl) {
  DRMS_Segment_t  * seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);
  DRMS_Array_t  * arr = (DRMS_Array_t  *)  _convert_handle(arr_hdl);

  return drms_segment_autoscale(seg, arr);
}
FCALLSCSUB2(f_drms_segment_autoscale, F_DRMS_SEGMENT_AUTOSCALE, f_drms_segment_autoscale, STRING, STRING)
#endif

// ##################################
// ##### FILE:: drms_segment.c ######
// ####          END             ####
// ##################################



// ##################################
// ##### FILE:: drms_series.c #######
// ####          START           ####
// ##################################
int f_drms_series_exists(char * drmsEnv_hdl, const char *sname, int *status) {
  DRMS_Env_t  * drmsEnv = (DRMS_Env_t  *)  _convert_handle(drmsEnv_hdl);

  return drms_series_exists(drmsEnv, sname, status);
}
FCALLSCFUN3(INT, f_drms_series_exists, F_DRMS_SERIES_EXISTS, f_drms_series_exists, STRING, STRING, PINT)


int f_drms_insert_series(char * session_hdl, int update, char * template_hdl, int perms) {
  DRMS_Session_t  * session = (DRMS_Session_t  *)  _convert_handle(session_hdl);
  DRMS_Record_t  * template = (DRMS_Record_t  *)  _convert_handle(template_hdl);

  return drms_insert_series(session, update, template, perms);
}
FCALLSCFUN4(INT, f_drms_insert_series, F_DRMS_INSERT_SERIES, f_drms_insert_series, STRING, INT, STRING, INT)

#if 0
/* No need for module writers to call this */
int f_drms_delete_series(char * env_hdl, char *series, int cascade) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_delete_series(env, series, cascade);
}
FCALLSCFUN3(INT, f_drms_delete_series, F_DRMS_DELETE_SERIES, f_drms_delete_series, STRING, STRING, INT)
#endif

/* NEW INTERFACE f_drms_series_createpkeyarray */
void f_drms_series_createpkeyarray(void * fpkw, char * env_hdl, const char *seriesName, int *nPKeys, int *status) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  char **ret = (char **) NULL;

  ret = drms_series_createpkeyarray(env, seriesName, nPKeys, status);

  if (!*status) {
    convert_C2F_string_array(  (Fort_Alloc_t *) fpkw, ret, *nPKeys, DRMS_MAXKEYNAMELEN);
  }
  return;
}
FCALLSCSUB5(f_drms_series_createpkeyarray, F_DRMS_SERIES_CREATEPKEYARRAY, f_drms_series_createpkeyarray, PVOID, STRING, STRING, PINT, PINT)

/* NEW INTERFACE f_drms_series_destroypkeyarray */
void f_drms_series_destroypkeyarray(void *fpkeys) {
  free_fort_alloc((Fort_Alloc_t *) fpkeys);
}
FCALLSCSUB1(f_drms_series_destroypkeyarray, F_DRMS_SERIES_DESTROYPKEYARRAY, f_drms_series_destroypkeyarray, PVOID)


int f_drms_series_checkseriescompat(char * drmsEnv_hdl, const char *series1, const char *series2, char * matchSegs_hdl, int *status) {
  DRMS_Env_t  * drmsEnv = (DRMS_Env_t  *)  _convert_handle(drmsEnv_hdl);
  HContainer_t  * matchSegs = (HContainer_t  *)  _convert_handle(matchSegs_hdl);

  return drms_series_checkseriescompat(drmsEnv, series1, series2, matchSegs, status);
}
FCALLSCFUN5(INT, f_drms_series_checkseriescompat, F_DRMS_SERIES_CHECKSERIESCOMPAT, f_drms_series_checkseriescompat, STRING, STRING, STRING, STRING, PINT)


int f_drms_series_checkrecordcompat(char * drmsEnv_hdl, const char *series, char * recTempl_hdl, char * matchSegs_hdl, int *status) {
  DRMS_Env_t  * drmsEnv = (DRMS_Env_t  *)  _convert_handle(drmsEnv_hdl);
  DRMS_Record_t  * recTempl = (DRMS_Record_t  *)  _convert_handle(recTempl_hdl);
  HContainer_t  * matchSegs = (HContainer_t  *)  _convert_handle(matchSegs_hdl);

  return drms_series_checkrecordcompat(drmsEnv, series, recTempl, matchSegs, status);
}
FCALLSCFUN5(INT, f_drms_series_checkrecordcompat, F_DRMS_SERIES_CHECKRECORDCOMPAT, f_drms_series_checkrecordcompat, STRING, STRING, STRING, STRING, PINT)


// ##################################
// ##### FILE:: drms_series.c #######
// ####          END             ####
// ##################################


// ##################################
// #### FILE:: drms_storageunit.c ###
// ####          START           ####
// ##################################
//#### DOUBLE CHECK ####
// found a [char **sudir]
/*long long f_drms_su_alloc(char * env_hdl, uint64_t size, char **sudir, int *status)*/

#if EXPANDAPI
long long f_drms_su_alloc(char * env_hdl, uint64_t size, char *fsudir, int bufsize, int *status) {
  char *sudir=fsudir;
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_su_alloc(env, size, &sudir, status);
}
FCALLSCFUN4(LONGLONG, f_drms_su_alloc, F_DRMS_SU_ALLOC, f_drms_su_alloc, STRING, DOUBLE, PSTRING, PINT)

int f_drms_su_newslots(char * env_hdl, int n, char *series, long long *recnum, DRMS_RecLifetime_t lifetime, int *slotnum, char * su_hdl, int size) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  ** su = (DRMS_StorageUnit_t  **)  _convert_handle(su_hdl);
  return drms_su_newslots(env, n, series, recnum, lifetime, slotnum, su);
}
FCALLSCFUN7(INT, f_drms_su_newslots, F_DRMS_SU_NEWSLOTS, f_drms_su_newslots, STRING, INT, STRING, PLONGLONG, INT, PINT, PSTRING)


int f_drms_su_getsudir(char * env_hdl, char * su_hdl, int retrieve) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  * su = (DRMS_StorageUnit_t  *)  _convert_handle(su_hdl);

  return drms_su_getsudir(env, su, retrieve);
}
FCALLSCFUN3(INT, f_drms_su_getsudir, F_DRMS_SU_GETSUDIR, f_drms_su_getsudir, STRING, STRING, INT)


int f_drms_commitunit(char * env_hdl, char * su_hdl) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  * su = (DRMS_StorageUnit_t  *)  _convert_handle(su_hdl);

  return drms_commitunit(env, su);
}
FCALLSCFUN2(INT, f_drms_commitunit, F_DRMS_COMMITUNIT, f_drms_commitunit, STRING, STRING)


int f_drms_commit_all_units(char * env_hdl, int *archive) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_commit_all_units(env, archive);
}
FCALLSCFUN2(INT, f_drms_commit_all_units, F_DRMS_COMMIT_ALL_UNITS, f_drms_commit_all_units, STRING, PINT)

/* DON'T KNOW how to handle argument HContainer_t  **
char * f_drms_su_lookup(char * env_hdl, char *series, long long sunum, char * scon_out_hdl) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  HContainer_t  ** scon_out = (HContainer_t  **)  _convert_handle(scon_out_hdl);
  DRMS_StorageUnit_t  * _ret_var;

  _ret_var = drms_su_lookup(env, series, sunum, scon_out);
  return _pointer2handle((void *)_ret_var, "DRMS_StorageUnit_t", "drms_su_lookup");
}
FCALLSCFUN4(STRING, f_drms_su_lookup, F_DRMS_SU_LOOKUP, f_drms_su_lookup, STRING, STRING, DOUBLE, PSTRING)
*/

void f_drms_su_freeunit(char * su_hdl) {
  DRMS_StorageUnit_t  * su = (DRMS_StorageUnit_t  *)  _convert_handle(su_hdl);

  return drms_su_freeunit(su);
}
FCALLSCSUB1(f_drms_su_freeunit, F_DRMS_SU_FREEUNIT, f_drms_su_freeunit, STRING)


void f_drms_freeunit(char * env_hdl, char * su_hdl) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  * su = (DRMS_StorageUnit_t  *)  _convert_handle(su_hdl);

  return drms_freeunit(env, su);
}
FCALLSCSUB2(f_drms_freeunit, F_DRMS_FREEUNIT, f_drms_freeunit, STRING, STRING)


int f_drms_su_freeslot(char * env_hdl, char *series, long long sunum, int slotnum) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);

  return drms_su_freeslot(env, series, sunum, slotnum);
}
FCALLSCFUN4(INT, f_drms_su_freeslot, F_DRMS_SU_FREESLOT, f_drms_su_freeslot, STRING, STRING, LONG, INT)


char * f_drms_su_markslot(char * env_hdl, char *series, long long sunum, int slotnum, int *state) {
  DRMS_Env_t  * env = (DRMS_Env_t  *)  _convert_handle(env_hdl);
  DRMS_StorageUnit_t  * _ret_var;

  _ret_var = drms_su_markslot(env, series, sunum, slotnum, state);
  return _pointer2handle((void *)_ret_var, "DRMS_StorageUnit_t", "drms_su_markslot");
}
FCALLSCFUN5(STRING, f_drms_su_markslot, F_DRMS_SU_MARKSLOT, f_drms_su_markslot, STRING, STRING, LONG, INT, PINT)
#endif /* EXPANDAPI */

// ##################################
// #### FILE:: drms_storageunit.c ###
// ####          END             ####
// ##################################


// ##################################
// ##### FILE:: drms_time.c #########
// ####          START           ####
// ##################################
// NO FORTRAN API
// ##################################
// ##### FILE:: drms_time.c #########
// ####          END             ####
// ##################################



// ##################################
// ##### FILE:: drms_types.c ########
// ####          START           ####
// ##################################
/* DON'T KNOW how to handle DB_Type ... as a handle?*/
/*
DB_Type_t drms2dbtype(DRMS_Type_t type)
FCALLSCFUN1(???, drms2dbtype, F_DRMS2DBTYPE, f_drms2dbtype, ???)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *drms_dst]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_copy_db2drms(DRMS_Type_t drms_type, DRMS_Type_Value_t *drms_dst, DB_Type_t db_type, char *db_src) {

  return drms_copy_db2drms(drms_type, drms_dst, db_type, db_src);
}
FCALLSCFUN4(INT, f_drms_copy_db2drms, F_DRMS_COPY_DB2DRMS, f_drms_copy_db2drms, INT, INT, STRING)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *dst]*/
/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *src]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
void f_drms_copy_drms2drms(DRMS_Type_t type, DRMS_Type_Value_t *dst, DRMS_Type_Value_t *src) {

  return drms_copy_drms2drms(type, dst, src);
}
FCALLSCSUB3(f_drms_copy_drms2drms, F_DRMS_COPY_DRMS2DRMS, f_drms_copy_drms2drms, INT)
*/


FCALLSCFUN1(INT, drms_str2type, F_DRMS_STR2TYPE, f_drms_str2type, STRING)


char * f_drms_type2str(DRMS_Type_t type) {

  return (char *) drms_type2str(type);
}
FCALLSCFUN1(STRING, f_drms_type2str, F_DRMS_TYPE2STR, f_drms_type2str, INT)


FCALLSCFUN1(INT, drms_sizeof, F_DRMS_SIZEOF, f_drms_sizeof, INT)


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_equal(DRMS_Type_t type, DRMS_Type_Value_t *x, DRMS_Type_Value_t *y) {

  return drms_equal(type, x, y);
}
FCALLSCFUN3(INT, f_drms_equal, F_DRMS_EQUAL, f_drms_equal, INT)
*/


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_missing(DRMS_Type_t type, DRMS_Type_Value_t *val) {

  return drms_missing(type, val);
}
FCALLSCFUN2(INT, f_drms_missing, F_DRMS_MISSING, f_drms_missing, INT)
*/


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_strval(DRMS_Type_t type, DRMS_Type_Value_t *val, char *str) {

  return drms_strval(type, val, str);
}
FCALLSCFUN3(INT, f_drms_strval, F_DRMS_STRVAL, f_drms_strval, INT, STRING)
*/


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_sprintfval_format(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val, char *format, int internal) {

  return drms_sprintfval_format(dst, type, val, format, internal);
}
FCALLSCFUN5(INT, f_drms_sprintfval_format, F_DRMS_SPRINTFVAL_FORMAT, f_drms_sprintfval_format, STRING, INT, STRING, INT)
*/


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_sprintfval(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val, int internal) {

  return drms_sprintfval(dst, type, val, internal);
}
FCALLSCFUN4(INT, f_drms_sprintfval, F_DRMS_SPRINTFVAL, f_drms_sprintfval, STRING, INT, INT)
*/


//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_printfval(DRMS_Type_t type, DRMS_Type_Value_t *val, int internal) {

  return drms_printfval(type, val, internal);
}
FCALLSCFUN3(INT, f_drms_printfval, F_DRMS_PRINTFVAL, f_drms_printfval, INT, INT)
*/


FCALLSCFUN2(INT, drms_printfval_raw, F_DRMS_PRINTFVAL_RAW, f_drms_printfval_raw, INT, PVOID)


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *dst]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_sscanf(char *str, DRMS_Type_t dsttype, DRMS_Type_Value_t *dst) {

  return drms_sscanf(str, dsttype, dst);
}
FCALLSCFUN3(INT, f_drms_sscanf, F_DRMS_SSCANF, f_drms_sscanf, STRING, INT)
*/


/*####can't find match for [DRMS_Type_Value_t] full_text [DRMS_Type_Value_t val]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
void f_drms_memset(DRMS_Type_t type, int n, void *array, DRMS_Type_Value_t val) {

  return drms_memset(type, n, array, val);
}
FCALLSCSUB4(f_drms_memset, F_DRMS_MEMSET, f_drms_memset, INT, INT, PVOID)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *val]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
void * f_drms_addr(DRMS_Type_t type, DRMS_Type_Value_t *val) {

  return drms_addr(type, val);
}
FCALLSCSUB2(PVOID, f_drms_addr, F_DRMS_ADDR, f_drms_addr, INT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *dst]*/
/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *src]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_convert(DRMS_Type_t dsttype, DRMS_Type_Value_t *dst, DRMS_Type_t srctype, DRMS_Type_Value_t *src) {

  return drms_convert(dsttype, dst, srctype, src);
}
FCALLSCFUN4(INT, f_drms_convert, F_DRMS_CONVERT, f_drms_convert, INT, INT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
char f_drms2char(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2char(type, value, status);
}
FCALLSCFUN3(SHORT, f_drms2char, F_DRMS2CHAR, f_drms2char, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
short f_drms2short(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2short(type, value, status);
}
FCALLSCFUN3(SHORT, f_drms2short, F_DRMS2SHORT, f_drms2short, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms2int(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2int(type, value, status);
}
FCALLSCFUN3(INT, f_drms2int, F_DRMS2INT, f_drms2int, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
long long f_drms2longlong(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2longlong(type, value, status);
}
FCALLSCFUN3(LONG, f_drms2longlong, F_DRMS2LONGLONG, f_drms2longlong, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
float f_drms2float(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2float(type, value, status);
}
FCALLSCFUN3(FLOAT, f_drms2float, F_DRMS2FLOAT, f_drms2float, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
double f_drms2double(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2double(type, value, status);
}
FCALLSCFUN3(DOUBLE, f_drms2double, F_DRMS2DOUBLE, f_drms2double, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
double f_drms2time(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2time(type, value, status);
}
FCALLSCFUN3(DOUBLE, f_drms2time, F_DRMS2TIME, f_drms2time, INT, PINT)
*/


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *value]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
char * f_drms2string(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status) {

  return drms2string(type, value, status);
}
FCALLSCFUN3(STRING, f_drms2string, F_DRMS2STRING, f_drms2string, INT, PINT)
*/

FCALLSCSUB3(drms_byteswap, F_DRMS_BYTESWAP, f_drms_byteswap, INT, INT, STRING)


/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *x]*/
/*####can't find match for [DRMS_Type_Value_t*] full_text [DRMS_Type_Value_t *y]*/
//#### DOUBLE CHECK ####
/* DON'T KNOW how to handle unions
## Function might be wrapped with cfortran entirely
int f_drms_daxpy(DRMS_Type_t type, const double alpha, DRMS_Type_Value_t *x, DRMS_Type_Value_t *y) {

  return drms_daxpy(type, alpha, x, y);
}
FCALLSCFUN4(INT, f_drms_daxpy, F_DRMS_DAXPY, f_drms_daxpy, INT, DOUBLE)
*/


// ##################################
// ##### FILE:: drms_types.c ########
// ####          END             ####
// ##################################

/* Stuff added by Art */
int f_drms_recordset_getnrecs(char * rs_hdl) {
  DRMS_RecordSet_t *rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);

  return drms_recordset_getnrecs(rs);
}
FCALLSCFUN1(INT, f_drms_recordset_getnrecs, F_DRMS_RECORDSET_GETNRECS, f_drms_recordset_getnrecs, STRING)

char *f_drms_recordset_getrec(char * rs_hdl, long long recnum) {
  DRMS_RecordSet_t *rs = (DRMS_RecordSet_t  *)  _convert_handle(rs_hdl);
  DRMS_Record_t *_ret_var = drms_recordset_getrec(rs, recnum);;
  return _pointer2handle((void *)_ret_var, "DRMS_Record_t", "drms_recordset_getrec");
}
FCALLSCFUN2(STRING, f_drms_recordset_getrec, F_DRMS_RECORDSET_GETREC, f_drms_recordset_getrec, STRING, INT)

int f_drms_segment_getnaxis(char * seg_hdl) {
  DRMS_Segment_t *seg = (DRMS_Segment_t  *)  _convert_handle(seg_hdl);

  return drms_segment_getnaxis(seg);
}
FCALLSCFUN1(INT, f_drms_segment_getnaxis, F_DRMS_SEGMENT_GETNAXIS, f_drms_segment_getnaxis, STRING)
