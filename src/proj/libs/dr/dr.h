#ifndef _DR_H
#define _DR_H

/*****************************************************************************/
/* from include/soi_version.h */
/*****************************************************************************/

#define dr_lib_version	("0.0")

/*****************************************************************************/
/* from include/soi_machine.h */
/*****************************************************************************/

#if defined __linux__
#  define IEEE
#  define IEEE_EL
#  undef IEEE_EB

#else
#  undef IEEE
#  undef IEEE_EL
#  undef IEEE_EB
#endif

/*****************************************************************************/
/* from include/soi_NaNs.h */
/*****************************************************************************/

/****************************************************************************/
/*******************************  TYPEDEFS  *********************************/
/****************************************************************************/

#if defined IEEE_EL
  typedef union {
    double d;
    struct {
      unsigned int frac2:32;
      unsigned int frac1:16;
      unsigned int frac	: 3;
      unsigned int quiet: 1;
      unsigned int exp	:11;
      unsigned int sign	: 1;
      } NaN_mask;
    struct {
      unsigned int frac1:32;
      unsigned int frac :20;
      unsigned int exp	:11;
      unsigned int sign	: 1;
      } fp_mask;
    } dNaN;
  typedef union {
    float f;
    struct {
      unsigned int frac1:16;
      unsigned int frac	: 6;
      unsigned int quiet: 1;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      } NaN_mask;
    struct {
      unsigned int frac :23;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      } fp_mask;
    } fNaN;

#elif defined IEEE_EB
  typedef union {
    double d;
    struct {
      unsigned int sign	: 1;
      unsigned int exp	:11;
      unsigned int quiet: 1;
      unsigned int frac	: 3;
      unsigned int frac1:16;
      unsigned int frac2:32;
      } NaN_mask;
    struct {
      unsigned int sign	: 1;
      unsigned int exp	:11;
      unsigned int frac	:20;
      unsigned int frac1:32;
      } fp_mask;
    } dNaN;
  typedef union {
    float f;
    struct {
      unsigned int sign	: 1;
      unsigned int exp	: 8;
      unsigned int quiet: 1;
      unsigned int frac	: 6;
      unsigned int frac1:16;
      } NaN_mask;
    struct {
      unsigned int sign	: 1;
      unsigned int exp	: 8;
      unsigned int frac	:23;
      } fp_mask;
    } fNaN;

#elif defined vax
  typedef union {
    double d;
    struct {
      unsigned int frac	: 6;
      unsigned int quiet: 1;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      unsigned int frac1:16;
      unsigned int frac2:32;
      } NaN_mask;
    struct {
      unsigned int frac	: 7;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      unsigned int frac1:16;
      unsigned int frac2:32;
      } fp_mask;
    } dNaN;
  typedef union {
    float f;
    struct {
      unsigned int frac	: 6;
      unsigned int quiet: 1;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      unsigned int frac1:16;
      } NaN_mask;
    struct {
      unsigned int frac	: 7;
      unsigned int exp	: 8;
      unsigned int sign	: 1;
      unsigned int frac1:16;
      } fp_mask;
    } fNaN;
#endif

/****************************************************************************/
/****************************  MACRO DEFINITIONS  ***************************/
/****************************************************************************/

#if defined IEEE
#  define IsdNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0x7ff) && \
	((((dNaN *)&(X))->fp_mask.frac != 0x0) || \
	(((dNaN *)&(X))->fp_mask.frac1 != 0x0)))
#  define IsdqNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0x7ff) && \
  	(((dNaN *)&(X))->NaN_mask.quiet != 0x0))
#  define IsdsNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0x7ff) && \
  	(((dNaN *)&(X))->NaN_mask.quiet == 0x0) && \
	((((dNaN *)&(X))->fp_mask.frac != 0x0) || \
	(((dNaN *)&(X))->fp_mask.frac1 != 0x0)))
#  define IsfNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
	(((fNaN *)&(X))->fp_mask.frac != 0x0))
#  define IsfqNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((fNaN *)&(X))->NaN_mask.quiet != 0x0))
#  define IsfsNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((fNaN *)&(X))->NaN_mask.quiet == 0x0) && \
	(((fNaN *)&(X))->fp_mask.frac != 0x0))
#  define IsdNaNorINF(X) (((dNaN *)&(X))->fp_mask.exp == 0x7ff)
#endif


/****************************************************************************/
/****************************  MACRO DEFINITIONS  from soi_missing.h ********/
/****************************************************************************/

#define is_B_MISSING(v) (v == B_MISSING)
#define is_S_MISSING(v) (v == S_MISSING)
#define is_I_MISSING(v) (v == I_MISSING)
#define is_UB_MISSING(v) (v == UB_MISSING)
#define is_US_MISSING(v) (v == US_MISSING)
#define is_UI_MISSING(v) (v == UI_MISSING)
#define is_F_MISSING(v) (IsfNaN(v))
#define is_D_MISSING(v) (IsdNaN(v))
#define is_C_MISSING(v) (IsfNaN((v).r) || IsfNaN((v).i))

#define is_T_MISSING(v) (v == T_MISSING)


/*****************************************************************************/
/* from include/soi_time.h */
/*****************************************************************************/

/****************************************************************************/
/*******************************  TYPEDEFS  *********************************/
/****************************************************************************/
  
/* typedef double TIME; */
#include "timeio.h"

/*****************************************************************************/
/* from include/soi_missing.h */
/*****************************************************************************/

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define B_MISSING	(-128)
#define S_MISSING	(-32768)
#define I_MISSING	(-2147483647-1)
#define F_MISSING	(dr_A_Quiet_fNaN())
#define D_MISSING	(dr_A_Quiet_dNaN())

#define T_MISSING	(-211087684800.0)
#define T_MISSING_STR	("-4712.01.01_12:00:00.000_UT")

/*****************************************************************************/
/* from include/soi_nr.h */
/*****************************************************************************/

typedef struct FCOMPLEX {
  float r, i;
} fcomplex;

/*****************************************************************************/
/* from include/soi_sds.h */
/*****************************************************************************/

typedef struct attribute {
  struct attribute   *next;	/*  Next attribute struct            */
  char               *name;	/*  Name of attribute                */
  void               *value;	/*  Pointer to attribute value       */
  char               *format;	/*  Advisory format for printing     */
  char               *comment;	/*  Holds a comment                  */
  int                datatype;	/*  Attribute value's type           */
} ATTRIBUTES;


typedef struct color {
  int		BitsPerPixel;
  int		*Red;
  int		*Green;
  int		*Blue;
} DR_COLOR;


typedef struct dr {
  int		rank;	    /*  dimension				*/
  int		*length;    /*  lengths of each dimension		*/
  void          *fillval;   /*  Empty spot in data symbol		*/
  void          *data;      /*  Pointer to data of the sds		*/
  int		data_avail; /*  0 if no data section, 1 otherwise	*/
  int           datatype;   /*  Type of data				*/
  int           datumsize;  /*  Bytes per datum				*/
  ATTRIBUTES    *attrib;    /*  holds ancillary attribute info		*/
  double	bscale;	    /*  advisory scaling for binary output	*/
  double	bzero;	    /*  advisory offset for binary output	*/
  int		scaling;    /* advice for fixed-length binary output	*/
  char		*comment;   /*  comment text				*/
  char		*history;   /*  history text				*/
  DR_COLOR	*color_table;  /* advisory color table			*/
} DR;

/*  The following two defs can probably be localized to the functions
    from sds_llist.c  */
/* Type llist supports all linked lists with this structure.
 * The member next must be the first member of the structure.
 */
typedef struct _ll {
  struct _ll *next;
} _llist;

/* Type _llistFn is used as a mapping function, i.e. a pointer 
 * to a function.  This, passed to dr_MapList, will be performed
 * on each element in the linked list.
 */
typedef int (*_llistFn)(_llist *ll, void *obj);

#define DR_MAXSTRING	(1023)
#define DR_MAXRANK      (100)
						      /*  datatypes defines  */
#define	DR_VOID		(0)
#define	DR_BYTE		(1)
#define DR_SHORT	(3)
#define DR_INT		(5)
#define DR_LONG		(7)
#define DR_FLOAT	(9)
#define DR_DOUBLE	(10)
#define DR_COMPLEX	(11)
#define DR_STRING	(12)
#define DR_TIME		(13)
#define DR_LOGICAL	(14)

/*****************************************************************************/
/* from include/soi_error.h */
/*****************************************************************************/

#define NO_ERROR		(0)
#define MODULE_ABORT		(1)
#define READ_FAILURE		(34)
#define WRITE_FAILURE		(35)
#define MALLOC_FAILURE		(41)

#define FILE_POINTER_NULL	(100)
#define DR_POINTER_NULL		(200)
#define ATTR_POINTER_NULL	(201)
#define DR_DATA_POINTER_NULL	(202)
#define DR_DATA_POINTER_EXISTS	(203)
#define DR_CREATE_FAILURE	(210)
#define DR_RANK_ERROR		(220)
#define DR_INVALID_DATATYPE	(230)
#define ATTRIBUTE_NOT_FOUND	(240)
#define ATTRNAME_NULL		(241)
#define ATTRVALUE_NULL		(242)

#define DATA_OUT_OF_RANGE	(321)

#define THIS_SHOULDNT_HAPPEN    (1020)
#define MAKES_NO_SENSE          (1021)
#define DOESNOT_COMPUTE         (1022)
#define NOT_IMPLEMENTED         (1023)

#define FITS_ERROR		(1600)
#define FITS_NONCONFORMING	(1601)


#endif /* _DR_H */
