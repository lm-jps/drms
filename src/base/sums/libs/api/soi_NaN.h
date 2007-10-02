/*
 *  soi_NaN.h				~soi/(version)/include/soi_NaN.h
 *
 *  The following mask definitions implement the standard for signaling and
 *    quiet NaN's and Infinity's adopted in SOI-TN-90-103 on IEEE machines
 *    and on VAXen.
 *  C++ programs should include the corresponding file "soi_NaN.hxx".
 *  Additional information is in the following man pages:
 *	NaN
 *
 *  Responsible:  Rick Bogart		RBogart@solar.Stanford.EDU
 *
 *  Bugs:
 *	There is no general test for NaN, only for double or single, and no
 *	  test for Infinity.
 *	soi_NaN.hxx does not exist.
 *
 *  Revision history is at the end of the file.
 */
#ifndef SOI_NaN_INCL

/****************************************************************************/
/**************************  INCLUDE STATEMENTS  ****************************/
/****************************************************************************/

#ifndef SOI_VERSION_INCL
#include <soi_version.h>
#endif

#ifndef SOI_machine_INCL
#include "soi_machine.h"
#endif

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define SOI_NaN_VERSION_NUM	(0.8)
#define SOI_NaN_INCL	1

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

#elif defined vax
#  define IsdNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0xff) && \
	(((dNaN *)&(X))->NaN_mask.frac == 0x3f) && \
	(((dNaN *)&(X))->fp_mask.frac1 == 0x0) && \
	(((dNaN *)&(X))->fp_mask.frac2 == 0x0))
#  define IsdqNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((dNaN *)&(X))->fp_mask.frac == 0x7f) && \
	(((dNaN *)&(X))->fp_mask.frac1 == 0x0))
#  define IsdsNaN(X)	((((dNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((dNaN *)&(X))->fp_mask.frac == 0x3f) && \
	(((dNaN *)&(X))->fp_mask.frac1 == 0x0))
#  define IsfNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
	(((fNaN *)&(X))->NaN_mask.frac == 0x3f) && \
	(((fNaN *)&(X))->fp_mask.frac1 == 0x0))
#  define IsfqNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((fNaN *)&(X))->fp_mask.frac == 0x7f) && \
	(((fNaN *)&(X))->fp_mask.frac1 == 0x0))
#  define IsfsNaN(X)	((((fNaN *)&(X))->fp_mask.exp == 0xff) && \
  	(((fNaN *)&(X))->fp_mask.frac == 0x3f) && \
	(((fNaN *)&(X))->fp_mask.frac1 == 0x0))
#endif

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/

			 /*  source file: ~soi/(version)/src/libM.d/NaNs.c  */
extern double A_Signaling_dNaN (void);

extern double A_Quiet_dNaN (void);

extern double dInfinity (void);

extern float A_Signaling_fNaN (void);

extern float A_Quiet_fNaN (void);

extern float fInfinity (void);

#endif
