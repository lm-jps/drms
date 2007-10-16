/*
 *  soi_machine.h			~soi/(version)/include/soi_machine.h
 *
 *  This file defines the numerical floating-point representation used by
 *    the machine architecture.  It is required by NaN.h and possibly other
 *    programs manipulating numerical bit patterns.  Only three possible
 *    representations are recognized and generally supported:
 *	IEEE End_Little	(0 address on least significant bit)
 *	IEEE End_Big	(0 address on most significant bit)
 *	non-IEEE
 *  C++ programs should include the corresponding file "soi_machine.hxx".
 *  Additional information is in the SOI Technical Note:
 *	SOI-TN-90-103
 *
 *  Responsible:  Rick Bogart		RBogart@solar.Stanford.EDU
 *
 *  Bugs:
 *	Only the following architectures are supported; all others are
 *      (perhaps wrongly) assumed to be non-IEEE:
 *		MIPS	(DEC RISC machines; SGI)
 *		SPARC
 *		NeXT
 *	soi_machine.hxx does not exist
 *
 *  Revision history is at the end of the file.
 */
#ifndef SOI_machine_INCL

#ifndef SOI_VERSION_INCL
#include <soi_version.h>
#endif

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#if defined MIPSEL
#  define IEEE
#  define IEEE_EL
#  undef IEEE_EB

#elif defined MIPSEB
#  define IEEE
#  define IEEE_EB
#  undef IEEE_EL

#elif defined __sparc
#  define IEEE
#  define IEEE_EB
#  undef IEEE_EL

#elif defined NeXT
#  define IEEE
#  define IEEE_EB
#  undef IEEE_EL

#elif defined __linux__
#  define IEEE
#  define IEEE_EL
#  undef IEEE_EB

#else
#  undef IEEE
#  undef IEEE_EL
#  undef IEEE_EB
#endif

#define SOI_MACHINE_VERSION_NUM	(0.8)
#define SOI_machine_INCL	1

#endif
