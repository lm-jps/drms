VPATH  = $(SRCDIR)
STATIC = 
DBNAME = POSTGRESQL

_JSOCROOT_ = ..

# XXX This PGIPATH var isn't correct - we don't put libpq inside the lib_third_party directory
# at Stanford.
PGIPATH	= /usr/include/pgsql
ECPGL = -L$(_JSOCROOT_)/lib_third_party/lib/$(JSOC_MACHINE)/

# This optional file has custom definitions created by the configure script
-include $(SRCDIR)/custom.mk

#***********************************************************************************************#
#
# COMPILER SELECTION
#
COMPILER = icc
FCOMPILER = ifort

ifneq ($(JSOC_AUTOCOMPILER),)
COMPILER = $(JSOC_AUTOCOMPILER)
endif

ifneq ($(JSOC_AUTOFCOMPILER),)
FCOMPILER = $(JSOC_AUTOFCOMPILER)
endif

# can set through custom.mk or through environment
ifneq ($(JSOC_COMPILER),)
COMPILER = $(JSOC_COMPILER)
endif

ifneq ($(JSOC_FCOMPILER),)
FCOMPILER = $(JSOC_FCOMPILER)
endif
#***********************************************************************************************#


#***********************************************************************************************#
#
# DEBUGGING
#
# Check for debug vs. release build - release is default.
#   To do a debug build, either set the environment variable JSOC_DEBUG to 1, OR
#   modify the following line so that DEBUG = 1.  The environment variable takes precedence.
#
DEBUG = 0

ifdef JSOC_DEBUG
ifeq ($(JSOC_DEBUG), 1)
DEBUG = 1
else
DEBUG = 0
endif
endif
#***********************************************************************************************#


#***********************************************************************************************#
#
# WARNINGS
#
# Warnings ARE displayed, by default, for a release build.
#
WARN = 1

# Builder can request warnings via environment variable (setenv JSOC_WARN 1).
#   The environment variable takes precedence.
ifdef JSOC_WARN
ifeq ($(JSOC_WARN), 1)
WARN = 1
else
WARN = 0
endif
endif

ICC_WARNMORE =
ifdef JSOC_WARNICC
ifeq ($(COMPILER), icc)
ICC_WARNMORE = $(JSOC_WARNICC)
endif
endif

#***********************************************************************************************#


#***********************************************************************************************#
#
# THIRD-PARTY LIBRARIES
#
# This section contains make variables that hold the paths to and names of third-party libraries.
# Variables that end in 'H' contain the -I link flags that contain the include paths 
# for the library headers, variables that end in 'L' contain the -L link flags that
# contain the paths to the library binaries, and variables
# that end in "LIBS" contain the full link cmd (the -L flag plus the -l flag)
#
# Path to 3rd-party library headers
FMATHLIBSH = -I$(_JSOCROOT_)/lib_third_party/include
CFITSIOH = -I$(_JSOCROOT_)/lib_third_party/include
GSLH = -I$(_JSOCROOT_)/lib_third_party/include

ifeq ($(JSOC_MACHINE), linux_x86_64) 
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-x86_64/ -lfftw3f -lcfitsio
  # Path to 64-bit 3rd-party libraries
  FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/
  CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/
  GSLL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/
#  ECPGL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/
endif
ifeq ($(JSOC_MACHINE), linux_ia32) 
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-ia32/ -lfftw3f -lcfitsio
  # Path to 32-bit 3rd-party libraries
  FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/
  CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/
  GSLL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/
#  ECPGL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/
endif
ifeq ($(JSOC_MACHINE), mac_osx_ppc) 
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-ia32/ -lfftw3f -lcfitsio
  # Path to appropriate 3rd-party libraries
  FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ppc/
  CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ppc/
  GSLL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ppc/
#  ECPGL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ppc/
endif
ifeq ($(JSOC_MACHINE), mac_osx_ia32) 
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-ia32/ -lfftw3f -lcfitsio
  # Path to appropriate 3rd-party libraries
  FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ia32/
  CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ia32/
  GSLL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ia32/
#  ECPGL = -L$(_JSOCROOT_)/lib_third_party/lib/mac_osx_ia32/
endif

# All 3rd-party libraries - local rules can define a subset
FMATHLIBS = $(FMATHLIBSL) -lfftw3f 
CFITSIOLIBS = $(CFITSIOL) -lcfitsio
ECPGLIBS = $(ECPGL) -lpq

ifeq ($(COMPILER), gcc)
	ifeq ($(JSOC_MACHINE), linux_x86_64) 
		ifneq ($(CFITSIOFNAME_GCC_X86_64),)
			FMATHLIBS = $(FMATHLIBSL) -lfftw3f -l$(CFITSIOFNAME_GCC_X86_64)
			CFITSIOLIBS = $(CFITSIOL) -l$(CFITSIOFNAME_GCC_X86_64)
		endif
	endif
endif

GSLLIBS = $(GSLL) -lgsl -lgslcblas 
#***********************************************************************************************#


#***********************************************************************************************#
#
# CUSTOM BUILDS
#
# Compilation define customizations (eg., for remote DRMS builds)
CUSTOMSW =
ifneq ($(DRMS_DEFAULT_RETENTION),)
#	CUSTOMSW = $(CUSTOMSW) -DDRMS_DEFAULT_RETENTION="\"$(DRMS_DEFAULT_RETENTION)\""
	CUSTOMSW := $(CUSTOMSW) -DDRMS_DEFAULT_RETENTION=$(DRMS_DEFAULT_RETENTION)
endif

ifneq ($(CUSTOM_DEFINES),)
CUSTOMSW := $(CUSTOMSW) -D$(CUSTOM_DEFINES)
endif

#
#***********************************************************************************************#


#***********************************************************************************************#
#
# WARNINGS
#
# NO LONGER USED - Disable several warnings/remarks when compiling with icc - icc's Wall is a bit picky, it 
# complains about extern declarations in .c files.
#   1418 (remark) - external function definition with no prior declaration
#   1419 (warning) - external declaration in primary source file
#   310 (remark) - old style function declaration (pre-ANSI)
#   279 ?
#   981 (remark) - operands are evaluted in unspecified order

# list of warnings to turn into errors
ICC_WARNTOERR = -we266

ifeq ($(WARN), 1)
# Show warnings (always true for a debug build).
ICC_WARN = $(ICC_WARNMORE)
GCC_WARN = -Wno-comment
FCOMPILER_WARN =
else
# Don't show warnings.
ICC_WARN = -w0 -vec-report0 -Wno-comment $(ICC_WARNTOERR)
GCC_WARN = -Wno-comment
ifeq ($(FCOMPILER), ifort)
FCOMPILER_WARN = -vec-report0
else
FCOMPILER_WARN =
endif
endif

#***********************************************************************************************#


#***********************************************************************************************#
#
# GLOBAL LINK FLAGS
#
# Link flags for all targets
#
LL_ALL		= $(SYSLIBS)
GCC_LF_ALL	= $(STATIC) 
ICC_LF_ALL	= $(STATIC) 

# Fortran global LINK flags
F_LF_ALL	= -nofor_main -no-ipo
#***********************************************************************************************#

#***********************************************************************************************#
#
# GLOBAL COMPILE FLAGS
#
GCC_CF_GCCCOMP  = -DGCCCOMP 
ICC_CF_ICCCOMP  = -DICCCOMP 

CCFLAGS_OPT	:=

ifeq ($(COMPILER), icc)
  ifeq ($(JSOC_MACHINE), linux_x86_64)
    CCFLAGS_OPT	:= -xW
  endif
endif

# can't figure out how to get stupid make to do if/else if/else
ifeq ($(DEBUG), 0)
  GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 $(GCC_WARN) $(GCC_CF_GCCCOMP) $(CUSTOMSW)
# -xW tells the icc compiler to optimize for Pentium 4
  ICC_CF_ALL = -I$(SRCDIR)/base/include -std=c99 -D_GNU_SOURCE $(CCFLAGS_OPT) $(ICC_WARN) $(ICC_CF_ICCCOMP) $(CUSTOMSW)

  ifeq ($(JSOC_MACHINE), linux_x86_64)
    GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 -march=opteron $(GCC_WARN) $(GCC_CF_GCCCOMP) $(CUSTOMSW)
  endif

  ifeq ($(JSOC_MACHINE), linux_ia64)
    ICC_CF_ALL	= -I$(SRCDIR)/base/include -std=c99 -D_GNU_SOURCE $(ICC_WARN) $(ICC_CF_ICCCOMP) $(CUSTOMSW)
  endif

  ifeq ($(JSOC_MACHINE), linux_ia32)
    GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 -march=i686 $(GCC_WARN) $(GCC_CF_GCCCOMP) $(CUSTOMSW)
  endif	

else
# -g tells the icc and gcc compilers to generate full debugging information
  GCC_CF_ALL = -I$(SRCDIR)/base/include -std=gnu99 -g $(GCC_WARN) $(GCC_CF_GCCCOMP) $(CUSTOMSW)
  ICC_CF_ALL = -I$(SRCDIR)/base/include -std=c99 -D_GNU_SOURCE -g $(ICC_WARN) $(ICC_CF_ICCCOMP) $(CUSTOMSW)
endif

# Fortran global COMPILE flags
ifeq ($(JSOC_MACHINE), linux_x86_64)
  ifeq ($(FCOMPILER), ifort)
    F_CF_ALL := -mcmodel=medium
  endif
endif

# Other compiler-specific Fortran COMPILE flags
ifeq ($(FCOMPILER), ifort)
  ifeq ($(JSOC_MACHINE), linux_x86_64)
    FCFLAGS_OPT	:= -xW
  endif
  FCFLAGS_INIT := -ftrapuv
else
  # must be gfortran
  FCFLAGS_OPT	:= 
  FCFLAGS_INIT  := 
endif

ifeq ($(DEBUG), 0)
# -xW optimizes ifort compilation for Pentium 4
# -ftrapuv initializes stack local variables to an unusual value to aid error detection. 
  F_CF_ALL	:= $(F_CF_ALL) $(FCFLAGS_OPT) $(FCOMPILER_WARN)
else
  F_CF_ALL	:= $(F_CF_ALL) -g $(FCFLAGS_INIT) $(FCOMPILER_WARN)
endif
#***********************************************************************************************#


#***********************************************************************************************#
#
# BUILD TOOLS
# 
# The C compiler named here must output full (header) dependencies in $(@).d.
# It may be necessary to create a script similar to ccd-gcc for your compiler.
# 
GCC_CMPLR	= $(SRCDIR)/build/ccd-gcc
ICC_CMPLR	= $(SRCDIR)/build/ccd-icc
ARCHIVE		= ar crus $@ $^

ECPG		= ecpg -o $@ -c $<
SWIG		= swig -perl5 -o $@ $<

GCC_COMP	= $(GCC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) -o $@ -c $<
ICC_COMP	= $(ICC_CMPLR) $(ICC_CF_ALL) $(CF_TGT) -o $@ -c $<

GCC_LINK	= $(GCC_CMPLR) $(GCC_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)
ICC_LINK	= $(ICC_CMPLR) $(ICC_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)

GCC_COMPLINK	= $(GCC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) $(GCC_LF_ALL) $(LF_TGT) -o $@ $< $(LL_TGT) $(LL_ALL)
ICC_COMPLINK	= $(ICC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) $(ICC_LF_ALL) $(LF_TGT) -o $@ $< $(LL_TGT) $(LL_ALL)

ifneq ($(COMPILER), icc)
  COMP 		= $(GCC_COMP)
  LINK		= $(GCC_LINK)
  COMPLINK	= $(GCC_COMPLINK)
else
  COMP 		= $(ICC_COMP)
  LINK		= $(ICC_LINK)
  COMPLINK	= $(ICC_COMPLINK)
endif

FCOMP		= $(FCOMPILER) $(F_CF_ALL) $(FF_TGT) -o $@ -c $<
FLINK		= $(FCOMPILER) $(F_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)

SLBIN           = ln -sf ../../_$(JSOC_MACHINE)/$@ ../bin/$(JSOC_MACHINE)/
SLLIB		= ln -sf ../../_$(JSOC_MACHINE)/$@ ../lib/$(JSOC_MACHINE)/
#***********************************************************************************************#


#***********************************************************************************************#
#
# LIBRARY COLLECTIONS
#
ALL_LIBS_FPIC = $(LIBDRMSCLIENT_FPIC) $(LIBDBCLIENT_FPIC) $(LIBCMDPARAMS_FPIC) $(LIBTHREADUTIL_FPIC) $(LIBRICECOMP_FPIC) $(LIBDEFS_FPIC) $(LIBMISC_FPIC) $(LIBDSTRUCT_FPIC) $(LIBTIMEIO_FPIC) $(LIBFITSRW_FPIC) 

### Standard parts
#
include	$(SRCDIR)/Rules.mk

# Libraries from src/util linked with all programs.
ifneq ($(COMPILER), icc)
  SYSLIBS = -lz -ldl -lpthread -lm
else
  SYSLIBS = -lz -ldl -lpthread 
endif
SRCLIBS = $(LIBTHREADUTIL) $(LIBRICECOMP) $(LIBCMDPARAMS) $(LIBTIMEIO) $(LIBFITSRW) $(LIBERRLOG) $(LIBMISC) $(LIBDSTRUCT)
FSRCLIBS = $(LIBTHREADUTIL) $(LIBRICECOMP) $(LIBCMDPARAMSF) $(LIBTIMEIO) $(LIBFITSRW) $(LIBERRLOG) $(LIBMISC) $(LIBDSTRUCT)

########## Libraries to link for server executables,    ##############
########## standalone executables and pipeline modules. ##############

# SERVERLIBS: Libraries linked with "server" programs that 
# need direct access to the DRMS databases.
SERVERLIBS = $(LIBDRMS) $(LIBDEFSSERVER) $(LIBDB) $(LIBSUMSAPI) $(SRCLIBS)

# EXELIBS: Libraries linked with standalone executables.
EXELIBS = $(LIBDRMSCLIENT) $(LIBDEFSCLIENT) $(LIBDBCLIENT) $(SRCLIBS)

# MODLIBS: Libraries linked with DRMS modules.
MODLIBS = $(LIBJSOC_MAIN) $(SERVERLIBS)

# MODLIBS_SOCK: Libraries linked with DRMS modules with socket connection to a drms_server
MODLIBS_SOCK = $(LIBJSOC_MAIN_SOCK) $(LIBDRMSCLIENT) $(LIBDEFSCLIENT) $(LIBDBCLIENT) $(LIBSUMSAPI) $(SRCLIBS)

# FMODLIBS: Libraries linked with DRMS Fortran modules
FMODLIBS_SOCK = $(LIBJSOC_MAIN_SOCK_F) $(LIBINTHANDLESF) $(LIBDRMSCLIENT) $(LIBDEFSCLIENT) $(LIBDBCLIENT) $(FSRCLIBS)
#***********************************************************************************************#


#***********************************************************************************************#
#
# PROJECT MAKE RULES
#
# Make rules that apply to all projects outside of the base DRMS/SUMS system
-include $(SRCDIR)/proj/make_basic.mk
#***********************************************************************************************#


#***********************************************************************************************#
#
# MODULE TYPES
#
# Make rules that apply to all projects, inside and outside of the base DRMS/SUMS system
$(CEXE):	%:	%.o $(EXELIBS)
		$(LINK)
		$(SLBIN)

$(FEXE):	%:	%.o $(FMATHLIBS)
		$(FLINK)
		$(SLBIN)

$(SERVEREXE):   LL_TGT := $(LL_TGT) $(ECPGLIBS) $(CFITSIOLIBS)
$(SERVEREXE):	%:	%.o $(SERVERLIBS)
			$(LINK)
			$(SLBIN)

$(MODEXE):      LL_TGT := $(LL_TGT) $(ECPGLIBS) $(CFITSIOLIBS)
$(MODEXE):	%:	%.o $(MODLIBS)
			$(LINK)
			$(SLBIN)

$(MODEXE_SOCK):	LL_TGT := $(LL_TGT) $(CFITSIOLIBS)
$(MODEXE_SOCK): %_sock: %.o $(MODLIBS_SOCK)
			$(LINK)
			$(SLBIN)
# FMODEXE_SOCK contains all Fortran modules - the DoIt() function is defined inside a .f file.
# These are socket-connect modules only. Assume they use third-party Fortran libraries
# (although this may not be the case).
$(FMODEXE_SOCK):	LL_TGT := $(LL_TGT) $(CFITSIOLIBS) $(FMATHLIBS)
$(FMODEXE_SOCK):     %_sock:	%.o $(FMODLIBS_SOCK) 
			$(FLINK)
			$(SLBIN)

# MODEXE_USEF contains all C direct-connect modules that use third-party Fortran libraries.
$(MODEXE_USEF):	LL_TGT := $(LL_TGT) $(ECPGLIBS) $(CFITSIOLIBS) $(FMATHLIBS)
$(MODEXE_USEF):     %:	%.o $(MODLIBS)
			$(FLINK)
			$(SLBIN)
# MODEXE_USEF contains all C socket-connect modules that use third-party Fortran libraries.
$(MODEXE_USEF_SOCK):	LL_TGT := $(LL_TGT) $(CFITSIOLIBS) $(FMATHLIBS)
$(MODEXE_USEF_SOCK): %_sock: %.o $(MODLIBS_SOCK)
			$(FLINK)
			$(SLBIN)
#***********************************************************************************************#
