VPATH  = $(SRCDIR)
STATIC = 
DBNAME = POSTGRESQL

# USED BY NEITHER linux_x86_64 nor linux_ia32
PGIPATH	= /usr/include/pgsql	


COMPILER = icc

# Check for debug vs. release build - release is default.
#   To do a debug build, either set the environment variable JSOC_DEBUG to 1, OR
#   modify the following line so that DEBUG = 1.  The environment variable takes precedence.
DEBUG = 0

ifdef JSOC_DEBUG
ifeq ($(JSOC_DEBUG), 1)
DEBUG = 1
else
DEBUG = 0
endif
endif

# No warnings are displayed, by default, for a release build.
WARN = 0

# Builder can request warnings via environment variable (setenv JSOC_WARN 1).  The
#   environment variable takes precedence.
ifdef JSOC_WARN
ifeq ($(JSOC_WARN), 1)
WARN = 1
else
WARN = 0
endif
endif

# Warnings are always displayed for a debug build.
ifeq ($(DEBUG), 1)
WARN = 1
endif

_JSOCROOT_ = ..

F77 = ifort
# if fortran compiler
D_GCC_FORT = 
ifeq ($(F77), ifort)
  D_GCC_FORT = -DINTEL_FORTRAN
endif

ifeq ($(JSOC_MACHINE), linux_x86_64)
  F77 = ifort -mcmodel=medium
endif

# Path to 3rd-party library headers
FMATHLIBSH = -I$(_JSOCROOT_)/lib_third_party/include
CFITSIOH = -I$(_JSOCROOT_)/lib_third_party/include

ifeq ($(COMPILER), icc)
  ifeq ($(JSOC_MACHINE), linux_x86_64)
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-x86_64/ -lfftw3f -lcfitsio
    # Path to 64-bit 3rd-party libraries
    FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/
    CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/

    # All 3rd-party math libraries - local rules can define a subset
    FMATHLIBS = $(FMATHLIBSL) -lfftw3f -lcfitsio
  else
#    FMATHLIBS = -lmkl_lapack -lmkl -L$(_JSOCROOT_)/lib_third_party/lib/linux-ia32/ -lfftw3f -lcfitsio
    # Path to 32-bit 3rd-party libraries
    FMATHLIBSL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/
    CFITSIOL = -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/

    # All 3rd-party math libraries - local rules can define a subset
    FMATHLIBS = $(FMATHLIBSL) -lfftw3f -lcfitsio
  endif
endif

### Build flags for all targets
#
LL_ALL		= $(SYSLIBS)

GCC_LF_ALL	= $(STATIC) -g 
ICC_LF_ALL	= $(STATIC) -xW

GCC_CF_GCCCOMP  = -DGCCCOMP $(D_GCC_FORT)
ICC_CF_ICCCOMP  = -DICCCOMP $(D_GCC_FORT)


# Disable several warnings/remarks when compiling with icc - icc's Wall is a bit picky, it 
# complains about extern declarations in .c files.
#   1418 (remark) - external function definition with no prior declaration
#   1419 (warning) - external declaration in primary source file
#   310 (remark) - old style function declaration (pre-ANSI)
#   981 (remark) - operands are evaluted in unspecified order

ifeq ($(WARN), 1)
# Show warnings (always true for a debug build).
ICC_WARN = -Winline -Wall -wd1418 -wd1419 -wd310 -wd279 -wd981 -Wno-comment
GCC_WARN = -Winline -Wall -Wno-comment
F77_WARN =
else
# Don't show warnings.
ICC_WARN = -w0 -vec-report0 -Wno-comment
GCC_WARN = -Wno-comment
F77_WARN = -vec-report0
endif

# can't figure out how to get stupid make to do if/else if/else
ifeq ($(DEBUG), 0)

  ICC_CF_ALL	= -I$(SRCDIR)/base/include -std=c99 -xW $(ICC_WARN) $(ICC_CF_ICCCOMP)

  ifeq ($(JSOC_MACHINE), linux_x86_64)
    GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 -march=opteron $(GCC_WARN) $(GCC_CF_GCCCOMP)
  endif

  ifeq ($(JSOC_MACHINE), linux_ia64)
    GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 -march=itanium2 $(GCC_WARN) $(GCC_CF_GCCCOMP)
    ICC_CF_ALL	= -I$(SRCDIR)/base/include -std=c99 $(ICC_WARN) $(ICC_CF_ICCCOMP)
  endif

  ifeq ($(JSOC_MACHINE), linux_ia32)
    GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -O2 -march=i686 $(GCC_WARN) $(GCC_CF_GCCCOMP)
  endif	

else

  GCC_CF_ALL	= -I$(SRCDIR)/base/include -std=gnu99 -g $(GCC_WARN) $(GCC_CF_GCCCOMP)
  ICC_CF_ALL	= -I$(SRCDIR)/base/include -std=c99 -g $(ICC_WARN) $(ICC_CF_ICCCOMP)

endif

# Fortran global LINK flags
F_LF_ALL	= -nofor_main -no-ipo

# Fortran global COMPILE flags
ifeq ($(DEBUG), 0)
FF_ALL		= -xW -ftrapuv $(F77_WARN)
else
FF_ALL		= -ftrapuv $(F77_WARN)
endif

### Build tools
# 
# The C compiler named here must output full (header) dependencies in $(@).d.
# It may be necessary to create a script similar to ccd-gcc for your compiler.
# 
GCC_CMPLR	= $(SRCDIR)/build/ccd-gcc
ICC_CMPLR	= $(SRCDIR)/build/ccd-icc
ARCHIVE		= ar crus $@ $^

ECPG		= ecpg -o $@ -c $<

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

FCOMP		= $(F77) $(FF_ALL) $(FF_TGT) -o $@ -c $<
FLINK		= $(F77) $(F_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)

SLBIN           = ln -sf ../../_$(JSOC_MACHINE)/$@ ../bin/$(JSOC_MACHINE)/
SLLIB		= ln -sf ../../_$(JSOC_MACHINE)/$@ ../lib/$(JSOC_MACHINE)/

ALL_LIBS_FPIC = $(LIBDRMSCLIENT_FPIC) $(LIBDBCLIENT_FPIC) $(LIBCMDPARAMS_FPIC) $(LIBTHREADUTIL_FPIC) $(LIBRICECOMP_FPIC) $(LIBDSTRUCT_FPIC) $(LIBMISC_FPIC) $(LIBTIMEIO_FPIC) $(LIBFITSRW_FPIC)

### Standard parts
#
include	$(SRCDIR)/Rules.mk

# Libraries from src/util linked with all programs.
SYSLIBS = -lz -ldl -lpthread -lm
SRCLIBS = $(LIBTHREADUTIL) $(LIBRICECOMP) $(LIBCMDPARAMS) $(LIBMISC) $(LIBDSTRUCT) $(LIBTIMEIO) $(LIBFITSRW)
FSRCLIBS = $(LIBTHREADUTIL) $(LIBRICECOMP) $(LIBCMDPARAMSF) $(LIBMISC) $(LIBDSTRUCT) $(LIBTIMEIO) $(LIBFITSRW)

########## Libraries to link for server executables,    ##############
########## standalone executables and pipeline modules. ##############

# SERVERLIBS: Libraries linked with "server" programs that 
# need direct access to the DRMS databases.
SERVERLIBS = $(LIBDRMS) $(LIBDB) $(LIBSUMSAPI) $(SRCLIBS)

# EXELIBS: Libraries linked with standalone executables.
EXELIBS = $(LIBDRMSCLIENT) $(LIBDBCLIENT) $(SRCLIBS)

# MODLIBS: Libraries linked with DRMS modules.
MODLIBS = $(LIBJSOC_MAIN) $(SERVERLIBS)

# MODLIBS_SOCK: Libraries linked with DRMS modules with socket connection to a drms_server
MODLIBS_SOCK = $(LIBJSOC_MAIN_SOCK) $(LIBDRMSCLIENT) $(LIBDBCLIENT) $(SRCLIBS)

# FMODLIBS: Libraries linked with DRMS Fortran modules
FMODLIBS_SOCK = $(LIBJSOC_MAIN_SOCK_F) $(LIBINTHANDLESF) $(LIBDRMSCLIENT) $(LIBDBCLIENT) $(FSRCLIBS)

MATHLIBS =  $(LIBLSQR) $(LIBBLAS) $(LIBLAPACK)

# Make rules that apply to all projects outside of the base DRMS/SUMS system
-include $(SRCDIR)/proj/make_basic.mk

# Make rules that apply to all projects, inside and outside of the base DRMS/SUMS system
$(CEXE):	%:	%.o $(EXELIBS)
		$(LINK)
		$(SLBIN)

$(FEXE):	%:	%.o $(MATHLIBS)
		$(FLINK)
		$(SLBIN)

$(SERVEREXE):   LL_TGT := $(LL_TGT) -lpq
$(SERVEREXE):	%:	%.o $(SERVERLIBS)
			$(LINK)
			$(SLBIN)

$(MODEXE):      LL_TGT := $(LL_TGT) -lpq
$(MODEXE):	%:	%.o $(MODLIBS)
			$(LINK)
			$(SLBIN)

$(MODEXE_SOCK): %_sock: %.o $(MODLIBS_SOCK)
			$(LINK)
			$(SLBIN)

$(FMODEXE):     %_sock:	%.o $(FMODLIBS_SOCK)
			$(FLINK)
			$(SLBIN)
