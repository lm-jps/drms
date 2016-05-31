# Define WORKINGDIR so that we don't get that '/auto/home1' crud
WORKINGDIR      = $(PWD)
VPATH     = $(SRCDIR)

###########################################
#                                         #
#           COMPILER SELECTION            #
#                                         #
###########################################
COMPILER = icc
FCOMPILER = ifort

# Can override compiler selection
ifneq ($(DRMS_COMPILER),)
COMPILER = $(DRMS_COMPILER)
endif

ifneq ($(DRMS_FCOMPILER),)
FCOMPILER = $(DRMS_FCOMPILER)
endif

###########################################
#                                         #
#                DEBUGGING                #
#                                         #
###########################################
DEBUG = 0

ifdef DRMS_DEBUG
ifeq ($(DRMS_DEBUG), 1)
DEBUG = 1
else
DEBUG = 0
endif
endif

###########################################
#                                         #
#             DRMS LIBRARIES              #
#                                         #
###########################################
DRMS_INCS = /home/jsoc/cvs/Development/JSOC/base/include

ifeq ($(MACHINE), linux_x86_64)
DRMS_LIBS = -L/home/jsoc/cvs/Development/JSOC/lib/linux_x86_64 -ldrms
endif

ifeq ($(MACHINE), linux_avx)
DRMS_LIBS = -L/home/jsoc/cvs/Development/JSOC/lib/linux_avx -ldrms
endif

###########################################
#                                         #
#            OTHER LIBRARIES              #
#                                         #
###########################################
EXELIBS =

###########################################
#                                         #
#         THIRD-PARTY LIBRARIES           #
#                                         #
###########################################
ifeq ($(MACHINE), linux_x86_64)
CFITSIO_INCS = /home/jsoc/include
CFITSIO_LIBS = /home/jsoc/lib/linux-x86_64
MPI_INCS = /home/jsoc/mpich2/include
MPI_LIBS = /home/jsoc/mpich2/lib
MPI_PATH = /home/jsoc/mpich2/bin
POSTGRES_INCS = /usr/include
POSTGRES_LIBS = /usr/lib64
FFTW_INCS = /home/jsoc/include
FFTW_LIBS = /home/jsoc/lib/linux-x86_64
GSL_INCS = /home/jsoc/include
GSL_LIBS = /home/jsoc/lib/linux-x86_64
endif

ifeq ($(MACHINE), linux_avx)
CFITSIO_INCS = /home/jsoc/avx/include
CFITSIO_LIBS = /home/jsoc/avx/lib
MPI_INCS = /home/jsoc/avx/include
MPI_LIBS = /home/jsoc/avx/lib
MPI_PATH = /home/jsoc/avx/bin
POSTGRES_INCS = /usr/include
POSTGRES_LIBS = /usr/lib64
FFTW_INCS = /home/jsoc/avx/include
FFTW_LIBS = /home/jsoc/avx/lib
GSL_LIBS = /home/jsoc/avx/lib
GSL_INCS = /home/jsoc/avx/include
endif

# PostgreSQL
PGH = -I$(POSTGRES_INCS)
PGL = -L$(POSTGRES_LIBS)
PGLIBS = $(PGL) -lpq

# CFITSIO
CFITSIOH = -I$(CFITSIO_INCS)
CFITSIOL = -L$(CFITSIO_LIBS)
CFITSIOLIBS =  $(CFITSIOL) -lcfitsio

# GSL
GSLH = -I$(GSL_INCS)
GSLL = -L$(GSL_LIBS)
GSLLIBS = $(GSLL) -lgsl

# FFTW
FFTWH = -I$(FFTW_INCS)
FFTWL = -L$(FFTW_LIBS)
FFTW3LIBS = $(FFTWL) -lfftw3
FFTW3FLIBS = $(FFTWL) -lfftw3f

###########################################
#                                         #
#                WARNINGS                 #
#                                         #
###########################################
GCC_WARN = -Wno-comment
ICC_WARNTOERR = -we266
FCOMPILER_WARN = -vec-report0

###########################################
#                                         #
#            GLOBAL LINK FLAGS            #
#                                         #
###########################################
# Libraries from src/util linked with all programs.
ifneq ($(COMPILER), icc)
  SYSLIBS = -lz -ldl -lpthread -lm -lutil
else
  SYSLIBS = -lz -ldl -lpthread -lutil
endif

LL_ALL          = $(SYSLIBS) 
GCC_LF_ALL      = 
ICC_LF_ALL      = -diag-disable 10237 -openmp -static-intel -Wl,-export-dynamic

# Fortran global LINK flags
ifeq ($(FCOMPILER), ifort)
F_LF_ALL        = -diag-disable 10237 -nofor-main -openmp -static-intel -Wl,-export-dynamic
endif

###########################################
#                                         #
#          GLOBAL COMPILE FLAGS           #
#                                         #
###########################################
GCC_CF_GCCCOMP  = -DGCCCOMP
ICC_CF_ICCCOMP  = -DICCCOMP -openmp
GLOBALSW = 

ifeq ($(DEBUG), 0)
  GCC_CF_ALL    = -I$(DRMS_INCS) -std=gnu99 -O2 $(GCC_WARN) $(GCC_CF_GCCCOMP) $(GLOBALSW) -DNDEBUG

  ifeq ($(MACHINE), linux_x86_64)
    ICC_CF_ALL = -I$(DRMS_INCS) -std=c99 -D_GNU_SOURCE $(ICC_WARN) $(ICC_CF_ICCCOMP) $(GLOBALSW) -DNDEBUG
    GCC_CF_ALL  = -I$(DRMS_INCS) -std=gnu99 -O2 -march=opteron $(GCC_WARN) $(GCC_CF_GCCCOMP) $(GLOBALSW)
  endif

  ifeq ($(MACHINE), linux_avx)
    ICC_CF_ALL = -xavx -I$(DRMS_INCS) -std=c99 -D_GNU_SOURCE $(ICC_WARN) $(ICC_CF_ICCCOMP) $(GLOBALSW) -DNDEBUG
  endif
else
# -g tells the icc and gcc compilers to generate full debugging information
  GCC_CF_ALL = -I$(DRMS_INCS) -std=gnu99 -g $(GCC_WARN) $(GCC_CF_GCCCOMP) $(GLOBALSW)
  ICC_CF_ALL = -I$(DRMS_INCS) -std=c99 -D_GNU_SOURCE -g $(ICC_WARN) $(ICC_CF_ICCCOMP) $(GLOBALSW)
endif

# Fortran global COMPILE flags
ifeq ($(MACHINE), linux_avx)
  ifeq ($(FCOMPILER), ifort)
    F_CF_ALL := -xavx -openmp
  endif
endif

ifeq ($(MACHINE), linux_x86_64)
  ifeq ($(FCOMPILER), ifort)
    F_CF_ALL := -openmp
  endif
endif

# Other compiler-specific Fortran COMPILE flags
ifeq ($(FCOMPILER), ifort)
  FCFLAGS_INIT := -ftrapuv
else
  # must be gfortran
  FCFLAGS_INIT  :=
endif

ifeq ($(DEBUG), 0)
# -xW optimizes ifort compilation for Pentium 4
# -ftrapuv initializes stack local variables to an unusual value to aid error detection.
  F_CF_ALL      := $(F_CF_ALL) $(FCOMPILER_WARN)
else
  F_CF_ALL      := $(F_CF_ALL) -g $(FCFLAGS_INIT) $(FCOMPILER_WARN)
endif

ICC_CMPLR	= icc
GCC_CMPLR	= gcc

GCC_COMP        = $(GCC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) -o $@ -c $<
ICC_COMP        = $(ICC_CMPLR) $(ICC_CF_ALL) $(CF_TGT) -o $@ -c $<

GCC_LINK        = $(GCC_CMPLR) $(GCC_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)
ICC_LINK        = $(ICC_CMPLR) $(ICC_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)

GCC_COMPLINK    = $(GCC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) $(GCC_LF_ALL) $(LF_TGT) -o $@ $< $(LL_TGT) $(LL_ALL)
ICC_COMPLINK    = $(ICC_CMPLR) $(GCC_CF_ALL) $(CF_TGT) $(ICC_LF_ALL) $(LF_TGT) -o $@ $< $(LL_TGT) $(LL_ALL)

ifneq ($(COMPILER), icc)
  COMP          = $(GCC_COMP)
  LINK          = $(GCC_LINK)
  COMPLINK      = $(GCC_COMPLINK)
else
  COMP          = $(ICC_COMP)
  LINK          = $(ICC_LINK)
  COMPLINK      = $(ICC_COMPLINK)
endif

FCOMP           = $(FCOMPILER) $(F_CF_ALL) $(FF_TGT) -o $@ -c $<
FLINK           = $(FCOMPILER) $(F_LF_ALL) $(LF_TGT) -o $@ $^ $(LL_TGT) $(LL_ALL)

###########################################
#                                         #
#               RECIPES                   #
#                                         #
###########################################
%.o:	        %.f
		$(FCOMP)

%.o:	    	%.f90
		$(FCOMP)

%.o:	    	%.c
		$(COMP)

$(CEXE):        %:      %.o $(EXELIBS)
		$(LINK)

$(FEXE):        %:      %.o
		$(FLINK)

$(MODEXE):      LL_TGT := $(LL_TGT) $(DRMS_LIBS) $(PGLIBS) $(CFITSIOLIBS)
$(MODEXE):      %:      %.o $(BLAH)
			$(LINK)

# MODEXE_USEF contains all C direct-connect modules that use third-party Fortran libraries.
$(MODEXE_USEF): LL_TGT := $(LL_TGT) $(DRMS_LIBS) $(PGLIBS) $(CFITSIOLIBS)
$(MODEXE_USEF):     %:  %.o $(BLAH)
			$(FLINK)

.PHONY:		clean
clean:
		rm -f $(CLEAN)
