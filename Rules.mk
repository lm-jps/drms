# Note: this design uses target-specific flags, because it's the only
# way to get the value of a variable that's different for each
# subdirectory, $(d), into the build recipes. Once you go that way,
# you can as well use the feature to specify (extra) objects and 
# libraries to be linked or archived.


# Standard stuff

.SUFFIXES:
.SUFFIXES:	.c .o 

all:		targets

# Subdirectories, in random order

dir	:= base
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= proj
-include		$(SRCDIR)/$(LOCALIZATIONDIR)/Rules.mk

# Non-default targets
idl:		$(LIBJSOC_MAIN_SOCK_I)
dsds:		$(LIBSOIJSOC) $(LIBDSDS)
examples:	$(EXAMPLES)
sums:		$(SUMS_BIN)
universe:	targets idl dsds examples sums $(LIBDSPUTIL)

# General directory-independent flags (MUST BE DEFINED BEFORE THE SECTION
# "General directory-independent rules"
$(FPICOBJ):	CF_TGT := $(CF_TGT) -fPIC

$(FIOBJ):	CF_TGT := $(CF_TGT) -DFLIB

$(IIOBJ):	CF_TGT := $(CF_TGT) -DIDLLIB

# General directory-independent rules
%.c:		%.pgc
		$(ECPG)
%.c:		%.i
		$(SWIG)

%.o:		%.f
		$(FCOMP)

%.o:		%.f90
		$(FCOMP)

# If a .f file is to be compiled more than one, the Rules.mk file that
# contains the rules for that .f file exists in a subdirectory of the 
# directory that contains the .f file.  As a result, the stem of the 
# of the corresponding .o file is the child of the stem of the .f file.
%.o:		../%.f
		$(FCOMP)

%.o:		../%.f90
		$(FCOMP)

%.o:		%.c
		$(COMP)

# If a .c file is to be compiled more than one, the Rules.mk file that
# contains the rules for that .c file exists in a subdirectory of the 
# directory that contains the .c file.  As a result, the stem of the 
# of the corresponding .o file is the child of the stem of the .c file.
%.o:		../%.c
		$(COMP)

%:		%.o
		$(LINK)

%:		%.c
		$(COMPLINK)


# These two targets collect real targets, i.e. ones that can be built.

.PHONY:		targets
targets:	$(TGT_BIN) $(TGT_LIB)

# These targets merely contain commands to be executed, i.e. they collect
# only .PHONY targets, even if they're not explicitly marked as such. 
# The install target does not collect dependencies (other than for forcing
# things to be built) because it's always considered 'out of date' anway as
# it's a .PHONY target. Instead, it collects installation commands that will be
# ran in addition to the standard ones to install the known targets.

.PHONY:		clean
clean:
		rm -f $(CLEAN)

# Prevent make from removing any build targets, including intermediate ones

.SECONDARY:	$(CLEAN)

