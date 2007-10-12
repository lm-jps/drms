# This is used to jump from a source directory to a target directory.

.SUFFIXES:


# Find the target directory(ies).
#
ifndef JSOC_MACHINE
  JSOC_MACHINE := $(shell build/jsoc_machine.csh)
  export JSOC_MACHINE
endif

OBJDIR 		:= _$(JSOC_MACHINE)
PROJOBJDIR	:= $(OBJDIR)/src/proj

all:    $(PROJOBJDIR) $(OBJDIR)

# Define the rules to build in the target subdirectories.
#
MAKETARGET = $(MAKE) --no-print-directory -C $@ -f $(CURDIR)/Makefile \
		SRCDIR=$(CURDIR) $(MAKECMDGOALS)

.PHONY: $(PROJOBJDIR) $(OBJDIR) 

# Create the project-specific directories too, if they exist.
-include $(CURDIR)/src/proj/target.mk

$(OBJDIR):
	+@[ -d bin/$(JSOC_MACHINE) ] || mkdir -p bin/$(JSOC_MACHINE)
	+@[ -d lib/$(JSOC_MACHINE) ] || mkdir -p lib/$(JSOC_MACHINE)
	+@[ -d $@ ] || mkdir -p $@
	+@[ -d $@/src/base/drms/apps ] || mkdir -p $@/src/base/drms/apps
	+@[ -d $@/src/base/drms/libs/api/client ] || mkdir -p $@/src/base/drms/libs/api/client
	+@[ -d $@/src/base/drms/libs/api/client_fpic ] || mkdir -p $@/src/base/drms/libs/api/client_fpic
	+@[ -d $@/src/base/drms/libs/api/server ] || mkdir -p $@/src/base/drms/libs/api/server
	+@[ -d $@/src/base/drms/libs/main/c ] || mkdir -p $@/src/base/drms/libs/main/c
	+@[ -d $@/src/base/drms/libs/main/f ] || mkdir -p $@/src/base/drms/libs/main/f
	+@[ -d $@/src/base/drms/libs/main/idl ] || mkdir -p $@/src/base/drms/libs/main/idl
	+@[ -d $@/src/base/drms/libs/meta ] || mkdir -p $@/src/base/drms/libs/meta
	+@[ -d $@/src/base/libs/cmdparams/fpic ] || mkdir -p $@/src/base/libs/cmdparams/fpic
	+@[ -d $@/src/base/libs/db/client ] || mkdir -p $@/src/base/libs/db/client
	+@[ -d $@/src/base/libs/db/client_fpic ] || mkdir -p $@/src/base/libs/db/client_fpic
	+@[ -d $@/src/base/libs/db/server ] || mkdir -p $@/src/base/libs/db/server
	+@[ -d $@/src/base/libs/dstruct/fpic ] || mkdir -p $@/src/base/libs/dstruct/fpic
	+@[ -d $@/src/base/libs/ricecomp/fpic ] || mkdir -p $@/src/base/libs/ricecomp/fpic
	+@[ -d $@/src/base/libs/inthandles ] || mkdir -p $@/src/base/libs/inthandles
	+@[ -d $@/src/base/libs/threads/fpic ] || mkdir -p $@/src/base/libs/threads/fpic
	+@[ -d $@/src/base/libs/timeio/fpic ] || mkdir -p $@/src/base/libs/timeio/fpic
	+@[ -d $@/src/base/libs/misc/fpic ] || mkdir -p $@/src/base/libs/misc/fpic
	+@[ -d $@/src/base/local/libs/dsds ] || mkdir -p $@/src/base/local/libs/dsds
	+@[ -d $@/src/base/local/libs/soi ] || mkdir -p $@/src/base/local/libs/soi
	+@[ -d $@/src/base/sums/apps ] || mkdir -p $@/src/base/sums/apps
	+@[ -d $@/src/base/sums/libs/api ] || mkdir -p $@/src/base/sums/libs/api
	+@[ -d $@/src/base/sums/libs/pg ] || mkdir -p $@/src/base/sums/libs/pg
	+@[ -d $@/src/base/util/apps ] || mkdir -p $@/src/base/util/apps
	+@[ -d $@/src/proj/example/apps ] || mkdir -p $@/src/proj/example/apps
	+@[ -d $@/src/proj/myproj/apps ] || mkdir -p $@/src/proj/myproj/apps
	+@$(MAKETARGET)

# These rules keep make from trying to use the match-anything rule below to
# rebuild the makefiles--ouch!  Obviously, if you don't follow my convention
# of using a `.mk' suffix on all non-standard makefiles you'll need to change
# the pattern rule.
#
Makefile : ;
%.mk :: ;


# Anything we don't know how to build will use this rule.  The command is a
# do-nothing command, but the prerequisites ensure that the appropriate
# recursive invocations of make will occur.
#
% :: $(PROJOBJDIR) $(OBJDIR) ; :


# The clean rule is best handled from the source directory: since we're
# rigorous about keeping the target directories containing only target files
# and the source directory containing only source files, `clean' is as trivial
# as removing the target directories!
#
.PHONY: clean
clean:
	rm -rf $(OBJDIR); rm -rf bin/$(JSOC_MACHINE); rm -rf lib/$(JSOC_MACHINE)

