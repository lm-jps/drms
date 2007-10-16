# This is used to jump from a source directory to a target directory.

.SUFFIXES:


# Find the target directory(ies).
#
ifndef JSOC_MACHINE
  JSOC_MACHINE := $(shell build/jsoc_machine.csh)
  export JSOC_MACHINE
endif

OBJDIR 		:= _$(JSOC_MACHINE)
PROJOBJDIR	:= $(OBJDIR)/proj

all:    $(PROJOBJDIR) $(OBJDIR)

# Define the rules to build in the target subdirectories.
#
MAKETARGET = $(MAKE) --no-print-directory -C $@ -f $(CURDIR)/Makefile \
		SRCDIR=$(CURDIR) $(MAKECMDGOALS)

.PHONY: $(PROJOBJDIR) $(OBJDIR) 

# Create the project-specific directories too, if they exist.
-include $(CURDIR)/proj/target.mk

$(OBJDIR):
	+@[ -d bin/$(JSOC_MACHINE) ] || mkdir -p bin/$(JSOC_MACHINE)
	+@[ -d lib/$(JSOC_MACHINE) ] || mkdir -p lib/$(JSOC_MACHINE)
	+@[ -d $@ ] || mkdir -p $@
	+@[ -d $@/base/drms/apps ] || mkdir -p $@/base/drms/apps
	+@[ -d $@/base/drms/libs/api/client ] || mkdir -p $@/base/drms/libs/api/client
	+@[ -d $@/base/drms/libs/api/client_fpic ] || mkdir -p $@/base/drms/libs/api/client_fpic
	+@[ -d $@/base/drms/libs/api/server ] || mkdir -p $@/base/drms/libs/api/server
	+@[ -d $@/base/drms/libs/main/c ] || mkdir -p $@/base/drms/libs/main/c
	+@[ -d $@/base/drms/libs/main/f ] || mkdir -p $@/base/drms/libs/main/f
	+@[ -d $@/base/drms/libs/main/idl ] || mkdir -p $@/base/drms/libs/main/idl
	+@[ -d $@/base/drms/libs/meta ] || mkdir -p $@/base/drms/libs/meta
	+@[ -d $@/base/libs/cmdparams/fpic ] || mkdir -p $@/base/libs/cmdparams/fpic
	+@[ -d $@/base/libs/db/client ] || mkdir -p $@/base/libs/db/client
	+@[ -d $@/base/libs/db/client_fpic ] || mkdir -p $@/base/libs/db/client_fpic
	+@[ -d $@/base/libs/db/server ] || mkdir -p $@/base/libs/db/server
	+@[ -d $@/base/libs/dstruct/fpic ] || mkdir -p $@/base/libs/dstruct/fpic
	+@[ -d $@/base/libs/ricecomp/fpic ] || mkdir -p $@/base/libs/ricecomp/fpic
	+@[ -d $@/base/libs/inthandles ] || mkdir -p $@/base/libs/inthandles
	+@[ -d $@/base/libs/threads/fpic ] || mkdir -p $@/base/libs/threads/fpic
	+@[ -d $@/base/libs/timeio/fpic ] || mkdir -p $@/base/libs/timeio/fpic
	+@[ -d $@/base/libs/misc/fpic ] || mkdir -p $@/base/libs/misc/fpic
	+@[ -d $@/base/local/libs/dsds ] || mkdir -p $@/base/local/libs/dsds
	+@[ -d $@/base/local/libs/soi ] || mkdir -p $@/base/local/libs/soi
	+@[ -d $@/base/sums/apps ] || mkdir -p $@/base/sums/apps
	+@[ -d $@/base/sums/libs/api ] || mkdir -p $@/base/sums/libs/api
	+@[ -d $@/base/sums/libs/pg ] || mkdir -p $@/base/sums/libs/pg
	+@[ -d $@/base/util/apps ] || mkdir -p $@/base/util/apps
	+@[ -d $@/proj/example/apps ] || mkdir -p $@/proj/example/apps
	+@[ -d $@/proj/myproj/apps ] || mkdir -p $@/proj/myproj/apps
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

