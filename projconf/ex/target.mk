# create paths, relative to JSOC/proj, to contain binary output (.o, .o.d, exes, libs, etc.)

.PHONY: proj1 proj2

$(PROJOBJDIR): proj1 proj2

proj1:
	+@[ -d $(PROJOBJDIR) ] || mkdir -p $(PROJOBJDIR)
#	+@[ -d $@/libs/exelib ] || mkdir -p $@/libs/exelib

# The configure script creates projtgts.mk if config.local contains projects
# that the NetDRMS would like to get. The file contains dependencies for
# proj2

-include $(LOCALIZATIONDIR)/projtgts.mk
