# create paths, relative to JSOC/src/proj, to contain binary output (.o, .o.d, exes, libs, etc.)

$(PROJOBJDIR):
	+@[ -d $@ ] || mkdir -p $@
#	+@[ -d $@/libs/exelib ] || mkdir -p $@/libs/exelib
