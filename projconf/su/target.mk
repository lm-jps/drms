$(PROJOBJDIR):
	+@[ -d $@ ] || mkdir -p $@
	+@[ -d $@/libs/astro ] || mkdir -p $@/libs/astro
	+@[ -d $@/libs/dr ] || mkdir -p $@/libs/dr
	+@[ -d $@/libs/dsputil ] || mkdir -p $@/libs/dsputil
	+@[ -d $@/libs/gapfiller ] || mkdir -p $@/libs/gapfiller
	+@[ -d $@/libs/json ] || mkdir -p $@/libs/json
	+@[ -d $@/datacapture/apps ] || mkdir -p $@/datacapture/apps
	+@[ -d $@/dsdsmigr/apps ] || mkdir -p $@/dsdsmigr/apps
	+@[ -d $@/util/apps ] || mkdir -p $@/util/apps
	+@[ -d $@/lev0/apps ] || mkdir -p $@/lev0/apps
	+@[ -d $@/lev1_aia/apps ] || mkdir -p $@/lev1_aia/apps
	+@[ -d $@/lev1_hmi/apps ] || mkdir -p $@/lev1_hmi/apps
	+@[ -d $@/export/apps ] || mkdir -p $@/export/apps
	+@[ -d $@/export/webapps ] || mkdir -p $@/export/webapps
