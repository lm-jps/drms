$(PROJOBJDIR):
	+@[ -d $@ ] || mkdir -p $@
	+@[ -d $@/libs/astro ] || mkdir -p $@/libs/astro
	+@[ -d $@/libs/dr ] || mkdir -p $@/libs/dr
	+@[ -d $@/libs/dsputil ] || mkdir -p $@/libs/dsputil
	+@[ -d $@/libs/gapfiller ] || mkdir -p $@/libs/gapfiller
	+@[ -d $@/libs/interpolate ] || mkdir -p $@/libs/interpolate
	+@[ -d $@/libs/json ] || mkdir -p $@/libs/json
	+@[ -d $@/libs/stats ] || mkdir -p $@/libs/stats
	+@[ -d $@/datacapture/apps ] || mkdir -p $@/datacapture/apps
	+@[ -d $@/dsdsmigr/apps ] || mkdir -p $@/dsdsmigr/apps
	+@[ -d $@/dsdsmigr/libs ] || mkdir -p $@/dsdsmigr/libs
	+@[ -d $@/maps_avgs/apps ] || mkdir -p $@/maps_avgs/apps
	+@[ -d $@/util/apps ] || mkdir -p $@/util/apps
	+@[ -d $@/lev0/apps ] || mkdir -p $@/lev0/apps
	+@[ -d $@/lev1/apps ] || mkdir -p $@/lev1/apps
	+@[ -d $@/jpe/apps ] || mkdir -p $@/jpe/apps
	+@[ -d $@/lev1_aia/apps ] || mkdir -p $@/lev1_aia/apps
	+@[ -d $@/lev1_hmi/apps ] || mkdir -p $@/lev1_hmi/apps
	+@[ -d $@/export/apps ] || mkdir -p $@/export/apps
	+@[ -d $@/export/libs/util ] || mkdir -p $@/export/libs/util
	+@[ -d $@/myproj/libs/interp ] || mkdir -p $@/myproj/libs/interp
	+@[ -d $@/globalhs/apps/ ] || mkdir -p $@/globalhs/apps
	+@[ -d $@/lev1.5_hmi/libs/lev15 ] || mkdir -p $@/lev1.5_hmi/libs/lev15
	+@[ -d $@/lev1.5_hmi/apps ] || mkdir -p $@/lev1.5_hmi/apps
	+@[ -d $@/flatfield/apps ] || mkdir -p $@/flatfield/apps
	+@[ -d $@/flatfield/libs/flatfieldlib ] || mkdir -p $@/flatfield/libs/flatfieldlib
	+@[ -d $@/globalhs/apps/src/ ] || mkdir -p $@/globalhs/apps/src
	+@[ -d $@/rings/apps/ ] || mkdir -p $@/rings/apps
	+@[ -d $@/mag/apps/ ] || mkdir -p $@/mag/apps
	+@[ -d $@/mag/pfss/apps ] || mkdir -p $@/mag/pfss/apps
	+@[ -d $@/mag/ambig/apps ] || mkdir -p $@/mag/ambig/apps
	+@[ -d $@/mag/ident/apps ] || mkdir -p $@/mag/ident/apps
	+@[ -d $@/mag/ident/libs/mex2c ] || mkdir -p $@/mag/ident/libs/mex2c
	+@[ -d $@/mag/ident/libs/mexfunctions ] || mkdir -p $@/mag/ident/libs/mexfunctions
	+@[ -d $@/mag/ident/libs/util ] || mkdir -p $@/mag/ident/libs/util
	+@[ -d $@/mag/patch/apps ] || mkdir -p $@/mag/patch/apps
	+@[ -d $@/limbfit/apps ] || mkdir -p $@/limbfit/apps
	+@[ -d $@/vfisv/apps ] || mkdir -p $@/vfisv/apps
	+@[ -d $@/workflow/apps ] || mkdir -p $@/workflow/apps
