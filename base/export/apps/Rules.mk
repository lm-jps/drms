# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

#dir	:= $(d)/test
#-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
# NOTE: Add the base of the module's filename below (next to mymod)
MODEXE_$(d)	:= $(addprefix $(d)/, jsoc_export_as_fits jsoc_export_as_is jsoc_export_SU_as_is jsoc_fetch jsoc_stats1 jsoc_export_clone drms_export_cgi segment-file-name)

MODEXE_ONLY_$(d)	:= $(addprefix $(d)/, drms-export-to-stdout jsoc_info jsoc_export_manage)

MODEXE		:= $(MODEXE) $(MODEXE_$(d)) $(MODEXE_ONLY_$(d))
CEXE_$(d)       := $(addprefix $(d)/, GetJsocRequestID jsoc_WebRequestID jsoc_export_make_index jsoc_manage_cgibin_handles)
CEXE            := $(CEXE) $(CEXE_$(d))


MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

EXE_$(d)        := $(MODEXE_$(d)) $(MODEXE_ONLY_$(d)) $(CEXE_$(d))
OBJ_$(d)	:= $(EXE_$(d):%=%.o) 
DEP_$(d)	:= $(OBJ_$(d):%=%.d)
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
		   $(MODEXE_SOCK_$(d))\
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d)) $(MODEXE_SOCK_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)) $(MODEXE_SOCK_$(d)))

# Local rules
ifeq ($(CFITSIOH),$(LIBTARH))
	TPARTYH	:= $(CFITSIOH)
else
	TPARTYH := $(CFITSIOH) $(LIBTARH)
endif

$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) $(TPARTYH) -I$(SRCDIR)/$(d)/../../libs/json -I$(SRCDIR)/$(d)/../../libs/jsmn -I$(SRCDIR)/$(d)/../libs/util
# DBMS is the POSTGRESQL macro needed so that jsoc_fetch can see db_int8_t
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -DEXPARCH="\""$(JSOC_MACHINE)"\"" -D$(DBMS)

$(CEXE_$(d)):		$(LIBJSON) $(LIBJSMN) $(LIBEXPDRMS) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_$(d)):		$(LIBJSON) $(LIBJSMN) $(LIBEXPDRMS) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_ONLY_$(d)):	$(LIBJSON) $(LIBJSMN) $(LIBEXPDRMS) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_SOCK_$(d)):	$(LIBJSON) $(LIBJSMN) $(LIBEXPDRMS) $(LIBEXPUTL) $(LIBQDECODER)

$(MODEXE_$(d)):		LL_TGT := $(LL_TGT) $(LIBTARL)
$(MODEXE_SOCK_$(d)):	LL_TGT := $(LL_TGT) $(LIBTARL)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
