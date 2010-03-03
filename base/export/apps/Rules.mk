# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

#dir	:= $(d)/test
#-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
# NOTE: Add the base of the module's filename below (next to mymod)
MODEXE_$(d)	:= $(addprefix $(d)/, jsoc_export_as_fits jsoc_export_as_is jsoc_export_SU_as_is jsoc_fetch jsoc_export_manage)

MODEXE_ONLY_$(d)	:= $(addprefix $(d)/, jsoc_info)

MODEXE		:= $(MODEXE) $(MODEXE_$(d)) $(MODEXE_ONLY_$(d))
CEXE_$(d)       := $(addprefix $(d)/, GetJsocRequestID jsoc_export_make_index)
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
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -I$(SRCDIR)/$(d)/../../libs/json -I$(SRCDIR)/$(d)/../libs/util
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -DCDIR="\"$(SRCDIR)/$(d)\""

$(CEXE_$(d)):		$(LIBJSON) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_$(d)):		$(LIBJSON) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_ONLY_$(d)):	$(LIBJSON) $(LIBEXPUTL) $(LIBQDECODER)
$(MODEXE_SOCK_$(d)):	$(LIBJSON) $(LIBEXPUTL) $(LIBQDECODER)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
