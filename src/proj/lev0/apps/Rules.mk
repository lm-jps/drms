# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
SUMEXE_$(d)	:= $(addprefix $(d)/, ingest_lev0)
CEXE_$(d)       := $(addprefix $(d)/, fix_hmi_config_file_date)
CEXE		:= $(CEXE) $(CEXE_$(d))
MODEXESUMS	:= $(MODEXESUMS) $(SUMEXE_$(d))

MODEXE_$(d)	:= $(addprefix $(d)/, convert_fds extract_fds_statev)
MODEXEDR_$(d)	:= $(addprefix $(d)/, hmi_import_egse_lev0 aia_import_egse_lev0)
MODEXEDR	:= $(MODEXEDR) $(MODEXEDR_$(d))
MODEXE		:= $(MODEXE) $(SUMEXE_$(d)) $(MODEXE_$(d)) $(MODEXEDR_$(d))

MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock) $(MODEXEDR_$(d):%=%_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))
MODEXEDR_SOCK	:= $(MODEXEDR_SOCK) $(MODEXEDR_$(d):%=%_sock)

MODEXEDROBJ	:= $(MODEXEDROBJ) $(MODEXEDR_$(d):%=%.o)

ALLEXE_$(d)	:= $(MODEXE_$(d)) $(MODEXEDR_$(d)) $(SUMEXE_$(d)) $(CEXE_$(d))
OBJ_$(d)	:= $(ALLEXE_$(d):%=%.o) 
DEP_$(d)	:= $(OBJ_$(d):%=%.o.d)
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(ALLEXE_$(d)) \
		   $(MODEXE_SOCK_$(d))\
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(ALLEXE_$(d)) $(MODEXE_SOCK_$(d))

S_$(d)		:= $(notdir $(ALLEXE_$(d)) $(MODEXE_SOCK_$(d)))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(SUMEXE_$(d)):		LL_TGT := -L/home/production/cvs/jsoc/lib/saved/$(JSOC_MACHINE) -lhmicomp_egse -lecpg -lpq
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -I$(SRCDIR)/$(d)/../../libs/astro
$(MODEXE_$(d)) $(MODEXE_SOCK_$(d)):	$(LIBASTRO)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
