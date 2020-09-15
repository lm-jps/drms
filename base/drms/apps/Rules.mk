# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
CF_$(d)		:= -D$(DBMS)

SERVEREXE_$(d)	:= $(addprefix $(d)/, drms_server masterlists vso_sum_alloc vso_sum_put vso_sum_getdo vso_sum_alloc_put)
SERVEREXE	:= $(SERVEREXE) $(SERVEREXE_$(d))

CEXE_$(d)	:= $(addprefix $(d)/, drms_run)
#CEXE_SUMS_$(d)	:= $(addprefix $(d)/, vso_sum_alloc vso_sum_put)
#CEXESUMS	:= $(CEXESUMS) $(CEXE_SUMS_$(d))
#CEXE		:= $(CEXE) $(CEXE_$(d)) $(CEXE_SUMS_$(d))
CEXE		:= $(CEXE) $(CEXE_$(d))

MODEXE_JSON_$(d)	:= $(addprefix $(d)/, rawingest)
JSON_OBJ_$(d)		:= $(MODEXE_JSON_$(d):%=%.o)

MODEXE_$(d)	:= $(addprefix $(d)/, drms_query drms_log createtabstructure createns accessreplogs drms_addkeys drms_dropkeys createshadow dropshadow drms_parsekeys drms_parserecset) $(MODEXE_JSON_$(d))
MODEXE		:= $(MODEXE) $(MODEXE_$(d))
MODEXE_SOCK_$(d)	:= $(addprefix $(d)/, drms_log_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

EXE_$(d)	:= $(SERVEREXE_$(d)) $(MODEXE_$(d)) $(CEXE_$(d)) $(CEXE_SUMS_$(d))
OBJ_$(d)	:= $(EXE_$(d):%=%.o)
DEP_$(d)	:= $(EXE_$(d):%=%.o.d)
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
		   $(MODEXE_SOCK_$(d)) \
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d)) $(MODEXE_SOCK_$(d))
SUMS_BIN	:= $(SUMS_BIN) $(CEXE_SUMS_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)) $(MODEXE_SOCK_$(d)))

# Local rules
$(SERVEREXE_$(d)):      LL_TGT := $(LL_TGT) -lrt

$(OBJ_$(d)):	CF_TGT := $(CF_$(d)) -I$(SRCDIR)/$(d)/../../libs/json -I$(SRCDIR)/$(d)/../../libs/qdecoder
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(JSON_OBJ_$(d)): CF_TGT := $(CF_TGT) -I$(SRCDIR)/$(d)/../../libs/jsmn $(CFITSIOH)
$(MODEXE_JSON_$(d)):	$(LIBJSMN)

$(MODEXE_$(d)): $(LIBJSON) $(LIBQDECODER)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

dir     := $(d)/test
-include                $(SRCDIR)/$(dir)/Rules.mk

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
