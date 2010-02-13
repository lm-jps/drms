# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
CF_$(d)		:= -D$(DBNAME)

SERVEREXE_$(d)	:= $(addprefix $(d)/, drms_server masterlists)
SERVEREXE	:= $(SERVEREXE) $(SERVEREXE_$(d))

CEXE_$(d)	:= $(addprefix $(d)/, drms_run)
CEXE		:= $(CEXE) $(CEXE_$(d))

MODEXE_$(d)	:= $(addprefix $(d)/, drms_query drms_log createtabstructure createns accessreplogs)
MODEXE		:= $(MODEXE) $(MODEXE_$(d))
MODEXE_SOCK_$(d)	:= $(addprefix $(d)/, drms_log_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

MODEXE_SUMS_$(d)	:=  $(addprefix $(d)/, remotesums_ingest)
MODEXE		:= $(MODEXE) $(MODEXE_SUMS_$(d))

EXE_$(d)	:= $(SERVEREXE_$(d)) $(MODEXE_$(d)) $(CEXE_$(d))
OBJ_$(d)	:= $(EXE_$(d):%=%.o) $(MODEXE_SUMS_$(d):%=%.o) 
DEP_$(d)	:= $(EXE_$(d):%=%.o.d) $(MODEXE_SUMS_$(d):%=%.o.d) 
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
 		   $(MODEXE_SUMS_$(d)) \
		   $(MODEXE_SOCK_$(d)) \
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d)) $(MODEXE_SOCK_$(d)) $(CEXE_$(d))
SUMS_BIN	:= $(SUMS_BIN) $(MODEXE_SUMS_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)) $(MODEXE_SOCK_$(d)) $(CEXE_$(d)) $(MODEXE_SUMS_$(d)))

# Local rules
$(OBJ_$(d)):	CF_TGT := $(CF_$(d))
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
