# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
CF_$(d)		:= -D$(DBNAME)

SERVEREXE_$(d)	:= $(addprefix $(d)/, drms_server masterlists)
SERVEREXE	:= $(SERVEREXE) $(SERVEREXE_$(d))

MODEXE_$(d)	:= $(addprefix $(d)/, drms_query drms_log)
MODEXE		:= $(MODEXE) $(MODEXE_$(d))

MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

EXE_$(d)	:= $(SERVEREXE_$(d)) $(MODEXE_$(d)) 
OBJ_$(d)	:= $(EXE_$(d):%=%.o) 
DEP_$(d)	:= $(EXE_$(d):%=%.o.d)
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
		   $(MODEXE_SOCK_$(d))\
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d)) $(MODEXE_SOCK_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)) $(MODEXE_SOCK_$(d)))

# Local rules
$(OBJ_$(d)):	CF_TGT := $(CF_$(d))
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(MODEXE_$(d):%=%.o) : CF_TGT := $(CF_$(d)) -DDRMS_CLIENT

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
