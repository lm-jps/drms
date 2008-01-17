# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
CF_$(d)		:= -D$(DBNAME)

MODEXE_$(d)	:= $(addprefix $(d)/, create_series describe_series delete_series modify_series plot_keys retrieve_dir retrieve_file set_keys show_keys show_series store_dir store_file)
MODEXE		:= $(MODEXE) $(MODEXE_$(d))

MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

EXE_$(d)	:= $(MODEXE_$(d)) 
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

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
