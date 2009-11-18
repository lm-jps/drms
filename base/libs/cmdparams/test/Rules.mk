# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
CEXE_$(d)		:= $(addprefix $(d)/, cptest)
CEXE			:= $(CEXE) $(CEXE_$(d))

OBJ_$(d)		:= $(CEXE_$(d):%=%.o) 

DEP_$(d)		:= $(OBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(OBJ_$(d)) \
			   $(CEXE_$(d)) \
			   $(DEP_$(d))

S_$(d)			:= $(notdir $(CEXE_$(d)))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk

$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -I$(SRCDIR)/$(d)/..
CEXE_$(d):		$(LIBCMDPARAMS)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
