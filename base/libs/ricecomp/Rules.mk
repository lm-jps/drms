# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBRICECOMP	:= $(d)/libricecomp.a

OBJ_$(d)	:= $(addprefix $(d)/, rice_decode1.o  rice_encode2.o rice_decode2.o rice_encode4.o rice_decode4.o rice_encode1.o)

LIBRICECOMP_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBRICECOMP) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBRICECOMP)

S_$(d)		:= $(notdir $(LIBRICECOMP))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBNAME)

$(LIBRICECOMP):		$(LIBRICECOMP_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
