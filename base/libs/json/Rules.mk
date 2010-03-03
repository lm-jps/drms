# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBJSON		:= $(d)/libjson.a

OBJ_$(d)	:= $(addprefix $(d)/, json.o json_helper.o)

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBJSON) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBJSON)

S_$(d)		:= $(notdir $(LIBJSON))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBJSON):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
