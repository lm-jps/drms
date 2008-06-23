# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBDEFS	:= $(d)/libdefs.a

OBJ_$(d)	:= $(addprefix $(d)/, defs.o)

LIBDEFS_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBDEFS) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBDEFS)

S_$(d)		:= $(notdir $(LIBDEFS))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBDEFS):	$(LIBDEFS_OBJ)
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

