# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBDSTRUCT	:= $(d)/libdstruct.a

OBJ_$(d)	:= $(addprefix $(d)/, hcontainer.o hash_table.o parse_params.o table.o list.o)

LIBDSTRUCT_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBDSTRUCT) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBDSTRUCT)

S_$(d)		:= $(notdir $(LIBDSTRUCT))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBDSTRUCT):	$(LIBDSTRUCT_OBJ)
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

