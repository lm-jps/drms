# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDSTRUCT_FPIC	:= $(d)/libdstruct_fpic.a

FPICOBJ_$(d)	:= $(addprefix $(d)/, hcontainer.o hash_table.o parse_params.o table.o list.o)
FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)	:= $(FPICOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBDSTRUCT_FPIC) $(DEP_$(d))

S_$(d)		:= $(notdir $(LIBDSTRUCT_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBDSTRUCT_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

