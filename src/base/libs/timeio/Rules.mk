# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBTIMEIO	:= $(d)/libtimeio.a

OBJ_$(d)	:= $(addprefix $(d)/, timeio.o)

DEP_$(d)	:= $(OBJ_$(d):%=%.d) $(IIOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(IIOBJ_$(d)) $(LIBTIMEIO) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBTIMEIO)

S_$(d)		:= $(notdir $(LIBTIMEIO))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBTIMEIO):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
