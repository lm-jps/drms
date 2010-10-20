# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBEXPUTL	:= $(d)/libexputl.a

OBJ_$(d)	:= $(addprefix $(d)/, exputil.o)
DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBEXPUTL) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBEXPUTL)

S_$(d)		:= $(notdir $(LIBEXPUTL))


# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBEXPUTL):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
