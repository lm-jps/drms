# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBASTRO	:= $(d)/libastro.a

OBJ_$(d)	:= $(addprefix $(d)/, procFdsData.o interp.o obs2helio.o apodize.o)
DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBASTRO) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBASTRO)

S_$(d)		:= $(notdir $(LIBASTRO))


# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBASTRO):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
