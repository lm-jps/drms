# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBERRLOG	:= $(d)/liberrlog.a

OBJ_$(d)	:= $(addprefix $(d)/, errlog.o)
DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBERRLOG) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBERRLOG)

S_$(d)		:= $(notdir $(LIBERRLOG))


# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBERRLOG):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
