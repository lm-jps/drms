# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBRICECOMP_FPIC	:= $(d)/libricecomp_fpic.a

FPICOBJ_$(d)		:= $(addprefix $(d)/, rice_decode1.o  rice_encode2.o rice_decode2.o rice_encode4.o rice_decode4.o rice_encode1.o)
FPICOBJ			:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)		:= $(FPICOBJ_$(d):%=%.d)

CLEAN			:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBRICECOMP_FPIC) $(DEP_$(d))

S_$(d)			:= $(notdir $(LIBRICECOMP_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(FPICOBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBNAME)

$(LIBRICECOMP_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
