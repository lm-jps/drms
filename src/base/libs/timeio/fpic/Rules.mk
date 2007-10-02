# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBTIMEIO_FPIC	:= $(d)/libtimeio_fpic.a

FPICOBJ_$(d)	:= $(addprefix $(d)/, timeio.o)
FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)	:= $(FPICOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBTIMEIO_FPIC) $(DEP_$(d))

S_$(d)		:= $(notdir $(LIBTIMEIO_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBTIMEIO_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
