# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBTHREADUTIL_FPIC	:= $(d)/libthreadutil_fpic.a

FPICOBJ_$(d)		:= $(addprefix $(d)/, fifo.o tagfifo.o)
FPICOBJ			:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)		:= $(FPICOBJ_$(d):%=%.d)

CLEAN			:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBTHREADUTIL_FPIC) $(DEP_$(d))

S_$(d)			:= $(notdir $(LIBTHREADUTIL_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBTHREADUTIL_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
