# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBMISC_FPIC	:= $(d)/libmisc_fpic.a

FPICOBJ_$(d)	:= $(addprefix $(d)/, byteswap.o timer.o util.o printk.o xmem.o ndim.o adler32.o tee.o)
ifeq ($(shell uname),Linux)
  FPICOBJ_$(d) := $(FPICOBJ_$(d)) $(d)/backtrace.o
endif

FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)	:= $(FPICOBJ_$(d):%=%.d) 

CLEAN		:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBMISC_FPIC) $(DEP_$(d))

S_$(d)		:= $(notdir $(LIBMISC_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBMISC_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

