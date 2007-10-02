# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBMISC		:= $(d)/libmisc.a

OBJ_$(d)	:= $(addprefix $(d)/, byteswap.o fpu_exception.o timer.o util.o printk.o xmem.o ndim.o adler32.o tee.o)
ifeq ($(shell uname),Linux)
  OBJ_$(d) := $(OBJ_$(d)) $(d)/backtrace.o
endif

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBMISC) $(DEP_$(d))
TGT_LIB		:= $(TGT_LIB) $(LIBMISC) 
S_$(d)		:= $(notdir $(LIBMISC))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBMISC):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

