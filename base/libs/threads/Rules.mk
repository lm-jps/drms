# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBTHREADUTIL	:= $(d)/libthreadutil.a

OBJ_$(d)	:= $(addprefix $(d)/, fifo.o tagfifo.o tdsignals.o)

LIBTHREADUTIL_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBTHREADUTIL) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBTHREADUTIL)

S_$(d)		:= $(notdir $(LIBTHREADUTIL))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk

$(LIBTHREADUTIL):	$(LIBTHREADUTIL_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
