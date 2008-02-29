# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBFITSRW	:= $(d)/libfitsrw.a

OBJ_$(d)	:= $(addprefix $(d)/, cfitsio.o)

LIBFITSRW_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBFITSRW) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBFITSRW)

S_$(d)		:= $(notdir $(LIBFITSRW))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):	CF_TGT := $(CF_TGT) $(CFITSIOH)

$(LIBFITSRW):	LL_TGT := $(LL_TGT) $(CFITSIOL) -lcfitsio

$(LIBFITSRW):	$(LIBFITSRW_OBJ)
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

