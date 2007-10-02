# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBGAPFILLER	:= $(d)/libgapfiller.a
OBJ_$(d)	:= $(addprefix $(d)/, fahlman_ulrych_C99.o pcg_C99.o levinson_C99.o multi_burg_C99.o detrend_C99.o cblas.o  xerbla.o reject_C99.o)

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(LIBGAPFILLER) \
		   $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBGAPFILLER)

S_$(d)		:= $(notdir $(LIBGAPFILLER))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk

$(LIBGAPFILLER):	$(OBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
