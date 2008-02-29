# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBFITSRW_FPIC	:= $(d)/libfitsrw_fpic.a

FPICOBJ_$(d)	:= $(addprefix $(d)/, cfitsio.o)
FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)	:= $(FPICOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBFITSRW_FPIC) $(DEP_$(d))

S_$(d)		:= $(notdir $(LIBFITSRW_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(FPICOBJ_$(d)):	CF_TGT := $(CF_TGT) $(CFITSIOH)

$(LIBFITSRW):	LL_TGT := $(LL_TGT) $(CFITSIOL) -lcfitsio

$(LIBFITSRW_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

