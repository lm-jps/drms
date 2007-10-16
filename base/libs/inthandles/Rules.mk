# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBINTHANDLESF		:= $(d)/libinthandles_f.a
LIBINTHANDLESIDL	:= $(d)/libinthandles_idl.a

FIOBJ_$(d)		:= $(addprefix $(d)/, inthandles_f.o)

FPICOBJ_$(d)		:= $(addprefix $(d)/, inthandles_idl.o)

# Used by global rule that creates Fortran-specific C files.
FIOBJ		:= $(FIOBJ) $(FIOBJ_$(d)) 

FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d))

IIOBJ		:= $(IIOBJ) $(FPICOBJ_$(d))

DEP_$(d)	:= $(FIOBJ_$(d):%=%.d) $(FPICOBJ_$(d):%=%.d) 

CLEAN		:= $(CLEAN) $(FIOBJ_$(d)) $(FPICOBJ_$(d)) $(LIBINTHANDLESF) $(LIBINTHANDLESIDL) $(DEP_$(d))
TGT_LIB		:= $(TGT_LIB) $(LIBINTHANDLESIDL) $(LIBINTHANDLESF)
S_$(d)		:= $(notdir $(LIBINTHANDLESF) $(LIBINTHANDLESIDL))

# Local rules
$(FIOBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBINTHANDLESF):	$(FIOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

$(LIBINTHANDLESIDL):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
