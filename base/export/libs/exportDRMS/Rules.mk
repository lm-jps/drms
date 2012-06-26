# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBEXPDRMS	:= $(d)/libexpdrms.a

OBJ_$(d)	:= $(addprefix $(d)/, fitsexport.o)

LIBEXPDRMS_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBEXPDRMS) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBEXPDRMS)

S_$(d)		:= $(notdir $(LIBEXPDRMS))


# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):    CF_TGT := $(CF_TGT) $(CFITSIOH)

$(LIBEXPDRMS):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
