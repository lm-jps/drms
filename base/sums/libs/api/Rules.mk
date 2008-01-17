# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBSUMSAPI	:= $(d)/libsumsapi.a 
OBJ_$(d)	:= $(addprefix $(d)/, atoinc.o key.o printkey.o soi_error.o str_utils.o sum_open.o sumopened.o sum_xdr.o)

LIBSUMSAPI_OBJ	:= $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBSUMSAPI) $(DEP_$(d)) 

TGT_LIB		:= $(TGT_LIB) $(LIBSUMSAPI)

S_$(d)		:= $(notdir $(LIBSUMSAPI))

# Local rules
$(OBJ_$(d)):   $(SRCDIR)/$(d)/Rules.mk
ifneq ($(COMPILER), icc)
$(OBJ_$(d)):	CF_TGT := -Wno-parentheses
endif
$(LIBSUMSAPI):	$(LIBSUMSAPI_OBJ)
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
