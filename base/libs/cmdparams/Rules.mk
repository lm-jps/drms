# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBCMDPARAMS		:= $(d)/libcmdparams.a
LIBCMDPARAMSF		:= $(d)/libcmdparams_f.a

COMMOBJ_$(d)		:= $(addprefix $(d)/, cmdparams.o)

FIOBJ_$(d)		:= $(addprefix $(d)/, cmdparams_f.o)
FIOBJ			:= $(FIOBJ) $(FIOBJ_$(d)) 

CMDPARAMSOBJ_$(d)	:= $(COMMOBJ_$(d))
CMDPARAMSFOBJ_$(d)	:= $(COMMOBJ_$(d)) $(FIOBJ_$(d))

LIBCMDPARAMS_OBJ	:= $(CMDPARAMSOBJ_$(d))
LIBCMDPARAMSF_OBJ	:= $(CMDPARAMSFOBJ_$(d))

DEP_$(d)		:= $(COMMOBJ_$(d):%=%.d) $(FIOBJ_$(d):%=%.d)  

CLEAN			:= $(CLEAN) \
			   $(COMMOBJ_$(d)) \
			   $(FIOBJ_$(d)) \
			   $(LIBCMDPARAMS) \
			   $(LIBCMDPARAMSF) \
			   $(DEP_$(d))

TGT_LIB			:= $(TGT_LIB) $(LIBCMDPARAMS) $(LIBCMDPARAMSF)

S_$(d)			:= $(notdir $(LIBCMDPARAMS) $(LIBCMDPARAMSF))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(FIOBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk


$(LIBCMDPARAMS):	$(LIBCMDPARAMS_OBJ)
			$(ARCHIVE)
			$(SLLIB)

$(LIBCMDPARAMSF):	$(LIBCMDPARAMSF_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
