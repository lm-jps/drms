# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
MODEXE_$(d)	:= $(addprefix $(d)/, arithtool)
MODEXEDR_$(d)	:= $(addprefix $(d)/, fits_into_drms)
MODEXE		:= $(MODEXE) $(MODEXE_$(d)) $(MODEXEDR_$(d))
MODEXEDR	:= $(MODEXEDR) $(MODEXEDR_$(d))

MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock)
MODEXEDR_SOCK_$(d)	:= $(MODEXEDR_$(d):%=%_sock)
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d)) $(MODEXEDR_SOCK_$(d))
MODEXEDR_SOCK	:= $(MODEXEDR_SOCK) $(MODEXEDR_SOCK_$(d))

MODEXEDROBJ	:= $(MODEXEDROBJ) $(MODEXEDR_$(d):%=%.o)

CEXE_$(d)	:= $(d)/time_convert
CEXE		:= $(CEXE) $(CEXE_$(d))

EXE_$(d)	:= $(MODEXE_$(d)) $(MODEXEDR_$(d)) $(CEXE_$(d)) 
OBJ_$(d)	:= $(EXE_$(d):%=%.o) 
DEP_$(d)	:= $(OBJ_$(d):%=%.o.d)
CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
		   $(MODEXE_SOCK_$(d)) \
		   $(MODEXEDR_SOCK_$(d)) \
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d)) $(MODEXE_SOCK_$(d)) $(MODEXEDR_SOCK_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)) $(MODEXE_SOCK_$(d)) $(MODEXEDR_SOCK_$(d)))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
