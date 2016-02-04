# Standard things
sp              := $(sp).x
dirstack_$(sp)  := $(d)
d               := $(dir)

# Local variables
MODEXE_$(d)    := $(addprefix $(d)/, test-fl-query)
MODEXE         := $(MODEXE) $(MODEXE_$(d))

MODEXE_SOCK_$(d):= $(MODEXE_$(d):%=%_sock)
MODEXE_SOCK   := $(MODEXE_SOCK) $(MODEXE_SOCK_$(d))

EXE_$(d)      	:= $(MODEXE_$(d))
OBJ_$(d)        := $(MODEXE_$(d):%=%.o)
DEP_$(d)        := $(OBJ_$(d):%=%.d)
CLEAN           := $(CLEAN) \
                   $(OBJ_$(d)) \
                   $(MODEXE_$(d)) \
                   $(MODEXE_SOCK_$(d))\
                   $(DEP_$(d))

S_$(d)          := $(notdir $(MODEXE_$(d)) $(MODEXE_SOCK_$(d)))

# Local rules
$(OBJ_$(d)):            $(SRCDIR)/$(d)/Rules.mk

# Shortcuts
.PHONY: $(S_$(d))
$(S_$(d)):      %:      $(d)/%
