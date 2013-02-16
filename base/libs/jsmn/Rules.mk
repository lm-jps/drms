sp              := $(sp).x
dirstack_$(sp)  := $(d)
d               := $(dir)

# Local variables                                                                                                                    
LIBJSMN         := $(d)/libjsmn.a

OBJ_$(d)        := $(addprefix $(d)/, jsmn.o)

DEP_$(d)        := $(OBJ_$(d):%=%.d)

CLEAN           := $(CLEAN) $(OBJ_$(d)) $(LIBJSMN) $(DEP_$(d))

TGT_LIB         := $(TGT_LIB) $(LIBJSMN)

S_$(d)          := $(notdir $(LIBJSMN))

# Local rules                                                                                                                        
$(OBJ_$(d)):    $(SRCDIR)/$(d)/Rules.mk

$(LIBJSMN):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts                                                                                                                          
.PHONY: $(S_$(d))
$(S_$(d)):      %:      $(d)/%

# Standard things                                                                                                                    
-include        $(DEP_$(d))

d               := $(dirstack_$(sp))
sp              := $(basename $(sp))
