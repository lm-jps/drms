sp              := $(sp).x
dirstack_$(sp)  := $(d)
d               := $(dir)

LIBCJSON         := $(d)/libcjson.a
LIBCJSONH        := $(CURDIR)/$(d)

# Local variables
OBJ_$(d)        := $(addprefix $(d)/, cJSON.o)
LIBCJSON_OBJ    := $(OBJ_$(d))

DEP_$(d)        := $(OBJ_$(d):%=%.d)

CLEAN           := $(CLEAN) $(OBJ_$(d)) $(LIBCSJON) $(DEP_$(d))

TGT_LIB         := $(TGT_LIB) $(LIBCJSON)

S_$(d)          := $(notdir $(LIBCJSON))

# Local rules
$(OBJ_$(d)):    $(SRCDIR)/$(d)/Rules.mk

$(LIBCJSON):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY: $(S_$(d))
$(S_$(d)):      %:      $(d)/%

# Standard things
-include        $(DEP_$(d))

d               := $(dirstack_$(sp))
sp              := $(basename $(sp))
