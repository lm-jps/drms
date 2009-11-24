# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

LIBQDECODER	:= $(d)/libqdecoder.a

# Local variables
MD5_OBJ_$(d)	:= $(addprefix $(d)/md5/, $(notdir $(patsubst %.c,%.o,$(wildcard $(SRCDIR)/$(d)/md5/*.c))))

OBJ_$(d)	:= $(addprefix $(d)/, $(notdir $(patsubst %.c,%.o,$(wildcard $(SRCDIR)/$(d)/*.c)))) $(MD5_OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d) 

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBQDECODER) $(DEP_$(d)) 

TGT_LIB		:= $(TGT_LIB) $(LIBQDECODER)

S_$(d)		:= $(notdir $(LIBQDECODER))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBQDECODER):	$(OBJ_$(d)) 
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
