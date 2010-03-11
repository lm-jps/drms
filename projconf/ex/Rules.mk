# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# ALWAYS put libs subdirectory before other subdirectories.
#dir	:= $(d)/libs
#-include		$(SRCDIR)/$(dir)/Rules.mk

# Subdirectories. Directory-specific rules are optional here. The
# order NOT matter.
dir	:= $(d)/example
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/myproj
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/cookbook
-include		$(SRCDIR)/$(dir)/Rules.mk

# A site might request a Stanford proj directory, other than the standard ones above
-include $(LOCALIZATIONDIR)/projRules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
