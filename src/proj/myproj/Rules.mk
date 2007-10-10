# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# ALWAYS put libs subdirectory before other subdirectories.
#dir	:= $(d)/libs
#-include		$(SRCDIR)/$(dir)/Rules.mk

# Subdirectories. Directory-specific rules are optional here. The
# order DOES matter, always define libraries before applications
# that use those libraries.
dir	:= $(d)/apps
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
