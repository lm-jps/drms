# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Libraries used by the projects must go first!
dir	:= $(d)/libs
-include		$(SRCDIR)/$(dir)/Rules.mk

# Subdirectories. Directory-specific rules are optional here. The
# order NOT matter.
dir	:= $(d)/apps
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
