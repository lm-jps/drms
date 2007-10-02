# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories. Directory-specific rules are optional here. The
# order NOT matter.
dir	:= $(d)/astro
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/dr
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/dsputil
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/gapfiller
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

