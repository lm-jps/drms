# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/c
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/f
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/idl
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
