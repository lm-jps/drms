# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/client
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/client_fpic
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/server
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
