# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/cmdparams
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/db
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/dstruct
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/ricecomp
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/inthandles
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/misc
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/threads
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/timeio
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/fitsrw
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
