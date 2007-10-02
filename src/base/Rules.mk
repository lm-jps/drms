# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# ALWAYS put libs subdirectory before other subdirectories.
dir	:= $(d)/libs
-include		$(SRCDIR)/$(dir)/Rules.mk

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/drms
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/example
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/local
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/sums
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/util
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
