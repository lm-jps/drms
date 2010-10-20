# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# ALWAYS put libs subdirectory before other subdirectories.
dir	:= $(d)/libs
-include		$(SRCDIR)/$(dir)/Rules.mk

# Order es muy importante!  DRMS can depend on SUMS libs, util can depend on DRMS libs, etc.
dir	:= $(d)/sums
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/drms
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/example
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/export
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/local
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/util
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
