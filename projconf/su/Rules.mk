# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# ALWAYS put libs subdirectory before other subdirectories.
dir	:= $(d)/libs
-include		$(SRCDIR)/$(dir)/Rules.mk

# Subdirectories. Directory-specific rules are optional here. The
# order NOT matter.
dir	:= $(d)/datacapture
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/dsdsmigr
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/example
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/cookbook
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/lev0
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/lev1
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/jpe
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/lev1_aia
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/lev1_hmi
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/myproj
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/util
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/export
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/globalhs
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
