# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order. Directory-specific rules are optional here.
dir	:= $(d)/api
-include		$(SRCDIR)/$(dir)/Rules.mk

USE_RPC :=

ifneq ($(SUMS_USEMTSUMS),1)
	USE_RPC := yes
endif

ifneq ($(SUMS_USEMTSUMS_ALL),1)
	USE_RPC := yes
endif

ifdef USE_RPC
	dir	:= $(d)/pg
	-include		$(SRCDIR)/$(dir)/Rules.mk
endif

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
