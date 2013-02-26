# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Order es muy importante!  The meta libs depend on the api libs.
dir	:= $(d)/api
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/main
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/meta
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
