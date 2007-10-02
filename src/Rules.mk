# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)


# Subdirectories. Directory-specific rules are optional here. The
# order DOES matter.  fdrms.o is built in base, and it is needed
# at COMPILE time by fortran modules in proj.
dir	:= $(d)/base
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/proj
-include		$(SRCDIR)/$(dir)/Rules.mk

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))


