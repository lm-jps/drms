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

# Flatfield has problems on ia32
ifeq ($(JSOC_MACHINE), linux_x86_64)
dir	:= $(d)/flatfield
-include		$(SRCDIR)/$(dir)/Rules.mk
endif

dir     := $(d)/mag
-include                $(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/maps_avgs
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/myproj
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/util
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/globalhs
-include		$(SRCDIR)/$(dir)/Rules.mk
dir	:= $(d)/lev1.5_hmi
-include		$(SRCDIR)/$(dir)/Rules.mk
dir     := $(d)/rings
-include                $(SRCDIR)/$(dir)/Rules.mk
dir     := $(d)/limbfit
-include                $(SRCDIR)/$(dir)/Rules.mk
dir     := $(d)/farside
-include                $(SRCDIR)/$(dir)/Rules.mk
dir     := $(d)/timed
-include                $(SRCDIR)/$(dir)/Rules.mk


# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
