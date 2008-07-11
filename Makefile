# Define WORKINGDIR so that we don't get that '/auto/home1' crud
WORKINGDIR	= $(PWD)

ifeq (,$(filter _%,$(notdir $(CURDIR))))
  include target.mk
else
#----- End Boilerplate

VPATH	  = $(SRCDIR)
include $(SRCDIR)/make_basic.mk

#----- Begin Boilerplate
endif
