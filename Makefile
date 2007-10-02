ifeq (,$(filter _%,$(notdir $(CURDIR))))
  include target.mk
else
#----- End Boilerplate

VPATH	  = $(SRCDIR)
include $(SRCDIR)/make_basic.mk

#----- Begin Boilerplate
endif
