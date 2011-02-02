# Define WORKINGDIR so that we don't get that '/auto/home1' crud
WORKINGDIR	= $(PWD)

# If the make command included LOCALIZATIONDIR='somedir', use that, otherwise
# default to JSOC/localizationdir/custom.mk
ifeq ($(LOCALIZATIONDIR),)
  LOCALIZATIONDIR = localization
endif

ifeq (,$(filter _%,$(notdir $(CURDIR))))
  include target.mk
else
#----- End Boilerplate

VPATH	  = $(SRCDIR)
include $(SRCDIR)/make_basic.mk

#----- Begin Boilerplate
endif
