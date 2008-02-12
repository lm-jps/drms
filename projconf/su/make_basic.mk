# Make rules that apply to all projects

$(CEXESUMS):			$(LIBSUMSAPI) $(LIBSUM)
$(MODEXESUMS):			$(LIBSUMSAPI) $(LIBSUM)

$(MODEXEDROBJ):			CF_TGT := $(CF_TGT) -I$(SRCDIR)/proj/libs/dr
$(MODEXEDR) $(MODEXEDR_SOCK):	$(LIBDR)

$(GSLOBJ):			CF_TGT := $(CF_TGT) -I$(_JSOCROOT_)/lib_third_party/include/ 

ifeq ($(COMPILER), icc)
  ifeq ($(JSOC_MACHINE), linux_x86_64)
    $(GSLEXE):		LL_TGT := $(LL_TGT) -L$(_JSOCROOT_)/lib_third_party/lib/linux_x86_64/ -lgsl
  else
    $(GSLEXE):		LL_TGT := $(LL_TGT) -L$(_JSOCROOT_)/lib_third_party/lib/linux_ia32/ -lgsl
  endif
endif
