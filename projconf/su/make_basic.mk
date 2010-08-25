# Make rules that apply to all projects

$(CEXESUMS):			$(LIBSUMSAPI) $(LIBSUM) $(LIBDSTRUCT)
$(MODEXESUMS):			$(LIBSUMSAPI) $(LIBSUM)

$(MODEXEDROBJ):			CF_TGT := $(CF_TGT) -I$(SRCDIR)/proj/libs/dr
$(MODEXEDR) $(MODEXEDR_SOCK):	$(LIBDR)
