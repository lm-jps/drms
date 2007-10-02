# Make rules that apply to all projects

$(CEXESUMS):			$(LIBSUMSAPI) $(LIBSUM)

$(MODEXEDROBJ):			CF_TGT := $(CF_TGT) -I$(SRCDIR)/src/proj/libs/dr
$(MODEXEDR) $(MODEXEDR_SOCK):	$(LIBDR)

