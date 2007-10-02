# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDRMSCLIENT_FPIC	:= $(d)/libdrmsclient_fpic.a

FPICOBJ_$(d)	:= $(addprefix $(d)/, drms_client.o drms_env.o drms_types.o drms_record.o drms_keyword.o drms_link.o drms_segment.o drms_protocol.o drms_binfile.o drms_series.o drms_parser.o drms_names.o drms_compress.o drms_tasfile.o drms_fits.o drms_array.o drms_storageunit.o drms_keymap.o drms_dsdsapi.o)

DRMSIDLOBJ_$(d)	:= $(d)/drms_idl.o

FPICOBJ		:= $(FPICOBJ) $(FPICOBJ_$(d)) $(DRMSIDLOBJ_$(d))

IIOBJ		:= $(IIOBJ) $(DRMSIDLOBJ_$(d))

DEP_$(d)	:= $(FPICOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(FPICOBJ_$(d)) $(LIBDRMSCLIENT_FPIC) $(DEP_$(d)) 

S_$(d)		:= $(notdir $(LIBDRMSCLIENT_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(FPICOBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBNAME) -DDRMS_CLIENT

$(LIBDRMSCLIENT_FPIC):	$(FPICOBJ_$(d)) $(DRMSIDLOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
