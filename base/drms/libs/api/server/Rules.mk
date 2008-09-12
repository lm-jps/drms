# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDRMS		:= $(d)/libdrmsserver.a

# Common to client and server - keep .o files in parent.
COMMOBJ_$(d)	:= $(addprefix $(d)/../, drms_types.o drms_keyword.o drms_link.o drms_segment.o drms_protocol.o drms_binfile.o drms_series.o drms_parser.o drms_names.o drms_compress.o drms_tasfile.o drms_fits.o drms_array.o drms_keymap.o drms_dsdsapi.o drms_defs.o drms_fitsrw.o drms_fitstas.o drms_cmdparams.o)

OBJ_$(d)	:= $(addprefix $(d)/, drms_client.o drms_env.o drms_record.o drms_storageunit.o drms_server.o)

LIBDRMS_OBJ	:= $(COMMOBJ_$(d)) $(OBJ_$(d))

DEP_$(d)	:= $(COMMOBJ_$(d):%=%.d) $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) \
		   $(COMMOBJ_$(d)) \
		   $(OBJ_$(d)) \
		   $(LIBDRMS) \
		   $(DEP_$(d)) 

TGT_LIB 	:= $(TGT_LIB) $(LIBDRMS)

S_$(d)		:= $(notdir $(LIBDRMS))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMMOBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBNAME) $(CFITSIOH)
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBNAME) $(CFITSIOH)

$(LIBDRMS):		$(LIBDRMS_OBJ)
			$(ARCHIVE)
			$(SLLIB)
# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
