# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

LIBDRMS		:= $(d)/libdrmsserver.a
LIBDRMSCLIENT	:= $(d)/libdrmsclient.a

# Common to client and server - keep .o files in parent.
COMMOBJ_$(d)	:= $(addprefix $(d)/, drms_types.o drms_keyword.o drms_link.o drms_segment.o drms_protocol.o drms_binfile.o drms_parser.o drms_names.o drms_array.o drms_dsdsapi.o drms_defs.o drms_fitsrw.o drms_fitstas.o drms_cmdparams.o)

SERVEROBJ_$(d)	:= $(addprefix $(d)/server/, drms_client.o drms_env.o drms_record.o drms_storageunit.o drms_server.o drms_series.o)
CLIENTOBJ_$(d)	:= $(addprefix $(d)/client/, drms_client.o drms_env.o drms_record.o drms_storageunit.o drms_series.o)

LIBDRMSSERVER_OBJ	:= $(COMMOBJ_$(d)) $(SERVEROBJ_$(d))
LIBDRMSCLIENT_OBJ	:= $(COMMOBJ_$(d)) $(CLIENTOBJ_$(d))

DEP_$(d)	:= $(COMMOBJ_$(d):%=%.d) $(SERVEROBJ_$(d):%=%.d) $(CLIENTOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) \
		   $(COMMOBJ_$(d)) \
		   $(SERVEROBJ_$(d)) \
		   $(CLIENTOBJ_$(d)) \
		   $(LIBDRMS) \
		   $(LIBDRMSCLIENT) \
		   $(DEP_$(d)) 

TGT_LIB 	:= $(TGT_LIB) $(LIBDRMS) $(LIBDRMSCLIENT)

S_$(d)		:= $(notdir $(LIBDRMS) $(LIBDRMSCLIENT))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMMOBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)
$(SERVEROBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(SERVEROBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)
$(CLIENTOBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(CLIENTOBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBMS) -DDRMS_CLIENT

$(LIBDRMS):		$(LIBDRMSSERVER_OBJ)
			$(ARCHIVE)
			$(SLLIB)
			
$(LIBDRMSCLIENT):	$(LIBDRMSCLIENT_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
