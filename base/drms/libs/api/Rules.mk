# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

LIBDRMS		:= $(d)/libdrmsserver.a
LIBDRMSCLIENT	:= $(d)/libdrmsclient.a
LIBDRMS_SERVER_FPIC	:= $(d)/libdrmsserver-fpic.a

COMM_OBJ_$(d)	:= $(addprefix $(d)/, drms_types.o drms_keyword.o drms_link.o drms_segment.o drms_protocol.o drms_binfile.o drms_parser.o drms_names.o drms_array.o drms_dsdsapi.o drms_defs.o drms_fitsrw.o drms_fitstas.o drms_cmdparams.o)
SERVER_OBJ_$(d)	:= $(addprefix $(d)/server/, drms_client.o drms_env.o drms_record.o drms_storageunit.o drms_server.o drms_series.o)
CLIENT_OBJ_$(d)	:= $(addprefix $(d)/client/, drms_client.o drms_env.o drms_record.o drms_storageunit.o drms_series.o)

# common to LIBDRMS_SERVER_FPIC_OBJ client and server - keep .o files in parent
COMM_OBJ_FPIC_$(d)	:= $(addprefix $(d)/server-fpic/, $(notdir $(COMM_OBJ_$(d))))
SERVER_OBJ_FPIC_$(d) := $(addprefix $(d)/server-fpic/, drms_client.o drms_env.o drms_record.o drms_series.o drms_server.o drms_storageunit.o)

FPICOBJ			:= $(FPICOBJ) $(COMM_OBJ_FPIC_$(d)) $(SERVER_OBJ_FPIC_$(d))

LIBDRMSSERVER_OBJ	:= $(COMM_OBJ_$(d)) $(SERVER_OBJ_$(d))
LIBDRMSCLIENT_OBJ	:= $(COMM_OBJ_$(d)) $(CLIENT_OBJ_$(d))
LIBDRMS_SERVER_OBJ_FPIC  := $(COMM_OBJ_FPIC_$(d)) $(SERVER_OBJ_FPIC_$(d))

DEP_$(d)	:= $(COMM_OBJ_$(d):%=%.d) $(SERVER_OBJ_$(d):%=%.d) $(CLIENT_OBJ_$(d):%=%.d) $(COMM_OBJ_FPIC_$(d):%=%.d) $(SERVER_OBJ_FPIC_$(d):%=%.d)

CLEAN		:= $(CLEAN) \
		   $(COMM_OBJ_$(d)) \
		   $(SERVER_OBJ_$(d)) \
		   $(CLIENT_OBJ_$(d)) \
		   $(COMM_OBJ_FPIC_$(d)) \
		   $(SERVER_OBJ_FPIC_$(d)) \
		   $(LIBDRMS) \
		   $(LIBDRMSCLIENT) \
		   $(LIBDRMS_SERVER_FPIC) \
		   $(DEP_$(d)) 

TGT_LIB 	:= $(TGT_LIB) $(LIBDRMS) $(LIBDRMSCLIENT) $(LIBDRMS_SERVER_FPIC)

S_$(d)		:= $(notdir $(LIBDRMS) $(LIBDRMSCLIENT) $(LIBDRMS_SERVER_FPIC))

# Local rules
$(SERVER_OBJ_$(d)):	$(d)/server/%.o	: $(SRCDIR)/$(d)/%.c
			$(COMP)
$(CLIENT_OBJ_$(d)):	$(d)/client/%.o : $(SRCDIR)/$(d)/%.c
			$(COMP)

$(COMM_OBJ_FPIC_$(d)):  $(d)/server-fpic/%.o : $(SRCDIR)/$(d)/%.c
												$(COMP)
												

# THIS IS BUGGY ON gnu make 3.81; it causes a circular dependency; with gnu make 3.82, there is no circular dependency;
# the problem is a bad interaction between VPATH and this recipe
# $(SERVER_OBJ_FPIC_$(d)):    $(d)/server-fpic/%.o : $(d)/%.c
#												$(COMP)

$(SERVER_OBJ_FPIC_$(d)):    $(d)/server-fpic/%.o : $(SRCDIR)/$(d)/%.c
												$(COMP)
$(d)/client/fdrms.o:	$(d)/fdrms.f
			$(FCOMP)

$(COMM_OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMM_OBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)
$(SERVER_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(SERVER_OBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)
$(CLIENT_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(CLIENT_OBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBMS) -DDRMS_CLIENT
$(COMM_OBJ_FPIC_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMM_OBJ_FPIC_$(d)):	CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)
$(SERVER_OBJ_FPIC_$(d)):    $(SRCDIR)/$(d)/Rules.mk
$(SERVER_OBJ_FPIC_$(d)):    CF_TGT := $(CF_TGT) -D$(DBMS) $(CFITSIOH)

$(LIBDRMS):	$(LIBDRMSSERVER_OBJ)
			$(ARCHIVE)
			$(SLLIB)

$(LIBDRMSCLIENT):	$(LIBDRMSCLIENT_OBJ)
					$(ARCHIVE)
					$(SLLIB)

$(LIBDRMS_SERVER_FPIC):		$(LIBDRMS_SERVER_OBJ_FPIC)
							$(ARCHIVE)
							$(SLLIB)


# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
