# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDB			:= $(d)/libdbserver.a
LIBDBCLIENT		:= $(d)/libdbclient.a
LIBDB_SERVER_FPIC := $(d)/libdbserver-fpic.a

# Common to client and server - keep .o files in current directory.
COMM_OBJ_$(d)		:= $(addprefix $(d)/, db_common.o db_network.o db_client.o db_sort.o db_postgresql.o)

# Unique to server - keep .o files in server directory
SERVER_OBJ_$(d)		:= $(addprefix $(d)/server/, db_server.o db_backend.o)
CLIENT_OBJ_$(d)		:= 
COMM_OBJ_FPIC_$(d)	:= $(addprefix $(d)/server-fpic/, $(notdir $(COMM_OBJ_$(d))))
SERVER_OBJ_FPIC_$(d)	:= $(addprefix $(d)/server-fpic/, db_server.o db_backend.o)

FPICOBJ			:= $(FPICOBJ) $(SERVER_OBJ_FPIC_$(d)) $(COMM_OBJ_FPIC_$(d))

LIBDB_OBJ		:= $(COMM_OBJ_$(d)) $(SERVER_OBJ_$(d))
LIBDB_CLIENT_OBJ		:= $(COMM_OBJ_$(d)) $(CLIENT_OBJ_$(d))
LIBDB_SERVER_OBJ_FPIC   := $(SERVER_OBJ_FPIC_$(d)) $(COMM_OBJ_FPIC_$(d))

DEP_$(d)		:= $(COMM_OBJ_$(d):%=%.d) $(SERVER_OBJ_$(d):%=%.d) $(CLIENT_OBJ_$(d):%=%.d) $(COMM_OBJ_FPIC_$(d):%=%.d) $(SERVER_OBJ_FPIC_$(d):%=%.d)

CLEAN			:= $(CLEAN) \
				$(COMM_OBJ_$(d)) \
				$(SERVER_OBJ_$(d)) \
				$(CLIENT_OBJ_$(d)) \
				$(COMM_OBJ_FPIC_$(d)) \
				$(SERVER_OBJ_FPIC_$(d)) \
				$(LIBDB) \
				$(LIBDBCLIENT) \
				$(LIBDB_SERVER_FPIC) \
				$(DEP_$(d))

TGT_LIB			:= $(TGT_LIB) $(LIBDB) $(LIBDBCLIENT) $(LIBDB_SERVER_FPIC)

S_$(d)			:= $(notdir $(LIBDB) $(LIBDBCLIENT) $(LIBDB_SERVER_FPIC))

# Local rules
$(COMM_OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMM_OBJ_$(d)):	CF_TGT := -D$(DBMS) $(PGH)
$(SERVER_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(SERVER_OBJ_$(d)):		CF_TGT := -D$(DBMS) $(PGH)
$(CLIENT_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(CLIENT_OBJ_$(d)):		CF_TGT := -D$(DBMS) $(PGH)
$(COMM_OBJ_FPIC_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMM_OBJ_FPIC_$(d)):	CF_TGT := -D$(DBMS) $(PGH)
$(SERVER_OBJ_FPIC_$(d)): $(SRCDIR)/$(d)/Rules.mk
$(SERVER_OBJ_FPIC_$(d)): CF_TGT := -D$(DBMS) $(PGH)

$(LIBDB):		$(LIBDB_OBJ)
			$(ARCHIVE)
			$(SLLIB)

$(LIBDBCLIENT):		$(LIBDB_CLIENT_OBJ)
			$(ARCHIVE)
			$(SLLIB)
			
$(COMM_OBJ_FPIC_$(d)):	$(d)/server-fpic/%.o : $(d)/%.c
						$(COMP)
							
$(SERVER_OBJ_FPIC_$(d)):	$(d)/server-fpic/%.o : $(d)/server/%.c
							$(COMP)
			
$(LIBDB_SERVER_FPIC):		$(LIBDB_SERVER_OBJ_FPIC)
							$(ARCHIVE)
							$(SLLIB)


# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
