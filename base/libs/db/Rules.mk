# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDB			:= $(d)/libdbserver.a
LIBDBCLIENT		:= $(d)/libdbclient.a

# Common to client and server - keep .o files in current directory.
COMMOBJ_$(d)		:= $(addprefix $(d)/, db_common.o db_network.o db_client.o db_sort.o db_postgresql.o)

# Unique to server - keep .o files in server directory
SERVEROBJ_$(d)		:= $(addprefix $(d)/server/, db_server.o db_backend.o)
CLIENTOBJ_$(d)		:= 

LIBDB_OBJ		:= $(COMMOBJ_$(d)) $(SERVEROBJ_$(d))
LIBDBCLIENT_OBJ		:= $(COMMOBJ_$(d)) $(CLIENTOBJ_$(d))

DEP_$(d)		:= $(COMMOBJ_$(d):%=%.d) $(SERVEROBJ_$(d):%=%.d) $(CLIENTOBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(COMMOBJ_$(d)) \
			   $(SERVEROBJ_$(d)) \
               $(CLIENTOBJ_$(d)) \
			   $(LIBDB) \
			   $(LIBDBCLIENT) \
			   $(DEP_$(d))

TGT_LIB			:= $(TGT_LIB) $(LIBDB) $(LIBDBCLIENT)

S_$(d)			:= $(notdir $(LIBDB) $(LIBDBCLIENT))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMMOBJ_$(d)):	CF_TGT := -D$(DBMS) $(PGH)
$(SERVEROBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(SERVEROBJ_$(d)):		CF_TGT := -D$(DBMS) $(PGH)
$(CLIENTOBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(CLIENTOBJ_$(d)):		CF_TGT := -D$(DBMS) $(PGH)

$(LIBDB):		$(LIBDB_OBJ)
			$(ARCHIVE)
			$(SLLIB)
			
$(LIBDBCLIENT):		$(LIBDBCLIENT_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
