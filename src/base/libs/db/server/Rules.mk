# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDB			:= $(d)/libdbserver.a

# Common to client and server - keep .o files in parent.
COMMOBJ_$(d)		:= $(addprefix $(d)/../, db_common.o db_network.o db_client.o db_sort.o)

# Unique to server - keep .o files in current dir.
DB_$(d)			:= $(d)/db_postgresql.o
OBJ_$(d)		:= $(addprefix $(d)/, db_server.o db_backend.o) $(DB_$(d))

LIBDB_OBJ		:= $(COMMOBJ_$(d)) $(OBJ_$(d))

DEP_$(d)		:= $(COMMOBJ_$(d):%=%.d) $(OBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(COMMOBJ_$(d)) \
			   $(OBJ_$(d)) \
			   $(LIBDB) \
			   $(DEP_$(d))

TGT_LIB			:= $(TGT_LIB) $(LIBDB)

S_$(d)			:= $(notdir $(LIBDB))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMMOBJ_$(d)):	CF_TGT := -D$(DBNAME) -I$(PGIPATH)
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := -D$(DBNAME) -I$(PGIPATH)

$(LIBDB):		$(LIBDB_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
