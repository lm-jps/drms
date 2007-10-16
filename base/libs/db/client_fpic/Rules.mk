# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDBCLIENT_FPIC	:= $(d)/libdbclient_fpic.a

# All files are unique to client_fpic - keep .o files in current dir.
FPICOBJ_$(d)		:= $(addprefix $(d)/, db_common.o db_network.o db_client.o db_sort.o)
FPICOBJ			:= $(FPICOBJ) $(FPICOBJ_$(d))

DEP_$(d)		:= $(FPICOBJ_$(d):%=%.d)

CLEAN			:= $(CLEAN) \
			   $(FPICOBJ_$(d)) \
			   $(LIBDBCLIENT_FPIC) \
			   $(DEP_$(d))

S_$(d)			:= $(notdir $(LIBDBCLIENT_FPIC))

# Local rules
$(FPICOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(FPICOBJ_$(d)):	CF_TGT := -D$(DBNAME) -I$(PGIPATH)

$(LIBDBCLIENT_FPIC):	$(FPICOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
