# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDRMS_PG		:= $(d)/libpg.so

CLEAN			:= $(CLEAN) \
			   $(LIBDRMS_PG) 

TGT_LIB		 	:= $(TGT_LIB) $(LIBDRMS_PG)

S_$(d)			:= $(notdir $(LIBDRMS_PG))

# Local rules
$(LIBDRMS_PG):		$(SRCDIR)/$(d)/Rules.mk

$(LIBDRMS_PG):		$(LIBJSOC_MAIN_OBJ) $(LIBDRMS_OBJ) $(LIBDEFSSERV_OBJ) $(LIBDB_OBJ) $(LIBSUMSAPI_OBJ) $(LIBCMDPARAMS_OBJ) $(BASELIBS_OBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
