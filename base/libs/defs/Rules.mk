# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories, in random order.
dir	:= $(d)/fpic
-include		$(SRCDIR)/$(dir)/Rules.mk

# Local variables
LIBDEFSSERVER	:= $(d)/libdefsserver.a
LIBDEFSCLIENT	:= $(d)/libdefsclient.a

COMMOBJ_$(d)	:= $(addprefix $(d)/, defs.o)
SERVOBJ_$(d)	:= $(addprefix $(d)/, drmssite_info_server.o)
CLNTOBJ_$(d)	:= $(addprefix $(d)/, drmssite_info_client.o)
OBJ_$(d)	:= $(COMMOBJ_$(d)) $(SERVOBJ_$(d)) $(CLNTOBJ_$(d))

LIBDEFSCLNT_OBJ	:= $(COMMOBJ_$(d)) $(CLNTOBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(LIBDEFSSERVER) $(LIBDEFSCLNT) $(DEP_$(d))

TGT_LIB		:= $(TGT_LIB) $(LIBDEFSSERVER) $(LIBDEFSCLIENT)

S_$(d)		:= $(notdir $(LIBDEFSSERVER) $(LIBDEFSCLIENT))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(SERVOBJ_$(d)):	%_server.o:	%.c
			$(COMP)
$(CLNTOBJ_$(d)):	CF_TGT := $(CF_TGT) -DDEFS_CLIENT
$(CLNTOBJ_$(d)):	%_client.o:	%.c
			$(COMP)

$(LIBDEFSSERVER):	$(COMMOBJ_$(d)) $(SERVOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

$(LIBDEFSCLIENT):	$(COMMOBJ_$(d)) $(CLNTOBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)


# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

