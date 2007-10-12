# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBJSOC_MAIN_SOCK_F 	:= $(d)/libjsoc_main_sock_f.a

# in this dir (must compile jsoc_main_sock.o with lang-specific flags)
JM_SOCK_OBJ_$(d)   	:= $(d)/jsoc_main_sock.o
FI_JM_SOCK_OBJ_$(d)	:= $(d)/jsoc_main_sock_f.o

# Need to combine all object files into a metalibrary
LIBJSOC_MAIN_SOCK_F_OBJ	:= $(JM_SOCK_OBJ_$(d)) $(FI_JM_SOCK_OBJ_$(d))

FIOBJ			:= $(FIOBJ) $(FI_JM_SOCK_OBJ_$(d)) $(JM_SOCK_OBJ_$(d))

DEP_$(d)		:= $(JM_SOCK_OBJ_$(d):%=%.d) \
			   $(FI_JM_SOCK_OBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(LIBJSOC_MAIN_SOCK_F) \
			   $(JM_SOCK_OBJ_$(d)) \
			   $(FI_JM_SOCK_OBJ_$(d)) \
			   $(DEP_$(d)) 

TGT_LIB		 	:= $(TGT_LIB) $(LIBJSOC_MAIN_SOCK_F) 

S_$(d)			:= $(notdir $(LIBJSOC_MAIN_SOCK_F))

# Local rules
$(FI_JM_SOCK_OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk

$(LIBJSOC_MAIN_SOCK_F):	$(LIBJSOC_MAIN_SOCK_F_OBJ)
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
