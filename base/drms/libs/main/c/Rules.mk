# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBJSOC_MAIN 		:= $(d)/libjsoc_main.a 
LIBJSOC_MAIN_SOCK 	:= $(d)/libjsoc_main_sock.a 


# in parent dir
JM_OBJ_$(d)		:= $(d)/../jsoc_main.o 
JM_SOCK_OBJ_$(d)   	:= $(d)/../jsoc_main_sock.o

# in this dir
CI_JM_OBJ_$(d)		:= $(d)/jsoc_main_c.o
CI_JM_SOCK_OBJ_$(d)	:= $(d)/jsoc_main_sock_c.o

# Need to combine all object files into a metalibrary
LIBJSOC_MAIN_OBJ	:= $(JM_OBJ_$(d)) $(CI_JM_OBJ_$(d))
LIBJSOC_MAIN_SOCK_OBJ	:= $(JM_SOCK_OBJ_$(d)) $(CI_JM_SOCK_OBJ_$(d))

DEP_$(d)		:= $(JM_OBJ_$(d):%=%.d) \
			   $(JM_SOCK_OBJ_$(d):%=%.d) \
			   $(CI_JM_OBJ_$(d):%=%.d) \
			   $(CI_JM_SOCK_OBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(LIBJSOC_MAIN) \
			   $(LIBJSOC_MAIN_SOCK) \
			   $(JM_OBJ_$(d)) \
			   $(JM_SOCK_OBJ_$(d)) \
			   $(CI_JM_OBJ_$(d) \
			   $(CI_JM_SOCK_OBJ_$(d)) \
			   $(DEP_$(d)) 

TGT_LIB		 	:= $(TGT_LIB) $(LIBJSOC_MAIN) $(LIBJSOC_MAIN_SOCK) 

S_$(d)			:= $(notdir $(LIBJSOC_MAIN) $(LIBJSOC_MAIN_SOCK))

# Local rules
$(JM_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(JM_OBJ_$(d)):		CF_TGT := $(CF_TGT) 
$(JM_SOCK_OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(JM_SOCK_OBJ_$(d)):	CF_TGT := $(CF_TGT) 
$(CI_JM_OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(CI_JM_OBJ_$(d)):	CF_TGT := $(CF_TGT) 
$(CI_JM_SOCK_OBJ_$(d)): $(SRCDIR)/$(d)/Rules.mk

$(LIBJSOC_MAIN):	$(LIBJSOC_MAIN_OBJ)
			$(ARCHIVE)
			$(SLLIB)

$(LIBJSOC_MAIN_SOCK):	$(LIBJSOC_MAIN_SOCK_OBJ)
			$(ARCHIVE)
			$(SLLIB)



# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
