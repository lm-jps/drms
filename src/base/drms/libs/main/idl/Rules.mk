# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBJSOC_MAIN_SOCK_I	:= $(d)/libidl.so
LIBJSOC_MAIN_SOCK_I_AUX	:= $(d)/libidl_aux.a
METALIB_FPIC		:= $(METALIB_FPIC) $(LIBJSOC_MAIN_SOCK_I)

WLOPTION_$(d)		:= -soname,$(notdir $(LIBJSOC_MAIN_SOCK_I))

# in this dir (must compile jsoc_main_sock.o with lang-specific flags)
JM_SOCK_OBJ_$(d)   	:= $(d)/jsoc_main_sock.o
II_JM_SOCK_OBJ_$(d)	:= $(d)/jsoc_main_sock_idl.o

IIOBJ			:= $(IIOBJ) $(JM_SOCK_OBJ_$(d)) $(II_JM_SOCK_OBJ_$(d))
FPICOBJ			:= $(FPICOBJ) $(JM_SOCK_OBJ_$(d)) $(II_JM_SOCK_OBJ_$(d))

DEP_$(d)		:= $(JM_SOCK_OBJ_$(d):%=%.d) \
			   $(II_JM_SOCK_OBJ_$(d):%=%.d) 

CLEAN			:= $(CLEAN) \
			   $(LIBJSOC_MAIN_SOCK_I) \
			   $(LIBJSOC_MAIN_SOCK_I_AUX) \
			   $(JM_SOCK_OBJ_$(d)) \
			   $(II_JM_SOCK_OBJ_$(d)) \
			   $(DEP_$(d)) 

S_$(d)			:= $(notdir $(LIBJSOC_MAIN_SOCK_I) $(LIBJSOC_MAIN_SOCK_I_AUX))

# Local rules
$(II_JM_SOCK_OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk

$(LIBJSOC_MAIN_SOCK_I):		LF_TGT := -shared -Wl,$(WLOPTION_$(d))
$(LIBJSOC_MAIN_SOCK_I):		$(LIBINTHANDLESIDL)

$(LIBJSOC_MAIN_SOCK_I_AUX):	$(JM_SOCK_OBJ_$(d)) $(II_JM_SOCK_OBJ_$(d))
				$(ARCHIVE)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
