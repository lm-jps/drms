# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBSUMSAPI_PERL	:= $(d)/libsumsapi_perl.so 
WLOPTION_$(d)	:= -soname,$(notdir $(LIBSUMSAPI_PERL))
SW_$(d)		:= $(addprefix $(d)/, $(notdir $(wildcard $(SRCDIR)/$(d)/*.i)))

OBJ_$(d)	:= $(SW_$(d):%.i=%.o) $(addprefix $(d)/, $(notdir $(patsubst %.c,%.o,$(wildcard $(SRCDIR)/$(d)/../*.c))))

FPICOBJ		:= $(FPICOBJ) $(OBJ_$(d))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(SW_$(d):%.i=%.c) $(OBJ_$(d)) $(LIBSUMSAPI_PERL) $(DEP_$(d)) 

TGT_LIB_$(d)	:= $(TGT_LIB)
TGT_LIB		:= $(TGT_LIB) $(LIBSUMSAPI_PERL)

ifeq ($(JSOC_MACHINE), mac_osx_ppc) 
TGT_LIB 	:= TGT_LIB_$(d)
endif

ifeq ($(JSOC_MACHINE), mac_osx_ia32) 
TGT_LIB 	:= TGT_LIB_$(d)
endif

S_$(d)		:= $(notdir $(LIBSUMSAPI_PERL) $(SW_$(d)))


# Local rules
#CF_TGT_$(d)     := -O0 -Wno-parentheses -fno-strict-aliasing
CF_TGT_$(d)     := -O0 -fno-strict-aliasing
ADD_TGT_$(d) := -DSUMT120 -DSUMNOAO

ifeq ($(HOST),dcs0.jsoc.Stanford.EDU)
        ADD_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),dcs1.jsoc.Stanford.EDU)
        ADD_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),dcs2.jsoc.Stanford.EDU)
        ADD_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),d00.Stanford.EDU)
        ADD_TGT_$(d) := -DSUMT120
endif
ifeq ($(HOST),d02.Stanford.EDU)
        ADD_TGT_$(d) := -DSUMT950
endif
CF_TGT_$(d) := $(CF_TGT_$(d)) $(ADD_TGT_$(d))

$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk $(SW_$(d))
$(OBJ_$(d)):	CF_TGT := $(CF_TGT_$(d)) `perl -MExtUtils::Embed -e ccopts`

$(LIBSUMSAPI_PERL):		LF_TGT := -shared -Wl,$(WLOPTION_$(d))
$(LIBSUMSAPI_PERL):		$(OBJ_$(d)) 
				$(LINK)
				$(SLLIB)
				ln -sf $(notdir $@) $(addprefix $(dir $@), SUMSAPI.so)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
