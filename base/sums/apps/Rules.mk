# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
sum_svc_obj_$(d)	:= $(addprefix $(d)/, sum_svc_proc.o sum_init.o du_dir.o)
tape_svc_obj_$(d)	:= $(addprefix $(d)/, tape_svc_proc.o tapeutil.o tape_inventory.o)
tapearc_obj_$(d)	:= $(addprefix $(d)/, padata.o)

CF_TGT_$(d)	:= -O0 -Wno-parentheses -fno-strict-aliasing
ifeq ($(HOST),dcs0.jsoc.Stanford.EDU)
	CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMDC
endif
ifeq ($(HOST),dcs1.jsoc.Stanford.EDU)
	CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMDC
endif
ifeq ($(HOST),dcs2.jsoc.Stanford.EDU)
        CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMDC
endif
ifeq ($(HOST),tenerife.tuc.noao.edu)
	CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMNOAO
endif
ifeq ($(HOST),xim.Stanford.EDU)
	CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMNOAO
endif

LL_TGT_$(d) := -lecpg -lpq

SUMSVC_$(d)	:= $(d)/sum_svc
TAPESVC_$(d)	:= $(d)/tape_svc
TARC_$(d)	:= $(d)/tapearc

BINTGT_$(d)	:= $(addprefix $(d)/, main main2 main3 tapeonoff driveonoff sum_rm impexp drive0_svc drive1_svc drive2_svc drive3_svc robot0_svc md5filter)

TGT_$(d)	:= $(BINTGT_$(d)) $(SUMSVC_$(d)) $(TAPESVC_$(d)) $(TARC_$(d))

SUMS_BIN	:= $(SUMS_BIN) $(TGT_$(d))

OBJ_$(d)	:= $(sum_svc_obj_$(d)) $(tape_svc_obj_$(d)) $(tapearc_obj_$(d)) $(TGT_$(d):%=%.o) 

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(TGT_$(d)) $(DEP_$(d)) 

S_$(d)		:= $(notdir $(TGT_$(d)))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):	CF_TGT := $(CF_TGT_$(d))

# Special rules for building driveX_svc.o, X=0,1,2,3 from from driven_svc.c
$(d)/drive0_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_0
$(d)/drive1_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_1
$(d)/drive2_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_2
$(d)/drive3_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_3
$(d)/drive%_svc.o:	$(d)/driven_svc.c
			$(GCC_COMP)

# Special rules for building robotX_svc.o, X=0,1,2,3 from from driven_svc.c
$(d)/robot0_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_0
$(d)/robot1_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_1
$(d)/robot2_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_2
$(d)/robot3_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_3
$(d)/robot%_svc.o:	$(d)/robotn_svc.c
			$(GCC_COMP)

$(filter-out $(d)/drive%_svc.o $(d)/robot%_svc.o, $(OBJ_$(d))):	%.o:	%.c
			$(GCC_COMP)

$(SUMSVC_$(d)):		$(sum_svc_obj_$(d))
$(TAPESVC_$(d)):	$(tape_svc_obj_$(d))
$(TARC_$(d)):		$(tapearc_obj_$(d))

# NOTE: tapearc.o depends on libsumspg.a, which in turn depends on padata.o.

$(TGT_$(d)):		LL_TGT :=  $(LIBSUM) $(LIBSUMSAPI) $(LL_TGT) $(LL_TGT_$(d))
$(TGT_$(d)):	%:	%.o $(LIBSUM) $(LIBSUMSAPI) $(LIBMISC)
			$(GCC_LINK)
			$(SLBIN)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

