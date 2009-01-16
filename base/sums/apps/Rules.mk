# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Compiler
SUMSCOMP		= $(COMP)
SUMSLINK		= $(LINK)

# Local variables
sum_svc_obj_$(d)	:= $(addprefix $(d)/, sum_svc_proc.o sum_init.o du_dir.o)
xsum_svc_obj_$(d)	:= $(addprefix $(d)/, sum_svc_proc.o sum_init.o du_dir.o)
tape_svc_obj_$(d)	:= $(addprefix $(d)/, tape_svc_proc.o tapeutil.o tape_inventory.o)
tapearc_obj_$(d)	:= $(addprefix $(d)/, padata.o)

CF_TGT_$(d)	:= -O0 -Wno-parentheses -fno-strict-aliasing
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
ifeq ($(HOST),dcs3.jsoc.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),d00.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMT120
endif
ifeq ($(HOST),d02.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMT950
endif
#ifeq ($(HOST),tenerife.tuc.noao.edu)
#	ADD_TGT_$(d) := -DSUMNOAO -DSUMT120
#endif
#ifeq ($(HOST),xim.Stanford.EDU)
#	ADD_TGT_$(d) := -DXXX -DSUMNOAO -DSUMT120
#endif
CF_TGT_$(d) := $(CF_TGT_$(d)) $(ADD_TGT_$(d))

LL_TGT_$(d) := $(ECPGL) -lecpg -lpgtypes -lpq

SUMSVC_$(d)	:= $(d)/sum_svc
XSUMSVC_$(d)	:= $(d)/xsum_svc
TAPESVC_$(d)	:= $(d)/tape_svc
TARC_$(d)	:= $(d)/tapearc
TARCINFO_$(d)	:= $(d)/tapearcinfo

BINTGT_$(d)	:= $(addprefix $(d)/, main main2 main3 main4 main5 sumget tapeonoff driveonoff sum_rm impexp drive0_svc drive1_svc drive2_svc drive3_svc drive4_svc drive5_svc drive6_svc drive7_svc drive8_svc drive9_svc drive10_svc drive11_svc robot0_svc md5filter sum_adv)

#BINTGT_$(d)	:= $(addprefix $(d)/, main main2 main3 main4 main5 sumget tapeonoff driveonoff sum_rm impexp drive0_svc drive1_svc drive2_svc drive3_svc robot0_svc md5filter)

TGT_$(d)	:= $(BINTGT_$(d)) $(SUMSVC_$(d)) $(XSUMSVC_$(d)) $(TAPESVC_$(d)) $(TARC_$(d)) $(TARCINFO_$(d))

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
$(d)/drive4_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_4
$(d)/drive5_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_5
$(d)/drive6_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_6
$(d)/drive7_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_7
$(d)/drive8_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_8
$(d)/drive9_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_9
$(d)/drive10_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_10
$(d)/drive11_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DDRIVE_11
$(d)/drive%_svc.o:	$(d)/driven_svc.c
			$(SUMSCOMP)

# Special rules for building robotX_svc.o, X=0,1,2,3 from from driven_svc.c
$(d)/robot0_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_0
$(d)/robot1_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_1
$(d)/robot2_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_2
$(d)/robot3_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_3
$(d)/robot%_svc.o:	$(d)/robotn_svc.c
			$(SUMSCOMP)

$(filter-out $(d)/drive%_svc.o $(d)/robot%_svc.o, $(OBJ_$(d))):	%.o:	%.c
			$(SUMSCOMP)

$(XSUMSVC_$(d)):	$(sum_svc_obj_$(d))
$(SUMSVC_$(d)):		$(sum_svc_obj_$(d))
$(TAPESVC_$(d)):	$(tape_svc_obj_$(d))
$(TARC_$(d)):		$(tapearc_obj_$(d))
$(TARCINFO_$(d)):	$(tapearc_obj_$(d))

# NOTE: tapearc.o depends on libsumspg.a, which in turn depends on padata.o.
# Make doesn't seem to use else ifeq.
ifeq ($(SUMSLINK), $(ICC_LINK))
SUMSICCLIBS_$(d)	:= -lirc
endif

ifeq ($(SUMSLINK), $(GCC_LINK))
SUMSICCLIBS_$(d)	:= 
endif

$(TGT_$(d)):		LL_TGT :=  $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LL_TGT) $(LL_TGT_$(d))
$(TGT_$(d)):	%:	%.o $(LIBSUM) $(LIBSUMSAPI) $(LIBMISC)
			$(SUMSLINK)
			$(SLBIN)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

