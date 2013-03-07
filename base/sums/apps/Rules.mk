# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Compiler
SUMSCOMP		= $(COMP)
SUMSLINK		= $(LINK)

# Local variables
sum_svc_comm_obj_$(d)	:= $(addprefix $(d)/, sum_init.o du_dir.o)
sum_svc_obj_$(d)	:= $(addprefix $(d)/, sum_svc_proc.o)
# xsum_svc_obj_$(d)	:= $(addprefix $(d)/, xsum_svc_proc.o)
xsum_svc_obj_$(d)	:= 
tape_svc_obj_$(d)	:= $(addprefix $(d)/, tape_svc_proc.o tapeutil.o tape_inventory.o)
tapearc_obj_$(d)	:= $(addprefix $(d)/, padata.o)

CF_TGT_$(d)	:= -O0 -Wno-parentheses -fno-strict-aliasing
ADD_TGT_$(d) := -DSUMT120 -DSUMNOAO

ifeq ($(HOST),dcs0.jsoc.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMDC -DDCS0
endif
ifeq ($(HOST),dcs1.jsoc.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMDC -DDCS1
endif
ifeq ($(HOST),dcs2.jsoc.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMDC -DDCS2
endif
ifeq ($(HOST),dcs3.jsoc.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMDC -DDCS3
endif
ifeq ($(HOST),d00.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMT120
endif
ifeq ($(HOST),d02.Stanford.EDU)
	ADD_TGT_$(d) := -DSUMT950
endif
#treat j1 like d02 (j1 now running sum_svc)
ifeq ($(HOST),j1)
	ADD_TGT_$(d) := -DSUMT950
endif

CF_TGT_$(d) := $(CF_TGT_$(d)) $(ADD_TGT_$(d))
LL_TGT_$(d) := $(PGL) -lecpg -lssl

MULTI_SUMS_C_$(d) := $(wildcard $(SRCDIR)/$(d)/Salloc*.c) $(wildcard $(SRCDIR)/$(d)/Sdelser*.c) $(wildcard $(SRCDIR)/$(d)/Sinfo*.c) $(wildcard $(SRCDIR)/$(d)/Sput*.c) $(wildcard $(SRCDIR)/$(d)/Sget*.c) $(wildcard $(SRCDIR)/$(d)/Sopen*.c)
MULTI_SUMS_$(d) := $(addprefix $(d)/, sum_svc $(notdir $(patsubst %.c,%,$(MULTI_SUMS_C_$(d)))))

# XSUMSVC_$(d)	:= $(d)/xsum_svc
XSUMSVC_$(d)	:= 
ifneq ($(SUMS_TAPE_AVAILABLE), 0)
TAPESVC_$(d)	:= $(d)/tape_svc
TARCINFO_$(d)	:= $(d)/tapearcinfo
endif
BINTGT_$(d)     := $(addprefix $(d)/, sumget sum_rm sum_rm_0 sum_rm_1 sum_rm_2 impexp exportclosed md5filter sum_adv sum_export_svc sum_export jmtx sum_chmown)

# debug apps
ifneq ($(SUMS_TAPE_AVAILABLE), 0)
BINTGT_2_$(d)	:= $(addprefix $(d)/, main main2 main3 main4 main5 main7)
endif

# other tape apps
ifneq ($(SUMS_TAPE_AVAILABLE), 0)
BINTGT_3_$(d)	:= $(addprefix $(d)/, tapeonoff driveonoff drive0_svc drive1_svc drive2_svc drive3_svc drive4_svc drive5_svc drive6_svc drive7_svc drive8_svc drive9_svc drive10_svc drive11_svc robot0_svc sum_forker tape_svc_restart sumrepartn)
endif

TGT_$(d)        := $(BINTGT_$(d)) $(BINTGT_2_$(d)) $(BINTGT_3_$(d))

# SUMS_BIN contains a list of applications that get built when 'make sums' is invoked
SUMS_BIN	:= $(SUMS_BIN) $(TGT_$(d)) $(XSUMSVC_$(d)) $(MULTI_SUMS_$(d))

OBJ_$(d)	:= $(sum_svc_comm_obj_$(d)) $(sum_svc_obj_$(d)) $(xsum_svc_obj_$(d)) $(tape_svc_obj_$(d)) $(tapearc_obj_$(d)) $(TGT_$(d):%=%.o) $(XSUMSVC_$(d):%=%.o) $(TAPESVC_$(d):%=%.o) $(TARCINFO_$(d):%:%.o) $(MULTI_SUMS_$(d):%=%.o)

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(OBJ_$(d)) $(TGT_$(d)) $(XSUMSVC_$(d)) $(TAPESVC_$(d)) $(TARCINFO_$(d)) $(DEP_$(d)) $(MULTI_SUMS_$(d))

S_$(d)		:= $(notdir $(TGT_$(d)) $(XSUMSVC_$(d)) $(TAPESVC_$(d)) $(TARCINFO_$(d)) $(MULTI_SUMS_$(d)))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):	CF_TGT := $(CF_TGT_$(d))

# Special rules for building driveX_svc.o, X=0,1,2,3 from from driven_svc.c
ifneq ($(SUMS_TAPE_AVAILABLE), 0)
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
endif

# Special rules for building robotX_svc.o, X=0,1,2,3 from from driven_svc.c
ifneq ($(SUMS_TAPE_AVAILABLE), 0)
$(d)/robot0_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_0
$(d)/robot1_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_1
$(d)/robot2_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_2
$(d)/robot3_svc.o:	CF_TGT := $(CF_TGT_$(d)) -DROBOT_3
$(d)/robot%_svc.o:	$(d)/robotn_svc.c
			$(SUMSCOMP)
endif

#$(filter-out $(d)/drive%_svc.o $(d)/robot%_svc.o, $(OBJ_$(d))):	%.o:	%.c
#			$(SUMSCOMP)

# NOTE: tapearc.o depends on libsumspg.a, which in turn depends on padata.o.
# Make doesn't seem to use else ifeq.
ifeq ($(SUMSLINK), $(ICC_LINK))
#SUMSICCLIBS_$(d)	:= -lirc
endif

ifeq ($(SUMSLINK), $(GCC_LINK))
SUMSICCLIBS_$(d)	:= 
endif

# Even though these two programs could have been included in $(TGT_$(d)), we need to define their rules separately so that
# we can control the order of their dependencies. If you omit the following two rules, then the obj files
# get put at the end of the list of dependencies, which is a problem because the obj files depend on 
# $(LIBSUM) and must precede them in the list of dependencies.
$(TAPESVC_$(d)):	LL_TGT :=  $(LL_TGT) $(LL_TGT_$(d))
$(TAPESVC_$(d)):   %:	%.o $(tape_svc_obj_$(d)) $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LIBMISC) $(LIBDSTRUCT)
			$(SUMSLINK)
			$(SLBIN)

$(TARCINFO_$(d)):	LL_TGT :=  $(LL_TGT) $(LL_TGT_$(d))
$(TARCINFO_$(d)):   %:	%.o $(tapearc_obj_$(d)) $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LIBMISC) $(LIBDSTRUCT)
			$(SUMSLINK)
			$(SLBIN)

$(XSUMSVC_$(d)):	LL_TGT :=  $(LL_TGT) $(LL_TGT_$(d))
$(XSUMSVC_$(d)):   %:	%.o $(xsum_svc_obj_$(d)) $(sum_svc_comm_obj_$(d)) $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LIBDEFSSERVER) $(LIBDB) $(LIBMISC) $(LIBDSTRUCT)
			$(SUMSLINK)
			$(SLBIN)

$(TGT_$(d)):		LL_TGT :=  $(LL_TGT) $(LL_TGT_$(d))
$(TGT_$(d)):	%:	%.o $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LIBMISC) $(LIBDSTRUCT)
			$(SUMSLINK)
			$(SLBIN)

$(MULTI_SUMS_$(d)):	LL_TGT := $(LL_TGT) $(LL_TGT_$(d))
$(MULTI_SUMS_$(d)):	%:	$(sum_svc_obj_$(d)) %.o $(sum_svc_comm_obj_$(d)) $(LIBSUM) $(LIBSUMSAPI) $(SUMSICCLIBS_$(d)) $(LIBDEFSSERVER) $(LIBDB) $(LIBMISC) $(LIBDSTRUCT) 
	                        $(SUMSLINK)
				$(SLBIN)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

