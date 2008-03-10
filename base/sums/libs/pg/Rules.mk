# Standard things

sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
ifeq ($(HOST),dcs0.jsoc.Stanford.EDU)
        CF_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),dcs1.jsoc.Stanford.EDU)
        CF_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),dcs2.jsoc.Stanford.EDU)
        CF_TGT_$(d) := -DSUMDC
endif
ifeq ($(HOST),d00.Stanford.EDU)
        CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMT120
endif
ifeq ($(HOST),d02.jsoc.Stanford.EDU)
        CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMT950
endif
ifeq ($(HOST),tenerife.tuc.noao.edu)
        CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMNOAO
endif
ifeq ($(HOST),xim.Stanford.EDU)
        CF_TGT_$(d) := $(CF_TGT_$(d)) -DSUMNOAO
endif

LIBSUM 		:= $(d)/libsumspg.a
PG_$(d)		:= $(addprefix $(d)/, $(notdir $(wildcard $(SRCDIR)/$(d)/*.pgc)))

OBJ_$(d)	:= $(PG_$(d):%.pgc=%.o) $(addprefix $(d)/, $(notdir $(patsubst %.c,%.o,$(wildcard $(SRCDIR)/$(d)/*.c))))

DEP_$(d)	:= $(OBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) $(PG_$(d):%.pgc=%.c) $(OBJ_$(d)) $(LIBSUM) $(DEP_$(d)) 

TGT_LIB 	:= $(TGT_LIB) $(LIBSUM)

S_$(d)		:= $(notdir $(LIBSUM))

# Local rules

#ifeq ($(CC),icc)
#  CFLAGS += -wd167
#endif

$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk $(PG_$(d))
$(OBJ_$(d)):	CF_TGT := -I$(PGIPATH) -O0 $(CF_TGT_$(d))
$(OBJ_$(d)):	%.o:	%.c
		$(ICC_COMP)

$(LIBSUM):	$(OBJ_$(d))
		$(ARCHIVE)
		$(SLLIB)

# Shortcuts

.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things

-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))

