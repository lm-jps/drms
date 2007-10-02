# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables

# There are two types of modules: direct-connect modules and socket-connect modules.
# To create a direct-connect module, place the name of the executable in MODEXE.
# To create a socket-connect module, place the name of the executable (with a
# _sock suffix) in MODEXE_SOCK. In the examples below, scalesegments, opendsdsrecs, 
# and demo_td08062007 are socket-connect modules, but there are both direct- and 
# socket-connect versions of hello_world.  Modules created from .f files are always
# created as sockect-connect modules (and placed in the FMODEXE variable, ie., 
# helloworld, xinterp).  
#
# Socket-connect modules can share a single DRMS session, so they are ideal when
# transferring data among them without having to save intermediate data as series.
# Direct-connect modules cannot share data without first saving them to a series, 
# but they can also operate in "read-only" mode.  They will not start up SUMS,
# unless there is an explicit call to do so.  They are ideal for situations like
# calling from a web-interface when you do not want users making changes.
#
# For most science processing, you would want to use socket-connect modules to
# take advantage of the ability to share data in a pipeline.

MODEXE_$(d)	:= $(addprefix $(d)/, hello_world)
MODEXE		:= $(MODEXE) $(MODEXE_$(d))

MODEXE_SOCK_$(d):= $(addsuffix _sock, $(addprefix $(d)/, hello_world scalesegments opendsdsrecs))
FMATHMOD_$(d)	:= $(addsuffix _sock, $(addprefix $(d)/, demo_td08062007))
MODEXE_SOCK	:= $(MODEXE_SOCK) $(MODEXE_SOCK_$(d)) $(FMATHMOD_$(d))

ifeq ($(COMPILER), icc)
  FMODEXE_$(d)	:= $(addsuffix _sock, $(addprefix $(d)/, helloworld xinterp))
  FMODEXE_GONG_$(d)	:= $(addsuffix _sock, $(addprefix $(d)/, f_ingest_gong_mrv f_dup_gong_mrv))
  GONGLIB_$(d)	:= $(d)/ingest_gong_mrv_lib.o
  FMODEXE	:= $(FMODEXE) $(FMODEXE_$(d)) $(FMODEXE_GONG_$(d))
endif

OBJFMATH_$(d)	:= $(FMATHMOD_$(d):%_sock=%.o)
OBJ_$(d)	:= $(MODEXE_SOCK_$(d):%_sock=%.o) $(OBJFMATH_$(d))
OBJF_$(d)	:= $(FMODEXE_$(d):%_sock=%.o) $(FMODEXE_GONG_$(d):%_sock=%.o)

DEP_$(d)	:= $(OBJ_$(d):%=%.o.d) $(OBJF_$(d):%=%.o.d)

CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(OBJF_$(d)) \
		   $(MODEXE_$(d)) \
		   $(FMATHMOD_$(d)) \
		   $(MODEXE_SOCK_$(d))\
		   $(FMODEXE_$(d) \
		   $(FMODEXE_GONG_$(d)) \
		   $(DEP_$(d))

TGT_BIN	        := $(TGT_BIN) 

EXAMPLES	:=  $(MODEXE_$(d)) \
		    $(MODEXE_SOCK_$(d)) \
		    $(FMATHMOD_$(d)) \
		    $(FMODEXE_$(d)) \
		    $(FMODEXE_GONG_$(d))

S_$(d)		:= $(notdir $(MODEXE_$(d))  $(MODEXE_SOCK_$(d)) $(FMATHMOD_$(d)) $(FMODEXE_$(d)) $(FMODEXE_GONG_$(d)))

# Local rules
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJF_$(d)):		$(SRCDIR)/$(d)/Rules.mk

$(OBJFMATH_$(d)):	CF_TGT := $(CF_TGT) $(FMATHLIBSH)
$(FMATHMOD_$(d)):	LL_TGT := $(LL_TGT) $(FMATHLIBSL) -lfftw3f

$(OBJF_$(d)):		CF_TGT := $(CF_TGT) $(FMATHLIBSH)
$(FMODEXE_$(d)):	LL_TGT := $(LL_TGT) $(FMATHLIBSL) -lfftw3f

$(FMODEXE_GONG_$(d)):	$(LIBGONGING)
$(FMODEXE_GONG_$(d)):	LL_TGT := $(LL_TGT) $(FMATHLIBSL) -lcfitsio

# FDRMSMODOBJ is used at COMPILE-time, so can't go in make_basic.mk
$(OBJF_$(d)):		$(FDRMSMODOBJ)
$(GONGLIB_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(GONGLIB_$(d)):	CF_TGT := $(CF_TGT) $(FMATHLIBSH)

$(FMODEXE_$(d)):	FF_TGT := -module $(dir $(FDRMSMODOBJ))
$(FMODEXE_GONG_$(d)):	FF_TGT := -module $(dir $(FDRMSMODOBJ))
$(FMODEXE_GONG_$(d)):	$(GONGLIB_$(d))

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
