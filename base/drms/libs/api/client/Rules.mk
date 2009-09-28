# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDRMSCLIENT	:= $(d)/libdrmsclient.a

# Common to client and server - keep .o files in parent.
COMMOBJ_$(d)	:= $(addprefix $(d)/../, drms_types.o drms_keyword.o drms_link.o drms_segment.o drms_protocol.o drms_binfile.o drms_series.o drms_parser.o drms_names.o drms_array.o drms_keymap.o drms_dsdsapi.o drms_defs.o drms_fitsrw.o drms_fitstas.o drms_cmdparams.o)

OBJ_$(d)	:= $(addprefix $(d)/, drms_client.o drms_env.o  drms_record.o drms_storageunit.o)

FIOBJ_$(d)	:= $(addprefix $(d)/, drms_fortran.o)
FIOBJ		:= $(FIOBJ) $(FIOBJ_$(d))

# FDRMSMODOBJ is referenced in proj/example/apps/Rules.mk; don't build it unless a fortran compiler
# was found in moreconfigure.pl
FDRMSMOD	=

ifneq ($(JSOC_AUTOCOMPILER),)
  FDRMSMOD	:= $(d)/fdrms.mod
endif

FDRMSMOBJ_$(d)	:= $(addprefix $(d)/, fdrms.o)
FDRMSMODOBJ	:= $(FDRMSMOBJ_$(d))

LIBDRMSCLIENT_OBJ	:= $(COMMOBJ_$(d)) $(OBJ_$(d)) $(FIOBJ_$(d))

DEP_$(d)	:= $(COMMOBJ_$(d):%=%.d) $(OBJ_$(d):%=%.d) $(FIOBJ_$(d):%=%.d) $(FDRMSMOBJ_$(d):%=%.d)

CLEAN		:= $(CLEAN) \
		   $(COMMOBJ_$(d)) \
		   $(OBJ_$(d)) \
		   $(FIOBJ_$(d):%=%.d) \
		   $(FDRMSMOBJ_$(d)) \
		   $(LIBDRMSCLIENT) \
		   $(FDRMSMOD) \
		   $(DEP_$(d)) 

TGT_LIB 	:= $(TGT_LIB) $(LIBDRMSCLIENT) $(FDRMSMOD)
# The following will turn off compilation of the fortran interface module.
# If you uncomment this line, then you must comment out the above line.
# TGT_LIB 	:= $(TGT_LIB) $(LIBDRMSCLIENT) 

S_$(d)		:= $(notdir $(LIBDRMSCLIENT) $(FDRMSMOD))

# Local rules
$(COMMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(COMMOBJ_$(d)):	CF_TGT := $(CF_TGT) -D$(DBNAME) $(CFITSIOH)
$(OBJ_$(d)):		$(SRCDIR)/$(d)/Rules.mk
$(OBJ_$(d)):		CF_TGT := $(CF_TGT) -D$(DBNAME) -DDRMS_CLIENT

$(FDRMSMOBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk


ifeq ($(FCOMPILER), ifort)
$(FDRMSMOBJ_$(d)):	FF_TGT := -module $(d)
else
$(FDRMSMOBJ_$(d)):	FF_TGT := -I $(d)
endif

$(LIBDRMSCLIENT):	$(LIBDRMSCLIENT_OBJ)
			$(ARCHIVE)
			$(SLLIB)

ifneq ($(JSOC_AUTOCOMPILER),)
  $(FDRMSMOD):		$(FDRMSMODOBJ)
endif

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
