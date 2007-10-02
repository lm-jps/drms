# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
EXE_$(d)	:= $(addprefix $(d)/, ingest_tlm soc_pipe_scp)
CEXE		:= $(CEXE) $(EXE_$(d))
CEXESUMS	:= $(CEXESUMS) $(EXE_$(d))

OBJ_$(d)	:= $(EXE_$(d):%=%.o) 
DEP_$(d)	:= $(EXE_$(d):%=%.o.d) 

CLEAN		:= $(CLEAN) \
		   $(OBJ_$(d)) \
		   $(EXE_$(d)) \
		   $(DEP_$(d)) 

TGT_BIN	        := $(TGT_BIN) $(EXE_$(d))

S_$(d)		:= $(notdir $(EXE_$(d)))

# Local rules
$(OBJ_$(d)):	$(SRCDIR)/$(d)/Rules.mk
$(EXE_$(d)):	LL_TGT :=  -L/home/production/cvs/jsoc/lib/saved/$(JSOC_MACHINE) -lhmicomp_egse -lecpg -lpq

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
-include	$(DEP_$(d))

d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
