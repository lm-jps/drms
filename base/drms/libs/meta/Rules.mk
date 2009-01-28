# Standard things
sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Local variables
LIBDRMS_META		:= $(d)/libdrms.a
LIBDRMS_META_SOCK	:= $(d)/libdrms_sock.a
LIBDRMS_META_SOCK_F	:= $(d)/libdrms_sock_f.a

BASELIBS_OBJ_$(d)	:= $(LIBTHREADUTIL_OBJ) $(LIBRICECOMP_OBJ) $(LIBMISC_OBJ) $(LIBDEFSCLNT_OBJ) $(LIBDSTRUCT_OBJ) $(LIBTIMEIO_OBJ) $(LIBFITSRW_OBJ)

CLEAN			:= $(CLEAN) \
			   $(LIBDRMS_META) \
			   $(LIBDRMS_META_SOCK) \
			   $(LIBDRMS_META_SOCK_F)

TGT_LIB		 	:= $(TGT_LIB) $(LIBDRMS_META) $(LIBDRMS_META_SOCK) $(LIBDRMS_META_SOCK_F)

S_$(d)			:= $(notdir $(LIBDRMS_META) $(LIBDRMS_META_SOCK) $(LIBDRMS_META_SOCK_F))

# Local rules
$(LIBDRMS_META):	$(SRCDIR)/$(d)/Rules.mk
$(LIBDRMS_META_SOCK):	$(SRCDIR)/$(d)/Rules.mk
$(LIBDRMS_META_SOCK_F):	$(SRCDIR)/$(d)/Rules.mk

$(LIBDRMS_META):	$(LIBJSOC_MAIN_OBJ) $(LIBDRMS_OBJ) $(LIBDB_OBJ) $(LIBSUMSAPI_OBJ) $(LIBCMDPARAMS_OBJ) $(BASELIBS_OBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

$(LIBDRMS_META_SOCK):	$(LIBJSOC_MAIN_SOCK_OBJ) $(LIBDRMSCLIENT_OBJ) $(LIBDBCLIENT_OBJ) $(LIBCMDPARAMS_OBJ) $(BASELIBS_OBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

$(LIBDRMS_META_SOCK_F):	$(LIBJSOC_MAIN_SOCK_F_OBJ) $(LIBINTHANDLESF) $(LIBDRMSCLIENT_OBJ) $(LIBDBCLIENT_OBJ) $(LIBCMDPARAMSF_OBJ) $(BASELIBS_OBJ_$(d))
			$(ARCHIVE)
			$(SLLIB)

# Shortcuts
.PHONY:	$(S_$(d))
$(S_$(d)):	%:	$(d)/%

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
