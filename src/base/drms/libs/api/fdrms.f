CC
CC This Fortran module contains the DRMS to Fortran
CC SUBROUTINE AND FUNCTION declarations
CC See examples:
CC    f_ingest_gong_mrv.f
CC    f_dup_gong_mrv.f
CC
      MODULE FDRMS
        INTEGER, PARAMETER :: FHANDLEKEYSIZE=256
        INTEGER, PARAMETER :: DRMS_MAXNAMELEN=31
        INTEGER, PARAMETER :: DRMS_MAXHASHKEYLEN=DRMS_MAXNAMELEN+21
        INTEGER, PARAMETER :: DRMS_MAXUNITLEN=32
        INTEGER, PARAMETER :: DRMS_MAXQUERYLEN=8191
        INTEGER, PARAMETER :: DRMS_MAXPATHLEN=511
        INTEGER, PARAMETER :: DRMS_MAXFORMATLEN=22
        INTEGER, PARAMETER :: DRMS_MAXRANK=16
        INTEGER, PARAMETER :: DRMS_MAXSEGMENTS=254
        INTEGER, PARAMETER :: DRMS_MAXCOMMENTLEN=254
        INTEGER, PARAMETER :: DRMS_MAXSEGFILENAME=256
CC      Max number of keywords in the primary index.
        INTEGER, PARAMETER :: DRMS_MAXPRIMIDX=5
CC      Max length of the string holding a keyword default value.
        INTEGER, PARAMETER :: DRMS_DEFVAL_MAXLEN=1000
        INTEGER, PARAMETER :: DRMS_MAXLINKDEPTH=20
CC      DRMS_Type_t enum
        INTEGER, PARAMETER :: DRMS_TYPE_CHAR=0, DRMS_TYPE_SHORT=1,
     2DRMS_TYPE_INT=2, DRMS_TYPE_LONGLONG=3, DRMS_TYPE_FLOAT=4,
CC     2DRMS_TYPE_DOUBLE=5, DRMS_TYPE_TIME=6, DRMS_TYPE_STRING=7,
     2DRMS_TYPE_RAW=8
        INTEGER, PARAMETER :: DRMS_TYPE_DOUBLE=5
   
CC      DRMS_RecLifetime_t enum
        INTEGER, PARAMETER :: DRMS_PERMANENT=0, DRMS_TRANSIENT=1

CC      DRMS_Link_Type_t enum
        INTEGER, PARAMETER ::  STATIC_LINK=0, DYNAMIC_LINK=1

CC      DRMS_Segment_Scope_t enum
        INTEGER, PARAMETER :: DRMS_CONSTANT=0, DRMS_VARIABLE=1,
     2DRMS_VARDIM=2

CC      DRMS_CloseAction_t enum
        INTEGER, PARAMETER ::  DRMS_FREE_RECORD=0, DRMS_INSERT_RECORD=1

CC      DRMS_CloneAction_t enum
        INTEGER, PARAMETER ::  DRMS_COPY_SEGMENTS=0,
     2DRMS_SHARE_SEGMENTS=1
        INTERFACE
          LOGICAL FUNCTION f_isnull(A1)
            CHARACTER*256 A1
          END FUNCTION f_isnull

          CHARACTER*256 FUNCTION f_drms_env_handle()
          END FUNCTION f_drms_env_handle

          CHARACTER*256 FUNCTION f_drms_array_create(N1,N2,N3,AA1,N4)
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2,N3,N4
          END FUNCTION f_drms_array_create

          CHARACTER*256 FUNCTION f_drms_array_create_empty(N1,N2,N3,N4)
            INTEGER N1,N2,N3,N4
          END FUNCTION f_drms_array_create_empty

          SUBROUTINE f_drms_free_array(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_array

          SUBROUTINE f_drms_array_convert_inplace(N1,R1,R2,A1)
            REAL*8 R1,R2
            INTEGER N1
            CHARACTER*256 A1
          END SUBROUTINE f_drms_array_convert_inplace

          CHARACTER*256 FUNCTION f_drms_array_convert(N1,R1,R2,A1)
            REAL*8 R1,R2
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_array_convert

          CHARACTER*256 FUNCTION f_drms_array_slice(N1,N2,A1)
            INTEGER N1,N2
            CHARACTER*256 A1
          END FUNCTION f_drms_array_slice

          CHARACTER*256 FUNCTION f_drms_array_permute(A1,N1,N2)
            INTEGER N1,N2
            CHARACTER*256 A1
          END FUNCTION f_drms_array_permute

          INTEGER FUNCTION f_drms_array_rawconvert(N1,N2,R1,R2,AA1,N3,
     2                     AA2)
            REAL*8 R1,R2
            INTEGER*2, ALLOCATABLE:: AA1(:),AA2(:)
            INTEGER N1,N2,N3
          END FUNCTION f_drms_array_rawconvert

          SUBROUTINE f_drms_array2missing(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_array2missing

          SUBROUTINE f_drms_array_print(A1,A2,A3)
            CHARACTER*256 A1,A2,A3
          END SUBROUTINE f_drms_array_print

          INTEGER FUNCTION f_drms_array2char(N1,N2,R1,R2,AA1,A1)
            REAL*8 R1,R2
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
            CHARACTER*256 A1
          END FUNCTION f_drms_array2char

          INTEGER FUNCTION f_drms_array2short(N1,N2,R1,R2,AA1,S1)
            REAL*8 R1,R2
            INTEGER*2 S1
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
          END FUNCTION f_drms_array2short

          INTEGER FUNCTION f_drms_array2int(N1,N2,R1,R2,AA1,N3)
            REAL*8 R1,R2
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2,N3
          END FUNCTION f_drms_array2int

          INTEGER FUNCTION f_drms_array2longlong(N1,N2,R1,R2,AA1,ML1)
            REAL*8 R1,R2
            INTEGER*8 ML1
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
          END FUNCTION f_drms_array2longlong

          INTEGER FUNCTION f_drms_array2float(N1,N2,R1,R2,AA1,MF1)
            REAL*8 R1,R2
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
            REAL MF1
          END FUNCTION f_drms_array2float

          INTEGER FUNCTION f_drms_array2double(N1,N2,R1,R2,AA1,R3)
            REAL*8 R1,R2,R3
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
          END FUNCTION f_drms_array2double

          INTEGER FUNCTION f_drms_array2time(N1,N2,R1,R2,AA1,R3)
            REAL*8 R1,R2,R3
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
          END FUNCTION f_drms_array2time

          INTEGER FUNCTION f_drms_array2string(N1,N2,R1,R2,AA1,AA2)
            REAL*8 R1,R2
            INTEGER*2, ALLOCATABLE:: AA1(:)
            CHARACTER*1, ALLOCATABLE:: AA2(:)
            INTEGER N1,N2
          END FUNCTION f_drms_array2string

          CHARACTER*256 FUNCTION f_drms_connect(A1,S1,A2,A3)
            INTEGER*2 S1
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_connect

          SUBROUTINE f_drms_disconnect(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END SUBROUTINE f_drms_disconnect

          INTEGER FUNCTION f_drms_commit(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_commit

          INTEGER FUNCTION f_drms_rollback(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_rollback

          CHARACTER*256 FUNCTION f_drms_query_txt(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_query_txt

          CHARACTER*256 FUNCTION f_drms_query_bin(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_query_bin

CC          CHARACTER*256 FUNCTION f_drms_query_binv(A1,A2)
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_query_binv

CC          CHARACTER*256 FUNCTION f_drms_query_bin_array(A1,A2,N1,N2)
CC            INTEGER N1,N2
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_query_bin_array

          INTEGER FUNCTION f_drms_dms(A1,N1,A2)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_dms

CC          INTEGER FUNCTION f_drms_dmsv(A1,N1,A2,N2)
CC            INTEGER N1,N2
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_dmsv

CC          INTEGER FUNCTION f_drms_dms_array(A1,N1,A2,N2,N3,N4)
CC            INTEGER N1,N2,N3,N4
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_dms_array

CC          INTEGER FUNCTION f_drms_bulk_insertv(A1,A2,N1,N2)
CC            INTEGER N1,N2
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_bulk_insertv

CC          INTEGER FUNCTION f_drms_bulk_insert_array(A1,A2,N1,N2,N3)
CC            INTEGER N1,N2,N3
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_bulk_insert_array

          INTEGER FUNCTION f_drms_sequence_drop(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_sequence_drop

          INTEGER FUNCTION f_drms_sequence_create(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_sequence_create

          SUBROUTINE f_drms_sequence_getnext(ML1,A1,A2,N1)
            INTEGER*8 ML1
            INTEGER N1
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_sequence_getnext

          INTEGER*8 FUNCTION f_drms_sequence_getcurrent(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_sequence_getcurrent

          INTEGER*8 FUNCTION f_drms_sequence_getlast(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_sequence_getlast

          SUBROUTINE f_drms_alloc_recnum(ML1,A1,A2,N1,N2)
            INTEGER*8 ML1
            INTEGER N1,N2
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_alloc_recnum

          CHARACTER*256 FUNCTION f_drms_getunit(A1,A2,M1,N1,N2)
            INTEGER N1,N2
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getunit

          INTEGER FUNCTION f_drms_newslots(A1,N1,A2,ML1,N2,N3,A3)
            INTEGER*8 ML1
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_newslots

          INTEGER FUNCTION f_drms_slot_setstate(A1,A2,M1,N1,N2)
            INTEGER N1,N2
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_slot_setstate

          INTEGER FUNCTION f_drms_create_series(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_create_series

          INTEGER FUNCTION f_drms_update_series(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_update_series

CC          INTEGER FUNCTION f_drms_compress(N1,N2,N3,AA1,N4,A1)
CC            CHARACTER*1, ALLOCATABLE:: AA1(:)
CC            INTEGER N1,N2,N3,N4
CC            CHARACTER*256 A1
CC          END FUNCTION f_drms_compress

CC          INTEGER FUNCTION f_drms_uncompress(N1,N2,A1,N3,N4,AA1)
CC            CHARACTER*1, ALLOCATABLE:: AA1(:)
CC            INTEGER N1,N2,N3,N4
CC            CHARACTER*256 A1
CC          END FUNCTION f_drms_uncompress

          INTEGER FUNCTION f_drms_writefits(A1,N1,N2,A2,A3)
            INTEGER N1,N2
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_writefits

          CHARACTER*256 FUNCTION f_drms_readfits(A1,N1,N2,AA1,N3)
            CHARACTER*1, ALLOCATABLE:: AA1 (:)
            INTEGER N1,N2,N3
            CHARACTER*256 A1
          END FUNCTION f_drms_readfits

          CHARACTER*256 FUNCTION f_drms_readfits2(A1,N1,N2,A2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_readfits2

          SUBROUTINE f_drms_print_fitsheader(N1,A1)
            INTEGER N1
            CHARACTER*256 A1
          END SUBROUTINE f_drms_print_fitsheader

          SUBROUTINE f_drms_free_template_keyword_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_template_keyword_struct

          SUBROUTINE f_drms_free_keyword_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_keyword_struct

          SUBROUTINE f_drms_copy_keyword_struct(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_copy_keyword_struct

          SUBROUTINE f_drms_keyword_print(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_keyword_print

          SUBROUTINE f_drms_keyword_printval(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_keyword_printval

          INTEGER FUNCTION f_drms_template_keywords(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_template_keywords

          CHARACTER*256 FUNCTION f_drms_keyword_lookup(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_keyword_lookup

          INTEGER FUNCTION f_drms_keyword_type(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_keyword_type

          INTEGER*2 FUNCTION f_drms_getkey_char(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_char

          INTEGER*2 FUNCTION f_drms_getkey_short(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_short

          INTEGER FUNCTION f_drms_getkey_int(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_int

          INTEGER*8 FUNCTION f_drms_getkey_longlong(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_longlong

          REAL FUNCTION f_drms_getkey_float(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_float

          REAL*8 FUNCTION f_drms_getkey_double(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_double

          CHARACTER*256 FUNCTION f_drms_getkey_string(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_getkey_string

          INTEGER FUNCTION f_drms_setkey_char(A1,A2,B1)
            CHARACTER*1 B1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_char

          INTEGER FUNCTION f_drms_setkey_short(A1,A2,S1)
            INTEGER*2 S1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_short

          INTEGER FUNCTION f_drms_setkey_int(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_int

          INTEGER FUNCTION f_drms_setkey_longlong(A1,A2,M1)
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_longlong

          INTEGER FUNCTION f_drms_setkey_float(A1,A2,MF1)
            REAL MF1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_float

          INTEGER FUNCTION f_drms_setkey_double(A1,A2,R1)
            REAL*8 R1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setkey_double

          INTEGER FUNCTION f_drms_setkey_string(A1,A2,A3)
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_setkey_string

          SUBROUTINE f_drms_free_template_link_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_template_link_struct

          SUBROUTINE f_drms_free_link_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_link_struct

          SUBROUTINE f_drms_copy_link_struct(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_copy_link_struct

          INTEGER FUNCTION f_drms_setlink_static(A1,A2,M1)
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_setlink_static

CC          INTEGER FUNCTION f_drms_setlink_dynamic(A1,A2,N1)
CC            INTEGER N1
CC            CHARACTER*256 A1,A2
CC          END FUNCTION f_drms_setlink_dynamic

          CHARACTER*256 FUNCTION f_drms_link_follow(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_link_follow

          CHARACTER*256 FUNCTION f_drms_link_followall(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_link_followall

          SUBROUTINE f_drms_link_print(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_link_print

          INTEGER FUNCTION f_drms_template_links(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_template_links

          SUBROUTINE f_drms_link_getpidx(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_link_getpidx

          SUBROUTINE f_free_record_set(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_free_record_set

          INTEGER FUNCTION f_drms_recordset_query(A1,A2,AA1,A3,N1,N2)
            CHARACTER*1, ALLOCATABLE:: AA1 (:)
            INTEGER N1,N2
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_recordset_query

          INTEGER FUNCTION f_get_rs_num(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_rs_num

          CHARACTER*256 FUNCTION f_get_rs_record(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_get_rs_record
CC
CC        START seriesinfo getors
CC
          CHARACTER*256 FUNCTION f_get_si_seriesname(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_seriesname

          CHARACTER*256 FUNCTION f_get_si_description(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_description

          CHARACTER*256 FUNCTION f_get_si_owner(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_owner

          INTEGER FUNCTION f_get_si_unitsize(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_unitsize

          INTEGER FUNCTION f_get_si_archive(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_archive

          INTEGER FUNCTION f_get_si_retention(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_retention

          INTEGER FUNCTION f_get_si_tapegroup(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_tapegroup

          INTEGER FUNCTION f_get_si_pidx_num(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_si_pidx_num

          SUBROUTINE f_get_keyword_handleV(A1, A2)
CC            CHARACTER*256 A1(DRMS_MAXPRIMIDX), A2
            CHARACTER(*) A1(:), A2
          END SUBROUTINE f_get_keyword_handleV

CC        END seriesinfo getors

          CHARACTER*256 FUNCTION f_get_record_env(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_env

          INTEGER*8 FUNCTION f_get_record_recnum(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_recnum

          INTEGER*8 FUNCTION f_get_record_sunum(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_sunum

          INTEGER FUNCTION f_get_record_init(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_init

          INTEGER FUNCTION f_get_record_readonly(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_readonly

          INTEGER FUNCTION f_get_record_lifetime(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_lifetime

          CHARACTER*256 FUNCTION f_get_record_su(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_su

          INTEGER FUNCTION f_get_record_slotnum(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_slotnum

          INTEGER*8 FUNCTION f_get_record_sessionid(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_sessionid

          CHARACTER*256 FUNCTION f_get_record_sessionns(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_sessionns

          CHARACTER*256 FUNCTION f_get_record_seriesinfo(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_seriesinfo

          CHARACTER*256 FUNCTION f_get_record_keywords(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_keywords

          CHARACTER*256 FUNCTION f_get_record_links(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_links

          CHARACTER*256 FUNCTION f_get_record_segments(A1)
            CHARACTER*256 A1
          END FUNCTION f_get_record_segments

          CHARACTER*256 FUNCTION f_drms_open_records(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_open_records

          CHARACTER*256 FUNCTION f_drms_create_records(A1,N1,A2,N2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_create_records

          CHARACTER*256 FUNCTION f_drms_create_records2(A1,N1,A2,N2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_create_records2

          CHARACTER*256 FUNCTION f_drms_clone_records(A1,N1,N2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1
          END FUNCTION f_drms_clone_records

          INTEGER FUNCTION f_drms_close_records(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_close_records

          SUBROUTINE f_drms_free_records(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_records

          CHARACTER*256 FUNCTION f_drms_create_record(A1,A2,N1,N2)
            INTEGER N1,N2
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_create_record

          CHARACTER*256 FUNCTION f_drms_clone_record(A1,N1,N2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1
          END FUNCTION f_drms_clone_record

          INTEGER FUNCTION f_drms_close_record(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_close_record

          INTEGER FUNCTION f_drms_closeall_records(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_closeall_records

          SUBROUTINE f_drms_free_record(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_record

          CHARACTER*256 FUNCTION f_drms_retrieve_record(A1,A2,M1,N1)
            INTEGER N1
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_retrieve_record

          CHARACTER*256 FUNCTION f_drms_retrieve_records(A1,A2,A3,N1,N2
     2,N3)
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_retrieve_records

          CHARACTER*256 FUNCTION f_drms_alloc_record2(A1,M1,N1)
            INTEGER N1
            INTEGER*8 M1
            CHARACTER*256 A1
          END FUNCTION f_drms_alloc_record2

          CHARACTER*256 FUNCTION f_drms_alloc_record(A1,A2,M1,N1)
            INTEGER N1
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_alloc_record

          CHARACTER*256 FUNCTION f_drms_template_record(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_template_record

          SUBROUTINE f_drms_free_template_record_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_template_record_struct

          SUBROUTINE f_drms_free_record_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_record_struct

          SUBROUTINE f_drms_copy_record_struct(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_copy_record_struct

          INTEGER FUNCTION f_drms_populate_record(A1,M1)
            INTEGER*8 M1
            CHARACTER*256 A1
          END FUNCTION f_drms_populate_record

          INTEGER FUNCTION f_drms_populate_records(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_populate_records

          CHARACTER*256 FUNCTION f_drms_field_list(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_field_list

          INTEGER FUNCTION f_drms_insert_records(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_insert_records

          INTEGER*8 FUNCTION f_drms_record_size(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_size

          SUBROUTINE f_drms_print_record(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_print_record

          INTEGER FUNCTION f_drms_record_numkeywords(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_numkeywords

          INTEGER FUNCTION f_drms_record_numlinks(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_numlinks

          INTEGER FUNCTION f_drms_record_numsegments(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_numsegments

          INTEGER FUNCTION f_drms_record_num_nonlink_segments(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_num_nonlink_segments

          SUBROUTINE f_drms_record_directory(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_record_directory

          CHARACTER*256 FUNCTION f_drms_record_fopen(A1,A2,A3)
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_record_fopen

          INTEGER*8 FUNCTION f_drms_record_memsize(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_record_memsize

          INTEGER FUNCTION f_drms_record_setseriesinfo(A1,N1,N2,N3,N4,
     2                     A2)
            INTEGER N1,N2,N3,N4
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_record_setseriesinfo

          INTEGER FUNCTION f_drms_record_setsegdims(A1,A2,A3)
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_record_setsegdims

          SUBROUTINE f_drms_record_copysegnames(AA1,A1,N1,N2)
            CHARACTER*1, ALLOCATABLE:: AA1 (:)
            INTEGER N1,N2
            CHARACTER*256 A1
          END SUBROUTINE f_drms_record_copysegnames

          SUBROUTINE f_drms_free_template_segment_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_template_segment_struct

          SUBROUTINE f_drms_free_segment_struct(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_free_segment_struct

          SUBROUTINE f_drms_copy_segment_struct(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_copy_segment_struct

          INTEGER FUNCTION f_drms_template_segments(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_template_segments

          SUBROUTINE f_drms_segment_print(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_segment_print

          INTEGER*8 FUNCTION f_drms_segment_size(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_size

          SUBROUTINE f_drms_segment_getdims(N1,A1,A2,A3)
            INTEGER N1
            CHARACTER*256 A1,A2,A3
          END SUBROUTINE f_drms_segment_getdims

          SUBROUTINE f_drms_segment_filename(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_segment_filename

          INTEGER FUNCTION f_drms_delete_segmentfile(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_delete_segmentfile

          INTEGER FUNCTION f_drms_segment_setscaling(A1,R1,R2)
            REAL*8 R1,R2
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_setscaling

          INTEGER FUNCTION f_drms_segment_getscaling(A1,R1,R2)
            REAL*8 R1,R2
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_getscaling

          CHARACTER*256 FUNCTION f_drms_segment_lookup(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_segment_lookup

          CHARACTER*256 FUNCTION f_drms_segment_lookupnum(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_lookupnum

          CHARACTER*256 FUNCTION f_drms_segment_read(A1,N1,N2)
            INTEGER N1,N2
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_read

          CHARACTER*256 FUNCTION f_drms_segment_readslice(A1,N1,N2,N3,
     2                           N4)
            INTEGER N1,N2,N3,N4
            CHARACTER*256 A1
          END FUNCTION f_drms_segment_readslice

          INTEGER FUNCTION f_drms_segment_write(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_segment_write

          SUBROUTINE f_drms_segment_setblocksize(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END SUBROUTINE f_drms_segment_setblocksize

          SUBROUTINE f_drms_segment_getblocksize(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END SUBROUTINE f_drms_segment_getblocksize

          SUBROUTINE f_drms_segment_autoscale(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_segment_autoscale

          INTEGER FUNCTION f_drms_series_exists(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_series_exists

          INTEGER FUNCTION f_drms_insert_series(A1,N1,A2,N2)
            INTEGER N1,N2
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_insert_series

          INTEGER FUNCTION f_drms_delete_series(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_delete_series

          SUBROUTINE f_drms_series_createpkeyarray(AA1,A1,A2,N1,N2)
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1,N2
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_series_createpkeyarray

          SUBROUTINE f_drms_series_destroypkeyarray(AA1)
            CHARACTER*1, ALLOCATABLE:: AA1(:)
          END SUBROUTINE f_drms_series_destroypkeyarray

          INTEGER FUNCTION f_drms_series_checkseriescompat(A1,A2,A3,A4,
     2                     N1)
            INTEGER N1
            CHARACTER*256 A1,A2,A3,A4
          END FUNCTION f_drms_series_checkseriescompat

          INTEGER FUNCTION f_drms_series_checkrecordcompat(A1,A2,A3,A4,
     2                     N1)
            INTEGER N1
            CHARACTER*256 A1,A2,A3,A4
          END FUNCTION f_drms_series_checkrecordcompat

          INTEGER*8 FUNCTION f_drms_su_alloc(A1,R1,A2,N1)
            REAL*8 R1
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_su_alloc

          INTEGER FUNCTION f_drms_su_newslots(A1,N1,A2,ML1,N2,N3,A3)
            INTEGER*8 ML1
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2,A3
          END FUNCTION f_drms_su_newslots

          INTEGER FUNCTION f_drms_su_getsudir(A1,A2,N1)
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_su_getsudir

          INTEGER FUNCTION f_drms_commitunit(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_commitunit

          INTEGER FUNCTION f_drms_commit_all_units(A1,N1)
            INTEGER N1
            CHARACTER*256 A1
          END FUNCTION f_drms_commit_all_units

CC          CHARACTER*256 FUNCTION f_drms_su_lookup(A1,A2,R1,A3)
CC            REAL*8 R1
CC            CHARACTER*256 A1,A2,A3
CC          END FUNCTION f_drms_su_lookup

          SUBROUTINE f_drms_su_freeunit(A1)
            CHARACTER*256 A1
          END SUBROUTINE f_drms_su_freeunit

          SUBROUTINE f_drms_freeunit(A1,A2)
            CHARACTER*256 A1,A2
          END SUBROUTINE f_drms_freeunit

          INTEGER FUNCTION f_drms_su_freeslot(A1,A2,M1,N1)
            INTEGER N1
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_su_freeslot

          CHARACTER*256 FUNCTION f_drms_su_markslot(A1,A2,M1,N1,N2)
            INTEGER N1,N2
            INTEGER*8 M1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_su_markslot

          INTEGER FUNCTION f_drms_tasfile_read(A1,N1,R1,R2,A2)
            REAL*8 R1,R2
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_read

          INTEGER FUNCTION f_drms_tasfile_readslice(A1,N1,R1,R2,N2,N3,
     2                     N4,A2)
            REAL*8 R1,R2
            INTEGER N1,N2,N3,N4
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_readslice

          INTEGER FUNCTION f_drms_tasfile_writeslice(A1,R1,R2,N1,A2)
            REAL*8 R1,R2
            INTEGER N1
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_writeslice

          INTEGER FUNCTION f_drms_tasfile_write(A1,N1,R1,R2,N2,N3,A2)
            REAL*8 R1,R2
            INTEGER N1,N2,N3
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_write

          INTEGER FUNCTION f_drms_tasfile_create(A1,N1,N2,N3,N4,N5,A2)
            INTEGER N1,N2,N3,N4,N5
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_create

          INTEGER FUNCTION f_drms_tasfile_defrag(A1,A2)
            CHARACTER*256 A1,A2
          END FUNCTION f_drms_tasfile_defrag

CC          INTEGER FUNCTION f_drms_copy_db2drms(N1,N2,A1)
CC            INTEGER N1,N2
CC            CHARACTER*256 A1
CC          END FUNCTION f_drms_copy_db2drms

CC          SUBROUTINE f_drms_copy_drms2drms(N1)
CC            INTEGER N1
CC          END SUBROUTINE f_drms_copy_drms2drms

          INTEGER FUNCTION f_drms_str2type(A1)
            CHARACTER*256 A1
          END FUNCTION f_drms_str2type

          CHARACTER*256 FUNCTION f_drms_type2str(N1)
            INTEGER N1
          END FUNCTION f_drms_type2str

          INTEGER FUNCTION f_drms_sizeof(N1)
            INTEGER N1
          END FUNCTION f_drms_sizeof

          INTEGER FUNCTION f_drms_printfval_raw(N1,AA1)
            CHARACTER*1, ALLOCATABLE:: AA1(:)
            INTEGER N1
          END FUNCTION f_drms_printfval_raw

          SUBROUTINE f_drms_byteswap(N1,N2,A1)
            INTEGER N1,N2
            CHARACTER*256 A1
          END SUBROUTINE f_drms_byteswap

          CHARACTER*256 FUNCTION f_new_hiterator(A1)
            CHARACTER(*) A1
          END FUNCTION f_new_hiterator

          CHARACTER*256 FUNCTION f_hiterator_getnext(A1)
            CHARACTER(*) A1
          END FUNCTION f_hiterator_getnext

          SUBROUTINE f_get_array_data(A1, A2)
            CHARACTER(*) A2
            INTEGER*2, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data

          SUBROUTINE f_get_array_data_byte(A1, A2)
            INTEGER*2, ALLOCATABLE :: A1(:)
            CHARACTER(*) A2
          END SUBROUTINE f_get_array_data_byte

          SUBROUTINE f_get_array_data_integer(A1, A2)
            CHARACTER(*) A2
            INTEGER, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data_integer

          SUBROUTINE f_get_array_data_integer8(A1, A2)
            CHARACTER(*) A2
            INTEGER*8, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data_integer8

          SUBROUTINE f_get_array_data_real(A1, A2)
            CHARACTER(*) A2
            REAL, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data_real

          SUBROUTINE f_get_array_data_real8(A1, A2)
            CHARACTER(*) A2
            REAL*8, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data_real8

          SUBROUTINE f_get_array_data_double(A1, A2)
            CHARACTER(*) A2
            DOUBLE PRECISION, ALLOCATABLE :: A1(:)
          END SUBROUTINE f_get_array_data_double

           SUBROUTINE DRAW_A_CROSS(A, AXIS)
             INTEGER*2, ALLOCATABLE :: A (:)
             INTEGER AXIS
           END SUBROUTINE
        END INTERFACE
      END MODULE FDRMS
