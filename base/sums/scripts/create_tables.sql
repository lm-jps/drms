!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
rem NOTE: !!!! see Postgres dir for latest
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
rem hmi0:/home/ora10/SUM/create_tables.sql
rem

rem Table_ Name: SUM_MAIN
rem Main table. One entry of every storage unit in the database.

create table SUM_MAIN (
 ONLINE_LOC             VARCHAR2(80),
 ONLINE_STATUS          VARCHAR2(5),
 ARCHIVE_STATUS         VARCHAR2(5),
 OFFSITE_ACK            VARCHAR2(5),
 HISTORY_COMMENT        VARCHAR2(80),
 OWNING_SERIES          VARCHAR2(80),
 STORAGE_GROUP          NUMBER(5),
 STORAGE_SET            NUMBER(5),
 BYTES                  NUMBER(20),
 DS_INDEX               NUMBER(38),
 CREATE_SUMID           NUMBER(38) NOT NULL,
 CREAT_DATE             DATE,
 ACCESS_DATE            DATE,
 USERNAME               VARCHAR2(10),
 ARCH_TAPE              NUMBER(20),
 ARCH_TAPE_POS          VARCHAR2(15),
 ARCH_TAPE_FN           NUMBER(5),
 ARCH_TAPE_DATE         DATE,
 WARNINGS               VARCHAR2(260),
 STATUS                 NUMBER(5),
 SAFE_TAPE              NUMBER(20),
 SAFE_TAPE_POS          VARCHAR2(15),
 SAFE_TAPE_FN           NUMBER(5),
 SAFE_TAPE_DATE         DATE,
constraint pk_summain primary key (DS_INDEX)
        );
commit;

rem ===============================================================

rem Table_Name: SUM_OPEN
rem One entry for every active SUM_Open() done by DRMS

create table SUM_OPEN (
    SUMID      NUMBER(38) not null,
    TBD        NUMBER(20) not null
);
commit;

rem ===============================================================

create table SUM_PARTN_ALLOC (
    wd                 VARCHAR2(80) not null,
    sumid              NUMBER(38) not null,
    status             NUMBER(5) not null,
    bytes              NUMBER(20),
    effective_date     VARCHAR2(10),
    archive_substatus  NUMBER(5),
    group_id           NUMBER(4),
    ds_index           NUMBER(38) not null,
    safe_id            NUMBER(4)
    );
commit;

rem ===============================================================
create table SUM_PARTN_AVAIL (
       partn_name    VARCHAR2(80) not null,
       total_bytes   NUMBER(20) not null,
       avail_bytes   NUMBER(20) not null,
       pds_set_num   NUMBER(4) not null
       );
commit;


rem ===============================================================

create sequence SUM_SEQ
  increment by 1
  start with 2
  nomaxvalue
  nocycle
  cache 50;

create sequence SUM_DS_INDEX_SEQ
  increment by 1
  start with 1
  nomaxvalue
  nocycle
  cache 10;

