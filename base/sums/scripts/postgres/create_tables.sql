/* /home/ora10/SUM/postgres/create_tables.sql */

/* Table_ Name: SUM_MAIN */
/* Main table. One entry of every storage unit in the database. */

create table SUM_MAIN (
 ONLINE_LOC             VARCHAR(80) NOT NULL,
 ONLINE_STATUS          VARCHAR(5),
 ARCHIVE_STATUS         VARCHAR(5),
 OFFSITE_ACK            VARCHAR(5),
 HISTORY_COMMENT        VARCHAR(80),
 OWNING_SERIES          VARCHAR(80),
 STORAGE_GROUP          integer,
 STORAGE_SET            integer,
 BYTES                  bigint,
 DS_INDEX               bigint,
 CREATE_SUMID           bigint NOT NULL,
 CREAT_DATE             timestamp(0),
 ACCESS_DATE            timestamp(0),
 USERNAME               VARCHAR(10),
 ARCH_TAPE              VARCHAR(20),
 ARCH_TAPE_POS          VARCHAR(15),
 ARCH_TAPE_FN           integer,
 ARCH_TAPE_DATE         timestamp(0),
 WARNINGS               VARCHAR(260),
 STATUS                 integer,
 SAFE_TAPE              VARCHAR(20),
 SAFE_TAPE_POS          VARCHAR(15),
 SAFE_TAPE_FN           integer,
 SAFE_TAPE_DATE         timestamp(0),
 constraint pk_summain primary key (DS_INDEX)
);
/*
Indexes:
    "sum_main_ds_index_idx" btree (ds_index)
And on dcs0:
  create index sum_main_owner_idx on sum_main (OWNING_SERIES);
*/
create index sum_main_ds_index_idx on sum_main (ds_index);
create index sum_main_owner_idx on sum_main (OWNING_SERIES);


--    "sum_main_owner_idx" btree (owning_series)

/* =============================================================== */

/* Table_Name: SUM_OPEN */
/* One entry for every active SUM_Open() done by DRMS */

create table SUM_OPEN (
    SUMID      bigint not null,
    OPEN_DATE  timestamp(0),
    constraint pk_sumopen primary key (SUMID)
);

/* =============================================================== */

create table SUM_PARTN_ALLOC (
    wd                 VARCHAR(80) not null,
    sumid              bigint not null,
    status             integer not null,
    bytes              bigint,
    effective_date     VARCHAR(20),
    archive_substatus  integer,
    group_id           integer,
    ds_index           bigint not null,
    safe_id            integer
);

/*
Indexes:
    "sum_partn_alloc_ds_index_idx" btree (ds_index)
    "sum_partn_alloc_effdate_idx" btree (effective_date)
    "sum_partn_alloc_sumid_idx" btree (sumid)

*/
create index sum_partn_alloc_ds_index_idx on SUM_PARTN_ALLOC (ds_index);
create index sum_partn_alloc_effdate_idx on SUM_PARTN_ALLOC (effective_date);
create index sum_partn_alloc_sumid_idx on SUM_PARTN_ALLOC (sumid);
/* ===============================================================*/
create table SUM_PARTN_AVAIL (
       partn_name    VARCHAR(80) not null,
       total_bytes   bigint not null,
       avail_bytes   bigint not null,
       pds_set_num   integer not null,
       constraint pk_sumpartnavail primary key (partn_name)
);

/* ===============================================================*/
create table SUM_TAPE (
        tapeid          varchar(20) not null,
        nxtwrtfn        integer not null,
        spare           integer not null,
        group_id        integer not null,
        avail_blocks    bigint not null,
        closed          integer not null,
        last_write      timestamp(0),
        constraint pk_tape primary key (tapeid)
);
/*
Indexes:
    "sum_tape_tapeid_idx" btree (tapeid)
*/
create index sum_tape_tapeid_idx on SUM_TAPE (tapeid);
/* ===============================================================*/

create sequence SUM_SEQ
  increment 1
  start 2
  no maxvalue
  no cycle
  cache 50;

create sequence SUM_DS_INDEX_SEQ
  increment 1
  start 1
  no maxvalue
  no cycle
  cache 10;

