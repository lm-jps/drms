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
       pds_set_prime integer not null,
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
  cache 50;

/* This is the file checksum table. For each file written to tape */
/* it will get an entry here of the file md5 checksum value. */
create table SUM_FILE (
	tapeid		varchar(20) not null,
	filenum		integer not null,
	gtarblock	integer,
	md5cksum	varchar(36) not null,
	constraint pk_file primary key (tapeid, filenum)
       );

create table SUM_ARCH_GROUP (
	group_id	integer not null,
	cadence_days	integer not null,
	sec1970_start	bigint not null,
	date_arch_start	timestamp(0),
	name		varchar(96),
	sum_set		integer not null,
	constraint pk_arch_group primary key (group_id)
);

/* The group id for a dataseries is defined by the drms definition */
/* for the series. */
/* The group id for a tape is automatically assigned to a free tape when */
/* it is first used. */
/* This table is the near-line (i.e. keep in tape robot) retention time */
/* in days for each group id. It is set by hand or by some util program. */
create table SUM_GROUP (
	group_id	integer not null,
	retain_days	integer not null,
	effective_date	VARCHAR(20),
	constraint pk_group primary key (group_id)
       );

