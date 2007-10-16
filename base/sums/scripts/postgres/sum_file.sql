/* This is the file checksum table. For each file written to tape */
/* it will get an entry here of the file md5 checksum value. */
create table SUM_FILE (
	tapeid		varchar(20) not null,
	filenum		integer not null,
	gtarblock	integer,
	md5cksum	varchar(36) not null,
	constraint pk_file primary key (tapeid, filenum)
       );

