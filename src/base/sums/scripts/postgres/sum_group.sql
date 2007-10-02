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

