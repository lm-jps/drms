/* base/drms/scripts/NetDRMS.sql */

create schema admin;
grant usage on schema "admin" to public;

create table admin.ns (
 name     text not null,
 nsgroup  text not null,
 owner    text not null
);

create table admin.sessionns (
 username   text not null,
 sessionns  text not null
);

grant select on "admin".sessionns to public;
grant select on "admin".ns to public;
