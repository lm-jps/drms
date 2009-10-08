create type rep_item as (
    tab_id      int8,
    tab_relname varchar(255),
    tab_nspname varchar(255),
    tab_set     int8,
    tab_idxname varchar(255),
    tab_comment text );
