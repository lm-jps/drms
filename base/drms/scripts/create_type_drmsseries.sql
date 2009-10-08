create type drmsseries as (
    seriesname  text,
    author      text,
    owner       text, 
    unitsize    int8,
    archive     int8,
    retention   int8,
    tapegroup   int8,
    primary_idx text,
    created     text,
    description text,
    dbidx       text,
    version     text );
