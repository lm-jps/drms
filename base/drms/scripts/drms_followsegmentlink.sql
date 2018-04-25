DROP TYPE IF EXISTS seginfo CASCADE;
CREATE TYPE seginfo AS 
(
    series varchar(64),
    segment varchar(32),
    datatype varchar(16),
    segnum int8,
    scope varchar(16),
    numaxes int8,
    dimensions varchar(32),
    unit varchar(32),
    protocol varchar(32),
    description text
); 

CREATE OR REPLACE FUNCTION drms_followsegmentlink(dataseries varchar, datasegment varchar) RETURNS SETOF seginfo AS $$
DECLARE
    parsed varchar[2];
    namespace varchar;
    relation varchar;
    series varchar;
    segment varchar;
    linkedseries varchar;
    linkedsegment varchar;
    count int8;
BEGIN
    
    series := dataseries;
    segment := datasegment;
    count := 5;
    
    LOOP
        SELECT S.linkedseries, S.linkedsegment FROM drms_linkedsegment(series, segment) AS S INTO linkedseries, linkedsegment;
        raise notice '%', linkedseries;

        IF linkedseries IS NULL THEN
            -- not a linked series
            SELECT regexp_split_to_array(series, E'\\.') INTO parsed;
            namespace := parsed[1];
            relation := parsed[2];
            RETURN QUERY EXECUTE 'SELECT seriesname::varchar AS series, segmentname::varchar AS segment, type::varchar AS datatype, segnum::int8 AS segnum, scope::varchar, naxis::int8 AS numaxes, axis::varchar AS dimensions, unit::varchar, protocol::varchar, description FROM '|| quote_ident(namespace) || '.drms_segment WHERE lower(seriesname) = $1 AND lower(segmentname) = $2' USING lower(series), lower(segment);
            EXIT;
        ELSE
            -- linked series (NOTE - I have not yet implemented recursive links)
            series := linkedseries;
            segment := linkedsegment;        
        END IF;
        
        count := count - 1;
        EXIT WHEN count = 0;
    END LOOP;
    
END;
$$
LANGUAGE plpgsql;
