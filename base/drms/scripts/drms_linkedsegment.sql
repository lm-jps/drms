-- stupid postgres - cannot figure out how to conditionally create a type; PG does not give us 
-- any decent options before 9.0 (when they introduced the DO statement, which does the obvious and
-- reasonable)
DROP TYPE IF EXISTS seriesseg CASCADE;
CREATE TYPE seriesseg AS 
(
    series varchar(64),
    segment varchar(32),
    linkedseries varchar(64),
    linkedsegment varchar(32)
); 

CREATE OR REPLACE FUNCTION drms_linkedsegment(dataseries varchar, datasegment varchar) RETURNS SETOF seriesseg AS $$
DECLARE
    parsed varchar[2];
    namespace varchar;
    relation varchar;
BEGIN
    SELECT regexp_split_to_array(dataseries, E'\\.') INTO parsed;
    namespace := parsed[1];
    relation := parsed[2];
    
    IF datasegment IS NULL OR datasegment = '' THEN
        RETURN QUERY EXECUTE 'SELECT S.seriesname::VARCHAR AS series, S.segmentname::VARCHAR as segment, L.target_seriesname::VARCHAR AS linkedseries, S.targetseg::VARCHAR AS linkedsegment FROM ' || quote_ident(namespace) || '.drms_segment AS S LEFT OUTER JOIN ' || quote_ident(namespace) || '.drms_link AS L ON lower(S.linkname) = lower(L.linkname) AND lower(S.seriesname) = lower(L.seriesname) WHERE lower(S.seriesname) = $1' USING lower(dataseries);
    ELSE
        RETURN QUERY EXECUTE 'SELECT S.seriesname::VARCHAR AS series, S.segmentname::VARCHAR as segment, L.target_seriesname::VARCHAR AS linkedseries, S.targetseg::VARCHAR AS linkedsegment FROM ' || quote_ident(namespace) || '.drms_segment AS S LEFT OUTER JOIN ' || quote_ident(namespace) || '.drms_link AS L ON lower(S.linkname) = lower(L.linkname) AND lower(S.seriesname) = lower(L.seriesname) WHERE lower(S.seriesname) = $1 AND lower(S.segmentname) = $2' USING lower(dataseries), lower(datasegment);
    END IF;
END;
$$
LANGUAGE plpgsql;
