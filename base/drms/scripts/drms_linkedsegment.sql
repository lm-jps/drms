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

CREATE OR REPLACE FUNCTION drms_linkedsegment(dataseries VARCHAR, datasegment VARCHAR) RETURNS SETOF seriesseg AS $$
DECLARE
    parsed VARCHAR[2];
    namespace VARCHAR;
    relation VARCHAR;
BEGIN
    SELECT regexp_split_to_array(dataseries, E'\\.') INTO parsed;
    namespace := parsed[1];
    relation := parsed[2];        
    RETURN QUERY EXECUTE 'SELECT S.seriesname::VARCHAR AS series, S.segmentname::VARCHAR as segment, L.target_seriesname::VARCHAR AS linkedseries, S.targetseg::VARCHAR AS linkedsegment FROM ' || quote_ident(namespace) || '.drms_segment AS S, ' || quote_ident(namespace) || '.drms_link AS L WHERE lower(S.seriesname) = $1 AND lower(S.segmentname) = $2 AND S.islink = 1 AND lower(S.linkname) = lower(L.linkname) AND lower(S.seriesname) = lower(L.seriesname)' USING lower(dataseries), lower(datasegment);
END;
$$
LANGUAGE plpgsql;
