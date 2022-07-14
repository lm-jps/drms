-- stupid postgres - cannot figure out how to conditionally create a type; PG does not give us 
-- any decent options before 9.0 (when they introduced the DO statement, which does the obvious and
-- reasonable)
DROP TYPE IF EXISTS serieskey CASCADE;
CREATE TYPE serieskey AS 
(
    series varchar(64),
    keyword varchar(32),
    linkedseries varchar(64),
    linkedkeyword varchar(32)
); 

CREATE OR REPLACE FUNCTION drms_linkedkeyword(dataseries varchar, datakeyword varchar) RETURNS SETOF serieskey AS $$
DECLARE
    parsed varchar[2];
    namespace varchar;
    relation varchar;
BEGIN
    SELECT regexp_split_to_array(dataseries, E'\\.') INTO parsed;
    namespace := parsed[1];
    relation := parsed[2];
    
    IF datakeyword IS NULL OR datakeyword = '' THEN
        RETURN QUERY EXECUTE 'SELECT K.seriesname::VARCHAR AS series, K.keywordname::VARCHAR as keyword, L.target_seriesname::VARCHAR AS linkedseries, K.targetkeyw::VARCHAR AS linkedkeyword FROM ' || quote_ident(namespace) || '.drms_keyword AS K LEFT OUTER JOIN ' || quote_ident(namespace) || '.drms_link AS L ON lower(K.linkname) = lower(L.linkname) AND lower(K.seriesname) = lower(L.seriesname) WHERE lower(K.seriesname) = $1' USING lower(dataseries);
    ELSE
        RETURN QUERY EXECUTE 'SELECT K.seriesname::VARCHAR AS series, K.keywordname::VARCHAR as keyword, L.target_seriesname::VARCHAR AS linkedseries, K.targetkeyw::VARCHAR AS linkedkeyword FROM ' || quote_ident(namespace) || '.drms_keyword AS K LEFT OUTER JOIN ' || quote_ident(namespace) || '.drms_link AS L ON lower(K.linkname) = lower(L.linkname) AND lower(K.seriesname) = lower(L.seriesname) WHERE lower(K.seriesname) = $1 AND lower(K.keywordname) = $2' USING lower(dataseries), lower(datakeyword);
    END IF;
END;
$$
LANGUAGE plpgsql;
