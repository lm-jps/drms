DROP TYPE IF EXISTS keyinfo CASCADE;
CREATE TYPE keyinfo AS 
(
    series varchar(64),
    keyword varchar(32),
    datatype varchar(16),
    keyworddefault text,
    format varchar(32),
    unit varchar(32),
    isconstant boolean,
    flags integer,
    description text
); 

CREATE OR REPLACE FUNCTION drms_followkeywordlink(dataseries varchar, datakeyword varchar) RETURNS SETOF keyinfo AS $$
DECLARE
    parsed varchar[2];
    namespace varchar;
    relation varchar;
    origkeyword varchar;
    linkedseries varchar;
    linkedkeyword varchar;
    linkmaprecord RECORD;
    origflags integer;
BEGIN
    -- origkeyword is needed in the case that the caller provides NULL for datakeyword (requesting information about all keyword in a series)
    FOR linkmaprecord IN SELECT K.keyword, K.linkedseries, K.linkedkeyword FROM drms_linkedkeyword(dataseries, datakeyword) AS K LOOP
        origkeyword := linkmaprecord.keyword;
        linkedseries := linkmaprecord.linkedseries;
        linkedkeyword := linkmaprecord.linkedkeyword;
        
        SELECT regexp_split_to_array(dataseries, E'\\.') INTO parsed;
        namespace := parsed[1];
        relation := parsed[2];

        LOOP
            IF linkedseries IS NULL THEN
                -- not a linked series
                -- return original series and keyword name so the caller can locate the information by input values easily
                RETURN QUERY EXECUTE 'SELECT seriesname::varchar AS series, keywordname::varchar AS keyword, type::varchar AS datatype, defaultval::text AS keyworddefault, format::varchar AS format, unit::varchar AS unit, (isconstant=1)::boolean AS isconstant, persegment::integer AS flags, description::text as description FROM ' || quote_ident(namespace) || '.drms_keyword WHERE lower(seriesname) = $1 AND lower(keywordname) = $2' USING lower(dataseries), lower(origkeyword);
                EXIT;
            ELSE
                -- linked series
                -- must return original implicit (bit 1), internal-prime (bit 2), and external-prime (bit 3) flags values, but linked per-segment (bit 0) value;
                -- also, must return original rank (top 16 bits)
                EXECUTE 'SELECT persegment FROM ' || namespace || '.drms_keyword WHERE lower(seriesname) = lower(' || quote_literal(dataseries) || ') AND lower(keywordname) = lower(' || quote_literal(origkeyword) || ')' INTO origflags;
                RETURN QUERY EXECUTE 'SELECT ' || quote_literal(dataseries) || '::varchar AS series, ' || quote_literal(origkeyword) || '::varchar AS keyword, datatype, keyworddefault, format, unit, isconstant, ((' || quote_literal(origflags) || '::integer::bit(32) & X''FFFFFFFE'') | (flags::bit(32) & X''00000001''))::integer AS flags, description FROM drms_followkeywordlink(' || quote_literal(linkedseries) || ',' || quote_literal(linkedkeyword) || ')';
                EXIT;
            END IF;
        END LOOP;
    END LOOP;
    
END;
$$
LANGUAGE plpgsql;
