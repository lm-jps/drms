create type drmskw as (
    seriesname  text,
    keywordname text,
    linkname    text, 
    targetkeyw  text, 
    type        text, 
    defaultval  text, 
    format      text, 
    unit        text, 
    islink      int8, 
    isconstant  int8, 
    persegment  int8,
    description text );
