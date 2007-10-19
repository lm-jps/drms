CREATE OR REPLACE FUNCTION drms_session() RETURNS SETOF hmi_ground.drms_session AS $$
DECLARE
  ns  RECORD;
  rec RECORD;
  next_row REFCURSOR;
BEGIN
  FOR ns IN SELECT name as tn FROM admin.ns order by name LOOP
     OPEN next_row FOR EXECUTE 'SELECT * FROM ' || ns.tn || '.drms_session';
     LOOP
       FETCH next_row INTO rec;
       rec.username := rec.username || '(' || ns.tn || ')';
       IF NOT FOUND THEN 
          EXIT;
       END IF;
       RETURN NEXT rec;
     END LOOP;
     CLOSE next_row;
  END LOOP; 
  RETURN;
END;
$$
LANGUAGE plpgsql;
