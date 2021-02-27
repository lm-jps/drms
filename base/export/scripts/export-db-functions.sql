CREATE OR REPLACE FUNCTION drms.image_exists(quality integer) RETURNS boolean AS
$$
DECLARE
  answer boolean;

BEGIN
  -- unless the top bit (bit 31) exists, an image is guaranteed to exist
  answer := (quality::bit(32) & X'80000000')::int = 0;
  RETURN answer;
END;
$$
LANGUAGE plpgsql;
