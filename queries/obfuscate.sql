CREATE OR REPLACE FUNCTION obfuscate(ignores integer, to_obfuscate text)
  RETURNS text AS
$BODY$
DECLARE
    pieces text[];
    i integer;
BEGIN
    if to_obfuscate is not null then
        pieces := regexp_split_to_array(to_obfuscate, E'\\s+');
        for i in 1..array_upper(pieces, 1) loop
            pieces[i] = trim(pieces[i]);
            if (i > ignores) then
                pieces[i] = substring(md5(pieces[i]) for char_length(pieces[i]));
            end if;
        end loop;
        return array_to_string(pieces,' ');
    else
        return null;
    end if;
END;
$BODY$
LANGUAGE 'plpgsql' VOLATILE COST 100;
