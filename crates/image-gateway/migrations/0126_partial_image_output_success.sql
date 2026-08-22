-- A multi-output image request is successful when at least one output is
-- durably available. Failed output partitions keep their own terminal facts,
-- quota release, and customer rating; they must not hide or discard successful
-- artifacts at the parent API boundary.
DO $migration$
DECLARE
    target REGPROCEDURE := 'assert_terminal_parent_completion(uuid)'::REGPROCEDURE;
    definition TEXT;
    patched TEXT;
BEGIN
    definition := pg_get_functiondef(target);
    patched := replace(
        definition,
        $old$expected_parent_state := CASE
        WHEN uncertain_count > 0 THEN 'uncertain'
        WHEN failed_count > 0 THEN 'failed'
        ELSE 'succeeded'
    END;$old$,
        $new$expected_parent_state := CASE
        WHEN uncertain_count > 0 THEN 'uncertain'
        WHEN succeeded_count > 0 THEN 'succeeded'
        ELSE 'failed'
    END;$new$
    );
    IF patched = definition THEN
        RAISE EXCEPTION 'expected terminal parent state policy was not found';
    END IF;
    EXECUTE patched;
END;
$migration$;
