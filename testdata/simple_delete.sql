DELETE FROM foo
WHERE id = @id::text
RETURNING
    id AS id_1;

