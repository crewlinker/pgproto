DELETE FROM foo
WHERE id = @id::text
RETURNING
    id::uuid AS id_1;

