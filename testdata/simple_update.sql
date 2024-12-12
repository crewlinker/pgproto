UPDATE
    foo
SET
    first_name = @first_name_1::text
RETURNING
    id::uuid AS id_1
