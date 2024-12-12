INSERT INTO bar.public.foo(id)
    VALUES (@id_1::uuid, @first_name_2::text)
RETURNING
    id::text AS id_1;

