INSERT INTO my_table(column1, column2, column3)
    VALUES (@val_1::int4, CAST(CAST(@val_2 AS integer) AS bigint), CAST(CAST(CAST(@val_3 AS integer) AS bigint) AS text));

