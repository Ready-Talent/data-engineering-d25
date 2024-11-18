MERGE INTO target_table AS target
USING staging_table AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        target.column1 = source.column1,
        target.column2 = source.column2
WHEN NOT MATCHED THEN
    INSERT (
        id,
        column1,
        column2
        )
    VALUES (
        source.id,
        source.column1,
        source.column2
        );
