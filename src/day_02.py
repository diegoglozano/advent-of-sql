import duckdb

from pathlib import Path

DATA_PATH = Path('data')

with open(DATA_PATH / 'day_02.sql') as f:
    sql = f.read()

conn = duckdb.connect()
conn.sql(sql)
out = conn.sql(
    '''
    WITH
    total AS (
        SELECT id, chr(value) AS message
        FROM letters_a
        UNION
        SELECT id, chr(value) AS message
        FROM letters_b
    ),
    sorted_total AS (
        SELECT *
        FROM total
        WHERE regexp_matches(message, '^[a-zA-Z\s\!\(\),\-\.:;?]+$')
        ORDER BY id
    ),
    aggregated_total AS (
        SELECT list(message ORDER BY id) AS message
        FROM sorted_total
    )
    SELECT list_aggregate(message, 'string_agg', '') AS message
    FROM aggregated_total
    '''
).fetchall()

print(
    out[0][0]
)