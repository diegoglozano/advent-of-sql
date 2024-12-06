import duckdb

from pathlib import Path

DATA_PATH = Path('data')

with open(DATA_PATH / 'day_01.sql') as f:
    sql = f.read()

conn = duckdb.connect()
conn.sql(sql)
out = conn.sql(
    '''
    WITH
    processed_children AS (
        SELECT child_id, name
        FROM children
    ),
    processed_wish_lists AS (
        SELECT
            child_id,
            wishes->>'$.first_choice' AS primary_wish,
            wishes->>'$.second_choice' AS backup_wish,
            LEN(CAST(wishes.colors AS VARCHAR[])) AS color_count,
            CAST(wishes.colors AS VARCHAR[])[1] AS favorite_color
        FROM wish_lists
    ),
    processed_toy_catalogue AS (
        SELECT
            toy_name,
            CASE
                WHEN difficulty_to_make = 1 THEN 'Simple Gift'
                WHEN difficulty_to_make = 2 THEN 'Moderate Gift'
                WHEN difficulty_to_make >= 3 THEN 'Complex Gift'
            END AS gift_complexity,
            CASE
                WHEN category = 'outdoor' THEN 'Outside Workshop'
                WHEN category = 'educational' THEN 'Learning Workshop'
                ELSE 'General Workshop'
            END AS workshop_assignment
        FROM toy_catalogue
    )
    SELECT
        name,
        primary_wish,
        backup_wish,
        favorite_color,
        color_count,
        gift_complexity,
        workshop_assignment
    FROM processed_children AS c
    LEFT JOIN processed_wish_lists AS w
    USING (child_id)
    LEFT JOIN processed_toy_catalogue AS t
    ON w.primary_wish = t.toy_name
    ORDER BY name ASC
    LIMIT 5
    '''
).fetchall()

for row in out:
    print(','.join(map(str, row)))
