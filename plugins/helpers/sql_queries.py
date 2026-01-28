class SqlQueries:
    songplay_table_insert = "INSERT INTO songplays SELECT ...;"
    user_table_insert = "INSERT INTO users SELECT ...;"
    song_table_insert = "INSERT INTO songs SELECT ...;"
    artist_table_insert = "INSERT INTO artists SELECT ...;"
    time_table_insert = "INSERT INTO time SELECT ...;"

    quality_checks = [
        {"sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", "expected": 0}
    ]
