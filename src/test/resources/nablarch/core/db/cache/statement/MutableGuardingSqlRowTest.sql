SELECT_TIMESTAMP =
select SQL_ROW_ID, TIMESTAMP_COL from IMMUTABLE_SQL_ROW ORDER BY SQL_ROW_ID

SELECT_BYTES =
select SQL_ROW_ID, BINARY_COL from IMMUTABLE_SQL_ROW ORDER BY SQL_ROW_ID
