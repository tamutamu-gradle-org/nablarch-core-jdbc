-- SQLID:SQL001
SQL001 =
select * from test
where col1 = :col1
order by col1

SQL002 =
select * from test
where col1 = :col1
and $if(col2) {col2 = :col2}
order by col1

SQL003 =
select * from test
where col1 = ?

SQL004 =
select * from test
where col1 = :col1
order by col2

SQL005 =
select * from test

SQL006 =
select *
from test
where col1 = '11111'

SQL007 =
BEGIN ? := '12345'; ?:= 100; END;

SQL008 =
{call test_proc(?, ?, ?, ?, ?, ?)}

TEST_PROC = 
CREATE OR REPLACE PROCEDURE TEST_PROC(ID1 IN CHAR, ID2 IN CHAR, ID3 IN CHAR, VAL1 IN CHAR, VAL2 IN CHAR, INSERT_COUNT OUT PLS_INTEGER)
  AS
  BEGIN
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID1, VAL1, VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID2, VAL1, VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID3, VAL1, VAL2);
    INSERT_COUNT := 3;
  END;