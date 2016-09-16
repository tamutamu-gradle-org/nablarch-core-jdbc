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
{call set_proc(?, ?)}

INIT_PROC = 
DROP PROCEDURE test_proc, set_proc


-- SQL007を利用するに当たり、必要なPROCを定義する。
SET_PROC =
CREATE PROCEDURE SET_PROC
@CHAR_PARAM CHAR(5) OUTPUT,
@INT_PARAM INT OUTPUT
  AS
  BEGIN
    SET @CHAR_PARAM = '12345';
    SET @INT_PARAM = 100;
  END;

SQL008 =
{call test_proc(?, ?, ?, ?, ?, ?)}

TEST_PROC = 
CREATE PROCEDURE TEST_PROC
@ID1 CHAR(4), 
@ID2 CHAR(4),
@ID3 CHAR(4), 
@VAL1 CHAR(4), 
@VAL2 CHAR(4), 
@INSERT_COUNT INT OUTPUT
  AS
  BEGIN
    INSERT INTO TEST (ID, COL1, COL2) VALUES (@ID1, @VAL1, @VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (@ID2, @VAL1, @VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (@ID3, @VAL1, @VAL2);
    SET @INSERT_COUNT = 3;
  END;