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

SET_PROC = 
CREATE OR REPLACE FUNCTION SET_PROC(ARG1 OUT CHAR, ARG2 OUT NUMERIC)
  AS
  $BODY$
  BEGIN
    ARG1 := '12345';
    ARG2 := 100;
  END;
  $BODY$
  LANGUAGE plpgsql


SQL008 =
{call test_proc(?, ?, ?, ?, ?, ?)}

TEST_PROC = 
CREATE OR REPLACE FUNCTION TEST_PROC(ID1 CHAR, ID2 CHAR, ID3 CHAR, VAL1 CHAR, VAL2 CHAR, INSERT_COUNT OUT INT)
  AS
  $BODY$
  BEGIN
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID1, VAL1, VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID2, VAL1, VAL2);
    INSERT INTO TEST (ID, COL1, COL2) VALUES (ID3, VAL1, VAL2);
    INSERT_COUNT := 3;
  END;
  $BODY$
  LANGUAGE plpgsql