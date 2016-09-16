SQL_001 = select col1 from test_table order by col1

SQL_002 = select col1, col2, col3 from test_table where col1 = ?

SQL_003 = select col1, col2, col3 from test_table where col1 = :col1

SQL_004 = select * from test_table where $if(col1){col1 = :col1} and $if(col2){col2 = :col2}

SQL_005 = select * from test_table where col1 in (:col1[]) order by col1
