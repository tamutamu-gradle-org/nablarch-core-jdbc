SQL1 = select '1' from statement_factory_test

SQL2 = select entity_id from statement_factory_test

SQL3 = select '1' from statement_factory_test

SQL4 = select * from statement_factory_test where $if(id){entity_id = :id}