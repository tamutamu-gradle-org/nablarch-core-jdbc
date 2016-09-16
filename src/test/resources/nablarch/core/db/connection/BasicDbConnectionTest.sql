
SQL1 = select '1' from dual

SQL2 = select '2' from dual

SQL3 = select * from user_master where $if(userId){user_id = :userId} and user_name = :userName

SQL4 = select * from user_master WHERE $if(userId){user_id = :userId} and user_name = :userName

procedure={?=call proc_name(?, ?)}

procedure2={? = call proc_name(?, ?)}

