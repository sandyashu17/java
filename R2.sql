
drop table niit.out1;
drop table niit.out2;

create table niit.out1 (custno int,firstname string,age int,profession string,amount double,product string)
row format delimited                                                                                  
fields terminated by ',';   


insert overwrite table niit.out1                                                                           
select a.custno,a.firstname,a.age,a.profession,b.amount,b.product                                     
from niit.customer a JOIN niit.txnrecords b ON a.custno = b.custno;     

--select * from niit.out1 limit 100;

create table niit.out2 (custno int,firstname string,age int,profession string,amount double,product string, level string)
row format delimited                                                                                  
fields terminated by ',';   

insert overwrite table niit.out2
select * , case when age<30 then 'low' when age>=30 and age < 50 then 'middle' when age>=50 then 'old' 
else 'others' end
from niit.out1;


--select * from niit.out2 limit 100; 

describe niit.out2;  
select a.level, round(sum(a.amount),2) as totalspent, round((sum(a.amount)/total*100),2) as salespercent  from niit.out2 a, niit.totalsales b group by a.level, b.total;


