--use niit;
set myage=45;
--select profession,count(*) from customer where age >=${hiveconf:myage} group by profession order by profession;

--select profession,count(*) from customer where age >=45 group by profession order by profession;
select * from niit.customer where age >=${hiveconf:myage};




