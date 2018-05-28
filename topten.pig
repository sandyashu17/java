-- to find the top buyers from txns1.txt and respective details for each customer

-- load the transaction data

txn = load '/home/hduser/txns1.txt' using PigStorage(',') as (txnid, txndt, custid:int, amount:double, category, product, city,state,spendby);

txn = foreach txn generate custid, amount;

--dump txn;

-- group the data on custid

groupbycust = group txn by custid;

--dump groupbycust;

--describe groupbycust;


--groupbycust: {group: int,txn: {(custid: int,amount: double)}}

--aggregating the amount for each customer

sumbycust = foreach groupbycust generate group, ROUND_TO(SUM(txn.amount),2) as total;

--dump sumbycust;

orderbyamt = order sumbycust by total desc;

top10 = limit orderbyamt 10;

--dump top10;

cust = load '/home/hduser/custs' using PigStorage(',') as (custid:int, firstname:chararray, lastname:chararray, age:int, profession:chararray);

joined_cust_top10 = join top10 by $0, cust by $0;

--dump joined_cust_top10;

final = foreach joined_cust_top10  generate $0, $3, $4, $5, $6, $1;

--dump final;

finaltop10 = order final by $5 desc;

--dump finaltop10;

store finaltop10 into '/home/hduser/pig/topten' using PigStorage(',');












































