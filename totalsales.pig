txn = load '/home/hduser/txns1.txt' using PigStorage(',') as (txnid, txndt, custid:int, amount:double, category, product, city, state, spendby);
txn = foreach txn generate amount;
--dump txn;
groupall = group txn all;
--dump groupall;
totalsales = foreach groupall generate ROUND_TO(SUM(txn.amount),2), MAX(txn.amount), MIN(txn.amount);
dump totalsales; 

