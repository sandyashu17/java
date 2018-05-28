txn = load '/home/hduser/txns.1' using PigStorage(',') as (txnid, txndt, custid:int, amount:double, category, product, city, state, spendby);
txn = foreach txn generate amount;
dump txn;

retail1 = load '/home/hduser/Retail_Data/D12' using PigStorage(';') as (trn_dt, custid:chararray, age, zip, category, product, qty, cost, sales:long);
retail1 = foreach retail1 generate trn_dt, custid, sales;                                                                                              
retail1 = limit retail1 10;
dump retail1;
 groupall = group retail1 all;  
maxSales = foreach groupall generate MAX(retail1.sales) as maxamt;   
dump maxSales;
 highvalcust = filter retail1 by sales == maxSales.maxamt; 
dump highvalcust;


