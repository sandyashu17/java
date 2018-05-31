total sales
txn = load '/home/hduser/txns1.txt' using PigStorage(',') as (txnid, txndt, custid:int, amount:double, category, product, city, state, spendby);
txn = foreach txn generate amount;
--dump txn;
groupall = group txn all;
--dump groupall;
totalsales = foreach groupall generate ROUND_TO(SUM(txn.amount),2), MAX(txn.amount), MIN(txn.amount);
dump totalsales; 



word count 

book = load '/home/hduser/file' using TextLoader() as (lines:chararray);

transform = foreach book generate TOKENIZE(lines) as word;
both flatten AND  tokenise
transform = foreach book generate FLATTEN(TOKENIZE(lines)) as word;

// separately flatten as --
transform = foreach book generate FLATTEN(word) as word1;//word col name // tokenise (array) to separate each word//flatten (after tokenise) tupple into separate tupple 

groupbyword = group transform by word1;
countofeachword = foreach groupbyword generate group, COUNT (transform);
dump countofeachword;
sorted = order countofeachword by $0 asc, $0;
dump stored;


medical

med = load '/home/hduser/Downloads/medical' using PigStorage('\t') as (name, department:chararray, claim:long);
groupbyname = group med by name;
	totalclaim = foreach groupbyname generate group, AVG(med.claim);
groupbyid = group nyse by stock_id;

payment gateway 

web = load '/home/hduser/Downloads/weblog' using PigStorage('\t') as (name:chararray, gateway:chararray, accestime);
gate = load '/home/hduser/Downloads/gateway' using PigStorage('\t') as (gateway:chararray, rate:double);
joine = join web by $1, gate by $0;
dump joine;
final = foreach joine  generate $0, $1, $4;
groupbyname = group final by name;
total = foreach groupbyname generate group, AVG(final.rate);
dump total;
highvalcust = filter total by $1>90; 
dump highvalcust;




payment gateway 

zero = load '/home/hduser/Downloads/2000.txt' using PigStorage(',') as (id:int, name:chararray, jan:double, feb:double, mar:double, apr:double, may:double, june:double, july:double, aug:double, sep:double, oct:double, nov:double, dec:double);
one = load '/home/hduser/Downloads/2001.txt' using PigStorage(',') as (id:int, name:chararray, jan:double, feb:double, mar:double, apr:double, may:double, june:double, july:double, aug:double, sep:double, oct:double, nov:double, dec:double);
two = load '/home/hduser/Downloads/2002.txt' using PigStorage(',') as (id:int, name:chararray, jan:double, feb:double, mar:double, apr:double, may:double, june:double, july:double, aug:double, sep:double, oct:double, nov:double, dec:double);
totzero = foreach zero generate id, name, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
totone = foreach one generate id, name, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
tottwo = foreach two generate id, name, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
joine = join totzero by $0, totone by $0, tottwo by $0;
final = foreach joine  generate $0, $1, $2, $5, $8;
add = foreach final generate $0, $1, ($3-$2)/$2*100, ($4-$3)/$3*100;
add1 = foreach add generate $0, $1, ($3+$2)/2;
1

filterdata = filter add1 by $2>10;

2

filterdata1 = filter add1 by $2<-5;

3

finalone = foreach joine generate $0, $1, ($2+$5+$8); 

bottomfive = order finalone by $2 asc;
bottomfivefinal = limit bottomfive 5; 
dump bottomfivefinal;

topfive = order finalone by $2 desc;
topfivefinal = limit topfive 5;
dump topfivefinal;

