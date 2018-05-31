--hdfs
--book = load '/niit/pig' using TextLoader() as (lines:chararray);
--dump book;
--transform = foreach book generate FLATTEN(TOKENIZE(lines)) as word;
--dump transform;	
--groupbyword = group transform by word;
--dump groupbyword;
--countofeachword = foreach groupbyword generate group, COUNT (transform);
--dump countofeachword;
--store countofeachword into '/niit/pig1';
--dump stored;



--local
book = load '$INPUTFILE' using TextLoader() as (lines:chararray);
dump book;
transform = foreach book generate FLATTEN(TOKENIZE(lines)) as word;
dump transform;	
groupbyword = group transform by word;
dump groupbyword;
countofeachword = foreach groupbyword generate group, COUNT (transform);
dump countofeachword;
store countofeachword into '$OUTDIRECTORY';
dump store;
--pig -x local -p INPUTFILE=/home/hduser/file -p OUTDIRECTORY=/home/hduser/pig2 -f wordcount.pig
--nano paramters 

--pig -x local -p parameters -f wordcount.pig
--pig -x local -param_file parameters -p OUTDIRECTORY=/home/hduser/pig6 -f wordcount.pig



