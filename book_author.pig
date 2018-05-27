book_info = load '/home/hduser/book-data' using PigStorage(',') as (book_id:int, price:int, author_id:int);

author_info = load '/home/hduser/author-data' using PigStorage(',') as (author_id:int, author_name:chararray);

filtered_book_info = filter book_info by price >= 200;

filtered_author_info = filter author_info by INDEXOF(author_name,'J',0) == 0;

joined_bag = join filtered_book_info by author_id, filtered_author_info by author_id;

final = foreach joined_bag generate $0, $1, $2, $4;

store final into '/home/hduser/pig/niit1' ;




