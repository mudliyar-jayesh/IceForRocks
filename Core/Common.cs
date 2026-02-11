namespace IceForRocks.Core.Common;

/*

--- part 1 
Column based files right? 
lets do this one step at a time.
first of all,
data is bytes, not fields.
bytes must be arranged on disk
bytes must be copied to ram
bytes must be written to disk
arranged bytes might need more space.
disk is basically virtual mem provided by OS. 
one page is usually 4KB (1024 * 4 = 4096).
data can be longer or shorter than a section. it can either fit in or maybe more 
depending on the data. if 
decimals have highest byte size of 16 bytes, long-double have 8, int-float have 4, short-char have 2

so basically, a page can be divided into (4096 / 16) = 256  decimals
(4096 / 8 ) = 512 double,longs
(4096 / 4) = 1024 ints, floats
(4096 / 2) = 2048 shorts, chars

that means by maintaing evenly spaced data together, 
each page can be loaded in a section, and isolated for operations.

moving on from pages to files.
a file can have any number of pages. now with an empty file, 
we have 0 bytes, so we initially give it 
a capacity of 1 page/section that is 4 KB
and size to the position of last written byte.

now two things can be done,
- section headeer can be saved in the section/page itself. going with the largest number of data types, 
that is 2058 shorts/chars in a page/section (4KB). we remove a sensible amount of section start,
to hold the meta values.
- as the meta data grows, a different file would be better, so scanning gets faster.

--- part 2
now that we have basic file sorted, before moving on i need to think about strings.
unpredictable, hated by the OS, hated by the developers. but people love it.
usually 32 bytes long, but padded with random BS. 
so why store it again and again.
Here comes a file that holds data in a non cpu friendly manner, as we dont have an option
a symbol table. each string will be just added to the file, 
another file aka header file must be present here to hold the following.
what is the offset count upto this point. that becomes the Id for the string
what is the offset itself. the memory space between start and the target string
but that keeps maintain things a bit difficult, so we only hold the offset spaces.
that is, first ulong - first elemtn fofset - zero
second ulong - second string offset - 32 bytes
third ulong - third string offset - 128 bytes 
fourth ulong - big string offset - 512 bytes


now we just jump to the positino by adding this size to the base pointer of the symbol table.
so the index of the offset from the header table is the id for  the string in the symbol table.

the problem arises when we search for one. but that is find. 
we maintain the active map of frequently used strings in memory. and the indices with them.
if its too heavy, we write it to a cache file, so a frequently used string table. a cached symbol table.

this solves the constant lookup issue. and if the cache is saved in the file, in the same format of original symbol table.
it might occupy on a few pages, and not a whole section. mimimum memory, i assume for each database table i am planning, 
no matter the data length, the cache table does not need to go beyond a few 30 to 40 KB

also, use bloom filters maybe murmurhash3 for strings 

-- part 3
The files structuure is ready to hold data now. how?
empty file structure. okay great. 
now i want to store a single row from a csv file. if done manually.
if look at the row. 
what are the columnns, 
what kind of columns am I looking at? 
what kind of storage i will need for each column? 
what missing values must I have to handle.

okay, figured that out. now what.
for numeric columns, i can just dump the data into individual files.
for classification columns, I have a choice, either do each row for each entry i have had in the csv file
or depending on what kind of classification i am looking at, it can go in two directions
- boolean bit packing (1 byte hold 8 records), i can use that, with a larger mask size that aligns with the CPU. ulong.
- enum packing, with a single ulong, i can hold about what 64 bits. if i have smaller classification i can use short or int as my 
classificatino mask for each record. but if I ahve larger clasifciation, which is rare, i still can use ulong.
that saves space.

for string columns, i have a lot to handle, first we need to make sure we row index, so in the string column file, we store the symbol table id.
so for that we first add the string to the symboltable. but here i have to check if the symbol exists. that can be done by using a bloom filter for the symbol table. 
or just parallel scanning the offsets by assigning custom and check for matching. strings
as the symbol table holds unique strings, this wont be a large data set.

this wrap up the row to column part.

-- part 4
The failures? writing means risk of half writes. aka data corruption. with open files. incomplete data
so WAL right? 
we dont put the data in column files.
we create a WAL entry for each. this frees up the ingestion engine to work on the next file.
and also lets us have a controlled log of data we are meant to write.

but how? 
a WAL has to be for each row of data. it can also be for each column but that means too many fiels.
so if i stick to a single entry for a single row. what means a single WAL entry might get as big as possible.
what I can create a wal block, and what wal block can have wal entries for each field in a row. that way, i know where i failed.
and make the decision to either roll back or continue.

this gives me finer control on the data.
but that also means overhead on the WAL file. 

but thats a trade off i can cover by batching. 
the table Wal must only hold N blocks at a time. then we flush them to columns. 

-- part 4
now that data is ready and safe in the files.
we have the query. there are two parts to this. 
first, the developer side
ssecond the engine side

developers usually dont need to know what exactly happens when using the tool
but must use the right structure to make srue the database runs 

engine needs to work on scans. each type of column will have a different type of scan

so whenever a report is in consideration, 
we only focus on column flow. the initial release does not bother with a single 
method toe use predicate to handle data. 

so i will just have my reports written using the core methods
we need some method to search 
we need some method to iterate with or without filter
we need specific filter for specific columntype, so methods here too.

with column types, 
for numeric we will have ranges,
for date (considered numeric, short as we use YYYYMMdd format), again ranges
that is less, greater, canEqual with these two
for bit packed or enum packed data, we run masks against them. 
all these can be done using SIMD. and in parallel batches. based on sections.

so the above will be as fast as we can get.
For strings,
we first visit the cached symbol table. if empty, we add frequency to it. in mem.
we drop the least frequency from the cache whenever we add a new value, else we increatement frequency
then on failing that,
 we have bloom filter to identify potential sections. 
 we parallel scan them for mathcing strings, using both the offset and heap files. 
 we get our string. 
 string scan is the last step to all the reports and aggregations.
 
 the problem here will be multiple tables,
 if i perform a multi-table search, i cannot have multiple symbol tables, that would result in mismatch in ids.
 so a global symbol table has to take the toll.
 OOOOOORRRRRR,
 we allow shared symbol tables for columns. that solves building a massive symbol table. like a foriegn table instead of a foriegn key
 
 --- part 5
 updates and  deletes 
 tombstone a record and insert the updated by marking the previous as dead and re-assigning the ids for the column files.
 compaction, we rrun this occasionaly to clean up the database.
*/

