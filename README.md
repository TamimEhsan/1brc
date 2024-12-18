# The One Billion Row Challenge
This is my attempt to solve the 1 billion row chalenge with cpp and go. The challenge is to calculate the min, max, and average of 1 billion measurements of temperature data. To learn about the 1brc visit the site [1brc](https://1brc.dev/). 

This is also a journal of my process to improve the initial runtime of 5m45s to 0m10s ie 10M data processing per second! 

## Attempts

| Sl | Time m:s:ms | Description |
| :- | :-: | :- |
| 1 | 05:46:00 | C++, Buffered reader, hash map |
| 2 | 02:48:00 | Go reimplementation |
| 3 | 11:16:00 | producer consumer, channels |
| 4 | 02:04:00 | buffered channels |
| 5 | 01:01:00 | SSD, memorypool |
| 6 | 00:20:00 | MapReduce |
| 7 | 00:10:00 | Custom Parser |

## The data
The data is in format `City Name;temp\n`. There are spaces in between the city name, some city name has unicode characters. The temperature can be both positive and negative. However the temperature has exactly 1 digit precision. 
```
Kansas City;-0.8
Damascus;19.8
Kansas City;28.0
La Ceiba;17.0
Darwin;29.1
```

## Hardware

**Processor:** AMD Ryzen 5 3600 6-Core Processor  
**Ram:** 16GB DDR4 32000MHz  
**HDD:** read speed 107MB/s  
**SSD:** read speed 1.5GB/s  

## Revisions
I will add small description of each attempts. To learn more about the whole journey read the [history](history.md). 

I will call a single line of input as simply a line. Some line batched together is a chunk. The line after processing will produce a string: city name  and a floating value: temperature. 
### Revision 1: C++, Buffered reader, hash map
It was supposed to be a simple implementation, however trying to write a custom buffered reader made it a mess. The logic is pretty straight forward. Read data in chunk, extract the individual lines and process them. The first revision took 5m46s
### Revision 2: Go reimplementation
It's the exact same thing as revision 1 but in go. As go has builtin buffered reader, the code is much simpler and readable. This one takes 2m48s. Simplicity is always better
### Revision 3: Producer consumer pattern using channels
This time I introduced concurrency. 1 thread will read the data and produce line by line and the consumers will consume them to process. It's a simple producer and consumer pattern. However, it took way longer than I expected,11m16s. 
### Revision 4: Buffered channels
The reason revision 3 took so long is that I was producing too much data too often in the channels and the workers were battling each other for locks and other concurrency related overheads. Instead of producing a single line, I bundled a batch and then send it to the workers. This worked better than before with run time 2m04s.

### Revision 5: SSD and memory pool
From this revision onward I started using SSD. After some profiling I found that there were significant garbage collector overhead. I tried to use a memory pool. Instead of sending a batch, I stored them in a global memory and sent the address. Honestly speaking this didn't improve performance that much and made the code horrible. Not proud. Shift to SSD improved our time to 1m01s.

### Revision 6: Map Reduce
As we have entered the 1m mark, I took a step back and tried to see the flow of data. Then it suddenly occured to me, the current structure resembles the Map Reduce structure a lot! I unknowingly wrote a map reduce job in go. After the realization I refactored the code to be more like map reduce and viola! Just changing some code from one stage to another improved the time to 0m20s. 

It is better to include that the thing I was doing wrong was to split a chunk into individual lines in the reader process. So, basically how many worker I use to process those data, I am bottlenecked by the single process. The map reduce paradigm at first splits the file into chunks, then the workers process this chunk and create outputs and finally the reducer merges all of those results from the mapper into the final output. It's very simple. As I mentioned earlier Simplicity is always better.

### Revision 7: Custom parser
Improving from revision 6 was quite hard! I profiled the whole codebase and identified the bottlenecks. There were significant time consumer in floating point parsing, line splitting etc. It was a clear choice to write a custom parser. On top of that changing the key of the map to a integer hash also helped. Finally making a runtime of 0m10s!

## Conclusion
The code can be optimized further. I did the whole thing without any help from other solutions. Turns out others used some what similar approach too! Some things they used were memory mapped files, custom hash map etc. I am fine with my current implementation. It's simple. In the end I managed to process 10M of data per second! 

I was aiming to come close to Ben Hoyt's runtime of 5s(in my machine) and coming up with something that only 1/2 times slower than his is a big win for me! It will take two lifetime for me to reach him! So, I am fine with this. I encourage you to read the [history](history.md). You will see how I iterated overtime and how I think. 
