
## First attempt

### Dumb reader
The inputs are given in format <city>;<temp>. The cities can have spaces in the name. So, we can't use normal input method here. My first thought was the simplest one, using `getline`. Result? reading 10^8 rows in 1m20s. Yes, just reading. 

### Fast IO
Solution? use `std::ios::sync_with_stdio(false); cin.tie(0)`. Why?
By default, the C++ standard streams (std::cin, std::cout, etc.) are synchronized with their C counterparts (stdin, stdout, etc.). This ensures that the input/output operations between C and C++ are consistent, so you can mix calls like std::cin with printf, or std::cout with scanf, without breaking things.

When you disable this synchronization, the C++ streams (std::cin, std::cout) become independent of the C streams (stdin, stdout). This allows C++ streams to perform input/output operations more efficiently because they no longer need to coordinate with the C standard streams. Thus making it faster.

Result? Reading all input take about 2m now. So, about 12s per 10^8 rows. Huge improvement. 


But we just dont need to read the inputs, we need to seperate the city names, temperature and then convert that temperature to float if it is not already. Sounds simple. However, with this added the time to process 10^8 rows increases drastically to 50s. I really don't know why. Need to check up on this further. 

### Buffered Reader

Solution? read inputs as buffered reader. This is the fastest way to read any kind of data. As disk io is slow, we read a chunk of data and process it. Using buffered reader in C is a bit tough but doable. With this we decrease the time to process 10^8 rows to 25 seconds. ie in total takes about 4m30s

So, i guess we have found our winner for reading the inputs. Then we need to actually do stuffs with them, ie find min, max, mean of the data.

well how can we store them? we definetely can't just store them all in an array. Its too much. We can optimize it later. But for now what we can do is we can create a map with the city name as key and the value as a struct with {sum,count,min,max} as value. Thus each time we read a new entry we update the map. Sounds simple. it is. So, after this, the time to process 10^8 rows is increased to 50ish seconds and to complete the whole process it takes 8m24s. So, this is our starting point. Or the time to beat. Let's optimize.