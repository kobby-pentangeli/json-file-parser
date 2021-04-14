# Orao Gap Calculation Task
JSON file parser to aid in calculating data-quality metrics

### The Task

We are receiving blocks of data from different providers, every data provider undertakes to send data with a certain frequency. 
For example, a provider can tell that it is sending data every one second. It means that during an hour we expect to have 3600 values. 
The problem is that data comes not exactly every second, it can be in interval 1 second +-100 milliseconds and of course, sometimes a provider can stop sending data or send the same value twice. 

We need to calculate the following metrics:

- *`amount of "good" lines`. We need to delete duplicates. If we have 2 values with near the same timestamp (near is +-100 ms) it means that we have a duplicate, for us, this is a bad line,
- *`maximum gap`. If we have at least one bad line, we have a gap of 1, if we have 10 bad lines sequencing, we have a gap of 10. We need to calculate the maximum gap
- *`average gap` - the same with 2, but average on all gaps
 
