## Mergesort

Here, we have a [funsies-based implementation](./mergesort.py) of the
mergesort algorithm, using recursion. This is quite possibly the least
efficient way to sort a list of integers (parallel though! 😁), but it does
demonstrate quite effectively dynamic DAG generation. The attached script will
sort a random list of 120 integers, which requires 7 nested workflows, all
generated recursively and dynamically. The final graph [is rather
interesting.](./graph.pdf)

The main challenge in this toy example is that we have to terminate our
recursions without explicitly extracting the funsies data (for euhm
performance reasons?). We use error propagation for this: basically in each
nested sub-workflow, we raise if less than 2 elements are present and stop
recursing deeper. What we get is basically recursive, nested
[MapReduce.](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)

Although this is rather silly, a similar approach could conceivably be used
for large-scale search problems using [a divide-and-conquer
algorithm](https://en.wikipedia.org/wiki/Bisection_method). It also
demonstrates how funsies can be used for pure python problems.



