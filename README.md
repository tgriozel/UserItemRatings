# What is this?

This is a little example of what can be done with Spark.  
It tackles a concrete business case: reading an input file, generating some Ids from raw identifiers, and doing weighted aggregations.
The results are written in the output files indicated in the *application.conf* file.   

See the code documentation for the precise description of the algorithm, what kind of input is expected, and what kind of output to expect
(this is not ideal, but this is not actual production code, as you could tell).

# Building and running

This is a Spark application, and can be built and run with sbt.

To run the application: `sbt "run input.csv"`
To run the unit tests: `sbt test`

