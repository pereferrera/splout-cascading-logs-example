splout-cascading-logs-example
=============================

A simple Cascading + Splout SQL integration example for analyzing logs for a "customer service" webapp use case.

The example
===========

This example uses Cascading for processing Apache logs of an imaginary e-Commerce website, and deploys the result to 
a Splout SQL cluster for being used by a "customer service" webapp.

This imaginary website has many users and several categories. There is a "customer service" department that takes care of troubleshooting, 
handling user calls and maintaining client loyalty. For that, it needs to be able to:

* For any user, retrieve the exact sequence of events that this user made in the website within a certain timeframe. 
This helps in detecting the root cause of an issue they user might have had, and can also be valuable information for the 
technical department in detecting and fixing new bugs.

![alt text](https://raw.github.com/pereferrera/splout-cascading-logs-example/master/raw-logs.png)

* For any user, be able to "visualize" an activity "footprint" for performing "loyalty actions" or campaigns. For instance, knowing the
top 5 categories the user interacted with in the past days allows the "customer service" to offer discounts or any other promotional
products on interesting categories for the user.   

![alt text](https://raw.github.com/pereferrera/splout-cascading-logs-example/master/pie-chart.png)
![alt text](https://raw.github.com/pereferrera/splout-cascading-logs-example/master/timeline.png)

The solution
============

We need a solution which is:

* Scalable both in processing and serving. The amount of data to be queried by the webapp is as Big Data as the amount of data 
to be analyzed as input (logs).
* Simple to implement.
* Flexible - we can add / change statistics, change the processing business logic and recompute everything easily. 
 
In this solution the Apache logs are parsed and analyzed using Cascading which produces two output files: one with the raw parsed logs and one with a 
consolidated "groupBy" (user, category, date). Both output files can be then transformed into SQL tables in a Splout SQL
tablespace and queried in real-time by the "customer service" webapp.

Using Cascading for the processing allows us to develop and iterate fast. Using Splout SQL for serving the output allows us to 
perform flexible SQL queries over the analyzed datasets and scale horizontally without having a complex and expensive system underneath.

Try it
======

- Start Splout SQL in your machine.

From an IDE (local mode):

- Run the "ApacheAccessLogGenerator"
- Run "LogIndexer" with parameters "-i access.log -o out-clogs -q http://localhost:4412"

From Hadoop pseudo or distributed mode:

	mvn install
	cd target/
	tar xvfz splout-cascading-logs-example-0.0.1-SNAPSHOT-distro.tar.gz 
	cd splout-cascading-logs-example-0.0.1-SNAPSHOT/
 	hadoop jar splout-cascading-logs-example-0.0.1-SNAPSHOT-hadoop.jar generator
	hadoop fs -put access.log .
	hadoop jar splout-cascading-logs-example-0.0.1-SNAPSHOT-hadoop.jar indexer -i access.log -o out-clogs -q http://localhost:4412

Finally:

- Open "timelines.html" in your browser of choice -> This html communicates via JavaScript with Splout's REST API and shows some Google Visualization charts.
- Go to Splout's webapp and execute any query you want using table "logs" or table "analysis", for example:

	Detailed activity of user0 since the beginning of times:
	(key = user0)
	SELECT * FROM logs WHERE user = "user0" ORDER BY time DESC;
	
	Activity footprint from user0 since the beginning of times:
	(key = user0)
	SELECT category, COUNT(*) as activity FROM analytics WHERE user = "user0" GROUP BY category ORDER BY activity DESC LIMIT 5;";