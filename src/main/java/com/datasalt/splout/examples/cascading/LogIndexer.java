package com.datasalt.splout.examples.cascading;

import java.util.Calendar;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Work-in-progress.
 */
public class LogIndexer implements Tool {

	@Parameter(required = true, names = { "-q", "--qnode" }, description = "A QNode address, will be used for deploying the log indexer analytics.")
	private String qnode;

	@Parameter(required = true, names = { "-i", "--input" }, description = "The input path where the Apache logs are. If using HDFS, it must be absolute (e.g. hdfs://localhost:8020/user/foo/foo). Otherwise it will be considered a local path in the local filesystem.")
	private String inputPath;

	@Parameter(required = true, names = { "-o", "--output" }, description = "The output path where the Cascading process will output its result.")
	private String outputPath;

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Splout-Hadoop Starter Example - log analysis with Cascading & serving with Splout SQL.");
		try {
			jComm.parse(args);
		} catch(ParameterException e) {
			System.err.println(e.getMessage());
			jComm.usage();
			return -1;
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			return -1;
		}

//		analyzeLogs(inputPath, outputPath);
		deployToSplout(outputPath, qnode, 2);

		return 1;
	}
	
	/**
	 * This method takes all the Apache logs as input and parses them and analyzes them using Cascading. What the analysis
	 * will do is pretty simple: provide aggregate counts for each day, each day and ip, each day and status code and each
	 * day and page. All the counts will be normalized into the same schema: ("day", "month", "year", "count", "metric",
	 * "value") that will be the final table we will load into Splout SQL.
	 */
	@SuppressWarnings("rawtypes")
	public void analyzeLogs(String inputPath, String outputPath) {
		// define what the input file looks like, "offset" is bytes from beginning
		TextLine scheme = new TextLine(new Fields("offset", "line"));

		// create SOURCE tap to read a resource from the local file system, if input is not an URL
		Tap logTap = inputPath.matches("^[^:]+://.*") ? new Hfs(scheme, inputPath) : new Lfs(scheme,
		    inputPath);

		// declare the field names we will parse out of the log file
		Fields apacheFields = new Fields("ip", "time", "method", "page", "code", "size");

		// define the regular expression to parse the log file with
		String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

		// declare the groups from the above regex we want to keep. each regex group will be given
		// a field name from 'apacheFields', above, respectively
		int[] allGroups = { 1, 2, 3, 4, 5, 6 };

		// create the parser
		RegexParser parser = new RegexParser(apacheFields, apacheRegex, allGroups);

		// create the input analysis Pipe
		Pipe analyzePipe = new Each("analyze", new Fields("line"), parser, Fields.RESULTS);

		// parse the date and split it into day + month + year
		analyzePipe = new Each(analyzePipe, new Fields("time"), new DateParser(new Fields("day", "month",
		    "year"), new int[] { Calendar.DAY_OF_MONTH, Calendar.MONTH, Calendar.YEAR },
		    "dd/MMM/yyyy:HH:mm:ss"), Fields.ALL);

		// 1) calculate daily total hits
		Pipe dailyHits = new GroupBy("dailyhits", analyzePipe, new Fields("day", "month", "year"));
		// count() function does the job of doing count(*) for each group
		dailyHits = new Every(dailyHits, new Count());
		// we add constant values: value = "", metric = "ALL"
		dailyHits = new Each(dailyHits, new Insert(new Fields("metric", "value"), "ALL", ""), Fields.ALL);

		// 2) calculate daily unique IP visits
		Pipe dailyIpHits = new GroupBy("dailyiphits", analyzePipe, new Fields("day", "month", "year", "ip"));
		dailyIpHits = new Every(dailyIpHits, new Count());
		// we add constant value: metric = "IP"
		dailyIpHits = new Each(dailyIpHits, new Insert(new Fields("metric"), "IP"), Fields.ALL);
		// we rename "ip" field to "value"
		dailyIpHits = new Rename(dailyIpHits, new Fields("ip"), new Fields("value"));

		// 3) calculate daily page views for each page
		Pipe dailyPageHits = new GroupBy("dailypagehits", analyzePipe, new Fields("day", "month", "year",
		    "page"));
		dailyPageHits = new Every(dailyPageHits, new Count());
		// we add constant value: metric = "PAGE"
		dailyPageHits = new Each(dailyPageHits, new Insert(new Fields("metric"), "PAGE"), Fields.ALL);
		// we rename "page" to "value"
		dailyPageHits = new Rename(dailyPageHits, new Fields("page"), new Fields("value"));

		// 4) calculate daily HTTP status code counts
		Pipe dailyCodeHits = new GroupBy("dailycodehits", analyzePipe, new Fields("day", "month", "year",
		    "code"));
		dailyCodeHits = new Every(dailyCodeHits, new Count());
		// we add constant value: metric = "CODE"
		dailyCodeHits = new Each(dailyCodeHits, new Insert(new Fields("metric"), "CODE"), Fields.ALL);
		// we rename "code" to "value"
		dailyCodeHits = new Rename(dailyCodeHits, new Fields("code"), new Fields("value"));

		// merge all the stats into the same pipe: ("day", "month", "year", "count", "metric", "value")
		// this will be the final SQL table we will use in the app.
		Pipe mergedPipe = new Merge(dailyHits, dailyIpHits, dailyPageHits, dailyCodeHits);

		// create a SINK tap to write to the default filesystem
		// by default, TextLine writes all fields out
		Tap remoteLogTap = new Hfs(new SequenceFile(Fields.ALL), outputPath, SinkMode.REPLACE);

		// set the current job jar
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, LogIndexer.class);

		// connect the assembly to the SOURCE and SINK taps
		Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(logTap, remoteLogTap, mergedPipe);

		// start execution of the flow (either locally or on a cluster)
		parsedLogFlow.start();

		// block until the flow completes
		parsedLogFlow.complete();
	}

	/**
	 * Takes the output of the Cascading process and deploys it to Splout SQL using a QNode address.
	 * There will be one table: ("day", "month", "year", "count", "metric", "value")
	 */
	public void deployToSplout(String outputPath, String qNode, int nPartitions) throws Exception {
		
		// define the Schema of the Splout SQL table
		CascadingTableGenerator.Args args = new CascadingTableGenerator.Args();
		args.setColumnNames("day", "month", "year", "count", "metric", "value");
		args.setTableName("apache_logs_analytics");
		args.setTablespaceName("apache_logs_analytics");
		args.setPartitionBy("metric");

		CascadingTableGenerator generator = new CascadingTableGenerator(args, conf);
		generator.deployToSplout(outputPath, qNode, nPartitions);
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogIndexer(), args);
	}
}
