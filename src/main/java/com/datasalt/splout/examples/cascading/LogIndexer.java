package com.datasalt.splout.examples.cascading;

import java.util.Calendar;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
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
		jComm.setProgramName("Splout-Hadoop-Cascading Example - log parsing with Cascading & serving with Splout SQL.");
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

		indexLogs(inputPath, outputPath);
		deployToSplout(outputPath, qnode, 2);

		return 1;
	}
	
	/**
	 * This method takes all the Apache logs as input and ...
	 */
	@SuppressWarnings("rawtypes")
	public void indexLogs(String inputPath, String outputPath) {
		// define what the input file looks like, "offset" is bytes from beginning
		TextLine scheme = new TextLine(new Fields("offset", "line"));

		// create SOURCE tap to read a resource from the local file system, if input is not an URL
		Tap logTap = inputPath.matches("^[^:]+://.*") ? new Hfs(scheme, inputPath) : new Lfs(scheme,
		    inputPath);

		// declare the field names we will parse out of the log file
		Fields apacheFields = new Fields("ip", "user", "time", "method", "page", "code", "size");

		// define the regular expression to parse the log file with
		String apacheRegex = "^([^ ]*) +[^ ]* +([^ ]*) +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

		// declare the groups from the above regex we want to keep. each regex group will be given
		// a field name from 'apacheFields', above, respectively
		int[] allGroups = { 1, 2, 3, 4, 5, 6, 7 };

		// create the parser
		RegexParser parser = new RegexParser(apacheFields, apacheRegex, allGroups);

		// create the input analysis Pipe
		Pipe parsePipe = new Each("parse", new Fields("line"), parser, Fields.RESULTS);

		// parse the date and split it into day + month + year
		parsePipe = new Each(parsePipe, new Fields("time"), new DateParser(new Fields("day", "month",
		    "year"), new int[] { Calendar.DAY_OF_MONTH, Calendar.MONTH, Calendar.YEAR },
		    "dd/MMM/yyyy:HH:mm:ss"), Fields.ALL);

		// create a SINK tap to write to the default filesystem
		// To use the output in Splout, save it in binary (SequenceFile).
		// In this way integration is both efficient and easy (no need to re-parse the file again).
		Tap remoteLogTap = new Hfs(new SequenceFile(Fields.ALL), outputPath, SinkMode.REPLACE);

		// set the current job jar
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, LogIndexer.class);

		// connect the assembly to the SOURCE and SINK taps
		Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(logTap, remoteLogTap, parsePipe);

		// start execution of the flow (either locally or on a cluster)
		parsedLogFlow.start();

		// block until the flow completes
		parsedLogFlow.complete();
	}

	/**
	 * Takes the output of the Cascading process and deploys it to Splout SQL using a QNode address.
	 */
	public void deployToSplout(String outputPath, String qNode, int nPartitions) throws Exception {
		
		// define the Schema of the Splout SQL table
		CascadingTableGenerator.Args args = new CascadingTableGenerator.Args();
		args.setColumnNames("ip", "user", "date", "method", "page", "response", "bytes", "day", "month", "year");
		args.setTableName("apache_logs_analytics");
		args.setTablespaceName("apache_logs_analytics");
		args.setPartitionBy("user");

		CascadingTableGenerator generator = new CascadingTableGenerator(args, conf);
		generator.deployToSplout(outputPath, qNode, nPartitions);
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogIndexer(), args);
	}
}
