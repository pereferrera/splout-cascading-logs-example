package com.datasalt.splout.examples.cascading;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
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
import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.hadoop.StoreDeployerTool;
import com.splout.db.hadoop.TableBuilder;
import com.splout.db.hadoop.TablespaceBuilder;
import com.splout.db.hadoop.TablespaceDepSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TupleSampler.DefaultSamplingOptions;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * Simple Cascading + Splout SQL integration example for a Q&A website application and "loyalty campaigns" use case.
 * Processes Apache logs using Cascading and produces two output files: one with the raw parsed logs and one with a 
 * consolidated "groupBy" (user, category, date). Both output files can be then transformed into SQL tables in a Splout SQL
 * tablespace and queried in real-time by a Q&A application. Example timelines and pie chart is showed in "timelines.html".
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
		jComm
		    .setProgramName("Splout-Hadoop-Cascading Example - log parsing with Cascading & serving with Splout SQL.");
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

		indexLogs(inputPath, outputPath + "-logs", outputPath + "-analytics");
		deployToSplout(outputPath + "-logs", outputPath + "-analytics", qnode, 2);

		return 1;
	}

	/**
	 * This method takes all the Apache logs as input and produces two outputs. One is just the raw parsed logs
	 * and the other one is a consolidated group-by (user, category, date). 
	 * <p>
	 * The raw logs parsed output can be used for Q&A : showing the exact activity for a user in a certain timeframe
	 * in a structured way.
	 * <p>
	 * The simple group-by can be used for "loyalty campaigns", showing the "activity footprint" of each user in the
	 * website: what categories does it visit the most, etc.  
	 */
	@SuppressWarnings("rawtypes")
	public void indexLogs(String inputPath, String outputPathLogs, String outputPathAnalytics) {
		// define what the input file looks like, "offset" is bytes from beginning
		TextLine scheme = new TextLine(new Fields("offset", "line"));

		// create SOURCE tap to read a resource from the local file system, if input is not an URL
		Tap logTap = inputPath.matches("^[^:]+://.*") ? new Hfs(scheme, inputPath) : new Lfs(scheme,
		    inputPath);

		// declare the field names we will parse out of the log file
		Fields apacheFields = new Fields("ip", "user", "time", "method", "category", "page", "code", "size");

		// define the regular expression to parse the log file with
		String apacheRegex = "^([^ ]*) +[^ ]* +([^ ]*) +\\[([^]]*)\\] +\\\"([^ ]*) /([^/]*)/([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

		// declare the groups from the above regex we want to keep. each regex group will be given
		// a field name from 'apacheFields', above, respectively
		int[] allGroups = { 1, 2, 3, 4, 5, 6, 7, 8 };

		// create the parser
		RegexParser parser = new RegexParser(apacheFields, apacheRegex, allGroups);

		// create the input analysis Pipe
		Pipe parsePipe = new Each("logs", new Fields("line"), parser, Fields.RESULTS);

		// parse the date and split it into day + month + year
		parsePipe = new Each(parsePipe, new Fields("time"), new DateParser(
		    new Fields("day", "month", "year"), new int[] { Calendar.DAY_OF_MONTH, Calendar.MONTH,
		        Calendar.YEAR }, "dd/MMM/yyyy:HH:mm:ss"), Fields.ALL);

		Pipe analyzePipe = new GroupBy("analyze", parsePipe, new Fields("day", "month", "year", "user",
		    "category"));
		analyzePipe = new Every(analyzePipe, new Count());

		// create a SINK tap to write to the default filesystem
		// To use the output in Splout, save it in binary (SequenceFile).
		// In this way integration is both efficient and easy (no need to re-parse the file again).
		Tap remoteLogTap = new Hfs(new SequenceFile(Fields.ALL), outputPathLogs, SinkMode.REPLACE);
		Tap remoteAnalyticsTap = new Hfs(new SequenceFile(Fields.ALL), outputPathAnalytics, SinkMode.REPLACE);

		// set the current job jar
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, LogIndexer.class);

		Map<String, Tap> sinks = new HashMap<String, Tap>();
		sinks.put("logs", remoteLogTap);
		sinks.put("analyze", remoteAnalyticsTap);

		// connect the assembly to the SOURCE and SINK taps
		Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(logTap, sinks, parsePipe,
		    analyzePipe);

		// start execution of the flow (either locally or on a cluster)
		parsedLogFlow.start();

		// block until the flow completes
		parsedLogFlow.complete();
	}

	/**
	 * Takes the output of the Cascading process and deploys it to Splout SQL using a QNode address.
	 */
	public void deployToSplout(String outputPathLogs, String outputPathAnalytics, String qNode,
	    int nPartitions) throws Exception {
		// add sqlite native libs to DistributedCache
		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
		}

		// delete tablespace-generated files if they already exist
		FileSystem outputPathFileSystem = new Path(outputPath).getFileSystem(conf);
		Path outputToGenerator = new Path(outputPath + "-generated");
		if(outputPathFileSystem.exists(outputToGenerator)) {
			outputPathFileSystem.delete(outputToGenerator, true);
		}

		TablespaceBuilder builder = new TablespaceBuilder();
		// built a Table instance of each table using the builder
		TableBuilder logsTable = new TableBuilder("logs", getConf());
		String[] logsColumns = new String[] { "ip", "user", "time", "method", "category", "page", "code", "size", "day", "month", "year" };
		logsTable.addCascadingTable(new Path(outputPathLogs), logsColumns, conf);
		logsTable.partitionBy("user");

		TableBuilder analyticsTable = new TableBuilder("analytics", getConf());
		String[] analyticsColumns = new String[] { "day", "month", "year", "user", "category", "count" };
		analyticsTable.addCascadingTable(new Path(outputPathAnalytics), analyticsColumns, conf);
		analyticsTable.partitionBy("user");

		builder.add(logsTable.build());
		builder.add(analyticsTable.build());
		// define number of partitions
		builder.setNPartitions(nPartitions);

		// instantiate and call the TablespaceGenerator with the output fo the TablespaceBuilder
		TablespaceGenerator viewGenerator = new TablespaceGenerator(builder.build(), outputToGenerator,
		    this.getClass());
		viewGenerator.generateView(conf, SamplingType.DEFAULT, new DefaultSamplingOptions());

		// finally, deploy the generated files
		StoreDeployerTool deployer = new StoreDeployerTool(qNode, conf);
		List<TablespaceDepSpec> specs = new ArrayList<TablespaceDepSpec>();
		specs.add(new TablespaceDepSpec("cascading_splout_logs_example", outputToGenerator.toString(), 1, null));

		deployer.deploy(specs);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogIndexer(), args);
	}
}
