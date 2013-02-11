package com.datasalt.splout.examples.cascading;

import static cascading.tuple.hadoop.TupleSerializationProps.HADOOP_IO_SERIALIZATIONS;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.WritableSerialization;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;

import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.hadoop.StoreDeployerTool;
import com.splout.db.hadoop.TablespaceDepSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TablespaceSpec;
import com.splout.db.hadoop.TupleSampler.DefaultSamplingOptions;
import com.splout.db.hadoop.TupleSampler.SamplingType;

public class CascadingTableGenerator {

	private Configuration conf;
	private Args args;

	public static class Args {

		private String tablespaceName;
		private String tableName;
		private String[] columnNames;
		private String[] partitionBy;

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public void setTablespaceName(String tablespaceName) {
			this.tablespaceName = tablespaceName;
		}

		public void setColumnNames(String... columnNames) {
	    this.columnNames = columnNames;
    }

		public void setPartitionBy(String... partitionBy) {
			this.partitionBy = partitionBy;
		}
	}

	public CascadingTableGenerator(Args args, Configuration conf) {
		this.args = args;
		this.conf = conf;
		setSerializations(conf);
	}

	/**
	 * Like in TupleSerialization.setSerializations() but accepting a Hadoop's Configuration rather than JobConf.
	 */
	public static void setSerializations(Configuration conf) {
		String serializations = conf.get(HADOOP_IO_SERIALIZATIONS);

		LinkedList<String> list = new LinkedList<String>();

		if(serializations != null && !serializations.isEmpty())
			Collections.addAll(list, serializations.split(","));

		// required by MultiInputSplit
		String writable = WritableSerialization.class.getName();
		String tuple = TupleSerialization.class.getName();

		list.remove(writable);
		list.remove(tuple);

		list.addFirst(writable);
		list.addFirst(tuple);

		// make writable last
		conf.set(HADOOP_IO_SERIALIZATIONS, Util.join(list, ","));
	}

	/**
	 * Takes the output of the Cascading process and deploys it to Splout SQL using a QNode address. There will be one
	 * table: ("day", "month", "year", "count", "metric", "value")
	 */
	public void deployToSplout(String outputPath, String qNode, int nPartitions) throws Exception {
		// add sqlite native libs to DistributedCache
		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			File nativeLibs = new File("native");
			if(nativeLibs.exists()) {
				SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
			}
		}

		// delete indexed files if they already exist
		Path inputToIndexer = new Path(outputPath);
		FileSystem outputPathFileSystem = inputToIndexer.getFileSystem(conf);
		Path outputToIndexer = new Path(outputPath + "-indexed");
		if(outputPathFileSystem.exists(outputToIndexer)) {
			outputPathFileSystem.delete(outputToIndexer, true);
		}

		// 1) call the TablespaceGenerator for generating the indexed SQL files
		CascadingInputFormat inputFormat = new CascadingInputFormat(args.tableName, args.columnNames);
		// we build a TablespaceSpec object with the partitioning strategy and a number of partitions
		// this is the short way of building a simple TablespaceSpec, otherwise you must use TablespaceBuilder.
		TablespaceSpec spec = TablespaceSpec.of(conf, args.partitionBy, inputToIndexer, inputFormat,
		    nPartitions);
		// instantiate and call the TablespaceGenerator
		TablespaceGenerator viewGenerator = new TablespaceGenerator(spec, outputToIndexer, this.getClass());
		viewGenerator.generateView(conf, SamplingType.DEFAULT, new DefaultSamplingOptions());

		// 2) finally, deploy the generated files
		StoreDeployerTool deployer = new StoreDeployerTool(qNode, conf);
		List<TablespaceDepSpec> specs = new ArrayList<TablespaceDepSpec>();
		specs.add(new TablespaceDepSpec(args.tablespaceName, outputToIndexer.toString(), 1, null));

		deployer.deploy(specs);
	}
}
