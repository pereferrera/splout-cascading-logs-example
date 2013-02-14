package com.datasalt.splout.examples.cascading;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.tuplemr.mapred.lib.input.CascadingTupleInputFormat;
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
		CascadingTupleInputFormat.setSerializations(conf);
	}

	/**
	 * Takes the output of the Cascading process and deploys it to Splout SQL using a QNode address.
	 */
	public void deployToSplout(String outputPath, String qNode, int nPartitions) throws Exception {
		// add sqlite native libs to DistributedCache
		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
		}

		// delete indexed files if they already exist
		Path inputToIndexer = new Path(outputPath);
		FileSystem outputPathFileSystem = inputToIndexer.getFileSystem(conf);
		Path outputToIndexer = new Path(outputPath + "-indexed");
		if(outputPathFileSystem.exists(outputToIndexer)) {
			outputPathFileSystem.delete(outputToIndexer, true);
		}

		// 1) call the TablespaceGenerator for generating the indexed SQL files
		CascadingTupleInputFormat inputFormat = new CascadingTupleInputFormat(args.tableName, args.columnNames);
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
