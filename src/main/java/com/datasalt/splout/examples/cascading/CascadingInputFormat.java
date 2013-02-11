package com.datasalt.splout.examples.cascading;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;

/**
 * A wrapper around a SequenceFile that contains Cascading's Tuples that implements a Pangool-friendly InputFormat.
 */
@SuppressWarnings("serial")
public class CascadingInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	private Schema pangoolSchema;

	public CascadingInputFormat(Schema pangoolSchema) {
		this.pangoolSchema = pangoolSchema;
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
	    throws IOException, InterruptedException {

		return new RecordReader<ITuple, NullWritable>() {

			RecordReader<cascading.tuple.Tuple, cascading.tuple.Tuple> delegatingRecordReader;
			ITuple tuple = new Tuple(pangoolSchema);

			@Override
			public void close() throws IOException {
			}

			@Override
			public ITuple getCurrentKey() throws IOException, InterruptedException {
				cascading.tuple.Tuple cTuple = delegatingRecordReader.getCurrentKey();
				if(tuple == null) {
					int i = 0;
					for(@SuppressWarnings("unused")
					Class<?> cl : cTuple.getTypes()) {
						tuple.set(i, cTuple.getObject(i));
						i++;
					}
				}
				return tuple;
			}

			@Override
			public NullWritable getCurrentValue() throws IOException, InterruptedException {
				return NullWritable.get();
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return delegatingRecordReader.getProgress();
			}

			@Override
			public void initialize(InputSplit iS, TaskAttemptContext ctx) throws IOException,
			    InterruptedException {
				delegatingRecordReader = new SequenceFileInputFormat<cascading.tuple.Tuple, cascading.tuple.Tuple>()
				    .createRecordReader(iS, ctx);
				delegatingRecordReader.initialize(iS, ctx);
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				return delegatingRecordReader.nextKeyValue();
			}
		};
	}
}
