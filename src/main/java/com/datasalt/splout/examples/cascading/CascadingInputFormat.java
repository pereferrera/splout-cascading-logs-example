package com.datasalt.splout.examples.cascading;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;

/**
 * A wrapper around a SequenceFile that contains Cascading's Tuples that implements a Pangool-friendly InputFormat.
 */
@SuppressWarnings("serial")
public class CascadingInputFormat extends FileInputFormat<ITuple, NullWritable> implements Serializable {

	private String tableName;
	private String[] fieldNames;

	public CascadingInputFormat(String tableName, String... fieldNames) {
		this.tableName = tableName;
		this.fieldNames = fieldNames;
	}

	@Override
	public RecordReader<ITuple, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
	    throws IOException, InterruptedException {

		return new RecordReader<ITuple, NullWritable>() {

			RecordReader<cascading.tuple.Tuple, cascading.tuple.Tuple> delegatingRecordReader;
			ITuple tuple;

			@Override
			public void close() throws IOException {
			}

			@Override
			public ITuple getCurrentKey() throws IOException, InterruptedException {
				cascading.tuple.Tuple cTuple = delegatingRecordReader.getCurrentValue();
				
				if(tuple == null) {
					int i = 0;
					List<Field> fields = new ArrayList<Field>();
					
					for(Class<?> cl : cTuple.getTypes()) {
						
						if(cl.equals(Integer.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.INT));
						} else if(cl.equals(Long.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.LONG));
						} else if(cl.equals(Float.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.FLOAT));
						} else if(cl.equals(Double.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.DOUBLE));
						} else if(cl.equals(String.class)) {
							fields.add(Field.create(fieldNames[i], Field.Type.STRING));
						} 
						
						i++;
					}
					
					// TODO Error checking etc
					
					tuple = new Tuple(new Schema(tableName, fields));
				}
				
				for(int i = 0; i < tuple.getSchema().getFields().size(); i++) {
					tuple.set(i, cTuple.getObject(i));
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
