package com.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author varun
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	protected void reduce(Text key, java.lang.Iterable<IntWritable> values, Context context) throws java.io.IOException ,InterruptedException {
		//Defining a local variable sum of type int
		int sum = 0;
		//Running for loop to iterate over the values present in
		//Iterator
		for (IntWritable val : values) {
			//We are adding the value to the variable over
			//every iteration
			sum = sum + val.get();
			//Finally writing the key and the value of sum(number of times
			//the word occurred in the input file) to the output file
		}
		context.write(key, new IntWritable(sum));
	}
}