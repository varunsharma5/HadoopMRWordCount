package com.hadoop.mr;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Defining a local variable one of type IntWritable
	private final static IntWritable one = new IntWritable(1);

	//Defining a local variable word of type Text
	private Text word = new Text();

	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		//Converting the record (single line) to String and storing it
		//in a String variable line
		String line = value.toString();

		//StringTokenizer is breaking the record (line) into words
		StringTokenizer tokenizer = new StringTokenizer(line);

		//Running while loop to get each token(word) one by one from
		//StringTokenizer
		while (tokenizer.hasMoreTokens()) {
			//Saving the token(word) in a variable word
			word.set(tokenizer.nextToken());
			//Writing the output as (word, one), the value
			//of word will be equal to token and value
			//of one is 1
			context.write(word, one);
		}
	}
}
