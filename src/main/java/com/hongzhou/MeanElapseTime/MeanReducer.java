package com.hongzhou.MeanElapseTime;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MeanReducer extends Reducer<Text, MeanPair, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<MeanPair> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		// Adding the partial sums and partial counts
		for (MeanPair value : values) {
			sum += value.getPartialSum();
			count += value.getPartialCount();
		}
		
		context.write(key, new DoubleWritable(sum / count));
	}

}
