package com.hongzhou.MeanElapseTime;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanCombiner extends Reducer<Text, MeanPair, Text, MeanPair> {
	public void reduce(Text key, Iterable<MeanPair> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		for (MeanPair value : values) {
			sum += value.getPartialSum();
			count += value.getPartialCount();
		}
		
		context.write(key, new MeanPair(sum, count));
	}

}
