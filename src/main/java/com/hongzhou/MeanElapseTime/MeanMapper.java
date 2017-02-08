package com.hongzhou.MeanElapseTime;

import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MeanMapper extends Mapper<LongWritable, Text, Text, MeanPair> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		// Extracting the user_id and elapsed time
		String[] parts = line.split("\\s+");
		if (parts.length >= 2) {
			String user_id = parts[0];
			String elapsed_time = parts[1];

			// Emitting key=user_id value- pair(elapsed_time, 1)
			context.write(new Text(user_id), new MeanPair(Double.parseDouble(elapsed_time), 1));
		} else
			throw new IOException("malformed input");
	}
}
