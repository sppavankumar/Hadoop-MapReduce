package YahooFinance.yahoofinance;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;


public class yahoofinance {

/*
 *Created by Pavan Kumar SP
 * */	

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
		
			String line = value.toString();
			String[] linevalue = line.split("\t");
			Text a = new Text(linevalue[0]);
			String keyid = linevalue[0].substring(0,3);
			Text b = new Text(linevalue[2]);
			Text c = new Text(linevalue[3]);

			Text keyidText = new Text(keyid);
		
			Text val = new Text(a + ";" + b + ";" + c);

			output.collect(keyidText,val);
			
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, DoubleWritable> {
	
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			
			Text maxKey = null;
			Text minKey = null;
			double maxvalue = 0.0;
			double minvalue = 0.0;
			String line;
			String[] linevalue;
			Double highstockvaltmp;
			Double lowstockvaltmp;
			HashMap<Text,Double> HighHashMap =new HashMap<Text, Double>();
			HashMap<Text,Double> LowHashMap =new HashMap<Text, Double>();
			
			while(values.hasNext())
			{
				line = values.next().toString();
				linevalue = line.split(";");
				Text dateval = new Text(linevalue[0]);
				highstockvaltmp = Double.parseDouble(linevalue[1]);
				lowstockvaltmp = Double.parseDouble(linevalue[2]);

				HighHashMap.put(dateval, highstockvaltmp);
				LowHashMap.put(dateval, lowstockvaltmp);
			}
			
			maxvalue = (Collections.max(HighHashMap.values()));
			minvalue = (Collections.min(LowHashMap.values()));
			
			 for (Entry<Text, Double> entryHigh : HighHashMap.entrySet()) 
		        {  // Iterate through hashmap
		            if (entryHigh.getValue()==maxvalue) {
		                maxKey = entryHigh.getKey();
		            }
		        }
			 
			 for (Entry<Text, Double> entryLow : LowHashMap.entrySet()) 
		        {  // Iterate through hashmap
		            if (entryLow.getValue()==minvalue) {
		                minKey = entryLow.getKey();
		            }
		        }
			output.collect(maxKey, new DoubleWritable(maxvalue));
			output.collect(minKey, new DoubleWritable(minvalue));

		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(yahoofinance.class);
		conf.setJobName("Yahoo Finance");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
//		conf.setPartitionerClass(Partitioner.class);
//		conf.setNumReduceTasks(2);;
//		conf.setNumMapTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
