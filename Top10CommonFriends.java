import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Top10CommonFriends {

	public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String[] friends = values.toString().split("\t");

			if (friends.length == 2) {

				int friend1ID = Integer.parseInt(friends[0]);
				String[] friendsList = friends[1].split(",");
				int friend2ID;
				Text tuple_key = new Text();
				for (String friend2 : friendsList) {
					friend2ID = Integer.parseInt(friend2);
					if (friend1ID < friend2ID) {
						tuple_key.set(friend1ID + "," + friend2ID);
					} else {
						tuple_key.set(friend2ID + "," + friend1ID);
					}
					context.write(tuple_key, new Text(friends[1]));
				}
			}
		}
	}

	public static class ReducerClass1 extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friendsHashMap = new HashMap<String, Integer>();
			int NumberOfCommonFriends = 0;
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				for (String eachFriend : friendsList) {
					if (friendsHashMap.containsKey(eachFriend)) {
						NumberOfCommonFriends++;
					} else {
						friendsHashMap.put(eachFriend, 1);
					}
				}
			}
			context.write(key, new IntWritable(NumberOfCommonFriends));
		}
	}

	public static class MapperClass2 extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			int newVal = Integer.parseInt(values.toString());
			count.set(newVal);
			context.write(count, key);
		}
	}

	public static class ReducerClass2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int idx = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (idx < 10) {
					idx++;
					context.write(value, key);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usuage: <Common friends input file path> <temp file path> <output_path>");
			System.exit(1);
		}

		{
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Mutual Friends");

			job1.setJarByClass(Top10CommonFriends.class);
			job1.setMapperClass(MapperClass1.class);
			job1.setReducerClass(ReducerClass1.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));

			if (!job1.waitForCompletion(true)) {
				System.exit(1);
			}

			{
				Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf2, "Top 10");

				job2.setJarByClass(Top10CommonFriends.class);
				job2.setMapperClass(MapperClass2.class);
				job2.setReducerClass(ReducerClass2.class);

				job2.setMapOutputKeyClass(LongWritable.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(LongWritable.class);

				job2.setInputFormatClass(KeyValueTextInputFormat.class);

				job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

				job2.setNumReduceTasks(1);

				FileInputFormat.addInputPath(job2, new Path(args[1]));
				FileOutputFormat.setOutputPath(job2, new Path(args[2]));

				System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
		}
	}
}