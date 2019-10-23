import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MaximumAvg {
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		java.util.Map<String, String> map1 = new HashMap<>();
		 private  LongWritable outKey =new LongWritable();
		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				URI[] cachedFilePaths = context.getCacheFiles();
				Path userFilePath = new Path(cachedFilePaths[0].getPath());
				FileSystem fileSystem = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(userFilePath)));
				String line;
				line = br.readLine();

                while(line != null){
                    String[] attributes = line.split(",");

                    String dob = attributes[9];
		    DateTimeFormatter dateTime = DateTimeFormatter.ofPattern("M/d/yyyy");
                    LocalDate localBirthDate = LocalDate.parse(dob, dateTime);
                    LocalDate today = LocalDate.now();
                    int age=Period.between(localBirthDate, today).getYears();
                    String street = attributes[3].trim();
                    String city = attributes[4].trim();
                    String state = attributes[5].trim();
                    String addressDetails = street + ", " + city + ", " + state;
                    String ageandaddr = Integer.toString(age)+" \t "+addressDetails;
                    map1.put(attributes[0],ageandaddr);
                    line = br.readLine();
                }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException 
		{
			String[] outputDataKey = value.toString().split("\t");
            
              if(!(outputDataKey.length <2)){
                String[] friendsList = outputDataKey[1].split(",");
                String str;
                String anad=map1.get(outputDataKey[0]);
                System.out.print(anad);
                String[] ana_arr=anad.split(" \t ");
                String age=ana_arr[0];
                String add=ana_arr[1];
                Text outValue=new Text();
                String outvalueS=outputDataKey[0]+" \t "+add+" ";
                long avga=0;
                for(String friendOne: friendsList)
                {
                    String anad1=map1.get(friendOne);
                   String[] ana_arr1=anad1.split(" \t ");
                    String age1=ana_arr[0];
                    long age2=Integer.parseInt(age1);
                    avga+=age2;
                }
                
                avga=-(avga/(friendsList.length));

                outKey.set(avga);
                
                context.write(outKey, new Text(outvalueS));
                
            }
	}}
	
	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
		static int count;
		
		@Override
		protected void setup(Reducer<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			count = 0;
		}
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException 
		{
			long positiveAge = (-1) * key.get();
			for(Text value : values){
                if( count < 15){
                    count++;
                    System.out.print(values.toString());
                    System.out.print("count:"+count);
                    String[] outputDataKeyVal1 = value.toString().split(" \t ");
                    long positive=(-1)*key.get();
                    String tempL = Long.toString(positiveAge);
                    String tempVal=(outputDataKeyVal1[1] + " \t " + tempL);
                    context.write(new Text(outputDataKeyVal1[0]), new Text(tempVal));
                }
            }
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length < 3) {
            System.err.println("Usage: Problem1 <in> <in2> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Bigdata_q4");
        job.addCacheFile(new Path(otherArgs[1]).toUri());
        job.setJarByClass(MaximumAvg.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

