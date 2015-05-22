package chaohBIM;
/*
 * get the data infor files
 * */


import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BIMGetFileInfo {
	//create a new counter
	enum filetype{
		Accepted,
		Refused,
		WholeLength
	}
	//output: key: passage name, value: how many word
	public static class BIM_Mapper extends Mapper<Text, BytesWritable, Text, IntWritable>{
	
		private Text keyInfo = new Text();
		private IntWritable valueInfo = new IntWritable();
		private Stemmer s = new Stemmer();

		public void map(Text key, BytesWritable value, Context context) throws  IOException, InterruptedException {
			//processing each word
			String tmp = new String( value.getBytes(), "UTF-8" );
			String text = parsexml.getContentlineFromXML(tmp).replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer( text );
			
			keyInfo.set(key.toString().replace("newsML.xml", "f"));
			
			int count = 0;
			while ( tokenizer.hasMoreTokens() ){
	        	s.add(tokenizer.nextToken());
	        	s.stem();
	        	if(stopword.findit(s.toString())){
	        		++count;
	        	}
				
	        }
			
			valueInfo.set(count);
			
			context.write(keyInfo,valueInfo);
			
			/*
			Configuration conf = context.getConfiguration();
			String counter = conf.get("counter");
			counter = "hello";
			conf.set("counter", counter);
			*/
			/*
			 * set the counter information
			long counter = context.getCounter(filetype.Accepted).getValue();
			context.getCounter(filetype.Accepted).setValue(counter+1);
			*/
		}
	}
	
	
	
	//return the average length of all the passages
	public static class BIM_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	    int count = 0;
	    public Text keyinfo = new Text();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		    for (IntWritable val : values) {
				count += val.get();
				keyinfo.set(key.toString()+":"+count);
				context.write(keyinfo,val);
		    }
		    //System.out.println(context.getCounter(filetype.Accepted).getValue());
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("counter", "1");
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "getPassageInfor");
	    job.setJarByClass(BIMGetFileInfo.class);

	    job.setMapperClass(BIM_Mapper.class);
	    job.setReducerClass(BIM_Reducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    
	    job.setInputFormatClass(ZipFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    //System.out.println(conf.get("counter"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	}
 }


