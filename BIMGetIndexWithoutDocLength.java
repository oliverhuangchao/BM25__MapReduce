package chaohBIM;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BIMGetIndexWithoutDocLength {
	//public static HashMap<String, Integer> docsword = new HashMap<String,Integer>();

	public static class tfidfMapper extends Mapper<Text, BytesWritable, Text, Text>{
	
		//private final static IntWritable one = new IntWritable(1);
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private Stemmer s = new Stemmer();

		public void map(Text key, BytesWritable value, Context context) throws  IOException, InterruptedException {
			String tmp = new String( value.getBytes(), "UTF-8" );
			String text = parsexml.getContentlineFromXML(tmp).replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer( text );
			tmp = key.toString().replace("newsML.xml", "f");
			valueInfo.set("1");
			while ( tokenizer.hasMoreTokens() ){
	        	s.add(tokenizer.nextToken());
	        	s.stem();
	        	if(stopword.findit(s.toString())){
		        	keyInfo.set(s.toString()+"-"+tmp);//"word-passage"
		        	context.write(keyInfo,valueInfo);
	        	}
	        }
			//System.out.println(tmp.toString()+count);
		}
	}
	
	public static class tfidfCombiner extends Reducer<Text,Text,Text,Text> {
		private Text valueInfo = new Text();
		private Text keyInfo = new Text();

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	String tmpStrings[] = key.toString().split("-");
	    	Integer count = 0;
	    	for (Text val : values) {
	    		//System.out.println(val.toString());//in order to make the warning disappear
	    		count += 1;
		    }
	    	keyInfo.set(tmpStrings[0]);
		    valueInfo.set(tmpStrings[1]+":"+count.toString());
		    context.write(keyInfo,valueInfo);
	    }
	}
	
	
	public static class tfidfdReducer extends Reducer<Text,Text,Text,Text> {
		private Text valueInfo = new Text();
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		    StringBuffer fileList = new StringBuffer();
		    for (Text val : values) {
				String string = val.toString();      
				fileList.append(string+"-");
		    }
		    valueInfo.set(fileList.toString());
		    context.write(key,valueInfo);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "getTfidf");
	    job.setJarByClass(BIMGetIndexWithoutDocLength.class);

	    job.setMapperClass(tfidfMapper.class);
	    
	    job.setCombinerClass(tfidfCombiner.class);	    
	    job.setReducerClass(tfidfdReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setInputFormatClass(ZipFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 }

