package chaohBIM;
//input: result from word_doc.txt; basic.xml-- basic paras
//return rank.txt
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class BIMSearch {
	public static HashSet<String> query_Words = new HashSet<String>();
	public static HashMap<String, Double> docs = new HashMap<String,Double>();
	//public static Text functionStat = new Text();
	private static Stemmer s = new Stemmer();
	
	// set the basic paras
	private static double nums;
	private static double k1;
	private static double bval;
	private static double lav;
	
	public static class BIMSearchMapper extends Mapper<Object, Text, Text, Text>{
	
		private Text valueInfo = new Text();
		private Text keyInfo = new Text();

		//each node would get a part of this index file but several of them will find the final word
		//they share a common hash table
		
		public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {	
			String[] indexString = value.toString().split("\t");
			String text = indexString[0];
			if(query_Words.contains(text)){
				keyInfo.set(text);
				valueInfo.set(indexString[1]);
				context.write(keyInfo, valueInfo);
			}
		}
	}
	
	public static class BIMSearchReducer extends Reducer<Text,Text,Text,Text> {
		private Text valueInfo = new Text();
		private Text keyInfo = new Text();
			
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val: values){// for each query word
				String docs_eachword[] = val.toString().split("-");
				for(String each: docs_eachword){// for each doc who contains this word
					String part[] = each.split(":");
					double tf_d_f = Double.valueOf(part[1]);
					String singleword[] = part[0].split("@");
					
					double ld = Double.valueOf(singleword[1]);
					// get RSV for each doc in each word
					Double RSV_d_t = Math.log(nums/docs_eachword.length) * (k1+1)*tf_d_f / (k1*(1-bval+bval*(ld/lav))+tf_d_f);
					
					if(docs.containsKey(singleword[0]))
						docs.put(singleword[0], RSV_d_t+docs.get(singleword[0]));//singleword[0] is the file name
					else
						docs.put(singleword[0], RSV_d_t);
	
				}
			}
			// Traversal the hash table
			/*	
		 	Iterator<Entry<String, Double>> it = docs.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, Double> entry1 = (Map.Entry<String, Double>) it.next();
				keyInfo.set(entry1.getKey());
				valueInfo.set(entry1.getValue().toString());
				context.write(keyInfo, valueInfo);
			}
			*/
			
			//sort the result using hash
			List<Map.Entry<String, Double>> infoIds = new ArrayList<Map.Entry<String, Double>>(docs.entrySet()); 
	        Collections.sort(infoIds, new Comparator<Map.Entry<String, Double>>() {  
	            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {  
	                return (o2.getValue()).toString().compareTo(o1.getValue().toString());
	            }  
	        }); 
	        
	        for (int i = 0; i < infoIds.size(); i++) {  
	        	keyInfo.set(i+"@"+infoIds.get(i).getKey());
	        	valueInfo.set(infoIds.get(i).getValue().toString());
	        	context.write(keyInfo, valueInfo);
	        }
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		//add the query word into a hash table
		String words[] = args[2].split("-");
		String text;
		for(String each: words){
			text = each.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
			s.add(text);
			s.stem();
			text = s.toString();
			query_Words.add(text);
		}
		
		//read from xml file
		//String filename = "data/basic.xml";
		String filename = args[3];
		k1 = Double.parseDouble(DealXML.ReaddomXMl(filename,"k1"));
		bval = Double.parseDouble(DealXML.ReaddomXMl(filename,"bval"));
		lav = Double.parseDouble(DealXML.ReaddomXMl(filename,"lav"));
		nums = Double.parseDouble(DealXML.ReaddomXMl(filename,"nums"));
		
		//----- job1 configuration part ------
		Job job = new Job(conf, "tfidf");
		//set classes
		job.setJarByClass(BIMSearch.class);
		job.setMapperClass(BIMSearchMapper.class);
		job.setReducerClass(BIMSearchReducer.class);

		// set map output key & value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// set output key & value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
