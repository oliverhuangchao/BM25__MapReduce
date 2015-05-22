package chaohBIM;
/*
 * anoterh method to get rank.txt, not used in this project
 * trying to use two mappers
 * */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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


import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class regionIndexSearch {
	public static HashSet<String> query_Words = new HashSet<String>();//query hash set
	public static HashMap<String, Double> docs_Rankval = new HashMap<String,Double>();//<doc,doc Rank value> hash table
	public static HashMap<String, Integer> docs_word_lenth = new HashMap<String,Integer>();//<doc,doc word length> hash table
	public static HashSet<String> docs_region = new HashSet<String>();//docs_region set
	
	public static Text functionStat = new Text();
	private static Stemmer s = new Stemmer();
	
	
	private static String searchTmpFileString;
	
	// set the basic parameters
	private static double nums;
	private static double k1;
	private static double bval;
	private static double lav;
	private static double ld =20;
	
	
	/* ------ first map function ------*/
	public static class BIM_Words_Mapper extends Mapper<Object, Text, Text, Text>{
		private Text valueInfo = new Text();
		private Text keyInfo = new Text();
		public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {	
			String[] indexString = value.toString().split("\t");//indexstring[0] aa ; indexstring[1]: xml~1111f:1-xmL~2222F:2
			
			if(query_Words.contains(indexString[0])){
				keyInfo.set(indexString[0]);
				valueInfo.set(indexString[1]);
				String[] alldocs = indexString[1].split("-");// alldocs[0]: xml~1111f:1
				for(String eachDoc_all:alldocs){
					String[] docName = eachDoc_all.split(":");// docName[0]: xml~1111f ; docName[1]: 1
					if(!docs_word_lenth.containsKey(docName[0])){
						docs_word_lenth.put(docName[0],0);
						docs_Rankval.put(docName[0], 0.0);
					}
				}
				context.write(keyInfo, valueInfo);
			}
		}
	}
	
	
	/*------ first reducer class ------*/
	public static class BIM_Words_Reducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val: values){
				context.write(key,val);
			}
		}
	}
	
	/* ---------- second mapper function   ---------- 
	 * read pageinfo.txt file
	 * example format: xml~1111f:1000	5
	 * */
	public static class BIM_DocInfo_Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {	
			String[] eachlength = value.toString().split("\t");//eachlength[0] is xml~1111f:1000 ; eachlength[1] is 5
			String[] doc_page = eachlength[0].split(":");// doc_page[0] is xml~111f ; doc_page[1] is 1000
			if(docs_word_lenth.containsKey(doc_page[0])){
				docs_word_lenth.put(doc_page[0],Integer.parseInt(eachlength[1]));
				//context.write(new Text(doc_page[0]), new Text(eachlength[1]));
				context.write(new Text("all Mapper Output"), new Text(doc_page[0]+":"+eachlength[1]));
			}
		}
	}
	
	/*------ second reducer class ------*/
	public static class BIM_DocInfo_Reducer extends Reducer<Text,Text,Text,Text>{
		private Text valueInfo = new Text();
		private Text keyInfo = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			getRank(searchTmpFileString);
			List<Map.Entry<String, Double>> infoIds = new ArrayList<Map.Entry<String, Double>>(docs_Rankval.entrySet()); 
			Collections.sort(infoIds, new Comparator<Map.Entry<String, Double>>() {  
	            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {  
	                return (o2.getValue()).compareTo(o1.getValue());
	            }  
	        });
			
	        
	        for (int i = 0; i < infoIds.size(); i++) {
	        	if(docs_region.contains(infoIds.get(i).getKey())){
		        	keyInfo.set(i+"@"+infoIds.get(i).getKey());
		        	valueInfo.set(infoIds.get(i).getValue().toString());
		        	context.write(keyInfo, valueInfo);
	        	}
	        }
		}
	}
	
	/* read from local file and then analysis
	*	input1: output file path
	*	intpu2: word_doc.txt input path
	*/
	public static void getRank(String wordDetailFileName){
		String[] content = readEeahLine(wordDetailFileName);
		for(String eachcontent: content){//eachcontent: xml~1111f:5-xml~2222f:10
			//System.out.println(eachcontent);
			String docs_eachword[] = eachcontent.split("-");
			for(String eachdoc: docs_eachword){//each: xml~1111f:5
				String part[] = eachdoc.split(":");//part[0] xml~1111f ; part[1]: 5
				
				double tf_d_f = Double.valueOf(part[1]);
				
				//System.out.println(part[0]);
				if(docs_word_lenth.containsKey(part[0])){
					ld = docs_word_lenth.get(part[0]);
				}

				Double RSV_d_t = Math.log(nums/docs_eachword.length) * (k1+1)*tf_d_f / (k1*(1-bval+bval*(ld/lav))+tf_d_f);

				if(docs_Rankval.containsKey(part[0]))
					docs_Rankval.put(part[0], RSV_d_t+docs_Rankval.get(part[0]));//part[0] is the file name

			}
		}
	}
	
	public static String[] readEeahLine(String fileName) {
        File file = new File(fileName);
        String[] result = new String[query_Words.size()];
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 0;
            while ((tempString = reader.readLine()) != null) {
                //System.out.println("line " + line + ": " + tempString);
            	String writeContent[] = tempString.split("\t");
                result[line++] = writeContent[1];
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return result;
    }
	
	public static void getAllQueryWords(String inputstring){
		String words[] = inputstring.split("-");
		String text;
		for(String each: words){
			text = each.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
			s.add(text);
			s.stem();
			text = s.toString();
			query_Words.add(text);
		}
	}
	
	
	
	/*
	 * args[0]: query sequence
	 * args[1]: doc_info.txt path---first mapper inputpath
	 * args[2]:	first reducer ouputpath
	 * args[3]: word_docs.txt path--- second mapper inputpath
	 * args[4]: second reducer outputpath
	 * args[5]: basic.xml path
	 * args[6]: region.xml path
	 * args[7]: region code
	 * 
	 * */
	
	public static void main(String[] args) throws Exception {
		
		docs_region = ReadFromFile.readRegionByLines(args[6], args[7]);
		
		//add the query word into a hash table
		getAllQueryWords(args[0]);
		searchTmpFileString = args[2]+"/part-r-00000";
		String basic_filename = args[5];
		System.out.println(args[5]);
		k1 = Double.parseDouble(DealXML.ReaddomXMl(basic_filename,"k1"));
		bval = Double.parseDouble(DealXML.ReaddomXMl(basic_filename,"bval"));
		lav = Double.parseDouble(DealXML.ReaddomXMl(basic_filename,"lav"));
		nums = Double.parseDouble(DealXML.ReaddomXMl(basic_filename,"nums"));
		
		
		/* ----- job1 configuration part ------ */
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"job1");
		job1.setJarByClass(regionIndexSearch.class);
		job1.setMapperClass(BIM_Words_Mapper.class);
		job1.setReducerClass(BIM_Words_Reducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		
		/*------ job2 configuration part ------*/
		Configuration conf2 = new Configuration();  
		Job job2 = new Job(conf2,"Job2"); 
		job2.setJarByClass(regionIndexSearch.class);
		job2.setMapperClass(BIM_DocInfo_Mapper.class);
		job2.setReducerClass(BIM_DocInfo_Reducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[3]));
		FileOutputFormat.setOutputPath(job2, new Path(args[4]));		
		
	
		/*------ set controlled job1 for the new tasks ------*/
		ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());
		cjob1.setJob(job1);    
		ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
		cjob2.setJob(job2);    
		cjob2.addDependingJob(cjob1);
		JobControl jc = new JobControl("nice");
		jc.addJob(cjob1);
		jc.addJob(cjob2);
		Thread jcThread = new Thread(jc);
		jcThread.start();
			//show some info about the job
		while(true){    
            if(jc.allFinished()){    
                System.out.println(jc.getSuccessfulJobList());    
                jc.stop();    
                return;    
            }    
            if(jc.getFailedJobList().size() > 0){    
                System.out.println(jc.getFailedJobList());    
                jc.stop();    
                return;    
            }    
        }  
	}
}
