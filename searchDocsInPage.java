package chaohBIM;

//��ǰ��ѯ�����ǰʮ���ĵ���ϸ��ץȡ
//get result from rank.txt
// �Ѿ��ĺ���˳��ֻ�ǽ��������
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class searchDocsInPage {

	public static HashMap<String, String> mysearchDocs = new HashMap<String,String>();
	public static int pageStart;
	public static int pageEnd;
	public static String detail_nameString;
	
	public static class docFinderMapper extends Mapper<Text, BytesWritable, Text, Text>{
	
		private Text valueInfo = new Text();

		public void map(Text key, BytesWritable value, Context context) throws  IOException, InterruptedException {
			String docName = key.toString().replace("newsML.xml", "f");
			if(mysearchDocs.containsKey(docName)){
				String tmp = new String( value.getBytes(), "UTF-8" );
				valueInfo.set(tmp);
				
				//write title to xml
				String title = parsexml.getTitleFromXML(tmp);
				DealXML.writefile(detail_nameString, mysearchDocs.get(docName), 0, title);
				String detail = parsexml.getContentlineFromXML(tmp);
				
				//write content to xml
				detail = detail.replace("<p>", "");
				detail = detail.replace("</p>", "");
				DealXML.writefile(detail_nameString, mysearchDocs.get(docName), 1, detail);
				
				//write page title to xml
				DealXML.writefile(detail_nameString, mysearchDocs.get(docName), 2, key.toString());
				
				context.write(key, valueInfo);
			}
		}
	}
	
	public static class docFinderReducer extends Reducer<Text,Text,Text,Text>{
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		   for(Text val: values){
		    context.write(key,val);
		   }
	    }
	}

	/*
	 * �������
	 * 1. zip �ļ�����·��
	 * 2. result ���·�������Ǻ���Ҫ�ˣ������·������Ҫ
	 * 3. ��һҳ�Ľ��������
	 * 4. ֮ǰ�õ���������
	 * 5. �����xml�����������
	 * */
	public static void main(String[] args) throws Exception {
	    
		Configuration conf = new Configuration();
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    pageStart = (Integer.parseInt(args[2])-1)*10;
	    pageEnd = Integer.parseInt(args[2])*10;
	    
	    //read the first 10 line in a docs
	    
	    mysearchDocs = ReadFromFile.readFileByLines(args[3], pageStart, pageEnd, args[2]);
	    
	    System.out.println(args[4]);
	    detail_nameString = args[4];
	    
	    Job job = new Job(conf, "getTfidf");
	    job.setJarByClass(searchDocsInPage.class);
	    job.setMapperClass(docFinderMapper.class);
	    job.setReducerClass(docFinderReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setInputFormatClass(ZipFileInputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 }
