package chaohBIM;
//local search, directly search in the folder
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


public class localSearch {
	public static int pageStart;
	public static int pageEnd;
	public static String detail_filename;
	
	
	public static String readFileinZip(String zipName, String entryname){
		String encoding = "ISO-8859-1";
		File file = new File(zipName);
		Long filelength = file.length();
		byte[] filecontent = new byte[filelength.intValue()];
		StringBuffer zipoutput = new StringBuffer();
		String returnval = "";
		try {
			@SuppressWarnings("resource")
			ZipFile zip = new ZipFile(file);
			ZipEntry ze = zip.getEntry(entryname);//根据文件名取得压缩包中的对应条目
			
			BufferedReader br = new BufferedReader(new InputStreamReader(zip.getInputStream(ze)));
			String line ;
			while((line = br.readLine()) != null){
				zipoutput.append(line);
			}
			br.close();
			filecontent = zipoutput.toString().getBytes();
			returnval = new String(filecontent, encoding);
		} catch (ZipException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return returnval;
}
	
	public static String readToString(String fileName) {
		String encoding = "ISO-8859-1";
		File file = new File(fileName);
		Long filelength = file.length();
		byte[] filecontent = new byte[filelength.intValue()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(filecontent);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			return new String(filecontent, encoding);
		} catch (UnsupportedEncodingException e) {
			System.err.println("The OS does not support " + encoding);
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * input parameters:
	 * 1. which page number 
	 * 2. rank.txt 's path
	 * 3. doc_detail.xml path
	 * 4. work path to get each news documents
	 * 
	 * output:
	 * doc_detail.xml file
	 * */
	public static void main(String[] args) throws Exception {			    
	    int page = Integer.parseInt(args[0]);	    
	    File file = new File(args[1]);
	    detail_filename = args[2];
	    String datapath = args[3];
	    
	    pageStart = (page-1)*10;
	    pageEnd = page*10;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 0;
            int order = 0;
            while ( (tempString = reader.readLine()) != null ) {
            	if(line<pageEnd && line >=pageStart){
            		// deal with each turple
            		String partString[] = tempString.split("\t");
            		String part[] = partString[0].split("@");//part[0]: order ; part[1]: zip_name~doc_name
            		String zipfilename[] = part[1].split("~");
            		String filename = zipfilename[1].replace("f","newsML.xml");
            		String zipString = zipfilename[0].concat(".zip");
            		
            		//int order = Integer.parseInt(part[0]) - (page-1)*10;//仍然保持0-9这九行
      		
            		//get content
            		//String content = readToString(datapath + "/" +filename);
            		String content = readFileinZip(datapath+"/"+zipString, filename);
            		//System.out.println(datapath+"/"+zipString+"---"+filename);
            		         
            		
            		//write title
        			String title = parsexml.getTitleFromXML(content);
        			DealXML.writefile(detail_filename, order, 0, title);
        			//DealXML.writefile(detail_filename, 0, title);
        			
        			//write detail
        			String detail = parsexml.getContentlineFromXML(content);
        			detail = detail.replace("<p>", "");
        			detail = detail.replace("</p>", "");
        			DealXML.writefile(detail_filename, order, 1, detail);
        			//DealXML.writefile(detail_filename, 1, detail);
        			
        			//write page title to xml
        			DealXML.writefile(detail_filename, order, 2, filename);
        			//DealXML.writefile(detail_filename, 2, filename);
        			
        			order++;
            	}
        		line++;
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
	}
}
