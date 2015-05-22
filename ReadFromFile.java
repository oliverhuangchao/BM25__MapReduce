package chaohBIM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;


public class ReadFromFile {
	public static HashMap<String,String> readFileByLines(String fileName, int start, int end, String page) {
		HashMap<String,String> mysearchDocs = new HashMap<String,String>();
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 0;
            while ((tempString = reader.readLine()) != null) {
            	if(line<end && line >=start){
            		String partString[] = tempString.split("\t");
            		String part[] = partString[0].split("@");
            		int tmp = Integer.parseInt(part[0]) - (Integer.parseInt(page)-1)*10;//仍然保持0-9这九行
            		//key: file name
            		//value: order between 0-9
            		mysearchDocs.put(part[1],Integer.toString(tmp));
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
        return mysearchDocs;
    }
	//read the region xml file
	
	public static HashSet<String> readRegionByLines(String fileName, String regioncode) {
		HashSet<String> mysearchDocs = new HashSet<String>();
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            
            while ((tempString = reader.readLine()) != null) {
            	String totalString[] = tempString.split("\t");
            	if(totalString[0].equals(regioncode)){
            		String part[] = totalString[1].split(",");
            		for (String each : part) {
            			String region[] = each.split("@");
						mysearchDocs.add(region[1].replace(".zip", "") + "~" + region[0].replace("newsML.xml", "f"));
					}
            	}
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
        return mysearchDocs;
    }
	
	 public static void main(String[] args) throws Exception {
		 HashSet<String> mysearchDocs = readRegionByLines("d:\\a.txt", "CANADA");
		 System.out.println(mysearchDocs);
	}
}
