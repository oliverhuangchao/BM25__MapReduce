package chaohBIM;

public class parsexml {
	
	public static String getContentlineFromXML(String row){
		String text=null;
		int start,end;
		start=row.indexOf("<text>");
		if (start>=0) start=start+6;
		end = row.indexOf("</text>");
		
		text = row.substring (start,end);
		
	
		return text;
	}
	
	public static String getTitleFromXML(String row){
		String text=null;
		int start,end;
		start=row.indexOf("<title>");
		if (start>=0) start=start+7;
		end = row.indexOf("</title>");
		text = row.substring (start,end);	
		return text;
	}
	
	public static String getRegionFromXML(String row){
		String text=null;
		int start,end;
		start=row.indexOf("<title>");
		if (start>=0) start=start+7;
		end = row.indexOf("</title>");
		text = row.substring (start,end);	
		return text;
	}
}
