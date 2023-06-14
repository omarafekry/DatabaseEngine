import java.io.*;
import java.util.ArrayList;

public class TablePages implements Serializable {
	String tableName;
	int numberofPages;
	ArrayList<Integer> allPages;
	
	public TablePages(String tableName) {
		this.tableName = tableName;
		allPages = new ArrayList<Integer>();
		
		File folder = new File(System.getProperty("user.dir") + File.separator + "Tables" + File.separator + tableName);
		File[] listOfFiles = folder.listFiles();
		
		for(File f: listOfFiles) {
			allPages.add(Integer.parseInt(f.getName().split("[.]",0)[0]));
		}
		
		numberofPages = allPages.size();
	}
	
	public String getTableName() {
		return tableName;
	}

	public int getNumberofPages() {
		return numberofPages;
	}
	
	public ArrayList<Integer> getAllPages(){
		return allPages;
	}
	
	public void addPage(int pageNumber) {
		allPages.add(pageNumber);
		numberofPages++;
	}
	
	public void removePage(int pageNumber) {
		if(pageExists(pageNumber)) {
			allPages.remove((Integer)pageNumber);
			numberofPages--;
		}	
	}
	
	public boolean pageExists(int pageNumber) {
		String pagePath = System.getProperty("user.dir");
		
		File pageFile = new File(pagePath + File.separator + "Tables" + File.separator + tableName + File.separator + pageNumber + ".csv");
		
		return pageFile.exists();
	}
	
	public int getNextPagetoCreate() {
		if(numberofPages == 0) 
			return 0;
			
		return allPages.get(allPages.size()-1) + 1;	
	}
			
}
	

