import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import exceptions.DBAppException;
import main.Column;
import main.Table;
import main.TablePages;


public class DBApp {

	ArrayList<TablePages> tablePagesInfo;
	
	public DBApp() {
		init();
	}
	
	public void init(){	
		String Path = System.getProperty("user.dir");
		// Create the Database folder that will have all the files of the database (optional)
//		File DatabaseFolder = new File(Path + File.separator + "Database");
//		if (!DatabaseFolder.exists()){
//			DatabaseFolder.mkdirs();
//		}
		
		tablePagesInfo = new ArrayList<TablePages>();
	}
	
	public void createTable(String strTableName, 
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax,
			Hashtable<String,String> htblForeignKeys,
			String[] computedCols ) throws DBAppException, IOException {
		
		String Path = System.getProperty("user.dir");
		
		File tableFolder = new File(Path + File.separator + "Tables" + File.separator + strTableName);
		if(!tableFolder.exists()) {
			
			// Create Table Folder with table name. This will contain the pages of the table.
			tableFolder.mkdirs();
			
			// adds meta-data of the new table in the metadata.csv
			addNewMetaData(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
		}
		
			// creates new TablePages Class to store relevant information about the pages of the table
			tablePagesInfo.add(new TablePages(strTableName));
	
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{}
	
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue ) throws DBAppException {}
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException{
		
		//check if there is a table with the given name
		if(!tableExists(strTableName)) {
			System.out.println("There is no table with this name");
			return;
		}
		
		Enumeration keys = htblColNameValue.keys();
		String key;
		
		//check if the columns are actually in the table and check if the type of the values are correct
		while(keys.hasMoreElements()) {
			key = (String)keys.nextElement();
			Object value = htblColNameValue.get(key);
			
			if(!colExists(strTableName, key)) {
				System.out.println(key + " column does not exist");
				return;
			}
			
			if(!iscorrectType(strTableName, key, value)) {
				throw new DBAppException("The value of column " + key + " is an incorrect data type");
			}
			
		}
		
		// get path of the table folder
		String Path = System.getProperty("user.dir") + File.separator + "Database" + File.pathSeparator + strTableName;
		
		//get number of pages in the table
		int numofPages = 0;
		for(TablePages tp : tablePagesInfo) {
			if(tp.getTableName().equals(strTableName)) {
				numofPages = tp.getNumberofPages();
			}
		}
		
		for(int i=1; i<=numofPages; i++) {
			deleteFromPage(strTableName, i, htblColNameValue);
		}
		
		shrinkTable(strTableName, numofPages);
		deleteEmptyPages(strTableName);
		
	}
   
    public void deleteFromPage(String strTableName, int pageNumber, Hashtable<String,Object> htblColNameValue) throws IOException {
    	
    	String pagePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName + File.separator + pageNumber +".csv";
    	
    	File pageFile = new File(pagePath);
    	
    	//get all the rows of the page
    	List<String[]> pageRows = null;
		
		FileReader filereader = new FileReader(pageFile);
		CSVReader csvreader = new CSVReader(filereader);
		
		pageRows = csvreader.readAll();
		filereader.close();
		
		String[] header = getHeader(strTableName);
		
		//find the rows that need to be deleted
		List<String[]> toBeRemoved = new ArrayList<String[]>();
		for(int i=0; i<pageRows.size(); i++) {
			
			String[] row = pageRows.get(i);
			boolean valuesFound = true;
			
			for(int j=0; j<header.length; j++) {
				Object value = htblColNameValue.get(header[j]);
				if(value!=null && !(value.toString().equals(row[j]))){
					valuesFound = false;
				}
			}
			
			if(valuesFound) {
				toBeRemoved.add(pageRows.get(i));
			}	
			
			valuesFound = true;
		}
		
		//delete the rows that should be deleted 
		pageRows.removeAll(toBeRemoved);
		
		//write the rest of the rows back to the page file

		FileWriter writer = new FileWriter(pageFile);
		CSVWriter csvwriter = new CSVWriter(writer);
			
		csvwriter.writeAll(pageRows);	
		writer.close();
		
    }
    
    public void shrinkTable(String strTableName, int numofPages) throws IOException {
    	
    	String Path = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName;
    	String currPagePath;
    	String nextPagePath;
    	
    	for(int i=1; i<numofPages; i++) {
    		
    		currPagePath = Path + File.separator + i +".csv";
    		File currFile = new File(currPagePath);
    		
    		List<String[]> currRows = null;
    		
    		CSVReader currReader = new CSVReader( new FileReader(currFile));
			currRows = currReader.readAll();
			currReader.close();
			
    		int nextPage = 1;
    		// fill current page to its maximum from next pages or till there no more pages to take from (assume maximum per page is 200) 
    		while(currRows.size()<200+1 && (i+nextPage)<=numofPages) {
    		
	    		nextPagePath = Path + File.separator + (i+nextPage) +".csv";
	    		File nextFile = new File (nextPagePath);
	    	
	    		List<String[]> NextRows = null;
	    		
				CSVReader nextReader = new CSVReader( new FileReader(nextFile));
				NextRows = nextReader.readAll();
				nextReader.close();
				
	    		// remove from next page and put in current page
	    		while(currRows.size()<200+1 && NextRows.size()>1) {
	    			currRows.add(NextRows.remove(1));
	    		}
	    		
	    		nextPage++;
	    		
				CSVWriter nextWriter = new CSVWriter(new FileWriter(nextPagePath));
					nextWriter.writeAll(NextRows);
					nextWriter.close();
    		}
    		
    		
    			FileWriter writer = new FileWriter(currPagePath);
				CSVWriter currWriter = new CSVWriter(writer);
				
				currWriter.writeAll(currRows);
				writer.close();
				
    	}
    }
    
    public void deleteEmptyPages(String strTableName) throws IOException {
		
		String tablePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName;
		TablePages PagesInfo = null;
		for(TablePages tp: tablePagesInfo) {
			if(tp.tableName.equals(strTableName)) {
				PagesInfo = tp;
			}
		}
		
		int newSize = PagesInfo.getNumberofPages();
		for(int i=1; i<=PagesInfo.getNumberofPages(); i++) {
			String pagePath = tablePath + File.separator + i +".csv";
			File pageFile = new File(pagePath);
			
			List<String[]> rows = new ArrayList<String[]>();
			
			FileReader filereader = new FileReader(pageFile);
			CSVReader csvreader = new CSVReader(filereader);
			rows = csvreader.readAll();
			filereader.close();
			
			if(rows.size()==1) {
				pageFile.delete();
				newSize = newSize - 1;
			}
		}
		
		PagesInfo.setNumberofPages(newSize);
		
		if(newSize!=0) {
			String pagePath = tablePath + File.separator + newSize +".csv"; 
			File pageFile = new File(pagePath);
			
			List<String[]> rows = new ArrayList<String[]>();
			
			FileReader filereader = new FileReader(pageFile);
			CSVReader csvreader = new CSVReader(filereader);
			rows = csvreader.readAll();
			filereader.close();
			
			PagesInfo.setCurrRowNumber(rows.size()-1);
		}
	}
    
	@SuppressWarnings("all")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		validateSelection(arrSQLTerms, strarrOperators);
		LinkedList<Hashtable<String, Comparable<Object>>> result = getSingleRelation(arrSQLTerms[0]);

		for (int i = 0; i < strarrOperators.length; i++) {
			String operator = strarrOperators[i];
			LinkedList<Hashtable<String, Comparable<Object>>> nextRelation = getSingleRelation(arrSQLTerms[i+1]);
			if (operator.equals("AND")){
				LinkedList<Hashtable<String, Comparable<Object>>> tempResult = new LinkedList<>();
				for (Hashtable<String, Comparable<Object>> row	: result)
					if (nextRelation.contains(row))
						tempResult.addLast(row);
				result = tempResult;
			}
			else{
				for (Hashtable<String, Comparable<Object>> row	: nextRelation) 
					if (!result.contains(row)) //i couldve changed the type to hashset but im lazy
						result.addLast(row);
			}
		}

		return result.iterator();
	}

	private LinkedList<Hashtable<String, Comparable<Object>>> getSingleRelation(SQLTerm term) throws DBAppException{
		LinkedList<Hashtable<String, Comparable<Object>>> result = new LinkedList<>();

		//check for index here

		Table table = new Table(term._strTableName);
		LinkedList<Hashtable<String, Comparable<Object>>> page;

		while((page = table.nextPage()) != null){
			switch(term._strOperator){
				case ">":
					for (Hashtable<String, Comparable<Object>> row : page) {
						if (row.get(term._strColumnName).compareTo(term._objValue) > 0)
							result.addLast(row);
					}
					break;
				case ">=":
					for (Hashtable<String, Comparable<Object>> row : page) {
						if (row.get(term._strColumnName).compareTo(term._objValue) >= 0)
							result.addLast(row);
					}
					break;
				case "=":
					for (Hashtable<String, Comparable<Object>> row : page) {
						if (row.get(term._strColumnName).compareTo(term._objValue) == 0)
							result.addLast(row);
					}
					break;
				case "<=":
					for (Hashtable<String, Comparable<Object>> row : page) {
						if (row.get(term._strColumnName).compareTo(term._objValue) <= 0)
							result.addLast(row);
					}
					break;
				case "<":
					for (Hashtable<String, Comparable<Object>> row : page) {
						if (row.get(term._strColumnName).compareTo(term._objValue) < 0)
							result.addLast(row);
					}
					break;
			}
			
		}
		return result;
	}

	private void validateSelection(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		for (SQLTerm term : arrSQLTerms) {
			Column[] columns  = Table.getColumns(term._strTableName);
			Column column = null;
			boolean exists = false;
			for (Column col : columns) 
				if (col.name.equals(term._strColumnName)){
					exists = true;
					column = col;
				}
			if (!exists)
				throw new DBAppException("Couldn't find a column with name " + term._strColumnName + " in table " + term._strTableName);
			String objType = term._objValue.getClass().toString().split(" ")[1];
			if (!objType.equals(column.type))
				throw new DBAppException("Incorrect data type for column " + term._strColumnName + ". Expected " + column.type + " but found " + objType);
			String op = term._strOperator;
			if (!op.equals(">") && !op.equals(">=") && !op.equals("=") && !op.equals("<=") && !op.equals("<"))
				throw new DBAppException("Incorrect operator " + op);			
		}
		for (String op : strarrOperators) {
			if (!op.equals("AND") &&!op.equals("OR"))
				throw new DBAppException("Incorrect operator " + op);
		}
	}
	
	public void addNewMetaData(String strTableName, 
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax,
			Hashtable<String,String> htblForeignKeys,
			String[] computedCols) throws IOException {
		
		List<String[]> newMetaData = new ArrayList<String[]>();
		
		Enumeration names = htblColNameType.keys();
		String key;
		int i = 0;
		
		// get all the meta-data values of the table
		while(names.hasMoreElements()) {
			String[] row = new String[12];
			key = (String) names.nextElement();
			System.out.println(key);
			String type = htblColNameType.get(key);
			String min = htblColNameMin.get(key);
			String max = htblColNameMax.get(key);
			String ForeginKey = htblForeignKeys.get(key);
			
			row[0] = strTableName;
			row[1] = key;
			row[2] = type;
			if(key.equals(strClusteringKeyColumn)){
				row[3] = "True";
			}
			else {
				row[3] = "False";
			}
			row[4] = null;
			row[5] = null;
			row[6] = min;
			row[7] = max;
			if(ForeginKey!=null) {
				row[8] = "True";
				row[9] = ForeginKey.split("[.]", 0)[0];
				row[10] = ForeginKey.split("[.]", 0)[1];
			}
			else {
				row[8] = "False";
				row[9] = null;
				row[10] = null;
			}
			
			boolean computed = false;
			for(int j=0; j<computedCols.length; j++){
				if(key.equals(computedCols[j]))
					computed = true;	
			}
			
			if(computed) {
				row[11] = "True";
			}	
			else {
				row[11] = "False";
			}	
			
			newMetaData.add(row);
			
			i++;
		}
		
		//put all the meta-data values in the metadata.csv
		String Path = System.getProperty("user.dir");
		Path = Path + File.separator + "metadata.csv";
	   
		System.out.println("metadataPath: " + Path);
	
		FileWriter outputfile = new FileWriter(Path, true);
		CSVWriter writer =  new CSVWriter(outputfile);
			
		for(int j=0; j<newMetaData.size(); j++) {
			System.out.println(newMetaData.get(j)[1]);
			writer.writeNext(newMetaData.get(j));
		}
			
		writer.close();
		
	}
	
	public boolean tableExists(String strTableName) {
		
		for(TablePages tp : tablePagesInfo) {
			if(tp.getTableName().equals(strTableName)) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean colExists(String strTableName, String colName) throws IOException {
		
		if(!tableExists(strTableName)) {
			return false;
		}
		
		Column[] tableMetaData = null;
		
		
		tableMetaData = Table.getColumns(strTableName);
		
		for(Column c : tableMetaData) {
			if(colName.equals(c.name)) {
				return true;
			}
		}
		
		return false;
		
	}

	public String[] getHeader(String strTableName) {
	    	
	    	Column[] metadataInfo = null;
			try {
				metadataInfo = Table.getColumns(strTableName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			String[] header = new String[metadataInfo.length];
			
	    	for(int i=0; i<metadataInfo.length; i++) {
	    		header[i] = metadataInfo[i].name;
	    	}
	    	
	    	return header;
	    }
	    
    public boolean iscorrectType(String strTableName, String colName, Object value){
		
    	Column[] tableMetaData = null;
    	String correctType = "";
    	
		try {
			tableMetaData = Table.getColumns(strTableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(Column c : tableMetaData) {
			if(c.name.equals(colName)) {
				correctType = c.type;
				break;
			}
		}
		
		if(correctType.equals("java.utl.Date") && value instanceof Date) {
			 Date date = null;
		        try {
		            SimpleDateFormat sdf = new SimpleDateFormat("dd.mm.yyyy");
		            date = sdf.parse(value.toString());
		        } catch (ParseException ex) {
		        	System.out.println("date is not the right format");
		        	return false;
		        }
			return true;
		}
		
		System.out.println("CorrectType: " + correctType);
		
		
		if(correctType.equals("java.lang.Integer") && value instanceof Integer){
			return true;
		}
		if(correctType.equals("java.lang.Double") && value instanceof Double){
			return true;
		}
		if(correctType.equals("java.lang.String") && value instanceof String){
			return true;
		}
		
		return false;
	
	}
	
}
