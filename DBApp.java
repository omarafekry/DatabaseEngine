import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;


public class DBApp {
	ArrayList<Index> indexes = new ArrayList<>();
	ArrayList<TablePages> tablePagesInfo = new ArrayList<>();
	int MaximumRowsCountinTablePage;
	
	public static void main(String[] args) throws DBAppException, CsvException, IOException {
		DBApp dbApp = new DBApp();
		// Hashtable<String, String> types = new Hashtable<>();
		// types.put("ProductID", "java.lang.Integer");
		// types.put("ProductName", "java.lang.String");
		// types.put("ProductPrice", "java.lang.Double");
		// Hashtable<String, String> min = new Hashtable<>();
		// min.put("ProductID", "1");
		// min.put("ProductName", "A");
		// min.put("ProductPrice", "0");
		// Hashtable<String, String> max = new Hashtable<>();
		// max.put("ProductID", "100000");
		// max.put("ProductName", "ZZZZZZZZZZZZZZZZZZ");
		// max.put("ProductPrice", "99999999999");
		// Hashtable<String, String> fkeys = new Hashtable<>();
		// dbApp.createTable("Product2", "ProductID", types, min, max, fkeys, new String[]{});

		// Hashtable<String, Object> row = new Hashtable<>();
		// row.put("ProductID", 1);
		// row.put("ProductName", "Pringles");
		// row.put("ProductPrice", 100d);

		// dbApp.insertIntoTable("Product", row);
		// row = new Hashtable<>();
		// row.put("ProductID", 2);
		// row.put("ProductName", "Laban");
		// row.put("ProductPrice", 50d);

		// dbApp.insertIntoTable("Product", row);
		// row = new Hashtable<>();
		// row.put("ProductID", 3);
		// row.put("ProductName", "Tea");
		// row.put("ProductPrice", 25d);

		
		// dbApp.insertIntoTable("Product", row);
		// row = new Hashtable<>();
		// row.put("ProductID", 4);
		// row.put("ProductName", "Tea");
		// row.put("ProductPrice", 30d);

		// dbApp.insertIntoTable("Product", row);

		SQLTerm[] terms = new SQLTerm[1];
		SQLTerm term = new SQLTerm();
		term._objValue = 4;
		term._strColumnName = "ProductID";
		term._strOperator = "<";
		term._strTableName = "Product";
		terms[0] = term;

		Iterator<Hashtable<String, Object>> it = dbApp.selectFromTable(terms, new String[]{});
		while(it.hasNext()){
			Hashtable<String, Object> result = it.next();
			for (Map.Entry<String, Object> entry : result.entrySet()) {
				System.out.print(entry.getValue() + ", ");
			}
			System.out.println();
		}

		

		// Hashtable<String, Object> delete = new Hashtable<>();
		// delete.put("ProductName", "Milka");
		// dbApp.deleteFromTable("Product", delete);

		//dbApp.createIndex("Product", new String[]{"ProductID", "ProductPrice"});
		// Hashtable<String, Object> row = new Hashtable<>();
		// row.put("ProductID", 2);

		//dbApp.deleteFromTable("Product", row);
		

	}


	public DBApp() throws DBAppException, CsvException {
		init();
	}
	
	public void init() throws DBAppException, CsvException{	
		
		String Path = System.getProperty("user.dir");
		String DBAppConfigPath = Path + "/resources" + "/DBApp.config";
		Properties appProps = new Properties();
		try {
			appProps.load(new FileInputStream(DBAppConfigPath));
			MaximumRowsCountinTablePage = Integer.parseInt(appProps.getProperty("MaximumRowsCountinTablePage"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		populateTables();
		populateIndexes();
	}

	public void populateTables() throws DBAppException, CsvException{
        try {
			File metadata = new File("metadata.csv");
			if (!metadata.exists())
				metadata.createNewFile();
			CSVReader reader = new CSVReader(new FileReader(metadata));
			

            List<String[]> lines = reader.readAll();
            for (int i = 0; i < lines.size(); i++) {
				String tableName;
				boolean found = false;
				tableName = lines.get(i)[0];
				for (TablePages tp : tablePagesInfo) {
					if (tp.tableName.equals(tableName))
						found = true;
				}
				if (!found)
					tablePagesInfo.add(new TablePages(tableName));
            }
        } catch (IOException e) {
            throw new DBAppException("Couldn't find metadata.csv");
        }
	}

	public void populateIndexes() throws DBAppException, CsvException{
        try {
			CSVReader reader = new CSVReader(new FileReader(new File("metadata.csv")));
            List<String[]> lines = reader.readAll();
            for (int i = 0; i < lines.size(); i++) {
                String indexName = lines.get(i)[4];
				String tableName, column1, column2 = "";
				if (!indexName.equals("")){
					boolean found = false;
					tableName = lines.get(i)[0];
					column1 = lines.get(i)[1];
					for (Index index : indexes) {
						if (index.indexName.equals(indexName))
							found = true;
					}
					if (!found){
						for (int j = i + 1; j < lines.size(); j++) {
							if (lines.get(j)[4].equals(indexName))
								column2 = lines.get(j)[1];
						}
						if (column2 == "") throw new DBAppException("fe column leeh index bas msh la2y el column el tany");
						indexes.add(new Index(tableName, column1, column2));
					}
				}
                
            }
        } catch (IOException e) {
            throw new DBAppException("Couldn't find metadata.csv");
        }
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
			
			//validate the table creation first
			validateTableCreation(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
			
			// Create Table Folder with table name. This will contain the pages of the table.
			tableFolder.mkdirs();
			
			// adds meta-data of the new table in the metadata.csv
			addNewMetaData(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
		}
		
			// creates new TablePages Class to store relevant information about the pages of the table
			tablePagesInfo.add(new TablePages(strTableName));
			
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException, IOException {
		if (!tableExists(strTableName) || !colExists(strTableName, strarrColName[0]) || !colExists(strTableName, strarrColName[1]))
			throw new DBAppException("fe 7aga msh mawgooda");

		CSVReader reader = new CSVReader(new FileReader(new File("metadata.csv")));
		try {
			List<String[]> lines = reader.readAll();
			for (int i = 0; i < lines.size(); i++) {
				if(lines.get(i)[0].equals(strTableName)){
					if(lines.get(i)[1].equals(strarrColName[0]) || lines.get(i)[1].equals(strarrColName[1])){
						if(lines.get(i)[4].equals("")){
							lines.get(i)[4] = strarrColName[0] + strarrColName[1] + strTableName;
							lines.get(i)[5] = "grid";
						}
						else
							throw new DBAppException("el column "+strarrColName[0]+" aw "+strarrColName[1]+" 3ando index");
					}
				}
			}
		
		CSVWriter writer = new CSVWriter(new FileWriter(new File("metadata.csv")));
		writer.writeAll(lines);
		writer.close();
		reader.close();
		} catch (CsvException e) {
			e.printStackTrace();
		}
		Index index = new Index(strTableName, strarrColName[0], strarrColName[1]);
		indexes.add(index);

		//add all rows in the table
		TablePages tp = null;
		for (TablePages tp2Pages : tablePagesInfo)
			if (tp2Pages.tableName.equals(strTableName))
				tp = tp2Pages;
		ArrayList<Integer> pages = tp.getAllPages();
		Column[] columns = Table.getColumns(strTableName);
		for (int i = 0; i < pages.size(); i++) {
			try {
				List<String[]> page = readPage(strTableName, pages.get(i));
				for (int j = 1; j < page.size(); j++) {
					Hashtable<String, Object> row = new Hashtable<>();
					Object keyvalue = null;
					for (int k = 0; k < columns.length; k++) {
						row.put(columns[k].name, setType(page.get(j)[k], columns[k].type));
						if (columns[k].clusteringKey)
							keyvalue = setType(page.get(j)[k], columns[k].type);
					}
					index.insert(keyvalue, row, pages.get(i));
				}
			} catch (CsvException e) {
				e.printStackTrace();
			}
		}

	}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, CsvException{
		
		if(!tableExists(strTableName)) {
			throw new DBAppException("The table " + strTableName + " does not exist");
		}
		
		String[] header = getHeader(strTableName);
		Column clusteringKey = Table.getClusteringKey(strTableName);
		Object keyValue = htblColNameValue.get(clusteringKey.name);
		int keyColNum = 0;
		TablePages pagesInfo = null;
		Index index = null;
		
		for(TablePages tp: tablePagesInfo) {
			if (strTableName.equals(tp.tableName))
				pagesInfo = tp;
		}
		
		for(Index i: indexes) {
			if(strTableName.equals(i.tableName) && (clusteringKey.name.equals(i.column1.name) || clusteringKey.name.equals(i.column2.name))) {
				index = i;
			}
		}
		
		String[] rowtoInsert = new String[header.length];
		for(int i=0; i<header.length; i++){
			Object colValue = htblColNameValue.get(header[i]);
			if(colValue==null) {
				throw new DBAppException("Value for " + header[i] + " is not givin");
			}
			if(!iscorrectType(strTableName, header[i], colValue)) {
				throw new DBAppException("Incorrect data type for column " + header[i]);
			}
			if(clusteringKey.name.equals(header[i])) {
				keyColNum = i;
			}	
			
			rowtoInsert[i] = "" + colValue;
		}	
		
		if(index!=null) {
			//insert using the index
			if(keyValue==null) {
				throw new DBAppException("There is no value for the clusteringKey");
			}
				
			Hashtable<String,Object> values = new Hashtable<String,Object>();
			values.put(clusteringKey.name, keyValue);
			int insertionPage = index.findPageforInsertion(values);
			
			if(insertionPage == -1) {
				createPage(strTableName, pagesInfo);
				insertionPage = 0;
			}
			
			List<String[]> Rows = readPage(strTableName, insertionPage);
			List<String[]> newRows = new ArrayList<String[]>();
			newRows.add(Rows.get(0));
			boolean found = false;
			for(int i=1; i< Rows.size(); i++) {
			
				if(!found && Compare(keyValue, setType(Rows.get(i)[keyColNum], clusteringKey.type))==0) {
					throw new DBAppException("The clustering key " + keyValue + " already exists");
				}
				if(!found && Compare(keyValue, setType(Rows.get(i)[keyColNum], clusteringKey.type))==-1) {
					newRows.add(rowtoInsert);
					found = true;
				}
				
				newRows.add(Rows.get(i));
			}
			
			if(!found) {
				newRows.add(rowtoInsert);
			}
			for(Index i: indexes) {
				if(strTableName.equals(i.tableName)){
					i.insert(keyValue, htblColNameValue, insertionPage);
				}
			}
			if(newRows.size() > MaximumRowsCountinTablePage+1) {
				pushDownTable(strTableName, insertionPage,  newRows.remove(newRows.size()-1), pagesInfo);
			}
			else{
				writePage(strTableName, insertionPage, newRows);
			}
		}
		else {
			//System.out.println("entered insertion without index");
			ArrayList<Integer> allPages = pagesInfo.getAllPages();
			int insertionPage = -1;
			
			for(int i=0; i<allPages.size(); i++) {
				List<String[]> Rows = readPage(strTableName, allPages.get(i));
				if(Compare(keyValue, setType(Rows.get(Rows.size()-1)[keyColNum], clusteringKey.type)) == -1 && Rows.size() > 1) {
					insertionPage = allPages.get(i);
				}
			}
			
			if(allPages.isEmpty()) {
				createPage(strTableName, pagesInfo);
			}
			
			if(insertionPage == -1 && !allPages.isEmpty()) {
				insertionPage = allPages.get(allPages.size()-1); 
			}
			
			List<String[]> Rows = readPage(strTableName, insertionPage);
			List<String[]> newRows = new ArrayList<String[]>();
			newRows.add(Rows.get(0));
			boolean found = false;
			for(int i=1; i< Rows.size(); i++) {
				//System.out.println(setType(Rows.get(i)[keyColNum], clusteringKey.type));
				if(!found && Compare(keyValue, setType(Rows.get(i)[keyColNum], clusteringKey.type))==0) {
					throw new DBAppException("The clustering key " + keyValue + " already exists");
				}
				if(!found && Compare(keyValue, setType(Rows.get(i)[keyColNum], clusteringKey.type))==-1) {
					newRows.add(rowtoInsert);
					found = true;
				}
				
				newRows.add(Rows.get(i));
			}
			
			if(!found) {
				newRows.add(rowtoInsert);
			}
			
			for(Index i: indexes) {
				if(strTableName.equals(i.tableName)){
					i.insert(keyValue, htblColNameValue, insertionPage);
				}
			}
			
			if(newRows.size() > MaximumRowsCountinTablePage+1) {                   
				pushDownTable(strTableName, insertionPage,  newRows.remove(newRows.size()-1), pagesInfo);
			}
			else {
				writePage(strTableName, insertionPage, newRows);
			}
		
		}
		
				
	}
	
	public void updateTable(String strTableName, 
			String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException {

		if(!tableExists(strTableName)) {
			throw new DBAppException("The table " + strTableName + " does not exist");
		}
		
		String[] header = getHeader(strTableName);
		Column clusteringKey = Table.getClusteringKey(strTableName);
		TablePages pagesInfo = null;
		Index index = null;
		int keyColNum = -1;
		
		if(!checkType(clusteringKey.type, strClusteringKeyValue)) {
			throw new DBAppException("The clustering key value has an incorrect data type");
		}
		
		Object keyValue = setType(strClusteringKeyValue, clusteringKey.type);
		
		if (htblColNameValue.get(clusteringKey.name) != null){
			throw new DBAppException("you cant update the clustering key, rakezzzzz");
		}

		for(TablePages tp: tablePagesInfo) {
			if (tp.tableName.equals(strTableName))
				pagesInfo = tp;
		}
		
		for(int i=0; i<header.length; i++) {
			if(clusteringKey.name.equals(header[i])) {
				keyColNum=i;
			}
		}
		
		for(Index i: indexes) {
			if(strTableName.equals(i.tableName) && (clusteringKey.name.equals(i.column1.name) || clusteringKey.name.equals(i.column2.name))) {
				index = i;
			}
		}		
		
		int updatePage = -1;
		if(index!=null) {	
			Hashtable<String,Object> values = new Hashtable<String,Object>();
			values.put(clusteringKey.name, keyValue);
			updatePage = index.findPageforUpdate(values);
			
			if(updatePage == -1) {
				throw new DBAppException("The given clustering key does not exist");
			}
		
		}
		else {
			ArrayList<Integer> allPages = pagesInfo.getAllPages();
			for(int i=0; i<allPages.size(); i++) {
				List<String[]> Rows = null;
				try {
					Rows = readPage(strTableName, allPages.get(i));
				} catch (CsvException e) {
					e.printStackTrace();
				}
				if(Compare(keyValue, setType(Rows.get(Rows.size()-1)[keyColNum], clusteringKey.type))==-1) {
					updatePage = allPages.get(i);
				}
			}
			
			if(updatePage==-1) {
				updatePage = allPages.get(allPages.size()-1);
			}
		}
		boolean found = false;
		List<String[]> Rows = null;
		try {
			Rows = readPage(strTableName, updatePage);
		} catch (CsvException e) {
			e.printStackTrace();
		}
		for(String[] r : Rows) {
			if(r[keyColNum].equals(strClusteringKeyValue)) {
				found = true;
				for(int i=0; i<header.length; i++) {
					Object value = htblColNameValue.get(header[i]);
					if(value!=null) {
						if(!iscorrectType(strTableName, header[i], value)) {
							throw new DBAppException("The value for column " + header[i] + " has an incorrect data type");
						}
						r[i] = value+"";
					}
				}
		
			}
		}
		
		if(!found) {
		throw new DBAppException("The given clustering key does not exist");
		}
		
		writePage(strTableName, updatePage, Rows);
	}

	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, CsvException{
		
		//check if there is a table with the given name
		if(!tableExists(strTableName)) {
			throw new DBAppException("The table " + strTableName + " does not exist");
		}
		
		Enumeration<String> keys = htblColNameValue.keys();
		String key;
		ArrayList<Index> tableIndexes = getTableIndexes(strTableName);
		boolean indexShouldBeUsed = false;
		//check if the columns are actually in the table and check if the type of the values are correct
		while(keys.hasMoreElements()) {
			key = (String)keys.nextElement();
			Object value = htblColNameValue.get(key);
			
			if(!colExists(strTableName, key)) {
				throw new DBAppException("The column " + key + " does not exist");
			}
			
			if(!iscorrectType(strTableName, key, value)) {
				throw new DBAppException("The value of column " + key + " is an incorrect data type");
			}

			for (int i = 0; i < tableIndexes.size(); i++) {
				if (tableIndexes.get(i).column1.name.equals(key) || tableIndexes.get(i).column2.name.equals(key))
					indexShouldBeUsed = true;
			}
		
		}
		
		// get path of the table folder
		
		//get all of pages in the table
		ArrayList<Integer> pages = null;


		

		if (indexShouldBeUsed){

			//convert hashtable of column values to sqlterms
			SQLTerm[] terms = new SQLTerm[htblColNameValue.size()];
			Iterator<String> it = htblColNameValue.keys().asIterator();
			int count = 0;
			while(it.hasNext()) {
				String columnName = it.next();
				SQLTerm term = new SQLTerm();
				term._strTableName = strTableName;
				term._strColumnName = columnName;
				term._objValue = htblColNameValue.get(columnName);
				term._strOperator = "=";
				terms[count] = term;
				count++;
			}
			String[] operators = new String[terms.length - 1];
			for (int i = 0; i < terms.length - 1; i++) {
				operators[i] = "AND";
			}
			ArrayList<SQLTermCollection> termCollections = getCollectedTerms(terms, operators);
			
			//initialize result with the output of the first term
			pages = new ArrayList<>();
			//get rest of terms' outputs
			ArrayList<BucketEntry> entries = termCollections.get(0).index.getResult(termCollections.get(0).terms);
			for (int i = 0; i < entries.size(); i++)
				if (!pages.contains(entries.get(i).page))
					pages.add(entries.get(i).page);
			
			for (int i = 1; i < operators.length - 1; i++) {
				ArrayList<Integer> tempPages = new ArrayList<>();
				if (termCollections.get(i).index != null){
					entries = termCollections.get(i).index.getResult(termCollections.get(i).terms);
					for (int j = 0; j < entries.size(); j++) 
						tempPages.add(entries.get(j).page);
				}
				//anding
				ArrayList<Integer> andedPages = new ArrayList<>();
				for (int j = 0; j < tempPages.size(); j++) {
					if (pages.contains(tempPages.get(i)))
						andedPages.add(tempPages.get(i));
				}
				pages = andedPages;
			}
		}
		else{
			for(TablePages tp : tablePagesInfo) {
				if(tp.getTableName().equals(strTableName)) {
					pages = tp.getAllPages();
				}
			}
		}
		for(int i=0; i<pages.size(); i++) {
			deleteFromPage(strTableName, pages.get(i), htblColNameValue);
		}
		
		deleteEmptyPages(strTableName);
		
	}
   
	 public void deleteFromPage(String strTableName, int pageNumber, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException, CsvException {
	    	
	    	if(!pageExists(strTableName, pageNumber))
	    		throw new DBAppException("Page " + pageNumber + " does not exist in the table " +  strTableName);
	    	
	    	//get all the rows of the page
	    	List<String[]> pageRows = readPage(strTableName, pageNumber);
			
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
			
			ArrayList<Index> tableIndexes = getTableIndexes(strTableName);

			for (int i = 0; i < toBeRemoved.size(); i++) {
				Column[] columns = Table.getColumns(strTableName);
				Hashtable<String, Object> row = new Hashtable<>();
				Object keyValue = null;
				for (int j = 0; j < columns.length; j++) {
					row.put(columns[j].name, setType(toBeRemoved.get(i)[j], columns[j].type));
					if (columns[j].clusteringKey)
						keyValue = setType(toBeRemoved.get(i)[j], columns[j].type);
				}

				for (int j = 0; j < tableIndexes.size(); j++) 
					tableIndexes.get(j).delete(keyValue, row);
				
			}

			//delete the rows that should be deleted 
			pageRows.removeAll(toBeRemoved);
			
			//write the rest of the rows back to the page file
			writePage(strTableName, pageNumber, pageRows);
			
	    }
    
    public void shrinkTable(String strTableName, int numofPages) throws IOException, CsvException {
    	
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
				currWriter.close();
				
    	}
    }
    
    public void deleteEmptyPages(String strTableName) throws IOException, DBAppException, CsvException {
		
		String tablePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName;
		TablePages PagesInfo = null;
		for(TablePages tp: tablePagesInfo) {
			if(tp.tableName.equals(strTableName)) {
				PagesInfo = tp;
			}
		}
		
		ArrayList<Integer> allPages = PagesInfo.getAllPages();
		for(int i=0; i<allPages.size(); i++) {
			String pagePath = tablePath + File.separator + allPages.get(i) +".csv";
			File pageFile = new File(pagePath);
			
			List<String[]> rows = readPage(strTableName, i);
			
			if(rows.size()==1) {
				pageFile.delete();
				PagesInfo.removePage(allPages.get(i));
			}
		}
		
	}
    
	@SuppressWarnings("all")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		validateSelection(arrSQLTerms, strarrOperators);
		LinkedList<Hashtable<String, Comparable<Object>>> result;

		ArrayList<Index> tableIndexes = getTableIndexes(arrSQLTerms[0]._strTableName);
		if (tableIndexes.size() > 0){
			//collect adjacent terms with columns on a specific index together
			ArrayList<SQLTermCollection> termCollections = getCollectedTerms(arrSQLTerms, strarrOperators);
			
			//initialize result with the output of the first term
			if (termCollections.get(0).index == null)
				result = getSingleRelation(termCollections.get(0).terms[0]);
			else{
				LinkedList<Hashtable<String, Comparable<Object>>> rowsInIndex = getRowsFromKeys(termCollections.get(0).index.getResult(termCollections.get(0).terms), termCollections.get(0).terms[0]._strTableName);
				result = selectFromRows(rowsInIndex, arrSQLTerms, strarrOperators);
			}
			//get rest of terms' outputs
			for (int i = 1; i < strarrOperators.length - 1; i++) {
				LinkedList<Hashtable<String, Comparable<Object>>> nextRelation = getSingleRelation(arrSQLTerms[i+1]);
				if (termCollections.get(i).index == null)
					nextRelation = getSingleRelation(termCollections.get(i).terms[i]);
				else{
					//Assuming selection is done on a single table i.e. no joins
					LinkedList<Hashtable<String, Comparable<Object>>> rowsInIndex = getRowsFromKeys(termCollections.get(i).index.getResult(termCollections.get(i).terms), termCollections.get(i).terms[0]._strTableName);
					nextRelation = selectFromRows(rowsInIndex, arrSQLTerms, strarrOperators);
				}
				result = performOperation(result, nextRelation, termCollections.get(i).operator);
			}
		}
		else{
			result = getSingleRelation(arrSQLTerms[0]);

			for (int i = 0; i < strarrOperators.length; i++) {
				String operator = strarrOperators[i];
				LinkedList<Hashtable<String, Comparable<Object>>> nextRelation = getSingleRelation(arrSQLTerms[i+1]);
				result = performOperation(result, nextRelation, operator);
			}
		}
		return result.iterator();
	}

	public LinkedList<Hashtable<String, Comparable<Object>>> selectFromRows(LinkedList<Hashtable<String, Comparable<Object>>> rows, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		LinkedList<Hashtable<String, Comparable<Object>>> result = select(rows, arrSQLTerms[0]);

		for (int i = 0; i < strarrOperators.length; i++) {
			String operator = strarrOperators[i];
			LinkedList<Hashtable<String, Comparable<Object>>> nextRelation = select(rows, arrSQLTerms[i+1]);
			result = performOperation(result, nextRelation, operator);
		}

		return result;
	}

	private LinkedList<Hashtable<String, Comparable<Object>>> getRowsFromKeys(ArrayList<BucketEntry> entries, String tableName) throws DBAppException {
		LinkedList<Hashtable<String, Comparable<Object>>> result = new LinkedList<>();
		Hashtable<Object, Hashtable<String, Comparable<Object>>> page = new Hashtable<>();
		//get rows from bucket entries
		if (entries.size() > 0)
			page = Table.getPageHashtable(tableName, entries.get(0).page);
		for (int i = 0; i < entries.size(); i++) {
			int currentPageNumber = entries.get(i).page;
			while(i < entries.size() && entries.get(i).page == currentPageNumber){
				Object key = entries.get(i).key;
				Hashtable<String, Comparable<Object>> rowInIndex = page.get(key);
				result.addLast(rowInIndex);
				i++;
			}
			if (i < entries.size())
				page = Table.getPageHashtable(tableName, entries.get(i).page);
			i--;
		}
		return result;
	}

	public LinkedList<Hashtable<String, Comparable<Object>>> performOperation(LinkedList<Hashtable<String, Comparable<Object>>> relation1, LinkedList<Hashtable<String, Comparable<Object>>> relation2, String operator){
		if (operator.equals("AND")){
			LinkedList<Hashtable<String, Comparable<Object>>> tempResult = new LinkedList<>();
			for (Hashtable<String, Comparable<Object>> row	: relation1)
				if (relation2.contains(row))
					tempResult.addLast(row);
			relation1 = tempResult;
		}
		else{
			for (Hashtable<String, Comparable<Object>> row	: relation2) 
				if (!relation1.contains(row)) //i couldve changed the type to hashset but im lazy
					relation1.addLast(row);
		}
		return relation1;
	}
	
	private ArrayList<SQLTermCollection> getCollectedTerms(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		ArrayList<SQLTermCollection> collections = new ArrayList<>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			int numberOfCollectedTerms = 1;
			Index currentIndex = getIndexOfColumn(indexes, arrSQLTerms[i]._strColumnName);

			for (int j = 0; j < 3 && i + j < arrSQLTerms.length - 1; j++) {
				if (currentIndex.equals(getIndexOfColumn(indexes, arrSQLTerms[i + j]._strColumnName)) && strarrOperators[i - 1 + j].equals("AND"))
					numberOfCollectedTerms++;
			}
			SQLTerm[] collection = new SQLTerm[numberOfCollectedTerms];
			for (int j = 0; j < numberOfCollectedTerms; j++) 
				collection[j] = arrSQLTerms[i + j];
			
			if (i + numberOfCollectedTerms < strarrOperators.length)
				collections.add(new SQLTermCollection(collection, currentIndex, strarrOperators[i + numberOfCollectedTerms]));
			else
				collections.add(new SQLTermCollection(collection, currentIndex, ""));
			
			i += numberOfCollectedTerms - 1;
		}
		return collections;
	}
	
	public Index getIndexOfColumn(ArrayList<Index> indexes, String colName){
		for (Index index : indexes) {
			if (index.column1.name.equals(colName) || index.column2.name.equals(colName))
				return index;
		}
		return null;
	}
	private ArrayList<Index> getTableIndexes(String tableName) {
		ArrayList<Index> result = new ArrayList<Index>(); 
		for (Index index : indexes) {
			if (index.tableName.equals(tableName))
				result.add(index);
		}
		return result;
	}
	public LinkedList<Hashtable<String, Comparable<Object>>> select(LinkedList<Hashtable<String, Comparable<Object>>> rows, SQLTerm term){
		LinkedList<Hashtable<String, Comparable<Object>>> result = new LinkedList<>();
		switch(term._strOperator){
			case ">":
				for (Hashtable<String, Comparable<Object>> row : rows) {
					if (row.get(term._strColumnName).compareTo(term._objValue) > 0)
						result.addLast(row);
				}
				break;
			case ">=":
				for (Hashtable<String, Comparable<Object>> row : rows) {
					if (row.get(term._strColumnName).compareTo(term._objValue) >= 0)
						result.addLast(row);
				}
				break;
			case "=":
				for (Hashtable<String, Comparable<Object>> row : rows) {
					if (row.get(term._strColumnName).compareTo(term._objValue) == 0)
						result.addLast(row);
				}
				break;
			case "<=":
				for (Hashtable<String, Comparable<Object>> row : rows) {
					if (row.get(term._strColumnName).compareTo(term._objValue) <= 0)
						result.addLast(row);
				}
				break;
			case "<":
				for (Hashtable<String, Comparable<Object>> row : rows) {
					if (row.get(term._strColumnName).compareTo(term._objValue) < 0)
						result.addLast(row);
				}
				break;
		}
		return result;
	}
	private LinkedList<Hashtable<String, Comparable<Object>>> getSingleRelation(SQLTerm term) throws DBAppException{
		LinkedList<Hashtable<String, Comparable<Object>>> result = new LinkedList<>();
		Table table = new Table(term._strTableName);
		TablePages tp = null;
		for (TablePages tp2Pages : tablePagesInfo) {
			if (tp2Pages.tableName.equals(term._strTableName))
				tp = tp2Pages;
		}
		LinkedList<Hashtable<String, Comparable<Object>>> page;
		for (int i = 0; i < tp.numberofPages; i++) {
			page = table.getPage(tp.allPages.get(i));
			result.addAll(select(page, term));
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
			if (!op.equals("AND") && !op.equals("OR"))
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
		
		Enumeration<String> names = htblColNameType.keys();
		String key;
		
		// get all the meta-data values of the table
		while(names.hasMoreElements()) {
			String[] row = new String[12];
			key = (String) names.nextElement();
			//System.out.println(key);
			
			//System.out.println(key);
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
				row[9] = ForeginKey.split(".")[0];
				row[10] = ForeginKey.split(".")[1];
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
			
		}
		
		//put all the meta-data values in the metadata.csv
		String Path = System.getProperty("user.dir");
		Path = Path + File.separator + "metadata.csv";
	   
		//System.out.println("metadataPath: " + Path);
	
		FileWriter outputfile = new FileWriter(Path, true);
		CSVWriter writer =  new CSVWriter(outputfile);
			
		for(int j=0; j<newMetaData.size(); j++) {
			//System.out.println(newMetaData.get(j)[1]);
			writer.writeNext(newMetaData.get(j));
		}
			
		writer.close();
		
	}
	
	public boolean validateTableCreation(String strTableName, 
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax,
			Hashtable<String,String> htblForeignKeys,
			String[] computedCols) throws DBAppException, IOException {
		
		Enumeration<String> keys = htblColNameType.keys();
		List<String> colNames = new ArrayList<>();
		
		while(keys.hasMoreElements()) {
			colNames.add(keys.nextElement());
		}
		
		//check if strClusteringKeyColumn exists
		boolean clustringKeyExists = false;
		for(String colName : colNames) { 
			String type = htblColNameType.get(colName);
			String min = htblColNameMin.get(colName);
			String max = htblColNameMax.get(colName);
			String ForeginKey = htblForeignKeys.get(colName);
			
			if(min==null) {
				throw new DBAppException("A min value for " + colName + " is not provided");
			}	
				
			if(max==null) {
				throw new DBAppException("A max value for " + colName + " is not provided");
			}
			
			if(!checkType(type, min)) {
				throw new DBAppException("min value for " + colName + " has an incorrect data type");
			}
			
			if(!checkType(type, max)) {
				throw new DBAppException("max value for " + colName + " has an incorrect data type");
			}
			
			if(ForeginKey!=null) {
				//System.out.println(ForeginKey);
				if(!colExists(ForeginKey.split("[.]",0)[0], ForeginKey.split("[.]",0)[1])) {
					throw new DBAppException("The Foregin Key " + ForeginKey + " does not exist");
				}	
			}
			
			if(strClusteringKeyColumn.equals(colName)) {
				clustringKeyExists = true;
			}
			
		}
		
		if(!clustringKeyExists) {
			throw new DBAppException("There name of the clustering key is incorrect");
		}
		
		return true;
		
	}
	
	public boolean tableExists(String strTableName) {
		
		for(TablePages tp : tablePagesInfo) {
			if(tp.getTableName().equals(strTableName)) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean pageExists(String strTableName, int pageNumber) {
		
		int numofPages = 0;
		
		for(TablePages tp : tablePagesInfo) {
			if(tp.getTableName().equals(strTableName)) {
				numofPages = tp.getNumberofPages();
			}
		}
		
		if(pageNumber + 1 <= numofPages)
			return true;
		
		return false;
	}

	public boolean colExists(String strTableName, String colName) throws IOException, DBAppException {
		
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

	public String[] getHeader(String strTableName) throws DBAppException {
	    	
	    	Column[] metadataInfo = null;
			metadataInfo = Table.getColumns(strTableName);
			
			String[] header = new String[metadataInfo.length];
			
	    	for(int i=0; i<metadataInfo.length; i++) {
	    		header[i] = metadataInfo[i].name;
	    	}
	    	
	    	return header;
	    }
	    
    public boolean iscorrectType(String strTableName, String colName, Object value) throws DBAppException{
		
    	Column[] tableMetaData = null;
    	String correctType = "";
    	
		tableMetaData = Table.getColumns(strTableName);
		
		for(Column c : tableMetaData) {
			if(c.name.equals(colName)) {
				correctType = c.type;
				break;
			}
		}
		
		if(correctType.equals("java.utl.Date") && value instanceof Date) {
			 try {
		            SimpleDateFormat sdf = new SimpleDateFormat("dd.mm.yyyy");
		            sdf.parse(value.toString());
		        } catch (ParseException ex) {
		        	throw new DBAppException("date is not the right format");
		        }
			return true;
		}
		
		//System.out.println("CorrectType: " + correctType);
		
		
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
	@SuppressWarnings("all")
    public boolean checkType(String Type, String value) throws DBAppException{

		if(Type.equals("java.util.Date")) {
		        try {
					Date date = null;
		            SimpleDateFormat sdf = new SimpleDateFormat("dd.mm.yyyy");
		            date = sdf.parse(value.toString());
		        } catch (ParseException ex) {
		        	return false;
		        }
			return true;
		}
		
		
		if(Type.equals("java.lang.Integer")){
			try {
		       Integer i = Integer.parseInt(value);
		    } catch (NumberFormatException nfe) {
		        return false;
		    }
			
			return true;
		}
		if(Type.equals("java.lang.Double")){
			try {
			    Double d = Double.parseDouble(value);
			} catch (NumberFormatException nfe) {
			    return false;
			}
			
			return true;
		}
		
		if(Type.equals("java.lang.String")){
			return true;
		}
		
		return false;
	
	}
    
    public void createPage(String strTableName, TablePages tp) throws DBAppException{
		String tablePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName;
	
		int newPageNumber = tp.getNextPagetoCreate();
		
		File newPageFile = new File(tablePath + File.separator + (newPageNumber) + ".csv");
		try {
			FileWriter outputfile = new FileWriter(newPageFile);
			CSVWriter writer =  new CSVWriter(outputfile);
			
			String[] header = getHeader(strTableName);
			
            writer.writeNext(header);
            writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		tp.addPage(newPageNumber);
	}
	
    
    public List<String[]> readPage(String strTableName, int pageNumber) throws DBAppException, CsvException {
    	
    	String pagePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName + File.separator + pageNumber +".csv";
    	File pageFile = new File(pagePath);
    	
    	List<String[]> pageRows = null;
		
		try {
			FileReader filereader = new FileReader(pageFile);
			CSVReader csvreader = new CSVReader(filereader);
			pageRows = csvreader.readAll();
			filereader.close();
			csvreader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return pageRows;
		
    }
    
    public void writePage(String strTableName, int pageNumber, List<String[]> rows) {
    	
    	String pagePath = System.getProperty("user.dir") + File.separator + "Tables" + File.separator + strTableName + File.separator + pageNumber +".csv";
    	File pageFile = new File(pagePath);
    	
		try {
			CSVWriter csvwriter = new CSVWriter(new FileWriter(pageFile));
			csvwriter.writeAll(rows);	
			csvwriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }
    
    public void pushDownTable(String strTableName, int currPage, String[] rowtoPush, TablePages tp) throws DBAppException, CsvException {
    	
    	ArrayList<Integer> allPages = tp.getAllPages();
    	int insertionPage = -1;
    	for(int i = 0; i<allPages.size(); i++) {
    		if(currPage==allPages.get(i) && i<allPages.size()-1) {
    			insertionPage = allPages.get(i+1);
    		}
    	}
    	
    	if(insertionPage==-1) {
    		createPage(strTableName, tp);
    		insertionPage = allPages.get(allPages.size()-1);
    	}
    	
    	updateIndexes(strTableName, rowtoPush, insertionPage);
    	
    	List<String[]> Rows = readPage(strTableName, insertionPage);
    	List<String[]> newRows = new ArrayList<String[]>();
    	newRows.add(Rows.get(0));
    	newRows.add(rowtoPush);
    	
    	for(int i=2; i<Rows.size();i++) {
    		newRows.add(Rows.get(i));
    	}
    	
    	if(newRows.size()>MaximumRowsCountinTablePage+1) {
    		String[] newRowtoPush = newRows.remove(newRows.size()-1);
    		writePage(strTableName, insertionPage, newRows);
    		pushDownTable(strTableName, insertionPage, newRowtoPush, tp);
    	}
    	else {
    		writePage(strTableName, insertionPage, newRows);
    	}
    	
    	
    	
    }

    public void updateIndexes(String strTableName, String[] rowtoUpdate, int newPageNum) throws DBAppException {
    	Hashtable<String, Object> values = new Hashtable<String, Object>();
    	String[] header = getHeader(strTableName);
    	Object Clusteringkey = null;
    	
    	for(int i=0; i<header.length; i++) {
    		Column[] columns = Table.getColumns(strTableName);
    		String type = null;
    		
    		for(Column c : columns) {
    			if(c.name.equals(header[i])) {
    				type = c.type;
					if(c.clusteringKey) {
						Clusteringkey = setType(rowtoUpdate[i], type);
						break;
					}
    			}
    			
    		}
    		
    		values.put(header[i], setType(rowtoUpdate[i], type));
    	}
    	
    	for(Index i : indexes) {
    		if(i.tableName.equals(strTableName)) {
    			i.update(Clusteringkey, values, newPageNum);
    		}
    	}
    	
    }
	
	public Object setType( String value, String type) {
    	Object result = null;
    	
    	switch(type) {
    	case "java.lang.Integer": result = Integer.parseInt(value); break;
		case "java.lang.Double": result = Double.parseDouble(value); break;
		case "java.lang.String": result = value; break;
		case "java.util.Date": SimpleDateFormat sdf = new SimpleDateFormat("dd.mm.yyyy");
			try {
				result = sdf.parse(value.toString());
			} catch (ParseException e) {
				e.printStackTrace();
			} break;
    	}
    	
    	return result;
    } 
    
	public int Compare(Object value1, Object value2) {
    	
        if(value1 instanceof Integer && value2 instanceof Integer) {
        	if ((Integer)(value1) < (Integer)(value2)){
               return -1;
            }
        	if ((Integer)(value1) > (Integer)(value2)){
                return 1;
             }
        	return 0;
        }
        
        if(value1 instanceof Double && value2 instanceof Double) {
        	if ((Double)(value1) < (Double)(value2)){
               return -1;
            }
        	if ((Double)(value1) > (Double)(value2)){
                return 1;
             }
        	return 0;
        }
        
        if(value1 instanceof String && value2 instanceof String) {
        	if(value1.equals(value2)) {
        		return 0;
        	}
        	if (((String) value1).length() <= ((String)value2).length()){
               return -1;
            }
        	if (((String) value1).length() > ((String)value2).length()){
                return 1;
             }
        	
        }
        
        if(value1 instanceof Date && value2 instanceof Date) {
        	if (((Date) value1).before((Date)value2)){
               return -1;
            }
        	if (((Date) value1).after((Date)value2)){
                return 1;
             }
        	return 0;
        }
  
       return -2;
    }

}
