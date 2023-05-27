import java.util.*;


public class DBApp {

	public DBApp() {
		init();
	}
	
	public void init(){	
	}
	
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax,
			Hashtable<String,String> htblForeignKeys,
			String[] computedCols ) throws DBAppException {}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{}
	
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue ) throws DBAppException {}
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{}
    
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
			if (!op.equals("AND") && !op.equals("OR"))
				throw new DBAppException("Incorrect operator " + op);
		}
	}
	
}
