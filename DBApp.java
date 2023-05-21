package main;

import java.util.*;
import java.io.*;
import java.util.Hashtable;

import com.opencsv.CSVWriter;

import exceptions.DBAppException;


public class DBApp {

	public DBApp() {
		init():	
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

	//public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{}
	
}
