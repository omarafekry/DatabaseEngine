package main;

import java.io.*;

public class TablePages implements Serializable {
	String tableName;
	int numberofPages;
	int currRowNumber;
	
	public TablePages(String tableName) {
		this.tableName = tableName;
		numberofPages = 0;
		currRowNumber = 0;
	}
	
	public String getTableName() {
		return tableName;
	}

	public int getNumberofPages() {
		return numberofPages;
	}

	public void setNumberofPages(int numberofPages) {
		this.numberofPages = numberofPages;
	}

	public int getCurrRowNumber() {
		return currRowNumber;
	}

	public void setCurrRowNumber(int currRowNumber) {
		this.currRowNumber = currRowNumber;
	}
	
	
}
