import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;


public class Index {
    int largestDivisions = 0;
    ArrayList<ArrayList<Cell>> grid = new ArrayList<>();
    String tableName, indexName, clusteringKeyType;
    String path;
    Column column1, column2;

    public Index(String tableName, String column1Name, String column2Name) throws DBAppException{
        String Path = System.getProperty("user.dir");
        String DBAppConfigPath = Path + "/resources" + "/DBApp.config";
        Properties appProps = new Properties();
        try {
            appProps.load(new FileInputStream(DBAppConfigPath));
            largestDivisions = Integer.parseInt(appProps.getProperty("maxIndexDivisions"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        this.tableName = tableName;
        String column1Type = "", column2Type = "";
        Column[] columns = Table.getColumns(tableName);
        for (int i = 0; i < columns.length; i++) {
            if (column1Name.equals(columns[i].name))
                this.column1 = columns[i];
            if (column2Name.equals(columns[i].name))
                this.column2 = columns[i];
            if (columns[i].clusteringKey)
                clusteringKeyType = columns[i].type;
        }
        this.indexName = column1.name + column2.name + tableName;
        column1Type = column1.type;
        column2Type = column2.type;

        path = System.getProperty("user.dir") + File.separator + "Indexes" + File.separator + indexName;
        File file = new File(path);
        file.mkdirs();

        int column1Divisions = largestDivisions, column2Divisions = largestDivisions;
        Object column1Range = null, column2Range = null, column1Min = null, column2Min = null;
        switch(column1Type){
            case "java.lang.Integer":
            if (Integer.parseInt(column1.max) - Integer.parseInt(column1.min) + 1 < largestDivisions){
                column1Divisions = Integer.parseInt(column1.max) - Integer.parseInt(column1.min) + 1;
                column1Range = 1;
            }
            else{
                column1Range = (Integer.parseInt(column1.max) - Integer.parseInt(column1.min) + 1) / largestDivisions;
            }
            column1Min = Integer.parseInt(column1.min);
            break;
            case "java.lang.Double":            
            column1Range = (Double.parseDouble(column1.max) - Double.parseDouble(column1.min) + 1) / largestDivisions;
            column1Min = Double.parseDouble(column1.min);
            break;
            case "java.lang.String":
            if (Integer.parseInt(column1.max) - Integer.parseInt(column1.min) + 1 < largestDivisions){
                column1Divisions = column1.max.length() - column1.min.length() + 1;
                column1Range = 1;
            }
            else{
                column1Range = (column1.max.length() - column1.min.length() + 1) / largestDivisions;
            }
            column1Min = column1.min.length();
            break;
            case "java.util.Date":
            column1Range = Duration.between(LocalDateTime.parse(column1.min), LocalDateTime.parse(column1.max)).dividedBy(largestDivisions);
            column1Min = LocalDateTime.parse(column1.min);
            break;
        }

        switch(column2Type){
            case "java.lang.Integer":
            if (Integer.parseInt(column2.max) - Integer.parseInt(column2.min) + 1 < largestDivisions){
                column2Divisions = Integer.parseInt(column2.max) - Integer.parseInt(column2.min) + 1;
                column2Range = 1;
            }
            else{
                column2Range = (Integer.parseInt(column2.max) - Integer.parseInt(column2.min) + 1) / largestDivisions;
            }
            column2Min = Integer.parseInt(column2.min);
            break;
            case "java.lang.Double":            
            column2Range = (Double.parseDouble(column2.max) - Double.parseDouble(column2.min) + 1) / largestDivisions;
            column2Min = Double.parseDouble(column2.min);
            break;
            case "java.lang.String":
            if (Integer.parseInt(column2.max) - Integer.parseInt(column2.min) + 1 < largestDivisions){
                column2Divisions = column2.max.length() - column2.min.length() + 1;
                column2Range = 1;
            }
            else{
                column2Range = (Integer.parseInt(column2.max) - Integer.parseInt(column2.min) + 1) / largestDivisions;
            }
            column2Min = column2.min.length();
            break;
            case "java.util.Date":
            column2Range = Duration.between(LocalDateTime.parse(column2.min), LocalDateTime.parse(column2.max)).dividedBy(largestDivisions);
            column2Min = LocalDateTime.parse(column2.min);
            break;
        }

        for (int i = 0; i < column1Divisions; i++) {
            grid.add(new ArrayList<>());
            for (int j = 0; j < column2Divisions; j++){
                Cell cell = new Cell(i, j, path);
                grid.get(i).add(cell);
                switch(column1Type){
                    case "java.lang.Integer":
                    cell.minFirstColumn = (Integer)column1Min + (Integer)column1Range * i;
                    break;
                    case "java.lang.Double":
                    cell.minFirstColumn = (Double)column1Min + (Double)column1Range * i;
                    break;
                    case "java.lang.String":
                    cell.minFirstColumn = (Integer)column1Min + (Integer)column1Range * i;
                    break;
                    case "java.util.Date":
                    LocalDateTime temp = (LocalDateTime)column1Min;
                    for (int k = 0; k < i; k++) 
                        temp.plus((Duration)column1Range);
                    cell.minFirstColumn = temp;
                    break;
                }
                switch(column2Type){
                    case "java.lang.Integer":
                    cell.minSecondColumn = (Integer)column2Min + (Integer)column2Range * j;
                    break;
                    case "java.lang.Double":
                    cell.minSecondColumn = (Double)column2Min + (Double)column2Range * j;
                    break;
                    case "java.lang.String":
                    cell.minSecondColumn = (Integer)column2Min + (Integer)column2Range * j;
                    break;
                    case "java.util.Date":
                    LocalDateTime temp = (LocalDateTime)column2Min;
                    for (int k = 0; k < j; k++)
                        temp.plus((Duration)column2Range);
                    cell.minSecondColumn = temp;
                    break;
                }
            }
        }
        

    }
    public ArrayList<BucketEntry> getResult(SQLTerm[] terms){
        ArrayList<Cell> result = findCells(terms[0]);
        for (int i = 1; i < terms.length; i++) {
            ArrayList<Cell> nextResult = findCells(terms[i]);
            for (Cell cell : result) {
                if (!nextResult.contains(cell))
                    result.remove(cell);
                }
        }
        ArrayList<BucketEntry> entries = new ArrayList<>();
        for (Cell cell : result) 
            entries.addAll(cell.getEntries(clusteringKeyType));
        return entries;
    }
    @SuppressWarnings("all")
    public ArrayList<Cell> findCells(SQLTerm term){
        ArrayList<Cell> result = new ArrayList<>();
        if (term._strColumnName.equals(column1.name)){
            for (int i = 0; i < grid.size(); i++){
                //finding the exact value row
                if (i < grid.get(0).size() - 1 
                && compareObjects((Comparable<Object>)grid.get(i).get(0).minFirstColumn, "<=", (Comparable<Object>)term._objValue)
                && compareObjects((Comparable<Object>)grid.get(i+1).get(0).minFirstColumn, ">", (Comparable<Object>)term._objValue)){
                    result.addAll(grid.get(i));
                    continue;
                }
                if (compareObjects((Comparable<Object>)grid.get(i).get(0).minFirstColumn, term._strOperator, (Comparable<Object>)term._objValue))
                    result.addAll(grid.get(i));
            }
        }
        else{
            for (int i = 0; i < grid.get(0).size(); i++){
                //finding the exact value column
                if (i < grid.get(0).size() - 1 
                && compareObjects((Comparable<Object>)grid.get(0).get(i).minSecondColumn, "<=", (Comparable<Object>)term._objValue)
                && compareObjects((Comparable<Object>)grid.get(0).get(i+1).minSecondColumn, ">", (Comparable<Object>)term._objValue)){
                    for (int j = 0; j < grid.size(); j++)
                        result.add(grid.get(j).get(i));
                    continue;
                }
                if (compareObjects((Comparable<Object>)grid.get(0).get(i).minSecondColumn, term._strOperator, (Comparable<Object>)term._objValue))
                    for (int j = 0; j < grid.size(); j++)
                        result.add(grid.get(j).get(i));
            }
        }
        return result;
    }
    public boolean compareObjects(Comparable<Object> obj1, String operator, Comparable<Object> obj2){
        switch(operator){
            case ">":
                return obj1.compareTo(obj2) > 0; //not always correct
            case ">=":
                return obj1.compareTo(obj2) >= 0; //not always correct
            case "=":
                return obj1.compareTo(obj2) == 0; //not always correct
            case "<=":
                return obj1.compareTo(obj2) <= 0; //correct
            case "<":
                return obj1.compareTo(obj2) < 0; //correct
        }
        return false;
    }

    public void insert(Object key, Hashtable<String, Object> values, int page){
        getCellFromRow(values).insertRow(key, page);
    }
    public void delete(Object key, Hashtable<String, Object> values){
        getCellFromRow(values).deleteRow(key);
    }
    
    private Cell getCellFromRow(Hashtable<String, Object> values){
        int x = grid.size() - 1, y = grid.get(0).size() - 1;
        boolean foundx = false, foundy = false;
        for (int i = 0; i < grid.size() - 1; i++) {
            for (int j = 0; j < grid.get(i).size() - 1; j++) {
                switch(column1.type){
                    case "java.lang.Integer":
                    if ((Integer)(values.get(column1.name)) < (Integer)(grid.get(i + 1).get(0)).minFirstColumn && !foundx){ 
                        x = i;
                        foundx = true;
                    }
                    break;
                    case "java.lang.Double":
                    if ((Double)(values.get(column1.name)) < (Double)(grid.get(i + 1).get(0)).minFirstColumn && !foundx){ 
                        x = i;
                        foundx = true;
                    }
                    break;
                    case "java.lang.String":
                    if (((String)(values.get(column1.name))).length() < (Integer)(grid.get(i + 1).get(0)).minFirstColumn && !foundx){
                         x = i;
                         foundx = true;
                    }
                    break;
                    case "java.util.Date":
                    if (((Date)(values.get(column1.name))).compareTo((Date)(grid.get(i + 1).get(0)).minFirstColumn) < 0 && !foundx){
                         x = i;
                         foundx = true;
                    }
                    break;
                }
                switch(column2.type){
                    case "java.lang.Integer":
                    if ((Integer)(values.get(column2.name)) < (Integer)(grid.get(0).get(j + 1)).minSecondColumn && !foundy){
                         y = j;
                         foundy = true;
                    }
                    break;
                    case "java.lang.Double":
                    if ((Double)(values.get(column2.name)) < (Double)(grid.get(0).get(j + 1)).minSecondColumn && !foundy){
                         y = j;
                         foundy = true;
                    }
                    break;
                    case "java.lang.String":
                    if (((String)(values.get(column2.name))).length() < (Integer)(grid.get(0).get(j + 1)).minSecondColumn && !foundy){
                         y = j;
                         foundy = true;
                    }
                    break;
                    case "java.util.Date":
                    if (((Date)(values.get(column2.name))).compareTo((Date)(grid.get(0).get(j + 1)).minSecondColumn) < 0 && !foundy){
                         y = j;
                         foundy = true;
                    }
                    break;
                }
                if (foundx && foundy)
                    break;
            }
            if (foundx && foundy)
                    break;
        }
        return grid.get(x).get(y);
    }

    public ArrayList<Cell> getCells(Hashtable<String, Object> values){
  	   boolean col1 = false;
  	   if(values.get(column1.name) != null) {
  		   col1 = true;
  	   }

  	   ArrayList<Cell> result = new ArrayList<Cell>();
  	   
         for (int i = 0; i < grid.size() - 1; i++) {
      	   boolean found1 = false, found2 = false;
             for (int j = 0; j < grid.get(i).size() - 1; j++) {
          	   if(col1) {
  	        	   switch(column1.type){
  	               case "java.lang.Integer":
  	                    if ((Integer)(values.get(column1.name)) < (Integer)(grid.get(i + 1).get(0)).minFirstColumn){
  	                        found1 = true;
  	                    } break;
  	               case "java.lang.Double":
  	                   if ((Double)(values.get(column1.name)) < (Double)(grid.get(i + 1).get(0)).minFirstColumn){
  	                       found1 = true;
  	                   } break;
  	               case "java.lang.String":
  	                   if (((String)values.get(column1.name)).length() < ((String)(grid.get(i + 1).get(0)).minFirstColumn).length()){
  	                   		found1 = true;
  	                   } break;
  	               case "java.util.Date":
  	                   if (((Date)values.get(column1.name)).before((Date)(grid.get(i + 1).get(0)).minFirstColumn)){
  	                   		found1 = true;
  	                   } break;
  	        	   }
          	   }
          	   else {
  	                switch(column2.type){
  	                case "java.lang.Integer":
  		                if ((Integer)(values.get(column2.name)) < (Integer)(grid.get(0).get(j + 1)).minSecondColumn){
  		                	found2 = true;
  		                } break;
  	                case "java.lang.Double":
  	                    if ((Double)(values.get(column2.name)) < (Double)(grid.get(0).get(j + 1)).minSecondColumn){
  	                    	found2 = true;
  	                    } break;
  	                case "java.lang.String":
  	                    if (((String)values.get(column2.name)).length() < ((String)(grid.get(0).get(j + 1)).minSecondColumn).length()){
  	                    	found2 = true;
  	                    } break;
  	                case "java.util.Date":
  	                	if (((Date)values.get(column2.name)).before((Date)(grid.get(0).get(j + 1)).minSecondColumn)){
  	                		found2 = true;
  	                    } break;
  	                }
          	   }     
  	                
  	                if(found1 && col1) {
  	                	result = grid.get(i);
  	                	return result;   
  	                }	
  	                
  	                if(found2 && !col1) {
  	             	   result.add(grid.get(i).get(j));
  	             	   break;
  	                }
             }
         }
         
         if(col1)
      	   return grid.get(grid.size()-1);
         
         if(result.size()==0) {
      	   for (int i = 0; i < grid.size(); i++) {
      		   int size = grid.get(i).size();
      		   result.add(grid.get(i).get(size-1));
      	   }
         }   
         return result;
     
     } 		
    
    public int findPageforInsertion(Hashtable<String, Object> values) throws IOException, DBAppException {
    	Column clusteringKey = Table.getClusteringKey(tableName);
    	ArrayList<Cell> requiredCells = new ArrayList<Cell>();
    	
    	requiredCells = getCells(values);
    
    	int lowerPageNumber = -1, upperPageNumber = -1;
    	Object lowerClosestValue = null, upperClosestValue = null;
    	List<BucketEntry> cellContents;
    	
    	for(Cell c : requiredCells) {
    		cellContents = c.getEntries(clusteringKey.type);
    	
    		Object cellValue = null;
    		
    		for(BucketEntry be : cellContents) {
    			
    			cellValue = be.key;
	    		
	    		if(Compare(cellValue, values.get(clusteringKey.name)) == -1) {
	    			if(lowerClosestValue==null) {
	    				lowerPageNumber = be.page;
	    				lowerClosestValue = cellValue;
	    			}
	    			else if(Compare(cellValue, lowerClosestValue) == 1) {
	    				lowerPageNumber = be.page;
	    				lowerClosestValue = cellValue;
	    			}
	    		}
	 
	    		
	    		if(Compare(cellValue, values.get(clusteringKey.name)) == 1) {
	    			if(upperClosestValue==null) {
	    				upperPageNumber = be.page;
	    				upperClosestValue = cellValue;
	    			}
	    			else if(Compare(cellValue, upperClosestValue) == -1) {
	    				upperPageNumber = be.page;
	    				upperClosestValue = cellValue;
	    			}
	    		}
    		}
    	}
    	
    	if(lowerClosestValue!=null)
    		return lowerPageNumber;

    	if(upperClosestValue!=null)
    		return upperPageNumber;
    	
    	return -1;
    }

    public int findPageforUpdate(Hashtable<String, Object> values) throws IOException, DBAppException {
    	Column clusteringKey = Table.getClusteringKey(tableName);
    	ArrayList<Cell> requiredCells = new ArrayList<Cell>();
    	
    	requiredCells = getCells(values);
    
    	int PageNumber = -1;
    	List<BucketEntry> cellContents;
    	
    	for(Cell c : requiredCells) {
    		cellContents = c.getEntries(clusteringKey.type);
    	
    		Object cellValue = null;
    		
    		for(BucketEntry be : cellContents) {
    			
    			cellValue = be.key;
	    		
	    		if(Compare(cellValue, values.get(clusteringKey.name)) == 0) {
	    			PageNumber = be.page;
	    		}
	 
    		}
    	}
    	
    	return PageNumber;
    }
    
    public Object setType(String type, String value) {
    	Object result = null;
    	
    	switch(type) {
    	case "java.lang.Integer": result = Integer.parseInt(value); break;
		case "java.lang.Double": result = Double.parseDouble(value); break;
		case "java.lang.String": result = value; break;
		case "java.util.Date": result = LocalDateTime.parse(value); break;
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
    public void update(Object clusteringkey, Hashtable<String, Object> values, int newPageNum) {
        delete(clusteringkey, values);
        insert(clusteringkey, values, newPageNum);
    }

}