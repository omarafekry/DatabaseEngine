import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Hashtable;

public class Index {
    final int largestDivisions = Integer.parseInt(System.getProperty("indexDivisions"));
    ArrayList<ArrayList<Cell>> grid = new ArrayList<>();
    String tableName, indexName;
    Column column1, column2;

    public Index(String tableName, String column1Name, String column2Name) throws DBAppException{
        this.tableName = tableName;
        String column1Type = "", column2Type = "";
        Column[] columns = Table.getColumns(tableName);
        for (int i = 0; i < columns.length; i++) {
            if (column1Name.equals(columns[i].name))
                this.column1 = columns[i];
            if (column2Name.equals(columns[i].name))
                this.column2 = columns[i];
        }
        this.indexName = column1.name + column2.name;

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
            for (int j = 0; j < column2Divisions; j++){
                Cell cell = new Cell(i, j, tableName, indexName);
                grid.get(i).add(cell);
                switch(column1Type){
                    case "java.lang.Integer":
                    cell.minFirstColumn = (Integer)column1Min + column1Divisions * i;
                    break;
                    case "java.lang.Double":
                    cell.minFirstColumn = (Double)column1Min + column1Divisions * i;
                    break;
                    case "java.lang.String":
                    cell.minFirstColumn = (Integer)column1Min + column1Divisions * i;
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
                    cell.minFirstColumn = (Integer)column2Min + column2Divisions * i;
                    break;
                    case "java.lang.Double":
                    cell.minFirstColumn = (Double)column2Min + column2Divisions * i;
                    break;
                    case "java.lang.String":
                    cell.minFirstColumn = (Integer)column2Min + column2Divisions * i;
                    break;
                    case "java.util.Date":
                    LocalDateTime temp = (LocalDateTime)column2Min;
                    for (int k = 0; k < j; k++)
                        temp.plus((Duration)column2Range);
                    cell.minFirstColumn = temp;
                    break;
                }
            }
        }
    }

    public void insertKey(Object key, Hashtable<String, Object> values, int page){
        int x, y;
        boolean found = false;
        for (int i = 0; i < grid.size() - 1; i++) {
            for (int j = 0; j < grid.get(i).size() - 1; j++) {
                switch(column1.type){
                    case "java.lang.Integer":
                    if ((Integer)(values.get(column1.name)) < (Integer)(grid.get(i + 1).get(j + 1)).minFirstColumn && (Integer)(values.get(column2.name)) < (Integer)(grid.get(i + 1).get(j + 1)).minSecondColumn){
                        grid.get(i).get(j).insertRow(key, page);
                        found = true;
                    }
                }
            }
        }
    }
}