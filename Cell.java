import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

public class Cell {
    Bucket firstBucket;
    Comparable<Object> minFirstColumn, maxFirstColumn;
    Comparable<Object> minSecondColumn, maxSecondColumn;

    public static void main(String[] args) throws IOException, DBAppException {
        Cell cell = new Cell(0, 0);
        Hashtable<String, Object> row = new Hashtable<>();
    }

    public Cell(int x, int y){
        firstBucket = new Bucket("" + x + "_" + y + ".csv");
    }

    public void insertRow(String strTableName, Hashtable<String,Object> row) throws IOException, DBAppException{
        Column[] columns = Table.getColumns(strTableName);
        BufferedWriter output = new BufferedWriter(new FileWriter(firstBucket.fileName, true));
        if (!new File(firstBucket.fileName).exists()){
            new File(firstBucket.fileName).createNewFile();
            output.append(getRowString(row, columns));
            output.close();
            return;
        }
        output.append(getRowString(row, columns));
        output.close();
    }

    public String getRowString(Hashtable<String,Object> row, Column[] columns){
        String result = row.get(columns[0].name).toString();
        for (int i = 1; i < columns.length; i++) {
            result += "," + row.get(columns[i].name).toString();
        }
        return result;
    }

    public void deleteRow(String strTableName, Hashtable<String,Object> row){}
    public void updateRow(String strTableName, Hashtable<String,Object> row) throws IOException, DBAppException{
        deleteRow(strTableName, row);
        insertRow(strTableName, row);
    }
}
