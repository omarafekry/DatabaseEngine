import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class Cell {
    Bucket firstBucket;
    Object minFirstColumn;
    Object minSecondColumn;

    public Cell(int x, int y, String tableName, String indexName){
        firstBucket = new Bucket("Tables/" + tableName + "/Indexes/" + indexName + "/" + x + "_" + y + ".csv");
    }

    public void insertRow(Object key, int page) {
        // TODO: Insert key next to same page
        try{
            BufferedWriter output = new BufferedWriter(new FileWriter(firstBucket.path, true));
            if (!new File(firstBucket.path).exists())
                new File(firstBucket.path).createNewFile();
            output.append(key + "," + page);
            output.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    public void deleteRow(Object key) {
        CSVReader reader = null;
        try{
            reader = new CSVReader(new FileReader(firstBucket.path));
            List<String[]> lines = reader.readAll();
            for (String[] strings : lines) {
                if (strings[0].equals("" + key.toString())){
                    lines.remove(strings);
                    break;
                }
            }
        } catch(IOException | CsvException e){
            e.printStackTrace();
        }
    }
    public void updateRow(Object key, int page){
        deleteRow(key);
        insertRow(key, page);
    }
}
