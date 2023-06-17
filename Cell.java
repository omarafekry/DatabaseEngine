import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

public class Cell {
    Bucket firstBucket;
    Object minFirstColumn;
    Object minSecondColumn;

    public Cell(int x, int y, String path){
        firstBucket = new Bucket(path + "/" + x + "_" + y + ".csv");
    }

    public ArrayList<BucketEntry> getEntries(String clusteringKeyType){
        ArrayList<BucketEntry> entries = new ArrayList<>();
        if (!new File(firstBucket.path).exists()) return entries;
        CSVReader reader = null;
        try{
            reader = new CSVReader(new FileReader(firstBucket.path));
            List<String[]> lines = reader.readAll();
             
            if (clusteringKeyType.equals("java.lang.Integer"))
                for (String[] strings : lines)
                    entries.add(new BucketEntry(Integer.parseInt(strings[0]), Integer.parseInt(strings[1])));
            else if(clusteringKeyType.equals("java.lang.String"))
                for (String[] strings : lines)
                    entries.add(new BucketEntry(strings[0], Integer.parseInt(strings[1])));
            else if(clusteringKeyType.equals("java.lang.Double"))
                for (String[] strings : lines)
                    entries.add(new BucketEntry(Double.parseDouble(strings[0]), Integer.parseInt(strings[1])));
            else if(clusteringKeyType.equals("java.util.Date")){
                for (String[] strings : lines){
                    SimpleDateFormat formatter = new SimpleDateFormat("DD.MM.YYYY");
                    Date date = null;
                    try {
                        date = formatter.parse(strings[0]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    entries.add(new BucketEntry(date, Integer.parseInt(strings[1])));
                }
            }
            
        } catch(IOException | CsvException e){
            e.printStackTrace();
        }
        return entries;
    }

    public void insertRow(Object key, int page) {       
        CSVReader reader = null;
        try{
            if (!new File(firstBucket.path).exists())
                new File(firstBucket.path).createNewFile();
            reader = new CSVReader(new FileReader(firstBucket.path));
            List<String[]> lines = reader.readAll();
            // for (int i = 0; i < lines.size(); i++) {
            //     if (lines.get(i)[1].equals("" + page)){
            //         lines.add(i, new String[]{key.toString(), "" + page});
            //         CSVWriter writer = new CSVWriter(new FileWriter(firstBucket.path));
            //         writer.writeAll(lines);
            //         break;
            //     }
            // }
            lines.add(new String[]{key.toString(), "" + page});
            CSVWriter writer = new CSVWriter(new FileWriter(firstBucket.path));
            writer.writeAll(lines);
            writer.close();
        } catch(IOException | CsvException e){
            e.printStackTrace();
        }
    }



    public void deleteRow(Object key) {
        CSVReader reader = null;
        CSVWriter writer = null;
        try{
            reader = new CSVReader(new FileReader(firstBucket.path));
            writer = new CSVWriter(new FileWriter(firstBucket.path));
            List<String[]> lines = reader.readAll();
            for (String[] strings : lines) {
                if (strings[0].equals("" + key.toString())){
                    lines.remove(strings);
                    break;
                }
            }
            writer.writeAll(lines);
            reader.close();
            writer.close();
            if (lines.size() == 0)
                new File(firstBucket.path).delete();
        } catch(IOException | CsvException e){
            e.printStackTrace();
        }
    }
    public void updateRow(Object key, int page){
        deleteRow(key);
        insertRow(key, page);
    }
} 
