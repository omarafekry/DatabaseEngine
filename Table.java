import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;


public class Table{
    String name;

    public Table(String name){
        this.name = name;
    }
    @SuppressWarnings("all")
    public LinkedList<Hashtable<String, Comparable<Object>>> getPage(int page) throws DBAppException, NumberFormatException{
        CSVReader reader = null;
        LinkedList<Hashtable<String, Comparable<Object>>> rows = new LinkedList<>();
        Column[] columns = getColumns(name);
        try {
            reader = new CSVReader(new FileReader(new File("Tables/" + name + "/" + page + ".csv")));
            String[] line = reader.readNext();
            while((line = reader.readNext()) != null){
                String[] values = line;
                Hashtable<String, Comparable<Object>> row = new Hashtable<>();
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].type.equals("java.lang.Integer"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)((Integer)Integer.parseInt(values[i])));
                    else if(columns[i].type.equals("java.lang.String"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)values[i]);
                    else if(columns[i].type.equals("java.lang.Double"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)Double.parseDouble(values[i]));
                    else if(columns[i].type.equals("java.util.Date")){
                        SimpleDateFormat formatter = new SimpleDateFormat("DD.MM.YYYY");
                        Date date = null;
                        try {
                            date = formatter.parse(values[i]);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        row.put(columns[i].name, (Comparable<Object>)(Object)date);
                    }
                }
                rows.addLast(row);
            }
            reader.close();
        } catch (IOException | CsvValidationException e) {
            return null;
        }
        return rows;
    }
    @SuppressWarnings("all")
    public static Hashtable<Object, Hashtable<String, Comparable<Object>>> getPageHashtable(String tableName, int pageNumber) throws DBAppException{
        CSVReader reader = null;
        Hashtable<Object, Hashtable<String, Comparable<Object>>> page = new Hashtable<>();
        Column[] columns = getColumns(tableName);
        try {
            reader = new CSVReader(new FileReader(new File("Tables/" + tableName + "/" + pageNumber + ".csv")));
            String[] line = reader.readNext();
            while((line = reader.readNext()) != null){
                String[] values = line;
                Hashtable<String, Comparable<Object>> row = new Hashtable<>();
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].type.equals("java.lang.Integer"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)((Integer)Integer.parseInt(values[i])));
                    else if(columns[i].type.equals("java.lang.String"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)values[i]);
                    else if(columns[i].type.equals("java.lang.Double"))
                        row.put(columns[i].name, (Comparable<Object>)(Object)Double.parseDouble(values[i]));
                    else if(columns[i].type.equals("java.util.Date")){
                        SimpleDateFormat formatter = new SimpleDateFormat("DD.MM.YYYY");
                        Date date = null;
                        try {
                            date = formatter.parse(values[i]);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        row.put(columns[i].name, (Comparable<Object>)(Object)date);
                    }
                }
                page.put(row.get(columns[0].name), row);
            }
            reader.close();
        } catch (IOException | CsvException e) {
            return null;
        }
        return page;
    }

    @SuppressWarnings("all")
    public static Column[] getColumns(String tableName) throws DBAppException{
        CSVReader reader = null;
        LinkedList<Column> columns = new LinkedList<>();
        try {
            reader = new CSVReader(new FileReader(new File("metadata.csv")));
        
            String[] line;
            columns = new LinkedList<>();
            while((line = reader.readNext()) != null){
                String tName = line[0];
                if (tName.equals(tableName)){
                    Column col = new Column();
                    col.name = line[1];
                    col.type = line[2];
                    col.clusteringKey = Boolean.parseBoolean(line[3]);
                    col.indexName = line[4];
                    col.indexType = line[5];
                    col.min = line[6];
                    col.max = line[7];
                    col.foreignKey = Boolean.parseBoolean(line[8]);
                    col.foreignTableName = line[9];
                    col.foreignColumnName = line[10];
                    col.computed = Boolean.parseBoolean(line[11]);
                    columns.addLast(col);
                }
            }
            reader.close();
        } catch (IOException e) {
            throw new DBAppException("Couldn't find metadata.csv");
        } catch (CsvValidationException e) {
            e.printStackTrace();
        }

        if (columns.isEmpty())
            throw new DBAppException("Couldn't find table with name " + tableName);
        Column[] result = new Column[columns.size()];
        columns.toArray(result);
        return result;
    }
    
    public static Column getClusteringKey(String tableName) throws IOException, DBAppException {
    	Column[] tableColumns = getColumns(tableName);
    	
    	for(Column c: tableColumns) {
    		if(c.clusteringKey)
    			return c;
    	}
    	
    	return null;
    }
}