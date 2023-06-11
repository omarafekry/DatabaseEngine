import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;

import exceptions.DBAppException;
import main.Column;

public class Table{
    private final static String delimitter = ",";
    String name;
    private int nextPage = 0;

    public Table(String name){
        this.name = name;
    }
    @SuppressWarnings("all")
    public LinkedList<Hashtable<String, Comparable<Object>>> nextPage() throws DBAppException{
        BufferedReader br = null;
        LinkedList<Hashtable<String, Comparable<Object>>> rows = new LinkedList<>();
        Column[] columns = getColumns(name);
        try {
            br = new BufferedReader(new FileReader(new File("Tables/" + name + "/" + nextPage + ".csv")));
            String line;
            while((line = br.readLine()) != null){
                String[] values = line.split(delimitter);
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
            br.close();
        } catch (IOException e) {
            return null;
        }
        nextPage++;
        return rows;
    }
    @SuppressWarnings("all")
    public static Hashtable<Object, Hashtable<String, Comparable<Object>>> getPageHashtable(String tableName, int pageNumber) throws DBAppException{
        BufferedReader br = null;
        Hashtable<Object, Hashtable<String, Comparable<Object>>> page = new Hashtable<>();
        Column[] columns = getColumns(tableName);
        try {
            br = new BufferedReader(new FileReader(new File("Tables/" + tableName + "/" + pageNumber + ".csv")));
            String line;
            while((line = br.readLine()) != null){
                String[] values = line.split(delimitter);
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
            br.close();
        } catch (IOException e) {
            return null;
        }
        return page;
    }
    
    public void resetPages(){
        nextPage = 0;
    }

    @SuppressWarnings("all")
    public static Column[] getColumns(String tableName) throws DBAppException{
        BufferedReader br = null;
        LinkedList<Column> columns = new LinkedList<>();
        try {
            br = new BufferedReader(new FileReader(new File("metadata.csv")));
        
            String line;
            columns = new LinkedList<>();
            while((line = br.readLine()) != null){
                String tName = line.split(delimitter)[0];
                if (tName.equals(tableName)){
                    Column col = new Column();
                    col.name = line.split(delimitter)[1];
                    col.type = line.split(delimitter)[2];
                    col.clusteringKey = Boolean.parseBoolean(line.split(delimitter)[3]);
                    col.indexName = line.split(delimitter)[4];
                    col.indexType = line.split(delimitter)[5];
                    col.min = line.split(delimitter)[6];
                    col.max = line.split(delimitter)[7];
                    col.foreignKey = Boolean.parseBoolean(line.split(delimitter)[8]);
                    col.foreignTableName = line.split(delimitter)[9];
                    col.foreignColumnName = line.split(delimitter)[10];
                    col.computed = Boolean.parseBoolean(line.split(delimitter)[11]);
                    columns.addLast(col);
                }
            }
            br.close();
        } catch (IOException e) {
            throw new DBAppException("Couldn't find metadata.csv");
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