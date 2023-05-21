import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class Table{
    final static String delimitter = ",";
    public static Column[] getColumns(String tableName) throws IOException{
        BufferedReader br = null;
        br = new BufferedReader(new FileReader(new File("metadata.csv")));
        String line;
        LinkedList<Column> columns = new LinkedList<>();
        while((line = br.readLine()) == null){
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
        return (Column[])columns.toArray();
    }
}