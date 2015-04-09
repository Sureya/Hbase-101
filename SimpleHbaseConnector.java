import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class SimpleHbaseConnector {

    SimpleHbaseConnector(){

    }

    public static void createSimpleHbaseTable(String tableName, String columnFamily  ) throws MasterNotRunningException, ZooKeeperConnectionException {
        try{
            HBaseConfiguration baseConfiguration = new HBaseConfiguration(new Configuration());
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));

            System.out.println("Establishing connection.");

            HBaseAdmin admin = new HBaseAdmin(baseConfiguration);

            System.out.println("Creating a Table.");

            try {
                admin.createTable(tableDescriptor);
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Successfully created the table : "+tableName+" with one column family.");

        }catch(Exception exception){
            System.out.println(exception.getMessage());
        }
    }

    public static void InsertSimpleHbaseTable(String tableName, HashMap<String,String[]> columnValues,String[] columns) throws IOException {


        try {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            HTable table = new HTable(config, tableName);

            Iterator it = columnValues.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry columnValue = (Map.Entry) it.next();
                Put row = new Put(Bytes.toBytes(String.valueOf(columnValue.getKey())));
                for (String columnName : columns) {
                    row.add(Bytes.toBytes(tableName), Bytes.toBytes(columnName), Bytes.toBytes(String.valueOf(columnValue.getValue())));
                }

                table.put(row);

            }

            System.out.println("Successfully added the values in the table");

        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
    }

    public static void ReadsimpleHbaseTable(String tableName, String[] rowNames, String[] columnNames,String columnFamily){

        try{
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            HTable table = new HTable(config, tableName);

            HashMap<String,byte[]> resultSet = new HashMap<String,byte[]>();

            for(String row :rowNames){
                Get getRow = new Get(Bytes.toBytes(row));
                Result result = table.get(getRow);
                for(String column: columnNames){
                    byte[] temp = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                    resultSet.put(row,temp);
                }
            }


        }catch (Exception exception){
            System.out.println(exception.getMessage());

        }


    }

    public static void deleteHbasetable(String tableName) throws IOException {

        try {
            HBaseConfiguration baseConfiguration = new HBaseConfiguration(new Configuration());
            HBaseAdmin admin = new HBaseAdmin(baseConfiguration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.close();
        }catch (Exception exception){
            System.out.println(exception.getMessage());
        }

    }




}
