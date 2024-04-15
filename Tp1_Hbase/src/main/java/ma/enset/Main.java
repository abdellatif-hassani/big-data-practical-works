package ma.enset;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {

    public static void main(String[] args) {
        try {
            // Create HBase configuration
            Configuration config = HBaseConfiguration.create();

            // Set HBase Zookeeper quorum address
            config.set("hbase.zookeeper.quorum", "zookeeper");

            // Create connection to HBase
            Connection connection = ConnectionFactory.createConnection(config);

            // Create admin instance
            Admin admin = connection.getAdmin();

            // Define table name
            TableName tableName = TableName.valueOf("enset");

            // Create table descriptor
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

            // Add column family
            tableDescriptor.addFamily(new HColumnDescriptor("info"));
            tableDescriptor.addFamily(new HColumnDescriptor("grades"));

            // Create the table
            admin.createTable(tableDescriptor);

            // Get table instance
            Table table = connection.getTable(tableName);

            // Create put with row key
            Put put = new Put("student1".getBytes());

            // Add data to the column family
            put.addColumn("info".getBytes(), "name".getBytes(), "John Doe".getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(),"20".getBytes());

            put.addColumn("grades".getBytes(), "math".getBytes(), "B".getBytes());
            put.addColumn("grades".getBytes(), "science".getBytes(),"A".getBytes());

            // Insert data into the table
            table.put(put);

            //Affichage des données
            // Create a Get object to retrieve data
            Get get = new Get("student1".getBytes());

            // Retrieve data from the table
            Result result = table.get(get);

            // Display the retrieved data of user with row key "student1"
            byte[] value1 = result.getValue("info".getBytes(), "name".getBytes());
            byte[] value2 = result.getValue("info".getBytes(), "age".getBytes());
            System.out.println("Retrieved value1: " + Bytes.toString(value1));
            System.out.println("Retrieved value2: " + Bytes.toString(value2));

            // Close the table and admin objects
            table.close();
            admin.close();
            connection.close();

            System.out.println("Table created and data inserted successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
