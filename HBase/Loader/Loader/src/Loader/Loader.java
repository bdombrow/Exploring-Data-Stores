package Loader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Loader {
    public static void main(String[] args) throws IOException {

        // Set up a connection to HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the table we are using
        HTable table = new HTable(config, "graph");

        // Create a new row and populate it's data
        Put p = new Put(Bytes.toBytes("1"));      // Set the row first
        p.add(Bytes.toBytes("nodes"), Bytes.toBytes("name"), Bytes.toBytes("Brent"));  // Column Family, Column Name, Value
        p.add(Bytes.toBytes("nodes"), Bytes.toBytes("gender"), Bytes.toBytes("Male"));
        p.add(Bytes.toBytes("nodes"), Bytes.toBytes("occupation"), Bytes.toBytes("Student"));

        // Put the new row into HBase
        table.put(p);

    }
}
