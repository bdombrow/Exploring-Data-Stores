package Loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Loader {
    public static void main(String[] args) throws IOException {

        // Set up a connection to HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the nodes we are using
        HTable nodes = new HTable(config, "graph");

        // Create a new row and populate it's data
        Put p1 = new Put(Bytes.toBytes("1"));      // Set the row first
        p1.add(Bytes.toBytes("nodes"), Bytes.toBytes("name"), Bytes.toBytes("Brent"));  // Column Family, Column Name, Value
        p1.add(Bytes.toBytes("nodes"), Bytes.toBytes("gender"), Bytes.toBytes("Male"));
        p1.add(Bytes.toBytes("nodes"), Bytes.toBytes("occupation"), Bytes.toBytes("Masters Student"));

        // Put the new row into HBase
        //nodes.put(p1);
        
        // Create a second row to test multiple put
        Put p2 = new Put(Bytes.toBytes("2"));
        p2.add(Bytes.toBytes("nodes"), Bytes.toBytes("name"), Bytes.toBytes("Steve"));
        p2.add(Bytes.toBytes("nodes"), Bytes.toBytes("gender"), Bytes.toBytes("Male"));
        p2.add(Bytes.toBytes("nodes"), Bytes.toBytes("occupation"), Bytes.toBytes("Architect"));
        
        // Multiple Put
        List mput = new ArrayList<Put>();
        mput.add(p1);
        mput.add(p2);

        //nodes.put(mput);
        
        // Add an edge and see what happens
        //HTable edges = new HTable(config, "graph");
        Put p3 = new Put(Bytes.toBytes("1"));
        p3.add(Bytes.toBytes("edges"), Bytes.toBytes("relation"), Bytes.toBytes("Brother"));
        p3.add(Bytes.toBytes("edges"), Bytes.toBytes("start"), Bytes.toBytes("1"));
        p3.add(Bytes.toBytes("edges"), Bytes.toBytes("end"), Bytes.toBytes("2"));

        //edges.put(p3);
        
        // Column families do not appear to be as distinct as Cassandra
        mput.add(p3);
        nodes.put(mput);

        // Clean up
        nodes.flushCommits();
        //edges.flushCommits();
        nodes.close();
        //edges.close();
        

    }
}
