package Loader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import static java.lang.System.exit;

public class Loader {
    public static void main(String[] args) throws IOException {

        // Open up the input file
        CSVReader reader = new CSVReader(new FileReader("/Users/brent/Documents/Education/PSU/Grad School/Data Stores/nodes.csv"));

        // Set up a connection to HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the table we are using
        HTable table = new HTable(config, "graph");
        
        // Grab the column names from the first row
        String headers[];
        headers = reader.readNext();
        if (headers == null) {
            throw new IOException("Empty Input File");
        }
        

        // Run through the rows  
        String line[];
        ArrayList<Put> mputs = new ArrayList<Put>();
        while ((line = reader.readNext()) != null) {
            mputs.add(parseToPut("nodes", headers, line));
        }
        
        // Put them in
        table.put(mputs);

        // Clean up
        table.flushCommits();
        table.close(); 

    }
    
    private static Put parseToPut(String cf, String headers[], String line[]) {
        Put p = new Put(Bytes.toBytes(line[0]));
        for (int i = 1; i < line.length; ++i) {
            p.add(Bytes.toBytes(cf), Bytes.toBytes(headers[i]), Bytes.toBytes(line[i]));
        }
        
        return p;
    }
}
