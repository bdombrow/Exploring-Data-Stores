/**
 * Loader class.
 *
 * This is a crude loader class to add nodes and edges into HBase.
 * It uses hard coded file and table names.
 * It does not batch, all lines in a file will be added at once.
 * !!This is not suitable for large files!!
 *
 * Possible improvements:
 *  Batch inserts.
 *  Multithread on each file.
 *
 *
 * @author Brent Dombrowski
 *
 */

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
        
        ArrayList<String> files = new ArrayList<String>();
        
        files.add("/Users/brent/Documents/Education/PSU/Grad School/Data Stores/nodes.csv");
        files.add("/Users/brent/Documents/Education/PSU/Grad School/Data Stores/uedges.csv");
        files.add("/Users/brent/Documents/Education/PSU/Grad School/Data Stores/wedges.csv");
        
        for (String file : files) {
            processFile(file);
            System.out.println("Processed " + file);
        }
    }

    /**
     * processFile.
     *
     * Processes a given file. The file name is used as the column family.
     * The column names will be pulled from the first row in the file.
     *
     * @param file Full path to the file to process.
     * @throws IOException
     */

    private static void processFile(String file) throws IOException {

        // Open up the input file
        CSVReader reader = new CSVReader(new FileReader(file));

        // Set up a connection to HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the table we are using
        HTable table = new HTable(config, "graph");
        
        // Parse the filename -> Will be used for the column family
        Filename fileName = new Filename(file, '/', '.');
        
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
            mputs.add(parseToPut(fileName.filename(), headers, line));
        }
        
        // Put them in
        table.put(mputs);

        // Clean up
        table.flushCommits();
        table.close(); 

    }

    /**
     * parseToPut
     *
     * Parsers a given string into a put object.
     *
     * @param cf Column Family
     * @param headers Column Names
     * @param line String to be converted to a put object
     * @return Put object ready to be added.
     */
    private static Put parseToPut(String cf, String headers[], String line[]) {
        Put p = new Put(Bytes.toBytes(line[0]));
        for (int i = 1; i < line.length; ++i) {
            p.add(Bytes.toBytes(cf), Bytes.toBytes(headers[i]), Bytes.toBytes(line[i]));
        }
        
        return p;
    }
}
