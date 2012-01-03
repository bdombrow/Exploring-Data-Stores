package FoF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.TreeSet;

/**
 * Friends of Friends
 *
 * Finds the friends of friends for a given node.
 *
 * Starting node, table, and column family are all hard coded.
 *
 * Created by IntelliJ IDEA.
 * User: brent
 * Date: 12/30/11
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class FoF {

    // Set the graph type
    private static final boolean UNDIRECTED = true;

    /**
     * main routine.
     *
     * Gets the friends of friends for the hard coded node.
     *
     * @param args Not used.
     * @throws Exception Passed up from HBase. No exception handling has been implemented.
     */
    
    public static void main(String[] args) throws Exception {

        TreeSet<String> friends = new TreeSet<String>();
        TreeSet<String> friendsofFriends = new TreeSet<String>();

        // Get the friends
        friends.addAll(getFriends("1"));

        // Get the friends of the friends
        for (String friend : friends) {
            friendsofFriends.addAll(getFriends(friend));
        }

        // Display the results
        System.out.println(friendsofFriends);

    }

    /**
     * getFriends
     *
     * Scans the table for the friends of a given node.
     *
     * @param node Starting node
     * @return TreeSet of Friends
     * @throws Exception Passed up from HBase
     */

    private static TreeSet<String> getFriends(String node) throws Exception {
        // Configure HBase connection
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the table
        HTable table = new HTable(config, "graph");
        
        String cf;
        
        if (UNDIRECTED) {
            cf = "wedges";
        } else {
            cf = "uedges";
        }

        // Filter results by Start qualifier (case sensitive)
        SingleColumnValueFilter startFilter;
        startFilter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes("Start"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(node));

        // Filter results by End qualifier (case sensitive)
        SingleColumnValueFilter endFilter;
        endFilter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes("End"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(node));

        // Set up the filter list
        FilterList filters;
        filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filters.addFilter(startFilter);
        if (UNDIRECTED) {
            filters.addFilter(endFilter);
        }

        // Create a new scan
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(cf),Bytes.toBytes("Start")); // Needed for filter and results
        s.addColumn(Bytes.toBytes(cf), Bytes.toBytes("End")); // Needed for filter and results
        s.setFilter(filters);

        // Get the results of the scan
        ResultScanner results = table.getScanner(s);

        TreeSet<String> friends = new TreeSet<String>();

        // Add the results of the scan to the friends set
        for (Result result : results) {
            // Results are jumbled in the undirected case
            String startNode = Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes("Start")));
            String endNode = Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes("End")));
            if (node.contentEquals(startNode)) {
                friends.add(endNode);
            } else {
                friends.add(startNode);
            }
        }

        // Clean up
        results.close();
        table.close();

        return friends;

    }
}
