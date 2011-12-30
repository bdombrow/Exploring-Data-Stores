package FoF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.TreeSet;

/**
 * Friends of Friends
 *
 * Finds the friends of friends for a given node.
 *
 * Starting node, table, and column family are all hard coded.
 * This is set up for a directed graph.
 *
 * Created by IntelliJ IDEA.
 * User: brent
 * Date: 12/30/11
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class FoF {
    public static void main(String[] args) throws Exception {
        TreeSet<String> friends = new TreeSet<String>();
        TreeSet<String> friendsofFriends = new TreeSet<String>();

        friends = getFriends("1");

        for (String friend : friends) {
            friendsofFriends.addAll(getFriends(friend));
        }

        System.out.println(friendsofFriends);

    }

    /**
     * getFriends
     *
     * Scans the table for the friends of a given node.
     *
     * @param start Starting node
     * @return TreeSet of Friends
     * @throws Exception
     */

    private static TreeSet<String> getFriends(String start) throws Exception {
        // Configure HBase connection
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.9.73.25");

        // Set the table
        HTable table = new HTable(config, "graph");

        // Filter results by Start qualifier in uedges column family (case sensitive)
        SingleColumnValueFilter singleColumnValueFilter;
        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("uedges"), Bytes.toBytes("Start"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(start));

        // Create a new scan
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("uedges"),Bytes.toBytes("Start")); // Start is needed for the filter
        s.addColumn(Bytes.toBytes("uedges"), Bytes.toBytes("End"));
        s.setFilter(singleColumnValueFilter);

        // Get the results of the scan
        ResultScanner results = table.getScanner(s);

        TreeSet<String> friends = new TreeSet<String>();

        // Add the results of the scan to the friends set
        for (Result result : results) {
            friends.add(
                    Bytes.toString(result.getValue(Bytes.toBytes("uedges"), Bytes.toBytes("End")))
            );
        }

        // Clean up
        results.close();
        table.close();

        return friends;

    }
}
