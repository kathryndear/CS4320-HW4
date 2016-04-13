import java.io.IOException;
import java.util.*;
import java.lang.Long;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
        
	String lo = context.getConfiguration().get("leftover");
	long leftover = Long.parseLong(lo);
	double leftoverD = (double)(leftover)/100000;

	String sz = context.getConfiguration().get("size");
	long size = Long.parseLong(sz);
	double sizeD = (double)(size);
	
	double prs = leftoverD/sizeD;

	Iterator it = Ns.iterator();
	int count = 0;
	while (it.hasNext()) {
		count++;
		Node N = (Node)it.next();
		double prime = alpha * (1/sizeD) + (1-alpha)*(prs + N.getPageRank());
		N.setPageRank(prime);
		context.write(nid,N);
	}


 

    }
}
