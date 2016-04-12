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
        //Implement
	System.out.println("HelloLR");
	System.out.println(MyCounter.Leftover.getCounter());
	System.out.println(context.getCounter(MyCounter.Leftover.getCounter()));
	long missing = Long.parseLong(context.getConfiguration().get(MyCounter.Leftover.toString()))/100000;

	System.out.println("Passed Missing");
	long size = Long.parseLong(context.getConfiguration().get(MyCounter.Counter.toString()));
	System.out.println("Passed Size");
	double prs = (double)(missing/size);

	Iterator it = Ns.iterator();
	int count = 0;
	while (it.hasNext()) {
		count++;
		Node N = (Node)it.next();
		double prime = alpha * (1/size) + (1-alpha)*(prs + N.getPageRank());
		N.setPageRank(prime);
		context.write(nid,N);
	}
	System.out.println("HelloLR2");
	System.out.println("count1: " + count);

 

    }
}
