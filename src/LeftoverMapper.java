import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class LeftoverMapper extends Mapper<IntWritable, Node, IntWritable, Node> {

    public void map(IntWritable nid, Node N, Context context) throws IOException, InterruptedException {
        
        //Implement

	long missing = Long.parseLong(context.getConfiguration().get(MyCounter.Leftover.toString()))/100000;
	long size = Long.parseLong(context.getConfiguration().get(MyCounter.Counter.toString()));

	double prs = (double)(missing/size);
	NodeOrDouble prs2 = new NodeOrDouble(prs);
	context.write(nid,N);
	
	//context.write(nod,prs2);
	//take total dangling pagerank/size of graph
	//emit node, node id
	//emit node, pagerank/size
	//Long.parseLong(str);

    }
}
