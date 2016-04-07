import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {

        //Implement
	if !(value.outgoingSize()==0) {
		double p = value.getPageRank()/value.outgoingSize();
		NodeOrDouble val = new NodeOrDouble(value);
		context.write(key, val);

		Iterator<Integer> outgoing = value.iterator();

		while (outgoing.hasNext()) {
			NodeOrDouble PR = new NodeOrDouble(p);
			IntWritable outnode = new IntWritable(outgoing.next());
			context.write(outnode,PR);
		}
	}
	else {
		context.write(key, val);		
		context.getCounter(MyCounter.Leftover).increment(value.PageRank);
	}
	
	context.getCounter(MyCounter.Counter).increment(1);
      
    }
}
