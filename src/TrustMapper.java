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
	double p = value.getPageRank()/value.outgoingSize();
	context.write(key, value);
	Iterator outgoing = value.iterator();

	while (outgoing.hasNext()) {
		context.write(outgoing.next(),p);
	} 
        
    }
}
