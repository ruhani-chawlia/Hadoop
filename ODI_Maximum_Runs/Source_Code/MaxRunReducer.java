import java.io.IOException; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.io.IntWritable ;

public class MaxRunReducer extends Reducer<CompositeGroupKey, IntWritable, CompositeGroupKey, IntWritable> {
	
	public void reduce(CompositeGroupKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int maxRuns = 0;
		for(IntWritable currVal : values) {
			if(maxRuns < currVal.get())
				maxRuns = currVal.get() ;
		}
		context.write(key, new IntWritable(maxRuns)) ;
	}
}
