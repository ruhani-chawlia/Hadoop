import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.mapreduce.Reducer; 
import java.io.IOException ;

public class NCDC_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int maxVal = Integer.MIN_VALUE ;
		for(IntWritable val : values)
			maxVal = Math.max(maxVal,  val.get()) ;
		context.write(key,  new IntWritable(maxVal)) ;
	}
}
