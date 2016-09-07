import java.io.IOException ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.LongWritable ;

public class MaxRunMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException{
		String fields[] = inputValue.toString().split(",") ;
		String teamMatchesRating = fields[0] + "," + fields[1] + "," + fields[3] ;
		String category = fields[4] ;
		context.write(new Text(category), new Text(teamMatchesRating)) ;
	}
}
