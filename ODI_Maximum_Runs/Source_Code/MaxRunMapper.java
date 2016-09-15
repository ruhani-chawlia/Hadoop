import java.io.IOException ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.IntWritable ;

public class MaxRunMapper extends Mapper<Object, Text, CompositeGroupKey, IntWritable>  {

	public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException{
		String fields[] = inputValue.toString().split(" ") ;
		String player = fields[0] ;
		int runs = Integer.parseInt(fields[1]) ;
		String date = fields[6] ;
		String dateFields[] = date.split("/") ;
		String year = dateFields[2] ;
		if(Integer.parseInt(year)>=1900 && Integer.parseInt(year)<=2013) {
				CompositeGroupKey playerAndYear = new CompositeGroupKey(player, year) ;
				context.write(playerAndYear, new IntWritable(runs)) ;
			}
		}
	}