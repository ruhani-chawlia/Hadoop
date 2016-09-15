import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.io.Text ;
import java.io.IOException ;

public class MaxRunReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int maxRuns = Integer.MIN_VALUE ;
		String team = "" ;
		String matches = "" ;
		for(Text val : values) {
			String teamMatchesRating[] = val.toString().split(",") ;
			int rating = Integer.parseInt(teamMatchesRating[2]) ;
			if(maxRuns < rating) {
				team = teamMatchesRating[0] ;
				matches = teamMatchesRating[1] ;
				maxRuns = rating ;
			}
		}
		context.write(new Text(team + " " + matches + " " + maxRuns), key) ;
	}
}
