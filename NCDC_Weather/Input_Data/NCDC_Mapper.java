import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.mapreduce.Mapper ;
import java.io.IOException ;

public class NCDC_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final int MISSING = 9999 ;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15,19) ;
		
		//code to extract air temperature 
		int airTemperature ;
		if(line.charAt(87) == '+') //if the temperature is preceded by '+', 
			airTemperature = Integer.parseInt(line.substring(88,92)) ; //skip reading it so that parseInt method doesn't throw NumberFormatException
		else
			airTemperature = Integer.parseInt(line.substring(87,92)) ;
		String quality = line.substring(92,93) ;
		
		//write the output only if the input record is valid
		if(airTemperature != MISSING && quality.matches("[01459]"))
			context.write(new Text(year), new IntWritable(airTemperature)) ;
	}
}
