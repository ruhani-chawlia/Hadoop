import java.io.IOException ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;


public class EmployeeMapper extends Mapper<Object, Text, Text, LongWritable> {
	
	public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
			String emp[] = inputValue.toString().split(",") ;
			Text unit = new Text(emp[2]) ;
			LongWritable salary = new LongWritable(Long.parseLong(emp[3])) ;
			context.write(unit,salary) ;
		} 
	}
