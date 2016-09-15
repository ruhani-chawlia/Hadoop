import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.IntWritable ;

public class NCDC_Driver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		Job job = Job.getInstance(conf, "maximum temperature") ;
		job.setMapperClass(NCDC_Mapper.class) ;
		job.setReducerClass(NCDC_Reducer.class) ;
		FileInputFormat.addInputPath(job,  new Path(args[0])) ;
		FileInputFormat.addInputPath(job,  new Path(args[1])) ;
		FileOutputFormat.setOutputPath(job, new Path(args[2])) ;
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class) ;
		System.exit(job.waitForCompletion(true) ? 0 : 1) ;
	}

}
