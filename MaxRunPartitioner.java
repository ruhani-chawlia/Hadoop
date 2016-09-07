
import org.apache.hadoop.mapreduce.Partitioner ;
import org.apache.hadoop.io.Text ;

public class MaxRunPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String fields[] = value.toString().split(",") ;
		int numOfMatches = Integer.parseInt(fields[1]) ;
		if(numOfMatches < 20)
			return 0;
		else if(numOfMatches>=20 && numOfMatches<=30)
			return 1 % numPartitions;
		else
			return 2 % numPartitions;
	}
}
