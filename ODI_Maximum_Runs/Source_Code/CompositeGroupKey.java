import org.apache.hadoop.io.WritableComparable ;
import java.io.DataInput ;
import java.io.DataOutput ;
import java.io.IOException ;

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {

	private String player ;
	private String year ;
	
	public CompositeGroupKey() {
		
	}
	
	public CompositeGroupKey(String player, String year) {
		this.player = player ;
		this.year = year ;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.player = in.readUTF() ;
		this.year = in.readUTF();
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(player) ;
		out.writeUTF(year) ;
	}
	
	@Override
	public int compareTo(CompositeGroupKey newRecord) {
		int compare = player.compareTo(newRecord.player) ;
		return compare == 0 ? year.compareTo(newRecord.year) : compare ;
	}
	
	
	@Override
	public String toString() {
		return player.toString() + "," + year.toString() ;
	}
	
}
