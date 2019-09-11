package pcl.myflink.sqlparser.cep;

import java.sql.Timestamp;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

public class WatermarkOutput implements AssignerWithPeriodicWatermarks<Row> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final long maxOutOfOrderness = 100; // 3.5 seconds

    private long currentMaxTimestamp;


    @Override
    public Watermark getCurrentWatermark() {
        // TODO Auto-generated method stub
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

	@Override
	public long extractTimestamp(Row element, long previousElementTimestamp) {
		// TODO Auto-generated method stub
		long timestamp= 0;
		timestamp = ((Timestamp) element.getField(3)).getTime();
		//timestamp = (Long)element.getField(1);
		currentMaxTimestamp = timestamp;
		return timestamp;
	}

}
