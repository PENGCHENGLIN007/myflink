package pcl.myflink.sqlparser.cep;

import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkParser {
	protected static final Logger logger = LoggerFactory.getLogger(FlinkParser.class);
	public static String parse(String flinkSql){
		String result = "true";
		try{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
			StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	        
	        ArrayList<Integer> data = new ArrayList<>();
            data.add(10);
            data.add(15);
            data.add(20);
	        
	        DataStream<String> stream = env.fromElements("test");
	        
	        @SuppressWarnings("rawtypes")
			TypeInformation[] types ={Types.STRING,Types.INT,Types.INT,Types.LONG};
			DataStream<Row> streamaa = stream.map(new MapFunction<String, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Row map(String value) throws Exception {
					// TODO Auto-generated method stub
					String[] str = value.split(",");
					Row row = new Row(4);
					for(int i=0 ;i<str.length;i++){
						switch(i){
						case 0:{
							row.setField(i,(String)str[i]);
							break;
						}
						case 1:
						case 2:{
							row.setField(i,Integer.valueOf(str[i]));
							break;
						}
						case 3:{
							Timestamp tms = Timestamp.valueOf(str[i]);
							row.setField(i,tms.getTime());
							break;
						}
						}
					}
					return row;
				}
		        }).returns(new RowTypeInfo(types) );
			DataStream<Row> ithTimestampsAndWatermarks = streamaa
	                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>(){

						private static final long serialVersionUID = 1L;

						private final long maxOutOfOrderness = 0; // 3.5 seconds

	                    private long currentMaxTimestamp;

	                    @Override
	                    public Watermark getCurrentWatermark() {
	                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	                    }

	                	@Override
	                	public long extractTimestamp(Row element, long previousElementTimestamp) {
	                		long timestamp= 0;
	                		timestamp = (long) element.getField(3);
	                		currentMaxTimestamp = timestamp;
	                		return timestamp;
	                	}
	                	
	                });
			
			tableEnv.registerDataStream("fence",ithTimestampsAndWatermarks,
					"phone,lac,cell,eventtime,rowtime.rowtime");
		           
				Table tb2 = tableEnv.sqlQuery(
		        		" SELECT "+
		        				" * "+
		        				" FROM fence "+
		        				""
		                );

		           DataStream<Row> appendStream2 =tableEnv.toAppendStream(tb2, Row.class);
		           appendStream2.print();
			
		}catch(Exception e){
			return e.toString();
		}
		return result;
		
	}

	public static void main(String[] args) {
		FlinkParser.parse("");

	}

}
