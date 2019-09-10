package pcl.myflink.cep;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Kafka
 * @author Administrator
 *
 *Flink的Kafka消费者被称为FlinkKafkaConsumer08（或09Kafka 0.9.0.x等）。它提供对一个或多个Kafka主题的访问。
 *
 *1.主题名称/主题名称列表     list
 *2.DeserializationSchema / KeyedDeserializationSchema用于反序列化来自Kafka的数据
 *3.Kafka消费者的属性。需要以下属性：
 *		bootstrap.servers （以逗号分隔的Kafka经纪人名单） 
 *		zookeeper.connect （逗号分隔的Zookeeper服务器列表）（仅Kafka 0.8需要）
 *		group.id 消费者群组的ID
 *
 */
public class TickerDown {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "172.16.12.127:9092");
		//properties.setProperty("zookeeper.connect", "172.16.44.28:2180,172.16.44.29:2180,172.16.44.30:2180");
		//properties.setProperty("group.id", "pcl01");
		
		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("topic-cep-test04",
				new SimpleStringSchema(Charset.forName("utf8")),properties);
		consumer.setStartFromEarliest();//从最早记录开始
		//consumer.setStartFromLatest();//从最新记录开始
		DataStream<String> stream = env.addSource(consumer);
		stream.print();
		@SuppressWarnings("rawtypes")
		TypeInformation[] types ={Types.STRING,Types.SQL_TIMESTAMP,Types.INT,Types.INT};
		DataStream<Row> streamaa = stream.map(new MapFunction<String, Row>() {
			/**
			 * 
			 */
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
					case 1:{
						String time = str[i].substring(1,str[i].length()-1);
						Timestamp tms = Timestamp.valueOf(time);
						row.setField(i,tms);
						break;
					}
					case 2:
					case 3:{
						row.setField(i,Integer.parseInt(str[i]));
						break;
					}
					}
				}
				return row;
			}
	        }).returns(new RowTypeInfo(types) );
		DataStream<Row> ithTimestampsAndWatermarks = streamaa
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>(){

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
                		timestamp = ((Timestamp) element.getField(1)).getTime();
                		//timestamp = (Long)element.getField(1);
                		currentMaxTimestamp = timestamp;
                		return timestamp;
                	}
                	
                });
			tableEnv.registerDataStream("Ticker",ithTimestampsAndWatermarks,
					"symbol,eventtime,price,tax,rowtime.rowtime");
			Table tb2 = tableEnv.sqlQuery(
	        		" SELECT * "+
	        				" FROM Ticker "+
	        				" MATCH_RECOGNIZE ( "+
	        				"     PARTITION BY symbol "+
	        				"     ORDER BY rowtime "+
	        				"     MEASURES "+
	        				"         START_ROW1彭.eventtime AS start_tstamp, "+
	        				"         LAST(PRICE_DOWN.eventtime) AS bottom_tstamp, "+
	        				"         LAST(PRICE_UP.eventtime) AS end_tstamp "+
	        				"     ONE ROW PER MATCH "+
	        				"     AFTER MATCH SKIP TO LAST PRICE_UP "+
	        				"     PATTERN (START_ROW1彭 PRICE_DOWN+ PRICE_UP)  "+
	        				"     WITHIN INTERVAL '1' minute  "+
	        				"     DEFINE "+
	        				"         PRICE_DOWN AS "+
	        				"             (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW1彭.price) OR "+
	        				"                 PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1), "+
	        				"         PRICE_UP AS "+
	        				"			PRICE_UP.price > LAST(PRICE_DOWN.price, 1) "+
	        				"     ) MR "+
	        				""
	                );

	           DataStream<Row> appendStream =tableEnv.toAppendStream(tb2, Row.class);
	           appendStream.print().name("TickerDown");

	            System.out.println("schema is:");
	            tb2.printSchema();
			
			
		env.execute();
		
	}
}
