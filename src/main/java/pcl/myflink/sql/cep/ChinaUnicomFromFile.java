package pcl.myflink.sql.cep;

import java.sql.Timestamp;

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

public class ChinaUnicomFromFile {

		protected static final Logger logger = LoggerFactory.getLogger(ChinaUnicomFromFile.class);

		public static void main(String[] arg) throws Exception {
			try{

				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
				StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	        
	        DataStream<String> stream = env.readTextFile
	        		//("C:///Users/Administrator/Desktop/KafkaTest/cep/ChinaUnicom3.csv");
	        		//("C:///Users/Administrator/Desktop/KafkaTest/cep/ChinaUnicom.csv");
	        		//("/home/pengchenglin/ChinaUnicom.csv");
	        		("hdfs://172.16.44.28:8020/flink/mayu/");
			stream.print();
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
		        				" MATCH_RECOGNIZE ( "+
		        				"     PARTITION BY phone "+
		        				"     ORDER BY rowtime "+
		        				"     MEASURES "+
		        				"          first(eventtime) AS first_time "+
		        				"        ,  LAST(eventtime) AS LAST_time "+
		        				"        ,  FIRST(USER_IN.eventtime) AS start_in_time "+
		        				"        , LAST(USER_IN.eventtime) AS end_in_time"+
		        				"		 , LAST(USER_IN.eventtime)-FIRST(USER_IN.eventtime) as stay_time "+
		        				"     ONE ROW PER MATCH "+
		        				"     AFTER MATCH SKIP PAST LAST ROW "+
		        				"     PATTERN (USER_IN +? USER_STADY   )  "+
		        				"     WITHIN INTERVAL '7' SECOND  "+
		        				"     DEFINE "+
		        				"		  USER_STADY AS "+
		        				"			 LAST(USER_IN.eventtime)"+
		        				"				-FIRST(USER_IN.eventtime)>3000,"+
		        				"         USER_IN AS "+
		        				"             USER_IN.lac = 123 "+
		        				" AND USER_IN.cell = 456 "+
		        				//"	      ,USER_OUT AS "+
		        				//"              USER_OUT.lac <>123  "+
		        				"     ) MR "+
		        				""
		                );

		           DataStream<Row> appendStream2 =tableEnv.toAppendStream(tb2, Row.class);
		           appendStream2.print();
		         /*  @SuppressWarnings("rawtypes")
				TypeInformation[] types2 ={Types.STRING,Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP,Types.STRING};
				   DataStream<Row> appendStream3 = appendStream2.map(new MapFunction<Row, Row>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Row map(Row value) throws Exception {
							// TODO Auto-generated method stub
							Row row = new Row(4);
						for(int i=0 ;i<4;i++){
							switch(i){
								case 0:
								{
									row.setField(i,value.getField(i));
									break;
								}
								case 1:
								case 2:
								{
									row.setField(i,new Timestamp((long) value.getField(i)));
									break;
								}
								case 3:{
									row.setField(i,value.getField(i)+"毫秒");
									break;
								}
							}
						}
							return row;
						}
				        }).returns(new RowTypeInfo(types2) );
				   appendStream3.print().name("ChinaUnicom");*/

		            //System.out.println("schema is:");
		           // tb2.printSchema();

	        env.execute("ChinaUnicomFromFile300W");  
			}catch(Exception e){
				System.out.println("出错了："+e.toString());
			}

	    }
}
