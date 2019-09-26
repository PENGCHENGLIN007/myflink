package pcl.myflink.checkpoint;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnceTest {
	private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceTest.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		logger.info("-----start execute exactlyoncetest----");
		String isRestart = args[0];
		//init execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
		//		.useBlinkPlanner().inStreamingMode().build();
		//StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env,bsSettings);
		if(isRestart.equals("0")){
			env.setRestartStrategy(RestartStrategies.noRestart());
		}else{
	        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
	        		  3, // 尝试重启次数
	        		  Time.of(30, TimeUnit.SECONDS) // 延迟时间间隔
	        		));
		}

        //StateBackend sb=new FsStateBackend("file:///E:/checkpoint");
        StateBackend sb=new FsStateBackend("file:///home/pengchenglin/flinkresult/checkpoint");
        env.setStateBackend(sb);
        
		env.enableCheckpointing(20000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(10000);
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
		
		DataStream<String> ds = env.socketTextStream("172.16.44.28",9001,"\n" );
		DataStream<String> mapds = ds.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<String> out)
					throws Exception {
				 for (String word : value.split("\\s")) {
	                    out.collect(word);
	                }
			}
		}).setParallelism(2);
		mapds.writeAsText("/home/pengchenglin/flinkresult/ExactlyOnceTest", WriteMode.OVERWRITE);
		env.execute("ExactlyOnceTest");
		
	}

}
