package pcl.myflink.sql.cep;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketWindowWordCount {
	protected static final Logger logger = LoggerFactory.getLogger("worldcountloglogloglog");
    public static void main(String[] args) throws Exception {
    	
    	System.out.println("开始执行world count！-----@@@@@@@@@@@@@@@@@@@@@@@@@@@@----------------------------------------");
    	logger.info("logger开始执行world count！-----@@@@@@@@@@@@@@@@@@@@@@@@@@@@----------------------------------------");
        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.getTableEnvironment(env);
        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        env.setRestartStrategy(RestartStrategies.noRestart());
        
        StateBackend sb=new FsStateBackend("file:///E:/checkpoint");

        	env.setStateBackend(sb);       

        	env.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);

        	env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        	env.getCheckpointConfig().setCheckpointTimeout(180000);

        	env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        /*//checkpoint配置
        // start a checkpoint every 1000 ms
        env.enableCheckpointing(3000);
        //set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 指定checkpoint执行的超时时间(单位milliseconds)，超时没完成就会被abort掉
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        // 用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，
        // （checkpoint执行时间超过了设置的时间间隔，执行结束后会立刻执行下一个checkpoint，如果经常超时，
        //系统会不间断的执行checkpoint，影响程序的性能。）
        // 当指定这个参数时，maxConcurrentCheckpoints的值为1
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //指定运行中的checkpoint最多可以有多少个
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // // 任务流取消和故障应保留检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //用于指定在checkpoint发生异常的时候，是否应该fail该task，默认为true，
        // 如果设置为false，则task会拒绝checkpoint然后继续运行
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
        
        Table sqlQuery = tableEnv.sqlQuery("SELECT TUMBLE_END(proctime, INTERVAL '10' SECOND) as processtime,"
        		+ "userId,count(*) as pvcount "
        		+ "FROM Users "
        		+ "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), userId");
*/

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text

                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })

                .keyBy("word")
               // .timeWindow(Time.seconds(5))

                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print();

        env.execute("Socket Window WordCount2019/5/27");
    }

    // ------------------------------------------------------------------------

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
