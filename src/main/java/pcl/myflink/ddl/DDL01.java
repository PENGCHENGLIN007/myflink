package pcl.myflink.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DDL01 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// SQL query with a registered table
		// register a table named "Orders"
		tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) "
				+ "WITH ("
				+ " 'connector.type' = 'filesystem', "//               -- required: specify to connector type
                + " 'connector.path' = 'C:///Users/Administrator/Desktop/KafkaTest/cep/ChinaUnicom.csv'"
                + ")");  //-- required: path to a file or directory)");
		// run a SQL query on the Table and retrieve the result as a new Table
		Table result = tableEnv.sqlQuery(
		  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

		// SQL update with a registered table
		// register a TableSink
		tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ()");
		// run a SQL update query on the Table and emit the result to the TableSink
		tableEnv.sqlUpdate(
		  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
	}

}
