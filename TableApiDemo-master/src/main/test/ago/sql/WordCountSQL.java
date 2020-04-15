package ago.sql;


import ago.model.WC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;



public class WordCountSQL {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("spf", 1),
                new WC("Hello", 1));

        tEnv.registerDataSet("WordCount", input, "word, counts");

        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(counts) as counts FROM WordCount GROUP BY word");

        DataSet<WC> result = tEnv.toDataSet(table, WC.class);

        result.print();
    }
}
