package ago.table;


import ago.model.WC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


public class WordCountTable {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("spf", 1),
                new WC("spf", 1),
                new WC("Hello", 1));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, count.sum as count")
                .filter("count = 2");

        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

        result.print();
    }
}
