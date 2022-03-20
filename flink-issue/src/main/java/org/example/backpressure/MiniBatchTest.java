package org.example.backpressure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class MiniBatchTest {

    private static String CREATE_ORDER_SQL = "\n" +
            "CREATE TABLE `orders` (\n" +
            "    order_id    STRING,\n" +
            "    price       INT,\n" +
            "    currency    STRING,\n" +
            "    order_time  BIGINT\n" +
            ")\n" +
            "COMMENT '' WITH (\n" +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
            "  'scan.topic-partition-discovery.interval' = '10000',\n" +
            "  'connector' = 'kafka',\n" +
            "  'format' = 'json',\n" +
            "  'topic' = 'test'\n" +
            ")";

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration).setParallelism(2);

        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        env.enableCheckpointing(30000);
        env.setStateBackend(new FsStateBackend("file:///Users/Desktop/ck"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(1));

        Configuration tableConf = tEnv.getConfig().getConfiguration();
        tableConf.setString("table.exec.mini-batch.enabled", "true");
        tableConf.setString("table.exec.mini-batch.allow-latency", "10s");
        tableConf.setString("table.exec.mini-batch.size", "5000");
        tableConf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

//        tEnv.getConfig().getConfiguration().set(
//                key("table.exec.mini-batch.enabled")
//                .stringType()
//                .noDefaultValue()
//                .withDescription("mini batch enabled"), "true");
//
//        tEnv.getConfig().getConfiguration().set(
//                key("table.exec.mini-batch.allow-latency")
//                        .stringType()
//                        .noDefaultValue()
//                        .withDescription("mini batch latency"), "5s");
//        tEnv.getConfig().getConfiguration().set(
//                key("table.exec.mini-batch.size")
//                        .stringType()
//                        .noDefaultValue()
//                        .withDescription("mini batch size"), "5000");

        //
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");


        tEnv.executeSql(CREATE_ORDER_SQL);

        Table table = tEnv.sqlQuery("SELECT count(order_id), currency FROM orders GROUP BY currency");

        DataStream<Tuple2<Boolean, Row>> src = tEnv.toRetractStream(table, Row.class);

        src.print();

        try {
            env.execute("WC");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
