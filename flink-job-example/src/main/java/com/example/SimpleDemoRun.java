package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

/**
 * @author lcy
 */
public class SimpleDemoRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new CustomSource());

        stringDataStreamSource.print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<String> {
        protected static Boolean running = true;
        private final Random random = new Random();
        private final String[] outputs = {"flink", "spark", "hadoop", "hbase","java"};

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                ctx.collect(new Tuple2<>(
                        outputs[random.nextInt(outputs.length)],
                        System.currentTimeMillis()
                ).toString());
                Thread.sleep(3000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
