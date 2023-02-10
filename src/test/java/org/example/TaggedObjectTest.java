package org.example;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TaggedObjectTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(2).setNumberTaskManagers(1).build());

    @Test
    public void testPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(100);
        env.setParallelism(2);
        DataStreamSource<TaggedObject> inputData = env.addSource(new TaggedObjectFunction());

        inputData
                .keyBy(TaggedObject::getAddress)
                .process(new TaggedObjectProcessFunction())
                .addSink(new CollectSink());
        env.execute("Test");

        assertEquals(3, CollectSink.values.size());
    }


    static class TaggedObjectFunction implements SourceFunction<TaggedObject> {

        @Override
        public void run(SourceContext<TaggedObject> ctx) throws Exception {
            ctx.collect(new TaggedObject("Zurich", new ArrayList<>(Arrays.asList("bank")), false));
            ctx.collect(new TaggedObject("Dubendorf", new ArrayList<>(Arrays.asList("museum")), false));
            ctx.collect(new TaggedObject("Zurich", new ArrayList<>(Arrays.asList("Lake Zurich")), false));
            ctx.collect(new TaggedObject("Kloten", new ArrayList<>(Arrays.asList("airport")), false));
            Thread.sleep(1000L);
            ctx.collect(new TaggedObject("", new ArrayList<>(), true));
        }

        @Override
        public void cancel() {

        }
    }

}