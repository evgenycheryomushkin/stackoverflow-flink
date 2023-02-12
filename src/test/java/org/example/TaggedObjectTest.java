package org.example;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TaggedObjectTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(2).setNumberTaskManagers(1).build());

    @Test
    public void testPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(100);
        env.setParallelism(2);
        DataStreamSource<TaggedObject> inputData = env.addSource(new TaggedObjectFunction());

        DataStreamSource<Control> controlStream = env.addSource(new ControlFunction());

        MapStateDescriptor<String, Control> ruleStateDescriptor = new MapStateDescriptor<>(
                "ControlBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Control>() {}));

        BroadcastStream<Control> controlBroadcastStream = controlStream
                .broadcast(ruleStateDescriptor);

        inputData
                .keyBy(TaggedObject::getAddress)
                .connect(controlBroadcastStream)
                .process(new TaggedObjectBroadcastFunction())
                .addSink(new CollectSink());
        env.execute("Test");

        assertEquals(3, CollectSink.values.size());
    }


    static class ControlFunction implements SourceFunction<Control> {

        @Override
        public void run(SourceContext<Control> ctx) throws Exception {
            Thread.sleep(1000L);
            ctx.collect(new Control());
        }

        @Override
        public void cancel() {

        }
    }


    static class TaggedObjectFunction implements SourceFunction<TaggedObject> {

        @Override
        public void run(SourceContext<TaggedObject> ctx) throws Exception {
            ctx.collect(new TaggedObject("Zurich", new ArrayList<>(Arrays.asList("bank"))));
            ctx.collect(new TaggedObject("Dubendorf", new ArrayList<>(Arrays.asList("museum"))));
            ctx.collect(new TaggedObject("Zurich", new ArrayList<>(Arrays.asList("Lake Zurich"))));
            ctx.collect(new TaggedObject("Kloten", new ArrayList<>(Arrays.asList("airport"))));
        }

        @Override
        public void cancel() {

        }
    }

}