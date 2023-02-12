package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class TaggedObjectBroadcastFunction extends KeyedBroadcastProcessFunction<String, TaggedObject, Control, TaggedObject> {

    ValueStateDescriptor<TaggedObject> valueStateDescriptor;

    @Override
    public void processElement(TaggedObject value, KeyedBroadcastProcessFunction<String, TaggedObject, Control, TaggedObject>.ReadOnlyContext ctx, Collector<TaggedObject> out) throws Exception {
        ValueState<TaggedObject> state = getRuntimeContext().getState(valueStateDescriptor);
        log.warn("process ("+value+")("+state+"):"+System.currentTimeMillis());
        if (state.value() == null) state.update(value);
        else state.value().getOrganizations().addAll(value.getOrganizations());
    }

    @Override
    public void processBroadcastElement(Control value, KeyedBroadcastProcessFunction<String, TaggedObject, Control, TaggedObject>.Context ctx, Collector<TaggedObject> out) throws Exception {
        log.warn("broadcast ("+value+")");
        ctx.applyToKeyedState(valueStateDescriptor, new KeyedStateFunction<String, ValueState<TaggedObject>>() {
            @Override
            public void process(String key, ValueState<TaggedObject> state) throws Exception {
                // todo
            }
        });
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        valueStateDescriptor = new ValueStateDescriptor<TaggedObject>(
                "value",
                TaggedObject.class
        );
    }
}
