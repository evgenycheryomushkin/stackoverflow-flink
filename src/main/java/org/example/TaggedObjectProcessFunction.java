package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class TaggedObjectProcessFunction extends KeyedProcessFunction<String, TaggedObject, TaggedObject> {

    MapStateDescriptor<String, TaggedObject> mapStateDescriptor;

    @Override
    public void processElement(TaggedObject value,
                               KeyedProcessFunction<String, TaggedObject, TaggedObject>.Context ctx,
                               Collector<TaggedObject> out) throws Exception {

        MapState<String, TaggedObject> state = getRuntimeContext().getMapState(mapStateDescriptor);
        log.warn("process(" + mapStateDescriptor + ")(" + value + "):" + System.currentTimeMillis());
//        if (value.isControl) {
//            for (String key : state.keys()) {
//                out.collect(state.get(key));
//            }
//            return;
//        }
        if (!state.contains(value.getAddress())) {
            state.put(value.getAddress(), value);
        } else {
            state.get(value.getAddress()).getOrganizations()
                    .addAll(value.getOrganizations());
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapStateDescriptor = new MapStateDescriptor<String, TaggedObject>(
                "map",
                String.class,
                TaggedObject.class
        );
    }
}
