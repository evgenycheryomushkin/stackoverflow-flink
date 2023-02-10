package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class CollectSink implements SinkFunction<TaggedObject> {
    public static final List<TaggedObject> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(TaggedObject value, SinkFunction.Context context) throws Exception {
        log.warn("collect(" + value + "):" + System.currentTimeMillis());
        values.add(value);
    }

}
