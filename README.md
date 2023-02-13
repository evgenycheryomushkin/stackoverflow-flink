# flink example

Code related to question: https://stackoverflow.com/questions/75406746/flink-pipeline-with-firing-results-on-event

I have stream of objects with address and list of organizations:
```java
@Data
class TaggedObject {
    String address;
    List<String> organizations;
}
```
Is there a way to do the following using apache flink:
1. Merge organization lists for objects with same address
2. Send all results to Sink when some event occurs. E.g. when user sends control message to a kafka topic or another DataSource
3. Keep all objects for future accumulations 

I tried using global window and custom trigger:
```java
public class MyTrigger extends Trigger<TaggedObject, GlobalWindow> {
    @Override
    public TriggerResult onElement(TaggedObject element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        if (element instanceof Control) return TriggerResult.FIRE;
        else return TriggerResult.CONTINUE;
    }

```
But it seems to give only Control element as a result. Other elements were ignored.
