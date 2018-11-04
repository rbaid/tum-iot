package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch2.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import org.apache.flink.api.common.functions.RuntimeContext;

public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //  StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
/*
    DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

    KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
      .keyBy(new KeySelector<WikipediaEditEvent, String>() {
        @Override
        public String getKey(WikipediaEditEvent event) {
          return event.getUser();
        }
      });

    DataStream<Tuple2<String, Long>> result = keyedEdits
      .timeWindow(Time.seconds(5))
      .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
          acc.f0 = event.getUser();
          acc.f1 += event.getByteDiff();
          return acc;
        }
      });

    result
    .map(new MapFunction<Tuple2<String,Long>, String>() {
        @Override
        public String map(Tuple2<String, Long> tuple) {
            return tuple.toString();
        }
    });
    */
   // .addSink(new FlinkKafkaProducer08<>("104.155.172.153:9092", "wiki-result", new SimpleStringSchema()));;

System.out.println("*****************Getting data from kafka***********************");

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "35.192.215.170:9094");
// only required for Kafka 0.8
properties.setProperty("zookeeper.connect", "35.192.215.170:2181");
properties.setProperty("group.id", "my-group");
//ParameterTool parameterTool = ParameterTool.fromArgs(args);
FlinkKafkaConsumer08<String> myConsumer =
    new FlinkKafkaConsumer08<>("my-topic", new SimpleStringSchema(), properties);
    myConsumer.setStartFromLatest(); 
//myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

DataStream<String> stream = env.addSource(myConsumer);
//env.execute();
stream.print();
//stream.writeAsText("stream.txt");
System.out.print(stream);
System.out.print(stream.print());

DataStream<String> input = stream;

Map<String, String> config = new HashMap<>();
config.put("cluster.name", "my-cluster-name");
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");

List<InetSocketAddress> transportAddresses = new ArrayList<>();
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("35.192.215.170"), 9300));
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

input.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
    public IndexRequest createIndexRequest(String element) {
        Map<String, String> json = new HashMap<>();
        json.put("data", element);
    
        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
    
    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}));
   
 env.execute();

  }
}