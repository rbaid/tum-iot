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
import org.apache.flink.streaming.connectors.elasticsearch.*;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import java.util.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import org.apache.flink.api.common.functions.RuntimeContext;

public class WikipediaAnalysis {

public static void main(String[] args) throws Exception {

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

System.out.println("*****************Getting data from kafka***********************");

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "35.226.75.212:9094");
// only required for Kafka 0.8
properties.setProperty("zookeeper.connect", "35.226.75.212:2181");
properties.setProperty("group.id", "my-group");
//ParameterTool parameterTool = ParameterTool.fromArgs(args);
FlinkKafkaConsumer08<String> myConsumer =
    new FlinkKafkaConsumer08<>("my-topic", new SimpleStringSchema(), properties);
    myConsumer.setStartFromLatest(); 
//myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

DataStream<String> stream = env.addSource(myConsumer);
stream.print();
//stream.writeAsText("stream.txt");
//System.out.print(stream);
//System.out.print(stream.print());
//env.execute();
writeElastic(stream);
env.execute("Viper Flink!");
  }

   public static void writeElastic(DataStream<String> input) {

     Map<String, String> config = new HashMap<>();
 
     // This instructs the sink to emit after every element, otherwise they would be buffered
     config.put("bulk.flush.max.actions", "1");
     config.put("cluster.name", "docker-cluster");
 
     try {
         System.out.print("in writeElastic..");
         // Add elasticsearch hosts on startup
         List<InetSocketAddress> transports = new ArrayList<>();
         transports.add(new InetSocketAddress("35.192.111.192", 9300)); // port is 9300 not 9200 for ES TransportClient
 //transports.add(new InetSocketAddress("elasticsearch", 9300)); 
         ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {
             public IndexRequest createIndexRequest(String element) {
               //  String[] logContent = element.trim().split("\t");
                 Map<String, String> esJson = new HashMap<>();
                 esJson.put("WOrkingIP", element);
                // esJson.put("info", logContent[1]);
 
                 return Requests
                         .indexRequest()
                         .index("viper-test")
                         .type("viper-log")
                         .source(esJson);
             }
 
             @Override
             public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                 indexer.add(createIndexRequest(element));
             }
         };
 
         ElasticsearchSink esSink = new ElasticsearchSink(config, transports, indexLog);
         input.addSink(esSink);
     } catch (Exception e) {
         System.out.println(e);
     }
 }
 
}