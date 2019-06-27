package com.me.flink.kafka;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {

  private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
    see.setParallelism(4);

    DataStreamSource<Integer> datasource =
        see.fromCollection(IntStream.range(1, 12 + 1).boxed().collect(Collectors.toList()));

    KeyedStream<Integer, Integer> keyed =
        datasource.keyBy(new KeySelector<Integer, Integer>() {

          @Override
          public Integer getKey(Integer value) throws Exception {
            return value & 7;
          }
        });

    keyed.map(new MapFunction<Integer, String>() {

      @Override
      public String map(Integer value) throws Exception {
        return RandomStringUtils.randomAlphabetic(4) + "-" + value;
      }

    }).addSink(
        new FlinkKafkaProducer<String>("homedocker:9092", "hello", new SimpleStringSchema()));

    // mapped.print();

    see.execute();
  }

}
