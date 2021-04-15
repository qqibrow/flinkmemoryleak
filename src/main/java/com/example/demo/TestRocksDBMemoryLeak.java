package com.example.demo;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestRocksDBMemoryLeak {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(300000L, CheckpointingMode.EXACTLY_ONCE);


    RocksDBStateBackend stateBackend = new RocksDBStateBackend(args[0], true);

    stateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);

    DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
    optionsFactory.setBlockCacheSize("4096m");

    stateBackend.setRocksDBOptions(optionsFactory);

    env.setStateBackend(stateBackend);


    DataStream<TestEvent> dataStreamSource = env.addSource(new RichParallelSourceFunction<TestEvent>() {
      private transient long lastStartTime;

      @Override
      public void open(Configuration parameters) {
        lastStartTime = System.currentTimeMillis();
      }


      @Override
      public void run(SourceContext<TestEvent> ctx) throws Exception {
        while (true) {
            if (getRuntimeContext().getIndexOfThisSubtask() == 1 && System.currentTimeMillis() - lastStartTime >= 5 * 60 * 1000) {
              lastStartTime = System.currentTimeMillis();
              throw new RuntimeException("Time to trigger exception: " + lastStartTime);
            }

          TestEvent event = new TestEvent();
          event.setGuid(UUID.randomUUID().toString());
          event.setTimestamp(ZonedDateTime.now().toEpochSecond());
          event.setData(new byte[1024 * 1024]);

          ctx.collectWithTimestamp(event, event.getTimestamp());
          ctx.emitWatermark(new Watermark(event.getTimestamp() - 1));
          Thread.sleep(10);
        }
      }

      @Override
      public void cancel() {
      }
    }).setParallelism(5)
        .slotSharingGroup("others");


    SingleOutputStreamOperator<TestSession> sessionDataStream =
        dataStreamSource
            .keyBy("guid")
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .allowedLateness(Time.minutes(3))
            .aggregate(new SessionAgg())
            .name("Session Window Operator")
            .setParallelism(3);

    sessionDataStream.addSink(new DiscardingSink<>())
        .name("Sink")
        .setParallelism(3);


    env.execute("Test job");
  }
}
