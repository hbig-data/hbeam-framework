package com.ryan.beam.spark;

import kafka.serializer.StringDecoder;
import org.apache.beam.runners.direct.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 * User:        Ryan
 * Date:        2017/3/6
 * Email:       liuwei412552703@163.com
 * Version      V1.0
 * Discription:
 */
public class BeamKafkaWordCount {


    public static void main(String[] args) {
        SparkPipelineOptions sparkPipelineOptions = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        sparkPipelineOptions.setSparkMaster("local[4]");
        sparkPipelineOptions.setJobName("kafkaWordCount");
        sparkPipelineOptions.setAppName("kafkaWordCount");
        sparkPipelineOptions.setStorageLevel(StorageLevels.MEMORY_AND_DISK.toString());
        sparkPipelineOptions.setEnableSparkMetricSinks(true);
        sparkPipelineOptions.setBatchIntervalMillis(10L);
        sparkPipelineOptions.setMaxRecordsPerBatch(500L);


        Pipeline pipeline = Pipeline.create(sparkPipelineOptions);


        /**
         * Beam Spark Kafka 数据
         * 接受Kafka 数据
         *
         */
        ImmutableMap<String, Object> immutableMap = ImmutableMap.<String, Object>of("receive.buffer.bytes", 1024 * 1024);

        pipeline.apply(KafkaIO.read().withBootstrapServers("192.168.1.102:9092,192.168.1.102:9092")
                .withTopics(ImmutableList.of("topic_a", "topic_b"))
                // above two are required configuration. returns PCollection<KafkaRecord<byte[], byte[]>

                // rest of the settings are optional :

                // set a Coder for Key and Value (note the change to return type)
                .withKeyCoder(BigEndianLongCoder.of()) // PCollection<KafkaRecord<Long, byte[]>
                .withValueCoder(StringUtf8Coder.of())  // PCollection<KafkaRecord<Long, String>

                // you can further customize KafkaConsumer used to read the records by adding more
                // settings for ConsumerConfig. e.g :
                .updateConsumerProperties(immutableMap)

                // custom function for calculating record timestamp (default is processing time)
                .withTimestampFn(new SerializableFunction<KV<Long, String>, Instant>() {
                    @Override
                    public Instant apply(KV<Long, String> input) {

                        return null;
                    }
                })

                // custom function for watermark (default is record timestamp)
                .withWatermarkFn(new SerializableFunction<KV<Long, String>, Instant>() {
                    @Override
                    public Instant apply(KV<Long, String> input) {
                        return null;
                    }
                })

                // finally, if you don't need Kafka metadata, you can drop it
                // PCollection<KV<Long, String>>
                .withoutMetadata())
                // PCollection<String>
                .apply(Values.<String>create());


    }
}
