package com.ryan.beam.spark.stream;

import kafka.serializer.StringDecoder;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 * User:        Ryan
 * Date:        2017/4/1
 * Email:       liuwei412552703@163.com
 * Version      V1.0
 * Discription:
 */
public class BeamStreamingSocketWC implements Serializable {

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
         * Spark  Streamging Kafka
         */
        SparkRunnerStreamingContextFactory streamContext = new SparkRunnerStreamingContextFactory(pipeline, sparkPipelineOptions);
        JavaStreamingContext javaStreamingContext = streamContext.create();

        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("", "");

        Set<String> topics = new HashSet<>();
        topics.add("test");

        JavaPairInputDStream<StringDecoder, StringDecoder> dStream = KafkaUtils.createDirectStream(javaStreamingContext, StringDecoder.class, StringDecoder.class, StringDecoder.class, StringDecoder.class, kafkaProps, topics);

        dStream.foreachRDD(new VoidFunction<JavaPairRDD<StringDecoder, StringDecoder>>() {
            @Override
            public void call(JavaPairRDD<StringDecoder, StringDecoder> stringDecoderStringDecoderJavaPairRDD) throws Exception {

            }
        });


    }
}
