package com.ryan.beam.spark.stream;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * <pre>
 * User:        Ryan
 * Date:        2017/4/1
 * Email:       liuwei412552703@163.com
 * Version      V1.0
 * Discription:
 */
public class BeamSparkStreamingWC implements Serializable {

    public static void main(String[] args) {
        SparkPipelineOptions sparkPipelineOptions = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        sparkPipelineOptions.setSparkMaster("local[4]");
        sparkPipelineOptions.setJobName("kafkaWordCount");
        sparkPipelineOptions.setAppName("kafkaWordCount");
        sparkPipelineOptions.setStorageLevel(StorageLevels.MEMORY_AND_DISK.toString());
        sparkPipelineOptions.setEnableSparkMetricSinks(true);
        sparkPipelineOptions.setBatchIntervalMillis(10L);
        sparkPipelineOptions.setMaxRecordsPerBatch(500L);


        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("wordcount");
        options.setOptionsId(1L);

        Pipeline pipeline = Pipeline.create(sparkPipelineOptions);


        SparkRunnerStreamingContextFactory streamContext = new SparkRunnerStreamingContextFactory(pipeline, sparkPipelineOptions);
        JavaStreamingContext streamingContext = streamContext.create();
        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("192.168.1.101", 9000);

        dStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> javaRDD) throws Exception {

            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
