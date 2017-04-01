package com.ryan.beam.spark;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.StorageLevels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/12 17:04.
 */
public class BeamWordCount implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BeamWordCount.class);


    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        SparkPipelineOptions sparkPipelineOptions = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        sparkPipelineOptions.setSparkMaster("local[4]");
        sparkPipelineOptions.setJobName("sparkWordCount");
        sparkPipelineOptions.setAppName("sparkWordCount");
        sparkPipelineOptions.setStorageLevel(StorageLevels.MEMORY_AND_DISK.toString());
        sparkPipelineOptions.setEnableSparkMetricSinks(true);

        Pipeline pipeline = Pipeline.create(sparkPipelineOptions);

        PCollection<String> collection = pipeline.apply(TextIO.Read.from("file:///e:/test/pending/JF_FTP_RAWLOGUSERBV_003_0001.txt"));

        PCollection<KV<String, Long>> pCollection = collection.apply("ExtractWords", ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] split = c.element().split("##");
                for (String word : split) {
                    if (!word.isEmpty()) {
                        c.output(word);
                    }
                }
            }
        })).apply(Count.<String>perElement());

//        PCollection<String> formatResults = pCollection.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//            @Override
//            public String apply(KV<String, Long> input) {
//                // LOG.info("统计结果为: {} -- {}", input.getKey(), input.getValue());
//
//                return input.getKey() + ": " + input.getValue();
//            }
//        }));

        //.apply(TextIO.Write.to("/test/out/d"));

        pipeline.run().waitUntilFinish();

        System.err.println("耗时：" + (System.currentTimeMillis() - start));


    }
}
