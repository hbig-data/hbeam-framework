package com.ryan.beam.spark;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/12 17:04.
 */
public class SparkTextFile implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkTextFile.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();


        SparkRunner sparkRunner = SparkRunner.fromOptions(options);

        Pipeline p = Pipeline.create(options);

        PCollection<String> pCollection = p.apply(TextIO.Read.from("file:///e:/test/pending/JF_FTP_RAWLOGUSERBV_003_0001.txt"));





        sparkRunner.run(p).waitUntilFinish();
    }
}
