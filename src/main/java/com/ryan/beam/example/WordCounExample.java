package com.ryan.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/14 10:07.
 */
public class WordCounExample implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(WordCounExample.class);

    private transient Pipeline pipeline = null;


    public WordCounExample() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("wordcount");

        pipeline = Pipeline.create(options);
    }

    /**
     *
     */
    public void transform() {
        PCollection<String> collection = pipeline.apply(TextIO.Read.from("file:///e:/test/pending/JF_FTP_RAWLOGUSERBV_003_0001.txt"));

        PCollection<String> extractWords = collection.apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                for (String word : c.element().split("[^a-zA-Z']+")) {
                    if (!word.isEmpty()) {
                        c.output(word);
                    }
                }
            }
        }));


        PCollection<KV<String, Long>> pCollection = extractWords.apply(Count.<String>perElement());

        PCollection<String> formatResults = pCollection.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
            @Override
            public String apply(KV<String, Long> input) {
                return input.getKey() + ": " + input.getValue();
            }
        }));

        formatResults.apply(TextIO.Write.to("hdfs://test/usr"));
    }

    /**
     * 开始运行
     */
    public void run(){
        pipeline.run();
    }


    public static void main(String[] args) {
        WordCounExample wordCounExample = new WordCounExample();
        wordCounExample.transform();

        wordCounExample.run();

        LOG.info("test");

    }


}
