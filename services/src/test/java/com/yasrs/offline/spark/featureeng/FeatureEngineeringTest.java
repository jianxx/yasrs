package com.yasrs.offline.spark.featureeng;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FeatureEngineeringTest {

    private SparkSession spark = null;
    private FeatureEngineering eng = null;

    private static final String movies = "data/sampledata/movies.csv";
    private Dataset<Row> movieSamples = null;

    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local");
        spark = SparkSession.builder().appName("featureEngineering").config(conf).getOrCreate();
        movieSamples = spark.read().format("csv").option("header", "true").load(movies);
        eng = new FeatureEngineering(spark);
    }

    @After
    public void tearDown() throws Exception {
        spark.stop();
    }

    @Test
    public void readMovies() throws Exception {
        Assert.assertNotNull(movieSamples);
    }

    @Test
    public void oneHotEncoder() throws Exception {
        Dataset<Row> oneHotEncoderSamples = eng.oneHotEncoder(movieSamples);
        Assert.assertNotNull(oneHotEncoderSamples);
        oneHotEncoderSamples.printSchema();
        oneHotEncoderSamples.show(10);
    }

    public void splitAndSaveTrainingData() throws Exception {

    }
}
