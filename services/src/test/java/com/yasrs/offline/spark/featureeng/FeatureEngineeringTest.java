package com.yasrs.offline.spark.featureeng;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FeatureEngineeringTest {

    private SparkSession spark = null;

    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local");
        spark = SparkSession.builder().appName("featureEngineering").config(conf).getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        spark.stop();
    }

    @Test
    public void readMovies() throws Exception {
        Dataset<Row> movieSamples = spark.read().format("csv").option("header", "true")
                .load("data/sampledata/movies.csv");

    }

}
