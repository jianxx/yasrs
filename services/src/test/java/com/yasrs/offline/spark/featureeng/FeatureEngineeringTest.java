package com.yasrs.offline.spark.featureeng;

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
        spark = SparkSession.builder().appName("featureEngineering").getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        spark.stop();
    }

    public void readMovies() throws Exception {
        Dataset<Row> movieSamples = spark.read().format("csv").option("header", "true")
                .load("examples/src/main/resources/people.csv");

    }

}
