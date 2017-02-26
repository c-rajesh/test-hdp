package com.rc.hadoop.test;

import static org.junit.Assert.fail;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BaseTest {

	private static SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkTest");
	private static JavaSparkContext jsc = null;
	private static SQLContext sqlContext = null;
	private static HiveContext hiveCtx = null;
	private static boolean initialized = false;
	
	public static JavaSparkContext getJsc() {
		return jsc;
	}
	
	public static SQLContext getSqlContext() {
		return sqlContext;
	}
	
	public static HiveContext getHiveCtx() {
		return hiveCtx;
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		initialized = (jsc != null);
		
		if (!initialized) {
			jsc = new JavaSparkContext(sparkConf);
			sqlContext = new SQLContext(jsc.sc());
			hiveCtx = new HiveContext(jsc.sc());
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		jsc.close();
		jsc = null;
		sqlContext = null;
		hiveCtx = null;
	}
	


}
