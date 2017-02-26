package com.rc.hadoop.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

public class TestNGBaseTest {
	
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
  public void beforeClass() {
		initialized = (jsc != null);
		
		if (!initialized) {
			jsc = new JavaSparkContext(sparkConf);
			sqlContext = new SQLContext(jsc.sc());
			hiveCtx = new HiveContext(jsc.sc());
		}
  }

  @AfterClass
  public void afterClass() {
		jsc.close();
		jsc = null;
		sqlContext = null;
		hiveCtx = null;
  }

}
