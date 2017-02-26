package com.rc.hadoop.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class DataFrameTest extends BaseTest {

	public static final String SOURCE_FILE = "src/test/resources/SampleTestData/userInfo.txt";

	@Test
	public void dataFrameTest() {
		StructType structType = getSchema();
		JavaRDD<String>  testRDD = this.getJsc().textFile(SOURCE_FILE, 1);
		DataFrame df = getDataFrame(testRDD, structType);
		String[] columns = df.columns();
	}

	public StructType getSchema() {
		StructType structType = null;
		List<StructField> schema = new ArrayList<StructField>();
		schema.add(DataTypes.createStructField("First_name", DataTypes.StringType, true));
		schema.add(DataTypes.createStructField("last_name", DataTypes.StringType, true));
		schema.add(DataTypes.createStructField("city", DataTypes.StringType, true));
		schema.add(DataTypes.createStructField("country", DataTypes.StringType, true));
		structType = DataTypes.createStructType(schema);
		return structType;
	}
	
	public DataFrame getDataFrame(JavaRDD<String> testRDD, StructType structType) {
		JavaRDD<Row> rowRdd = testRDD.map(line -> line.split("\\|",-1))
									.map(row -> RowFactory.create(row[0],row[1],row[2],row[3],row[4],row[5],row[6]));
		return this.getSqlContext().createDataFrame(rowRdd, structType);
	}
	
	public static String getJsonConfig(String filePath) throws IOException {
		String jsonConfig = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(filePath)));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			jsonConfig = sb.toString();
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
		return jsonConfig;
	}
	
}
