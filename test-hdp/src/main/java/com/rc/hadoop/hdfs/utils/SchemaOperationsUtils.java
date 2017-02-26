package com.rc.hadoop.hdfs.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.QueryExecutionException;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

public class SchemaOperationsUtils {

	
	public void processFeed() throws IOException,InvalidPathException, Exception{
		SparkConf sparkConf = new SparkConf().setAppName("");
		sparkConf.set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc.sc());
		HiveContext hiveCtx = new HiveContext(jsc.sc());
		hiveCtx.setConf("hive.exec.dynamic.partition", "true");
		hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		JavaRDD<String> inputData = jsc.textFile("");
		StructType tableSchema = getSchemaFromHive("", hiveCtx);
		String[] schemaFileds = new String[1];
		Broadcast<String[]> structFields = jsc.broadcast(schemaFileds);
		Accumulator[] accumulatorArr = new Accumulator[4];
		for (int i = 0; i < accumulatorArr.length; i++) {
			accumulatorArr[i] = jsc.accumulator(0, "");
		}
		DataFrame rawTableDf = convertSourceToDataFrame(inputData,
				tableSchema,
				structFields,
				hiveCtx,
				accumulatorArr);
		
	}
	
	public static DataFrame convertSourceToDataFrame(JavaRDD<String> sourceRDD,
														StructType rawTableSchema,
														Broadcast<String[]> structFields,
														HiveContext hiveContext,
														Accumulator[] accumulatorArr) {
		JavaRDD<Row> rowRdd = sourceRDD.map(line -> line.split("\\|",-1))
									.map(row -> RowFactory.create(SchemaOperationsUtils.createSourceMappingRowObjects(row, structFields, accumulatorArr)));
		rowRdd.toDebugString();
		return hiveContext.createDataFrame(rowRdd, rawTableSchema);
	}
	
	public static Object[] createSourceMappingRowObjects(String[] rowColumns,
														Broadcast<String[]> structFields,
															Accumulator[] accumulatorArr) throws Exception {
		String[] structField = structFields.value();
		Object[] fieldObjects = new Object[rowColumns.length];
		if (rowColumns.length == structField.length) {
			for (int i = 0; i < structField.length; i++) {
				String dataType = structField[i];
				String columnVal = rowColumns[i];
				String[] precisionArr = null;
				if (-1 != dataType.trim().indexOf("(")) {
					precisionArr = getDecimalTypeAndString(dataType);
					dataType = precisionArr[0];
				}
				try {
					switch (dataType.trim()) {
					case "StringType":
						fieldObjects[i] = columnVal;
						break;
					case "IntegerType":
						fieldObjects[i] = Integer.parseInt(columnVal);
						break;
					case "LongType":
						fieldObjects[i] = Long.parseLong(columnVal);
						break;
					case "FloatType":
						fieldObjects[i] = Float.parseFloat(columnVal);
						break;
					case "DoubleTye":
						fieldObjects[i] = Double.parseDouble(columnVal);
						break;
					case "ByteType":
						fieldObjects[i] = Byte.parseByte(columnVal);
						break;
					case "DecimalType":
						if (null != precisionArr && precisionArr.length == 3) {
							fieldObjects[i] = new BigDecimal(columnVal).setScale(Integer.parseInt(precisionArr[2]));
						} else {
							fieldObjects[i] = new BigDecimal(columnVal);
						}					
						break;
					case "BooleanType":
						if (!columnVal.toLowerCase().trim().equals("false") && !columnVal.toLowerCase().trim().equals("true")) {
							fieldObjects[i] = null;
						} else {
							fieldObjects[i] = Boolean.parseBoolean(columnVal);
						}						 
						break;
					case "ShortType":
						fieldObjects[i] = Short.parseShort(columnVal);
						break;
					case "TimestampType":
						fieldObjects[i] = Timestamp.valueOf(columnVal);
						break;
					case "BinaryType":
						fieldObjects[i] = Binary.fromString(columnVal);
						break;
					default:
						throw new RuntimeException("Unsupported datatype '" + dataType + "' for value '" + columnVal + "'.");
					}
				} catch(Exception e) {
					fieldObjects[i] = null;
					accumulatorArr[i].add(1);
				}
			}
		} else {
			String msg = " columns not matching";
			throw new Exception(msg);
		}
		return fieldObjects;
	}
	
	
	public static StructType getSchemaFromHive(String hiveTableName, HiveContext hiveCtx) throws QueryExecutionException, AnalysisException {
		StructType targetSchema = null;
		try {
			targetSchema = hiveCtx.table(hiveTableName).schema();
		} catch(Exception e) {
			throw e;
		}
		return targetSchema;
	}
	
	public static String[] getDecimalTypeAndString(String dataType) {
		String[] precisionArr = new String[3];
		precisionArr[0] = dataType.substring(0, dataType.indexOf("(")).trim();
		String[] precisionScale = dataType.substring(dataType.indexOf("(") + 1, dataType.lastIndexOf(")")).split(",");
		if (null != precisionScale && "".equals(precisionScale[0].trim())) {
			precisionArr[1] = "999";
		} else {
			precisionArr[1] = precisionArr[0].trim();
		}
		if (null != precisionScale && precisionScale.length > 1) {
			precisionArr[2] = precisionArr[1].trim();
		} else {
			precisionArr[2] = "0";
		}
		return precisionArr;
	}
}
