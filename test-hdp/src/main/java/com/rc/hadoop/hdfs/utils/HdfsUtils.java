package com.rc.hadoop.hdfs.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;


public class HdfsUtils {
	
	public static String readHdfsFile(FileSystem fs, Path path) 
									throws Exception {
		  BufferedReader br = null;
		  String line = null;
		  StringBuilder sb = new StringBuilder();
		  try {
			  if (fs.exists(path)) {
				  if (fs.isFile(path)) {
					  br = new BufferedReader(new InputStreamReader(fs.open(path)));
					  while ((line=br.readLine()) != null) {
						  sb.append(line.trim());
					  }
				  } else {
					  throw new InvalidPathException(path.toString(), "is a directory. Please provide full path");
				  }
			  } else {
				  throw new InvalidPathException(path.toString(), " Invalid file path");
			  }
		  } catch(InvalidPathException e) {
			  throw e;
		  } catch(Exception e) {
			  throw e;
		  } finally {
			  if (br != null) {
				  try {
					  br.close();
				  } catch(Exception e) {
					  throw e;
				  }
			  }
		  }
		  return sb.toString();
	}
	
	public static boolean deleteFromHdfs(FileSystem fs, Path path) 
			throws Exception {
		try {
			if (fs.exists(path)) {
				if (fs.isDirectory(path)) {
					FileStatus[] status = fs.listStatus(path);
					for (FileStatus flSts: status) {
						fs.delete(flSts.getPath(), true);
					}
				} else {
					fs.delete(path, true);
				}
			} else {
				throw new InvalidPathException(path.toString(), " Invalid file path");
			}
		} catch(InvalidPathException e) {
			  throw e;
		} catch(Exception e) {
			e.printStackTrace();
			  throw e;
		}
		
		return true;
	}
	
	public static void archiveFile(FileSystem fs, String srcLocation, 
					String targetLocation, Configuration conf, boolean deleteSource) throws IOException {
		try {
			if (fs.exists(new Path(srcLocation.trim()))) {
				if (fs.isFile(new Path(srcLocation.trim()))) {
					String[] dirAndFile = getDirAndFile(srcLocation.trim());
					String targetDir = getFileNameAppendedPath(targetLocation, dirAndFile[1]);
					FileUtil.copy(fs, new Path(srcLocation.trim()), fs, new Path(targetLocation.trim()), deleteSource, conf);
				} else if (fs.isDirectory(new Path(srcLocation.trim()))){
					FileStatus[] status = fs.listStatus(new Path(srcLocation.trim()));
					for (FileStatus flSts: status) {
						if (flSts.isFile()) {
							String fileName = getFileNameFromPath(flSts.getPath().toString());
							String SourceFile = getFileNameAppendedPath(srcLocation.trim(), fileName);
							String targetDir = getFileNameAppendedPath(targetLocation, fileName);
							FileUtil.copy(fs, new Path(SourceFile.trim()), fs, new Path(targetLocation.trim()), deleteSource, conf);
						}
					}
				}
			}
 
		} catch(IOException e) {
			e.printStackTrace();
			  throw e;
		}
	}

	public static String getFileNameFromPath(String filePath) {
		String[] dirAndFile = getDirAndFile(filePath);
		return dirAndFile[1];
	}
	
	public static String[] getDirAndFile(String filePath) {
		String[] dirAndFile = new String[2];
		dirAndFile[0] = filePath.substring(0, (filePath.lastIndexOf("/") + 1));
		dirAndFile[1] = filePath.substring(filePath.lastIndexOf("/") + 1);
		return dirAndFile;
	}
	
	public static String getFileNameAppendedPath(String targetDir, String fileName) {
		StringBuilder sb = new StringBuilder();
		if (targetDir.endsWith("/")) {
			sb.append(targetDir.trim()).append(fileName.trim());
		} else {
			sb.append(targetDir.trim()).append("/").append(fileName.trim());
		}
		return sb.toString();
	}
}
