package com.zhuinden.sparkexperiment;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@RequestMapping("api")
@Controller
public class ApiController {

	public SparkSession skSession() {
		final SparkSession sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[5]")
				.getOrCreate();
		return sparkSession;
    }

	static String filename="";
	@RequestMapping(value = "/send", method = RequestMethod.GET)
	@ResponseBody
	public String getFileName(@RequestParam("fn") String fn) {
		filename=fn;
		return ("file path changed to: ./Upload/"+filename);
	}
	public Dataset<org.apache.spark.sql.Row> csvdata() {

		final DataFrameReader dataFrameReader = skSession().read();
		dataFrameReader.option("header", "true");
		dataFrameReader.option("delimiter", ";");
		final Dataset<org.apache.spark.sql.Row> csvDataFrame = dataFrameReader.csv("./uploads/"+filename);
		return csvDataFrame;
	}

	@ResponseBody
	@RequestMapping("/header")
	public Object header() {
		csvdata().printSchema();
		return csvdata().toDF().toJSON().first();
	}
	
	static String q="";
	@RequestMapping(value = "/sendquerry", method = RequestMethod.GET)
	@ResponseBody
	public void getQuery(@RequestParam("q") String querry) {
		q=querry;
		System.out.println(q);
	}


	@ResponseBody
	@RequestMapping("/querry")
	public Object  querry() {
		
		csvdata().createOrReplaceTempView("CSV_OCCUPANCY");
		final Dataset<org.apache.spark.sql.Row> roomOccupancyData = skSession().
				sql("SELECT * FROM CSV_OCCUPANCY "+q);
		// Print Schema to see column names, types and other metadata
		roomOccupancyData.show();
		org.apache.spark.sql.Row[] rows = (org.apache.spark.sql.Row[]) roomOccupancyData.take((int) roomOccupancyData.count());
		List<String> topX = new ArrayList<>();
		 for (org.apache.spark.sql.Row row : rows) {
	            topX.add(row.toString());
	        }
		 return roomOccupancyData.toDF().toJSON().collect();
		// return roomOccupancyData.toDF().toJSON().show();.takeAsList((int) roomOccupancyData.count());
		 //return roomOccupancyData.toDF().toJSON().take((int) roomOccupancyData.count());
		//return topX;
	}

}
