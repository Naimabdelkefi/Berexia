package com.zhuinden.sparkexperiment;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.zhuinden.sparkexperiment.FileStorageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableConfigurationProperties({
    FileStorageProperties.class
})
public class SparkExperimentApplication {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils.exe");
		SpringApplication.run(SparkExperimentApplication.class, args);
	}
}
