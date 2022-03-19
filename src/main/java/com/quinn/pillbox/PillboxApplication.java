package com.quinn.pillbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

// exclude DataSourceAutoConfiguration.class, 在資料庫連線設定完成前都須排除
@SpringBootApplication(exclude= {DataSourceAutoConfiguration.class})
public class PillboxApplication {

	public static void main(String[] args) {
		SpringApplication.run(PillboxApplication.class, args);
	}

}
