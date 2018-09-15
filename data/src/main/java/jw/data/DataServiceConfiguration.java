
package jw.data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:jwdata.properties")
@ConfigurationProperties
public class DataServiceConfiguration {
	private String dataFsRoot;

	public String getDataFsRoot() {
		return dataFsRoot;
	}

	public void setDataFsRoot(String dataFsRoot) {
		this.dataFsRoot = dataFsRoot;
	}

}
