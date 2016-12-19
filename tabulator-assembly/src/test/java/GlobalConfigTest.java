import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.hepek.tabulator.Constants;
import net.hepek.tabulator.DataSourceConfiguration;
import net.hepek.tabulator.Util;

public class GlobalConfigTest {

	@Test(expected = IllegalStateException.class)
	public void test_no_config_file() {
		Util.getGloballyConfiguredDataSources();
	}
	
	@Test(expected = IllegalStateException.class)
	public void test_no_config_file_set_wrong() {
		System.setProperty(Constants.CONFIG_FILE_PATH_PROP_NAME, "abc");
		Util.getGloballyConfiguredDataSources();
	}
	
	@Test
	public void test_classpath_config() {
		System.setProperty(Constants.CONFIG_FILE_PATH_PROP_NAME, "parquet-test.conf");
		List<DataSourceConfiguration> dsc = Util.getGloballyConfiguredDataSources();
		Assert.assertNotNull(dsc);
	}

}
