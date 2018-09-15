package jw.data.test;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import jw.data.DataService;
import jw.data.Hdf5CompoundDataType;
import jw.data.Hdf5ReadWriteService;


@RunWith(SpringRunner.class)
@SpringBootTest
public class DataServiceTest {
	
	@Autowired
	DataService ds;
	
	@Autowired
	Hdf5ReadWriteService hrws;
    
    @Test
    public void testImportData() {
        ds.importData("/Users/chengyi/project/my/data/CME/data/", "ADF2018.csv");
    }
    
    @Test
    public void testWrite(){
    	String[] names = new String[]{"tid", "Date", "Open"};
    	int[] types = new int[]{Hdf5CompoundDataType.TYPE_F_STR, Hdf5CompoundDataType.TYPE_DT, Hdf5CompoundDataType.TYPE_DOUBLE};
    	List<String> row1 = new ArrayList<String>();
    	row1.add("2010-10-01");
    	row1.add("1.5");
    	List<String> row2 = new ArrayList<String>();
    	row2.add("2010-10-02");
    	row2.add("2.1");
    	List<Iterable<String>> values = new ArrayList<Iterable<String>>();
    	values.add(row1);
    	values.add(row2);
    	hrws.writeHdf5File("test1.hdf5", names, types, values);
    }
}
