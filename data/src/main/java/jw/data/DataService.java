

package jw.data;

import java.io.File;
import java.io.FileFilter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class DataService {
	Logger logger = Logger.getLogger(DataService.class);
	
	@Autowired
	DataServiceConfiguration dsc;
	
	@Autowired
	Hdf5ReadWriteService hrws;
	
	public void importData(String dataDir, String filterExp){
		File dir = new File(dataDir);
		FileFilter ff = new RegexFileFilter(filterExp);
		File[] files = dir.listFiles(ff);
		for (File f: files){
			try{
				CSVParser parser = CSVParser.parse(f, Charset.defaultCharset(), CSVFormat.RFC4180.withFirstRecordAsHeader());
				String fileName = f.getName();
				List<CSVRecord> reclist = parser.getRecords();
				List<Iterable<String>> rlist = new ArrayList<Iterable<String>>();
				reclist.forEach(a->rlist.add(a));
				Map<String, Integer> nameIdx = parser.getHeaderMap();
				int colNum = nameIdx.keySet().size();
				colNum++;
				String[] names = new String[colNum];
				int[] types = new int[colNum];
				names[0]="tid";
				types[0]=Hdf5CompoundDataType.TYPE_F_STR;
				int i=1;
				Iterator<String> nameIt = nameIdx.keySet().iterator();
				while(nameIt.hasNext()){
					String name = nameIt.next();
					names[i]=name;
					if ("Date".equals(name)){
						types[i]=Hdf5CompoundDataType.TYPE_DT;
					}else{
						types[i]=Hdf5CompoundDataType.TYPE_FLOAT;
					}
					i++;
				}
				hrws.writeHdf5File(fileName+".hdf5", names, types, rlist);
			}catch(Exception e){
				logger.error("", e);
			}
		}
		logger.info(dsc.getDataFsRoot());
	}
}
