package jw.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

@Service
public class Hdf5ReadWriteService {
	
	private static final int RANK = 1;
	private static final String DATASETNAME = "ds1";
	
	private Logger logger = Logger.getLogger(Hdf5ReadWriteService.class);
	
	private String getFirstPart(String s){
		return s.substring(0, s.indexOf("."));
	}
	
	public void writeHdf5File(String fileName, String[] names, int[] types, List<Iterable<String>> values) {
        long file_id = -1;
        long strtype_id = -1;
        long memtype_id = -1;
        long filetype_id = -1;
        long dataspace_id = -1;
        long dataset_id = -1;
        long[] dims = { values.size() };
        byte[] dset_data = null;

        Hdf5CompoundDataType cdt = new Hdf5CompoundDataType(names, types);
        
        // Create a new file using default properties.
        try {
            file_id = H5.H5Fcreate(fileName, HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
        	logger.error("", e);
        }
        
        strtype_id=cdt.createStringTypeId();
        filetype_id = cdt.createFileTypeId(strtype_id);
        memtype_id = cdt.createMemTypeId(strtype_id);
        
        // Create dataspace. Setting maximum size to NULL sets the maximum size to be the current size.
        try {
            dataspace_id = H5.H5Screate_simple(RANK, dims, null);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Create the dataset.
        try {
            if ((file_id >= 0) && (dataspace_id >= 0) && (filetype_id >= 0))
                dataset_id = H5.H5Dcreate(file_id, DATASETNAME, filetype_id, dataspace_id, HDF5Constants.H5P_DEFAULT,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
        	logger.error("", e);
        }

        // Write the compound data to the dataset.
        // allocate memory for read buffer.
        
        dset_data = new byte[(int)dims[0] * (int)cdt.getDataSize()];
        logger.info(String.format("size of dset:%d", (int)dims[0] * (int)cdt.getDataSize()));
        ByteBuffer outBuf = ByteBuffer.wrap(dset_data);
        outBuf.order(ByteOrder.nativeOrder());
        String tid = getFirstPart(fileName);
        for (int indx = 0; indx < (int) dims[0]; indx++) {
        	Iterable<String> rec = values.get(indx);
        	//append filename to the rec
        	List<String> totalRec = new ArrayList<String>();
        	totalRec.add(tid);
        	Iterator<String> it = rec.iterator();
        	while(it.hasNext()){
        		totalRec.add(it.next());
        	}
        	logger.debug(String.format("to write:%s", totalRec.toString()));
            cdt.writeBuffer(outBuf, indx * (int)cdt.getDataSize(), totalRec.iterator());
        }
        try {
            if ((dataset_id >= 0) && (memtype_id >= 0))
                H5.H5Dwrite(dataset_id, memtype_id, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dset_data);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // End access to the dataset and release resources used by it.
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Terminate access to the data space.
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Terminate access to the file type.
        try {
            if (filetype_id >= 0)
                H5.H5Tclose(filetype_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Terminate access to the mem type.
        try {
            if (memtype_id >= 0)
                H5.H5Tclose(memtype_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        try {
            if (strtype_id >= 0)
                H5.H5Tclose(strtype_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Close the file.
        try {
            if (file_id >= 0)
                H5.H5Fclose(file_id);
        } catch (Exception e) {
        	logger.error("",e);
        }
    }

    public List<String> readHdf5File(String fileName, String[] names, int[] types) {
    	List<String> ret = new ArrayList<String>();
    	
        long file_id = -1;
        long strtype_id = -1;
        long memtype_id = -1;
        long dataspace_id = -1;
        long dataset_id = -1;
        long[] dims = { 0 };//
        byte[] dset_data;

        Hdf5CompoundDataType cdt = new Hdf5CompoundDataType(names, types);
        // Open an existing file.
        try {
            file_id = H5.H5Fopen(fileName, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Open an existing dataset.
        try {
            if (file_id >= 0)
                dataset_id = H5.H5Dopen(file_id, DATASETNAME, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Get dataspace and allocate memory for read buffer.
        try {
            if (dataset_id >= 0)
                dataspace_id = H5.H5Dget_space(dataset_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        try {
            if (dataspace_id >= 0)
                H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Create string datatype.
        strtype_id = cdt.createStringTypeId();

        // Create the compound datatype for memory.
        memtype_id = cdt.createStringTypeId();

        // allocate memory for read buffer.
        dset_data = new byte[(int) dims[0] * (int)cdt.getDataSize()];

        // Read data.
        try {
            if ((dataset_id >= 0) && (memtype_id >= 0))
                H5.H5Dread(dataset_id, memtype_id, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dset_data);

            ByteBuffer inBuf = ByteBuffer.wrap(dset_data);
            inBuf.order(ByteOrder.nativeOrder());
            for (int indx = 0; indx < (int) dims[0]; indx++) {
            	String v = cdt.readBuffer(inBuf, indx * (int)cdt.getDataSize());
            	ret.add(v);
            }
        } catch (Exception e) {
        	logger.error("",e);
        }
        
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Terminate access to the data space.
        try {
            if (dataspace_id >= 0)
                H5.H5Sclose(dataspace_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Terminate access to the mem type.
        try {
            if (memtype_id >= 0)
                H5.H5Tclose(memtype_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        try {
            if (strtype_id >= 0)
                H5.H5Tclose(strtype_id);
        } catch (Exception e) {
        	logger.error("",e);
        }

        // Close the file.
        try {
            if (file_id >= 0)
                H5.H5Fclose(file_id);
        } catch (Exception e) {
        	logger.error("",e);
        }
        return ret;
    }
}
