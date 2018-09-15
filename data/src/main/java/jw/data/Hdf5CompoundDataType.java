
package jw.data;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;

public class Hdf5CompoundDataType {
	private Logger logger = Logger.getLogger(Hdf5CompoundDataType.class);
	
	public static String DATE_FORMAT="yyyy-MM-dd";
	public static SimpleDateFormat date_sdf = new SimpleDateFormat(DATE_FORMAT);
	
    public static final int TYPE_LONG=1;
    public static final int TYPE_DOUBLE=2;
    public static final int TYPE_F_STR=3;
    public static final int TYPE_DT=4;
    public static final int TYPE_FLOAT=5;
    public static final int TYPE_INT=6;
    
    protected static final int INTSIZE=4;
	protected static final int LONGSIZE = 8;
	protected static final int FLOATSIZE = 4;
    protected static final int DOUBLESIZE = 8;
    protected final static int MAXSTRINGSIZE = 10;
    
	private int numberMembers;
	
    private int[] memberDims;
    private String[] memberNames;
    private int[] types;
    private long[] memberMemTypes;
    private long[] memberFileTypes;
    private int[] memberStorage;

    public Hdf5CompoundDataType(String[] names, int[] types){
    	this.types = types;
    	this.numberMembers = names.length;
    	assert(names.length==types.length);
    	memberDims = new int[this.numberMembers];
    	memberNames = new String[this.numberMembers];
    	memberMemTypes = new long[this.numberMembers];
    	memberFileTypes = new long[this.numberMembers];
    	memberStorage = new int[this.numberMembers];
    	for (int i=0; i<this.numberMembers; i++){
    		memberDims[i]=1;
    		memberNames[i]=names[i];
    		int type = types[i];
    		if (type==TYPE_INT){
    			memberMemTypes[i]=HDF5Constants.H5T_NATIVE_INT;
    			memberFileTypes[i]=HDF5Constants.H5T_STD_I32BE;
    			memberStorage[i]=INTSIZE;
    		}else if (type==TYPE_LONG){
    			memberMemTypes[i]=HDF5Constants.H5T_NATIVE_LONG;
    			memberFileTypes[i]=HDF5Constants.H5T_STD_I64BE;
    			memberStorage[i]=LONGSIZE;
    		}else if(type==TYPE_FLOAT){
    			memberMemTypes[i]=HDF5Constants.H5T_NATIVE_FLOAT;
    			memberFileTypes[i]=HDF5Constants.H5T_IEEE_F32BE;
    			memberStorage[i]=FLOATSIZE;
    		}else if(type==TYPE_DOUBLE){
    			memberMemTypes[i]=HDF5Constants.H5T_NATIVE_DOUBLE;
    			memberFileTypes[i]=HDF5Constants.H5T_IEEE_F64BE;
    			memberStorage[i]=DOUBLESIZE;
    		}else if (type==TYPE_F_STR){
    			memberMemTypes[i]=HDF5Constants.H5T_C_S1;
    			memberFileTypes[i]=HDF5Constants.H5T_C_S1;
    			memberStorage[i]=MAXSTRINGSIZE;
    		}else if (type==TYPE_DT){
    			memberMemTypes[i]=HDF5Constants.H5T_NATIVE_LONG;
    			memberFileTypes[i]=HDF5Constants.H5T_STD_I64BE;//HDF5Constants.H5T_UNIX_D32LE;
    			memberStorage[i]=LONGSIZE;
    		}else{
    			logger.error(String.format("type %d not supported.", type));
    		}
    	}
    }
    // Data size is the storage size for the members.
    public long getTotalDataSize(long length) {
        long data_size = 0;
        for (int indx = 0; indx < getNumberMembers(); indx++)
            data_size += memberStorage[indx] * memberDims[indx];
        return length * data_size;
    }

    public long getDataSize() {
        long data_size = 0;
        for (int indx = 0; indx < getNumberMembers(); indx++)
            data_size += memberStorage[indx] * memberDims[indx];
        return data_size;
    }

    public int getOffset(int memberItem) {
        int data_offset = 0;
        for (int indx = 0; indx < memberItem; indx++)
            data_offset += memberStorage[indx];
        return data_offset;
    }
    
    public void writeBuffer(ByteBuffer databuf, int dbposition, Iterator<String> values) {
    	int i=0;
    	while (values.hasNext()){
    		String v = values.next();
    		//process
    		int type = types[i];
    		logger.debug(String.format("type:%d, v:%s", type, v));
    		if (type==TYPE_LONG){
    			long l = -1;
    			try{
    				l = Long.parseLong(v);
    			}catch(Exception e){
    				logger.error(String.format("parse long %s failed", v));
    			}
    			databuf.putLong(dbposition + getOffset(i), l);
    		}else if (type==TYPE_INT){
    			int iv = -1;
    			try{
    				iv = Integer.parseInt(v);
    			}catch(Exception e){
    				logger.error(String.format("parse integer %s failed", v));
    			}
    			databuf.putInt(dbposition + getOffset(i), iv);
    		}else if (type==TYPE_DOUBLE){
    			double d = -1;
    			try{
    				d = Double.parseDouble(v);
    			}catch(Exception e){
    				logger.error(String.format("parse double %s failed", v));
    			}
    			databuf.putDouble(dbposition + getOffset(i), d);
    		}else if (type==TYPE_FLOAT){
    			float f = -1;
    			try{
    				f = Float.parseFloat(v);
    			}catch(Exception e){
    				logger.error(String.format("parse float %s failed", v));
    			}
    			databuf.putFloat(dbposition + getOffset(i), f);
    		}else if (type==TYPE_F_STR){
    			byte[] temp_str = v.getBytes(Charset.forName("UTF-8"));
    	        int arraylen = (temp_str.length > MAXSTRINGSIZE) ? MAXSTRINGSIZE : temp_str.length;
    	        for (int ndx = 0; ndx < arraylen; ndx++)
    	            databuf.put(dbposition + getOffset(i) + ndx, temp_str[ndx]);
    	        for (int ndx = arraylen; ndx < MAXSTRINGSIZE; ndx++)
    	            databuf.put(dbposition + getOffset(i) + arraylen, (byte) 0);
    		}else if (type==TYPE_DT){
    			try {
    				Date d = date_sdf.parse(v);
        			long dt = d.getTime()/1000L;
        			logger.debug(String.format("date:%s, dt:%d", v, dt));
        			databuf.putLong(dbposition + getOffset(i), dt);
    			}catch(Exception e){
    				logger.error("", e);
    			}
    		}else{
    			logger.error(String.format("type %d not supported.", type));
    		}
    		//
    		i++;
    	}
    }

    public String readBuffer(ByteBuffer databuf, int dbposition) {
    	List<String> vlist = new ArrayList<String>();
    	for (int i=0; i<this.numberMembers; i++){
    		int type = types[i];
    		String v = "";
    		//process
    		if (type==TYPE_INT){
    			int iv = databuf.getInt(dbposition + getOffset(i));
    			v = String.valueOf(iv);
    		}else if (type==TYPE_LONG){
    			long l = databuf.getLong(dbposition + getOffset(i));
    			v = String.valueOf(l);
    		}else if (type==TYPE_DOUBLE){
    			double d = databuf.getDouble(dbposition + getOffset(i));
    			v = String.valueOf(d);
    		}else if (type==TYPE_FLOAT){
    			float f = databuf.getFloat(dbposition + getOffset(i));
    			v = String.valueOf(f);
    		}else if (type==TYPE_F_STR){
    			ByteBuffer stringbuf = databuf.duplicate();
    	        stringbuf.position(dbposition + getOffset(i));
    	        stringbuf.limit(dbposition + getOffset(i) + MAXSTRINGSIZE);
    	        byte[] bytearr = new byte[stringbuf.remaining()];
    	        stringbuf.get(bytearr);
    	        v = new String(bytearr, Charset.forName("UTF-8")).trim();
    		}else if (type==TYPE_DT){
    			long l = databuf.getLong(dbposition + getOffset(i));
    			Date d = new Date(l*1000L);
    			v = date_sdf.format(d);
    		}else{
    			logger.error(String.format("type %d not supported.", type));
    		}
    		vlist.add(v);
    		//
    		i++;
    	}
    	return String.join(",", vlist);
    }
    
    public long createStringTypeId(){
	    long strtype_id=-1;
    	try {
	        strtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
	        if (strtype_id >= 0)
	            H5.H5Tset_size(strtype_id, MAXSTRINGSIZE);
	    }catch (Exception e) {
	    	logger.error(e.toString());
	    }
    	return strtype_id;
    }
    
    public long createMemTypeId(long strtype_id){
    	long memtype_id=-1;
    	// Create the compound datatype for memory.
	    try {
	    	memtype_id = H5.H5Tcreate(HDF5Constants.H5T_COMPOUND, getDataSize());
	        if (memtype_id >= 0) {
	            for (int indx = 0; indx < numberMembers; indx++) {
	                long type_id = memberMemTypes[indx];
	                if (type_id == HDF5Constants.H5T_C_S1)
	                    type_id = strtype_id;
	                H5.H5Tinsert(memtype_id, memberNames[indx], getOffset(indx),
	                        type_id);
	            }
	        }
	    }catch (Exception e) {
	    	logger.error(e);
	    }
        return memtype_id;
    }
    
    public long createFileTypeId(long strtype_id){
	    // Create the compound datatype for the file. Because the standard
	    // types we are using for the file may have different sizes than
	    // the corresponding native types, we must manually calculate the
	    // offset of each member.
    	long filetype_id=-1;
	    try {
	        filetype_id = H5.H5Tcreate(HDF5Constants.H5T_COMPOUND, getDataSize());
	        if (filetype_id >= 0) {
	            for (int indx = 0; indx < numberMembers; indx++) {
	                long type_id = memberFileTypes[indx];
	                if (type_id == HDF5Constants.H5T_C_S1)
	                    type_id = strtype_id;
	                H5.H5Tinsert(filetype_id, memberNames[indx], getOffset(indx),
	                        type_id);
	            }
	        }
	    }
	    catch (Exception e) {
	    	logger.error(e);
	    }
	    return filetype_id;
    }
    
    ///
	public int getNumberMembers() {
		return numberMembers;
	}

	public void setNumberMembers(int numberMembers) {
		this.numberMembers = numberMembers;
	}
}
