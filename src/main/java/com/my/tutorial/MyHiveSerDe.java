package com.my.tutorial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.my.tutorial.MyProtoBuf.CommonDump;

/**
 * Description: 
 * https://analyticsanvil.wordpress.com/2016/03/06/creating-a-custom-hive-input-format-and-record-reader-to-read-fixed-format-flat-files/
 * @author saboloh
 * @data   Jun 29, 2016
 */

/**
 * @author sabo
 *
 */
@SuppressWarnings("deprecation")
public class MyHiveSerDe implements SerDe {
    
    private StructObjectInspector objectInspector;
    private List<String> columnNames;
    private List<Object> row;
    private int numColumns;
    private List<PrimitiveCategory> columnTypes;
    
    private static final Logger LOG = LoggerFactory.getLogger(MyHiveSerDe.class);

    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        // TODO Auto-generated method stub
        
        LOG.debug("Initializing WTableHiveSerDe");
        // Get column names and types
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        // all table column names
        if (columnNameProperty.length() == 0) {
          columnNames = new ArrayList<String>();
        } else {
          columnNames = Arrays.asList(columnNameProperty.split(","));
        }
        numColumns = columnNames.size();
        String[] hiveColumnTypes = columnTypeProperty.split(":");
        
        // all column types
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(numColumns);
        columnTypes = new ArrayList<PrimitiveCategory>(numColumns);
        
        for (int i = 0; i < numColumns; i++) {
            String typeName = hiveColumnTypes[i];
            PrimitiveCategory type = null;
            try {
                type = PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(typeName).primitiveCategory;
                columnTypes.add(type);
            } catch (Exception e) {
                throw new SerDeException(typeName + " is not supported!");
            }
            
            fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(type));
        }

        LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
        LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);
        
        assert (columnNames.size() == columnTypes.size());

        
        row = new ArrayList<Object>(numColumns);
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldInspectors);
    }

    public Object deserialize(Writable blob) throws SerDeException {
        // TODO Auto-generated method stub
        BytesWritable writable = (BytesWritable) blob;
        CommonDump commonDump = null;
        try {
            commonDump = CommonDump.parseFrom(writable.copyBytes());
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            return null;
        }
        
        Map<FieldDescriptor, Object> _fields = commonDump.getAllFields();
        Map<String, Object> fields = new HashMap<String, Object>(_fields.size());
        row.clear();
        
        Iterator<FieldDescriptor> fieldKeys = _fields.keySet().iterator();
        while (fieldKeys.hasNext()) {
            
            FieldDescriptor fieldKey = fieldKeys.next();
            fields.put(fieldKey.getName().toLowerCase(), _fields.get(fieldKey));
        }
        
        for (int i = 0; i < numColumns; i++) {
            
            String colName = columnNames.get(i).toLowerCase();
            row.add(optVal(columnTypes.get(i), fields.get(colName)));
        }
        return row;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        // TODO Auto-generated method stub
        return objectInspector;
    }

    public SerDeStats getSerDeStats() {
        // TODO Auto-generated method stub
        return new SerDeStats();
    }

    public Class<? extends Writable> getSerializedClass() {
        // TODO Auto-generated method stub
        return MyWritable.class;
    }

    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        // TODO Auto-generated method stub
        throw new SerDeException("Serialize to db does not supported. Pull-request is appreciated.");
    }
    
    public static final Object optVal(PrimitiveCategory pcat, Object val) {
        switch (pcat) {
        case INT:
            return Integer.valueOf(String.valueOf(val));
        case LONG:
            return Long.valueOf(String.valueOf(val));
        case BINARY:
            return ((ByteString) val).toByteArray();
        case STRING:
        default:
            return new String(((ByteString) val).toByteArray());
        }
    }
    
}
