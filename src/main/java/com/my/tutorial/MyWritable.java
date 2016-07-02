package com.my.tutorial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.my.tutorial.MyProtoBuf.CommonDump;

public class MyWritable implements Writable {
    
    private Map<String, BytesWritable> fields;
    private List<String> colKeys;
    private int size = 0;

    public void set(CommonDump commonDump) {
        
        Map<FieldDescriptor, Object> fields = commonDump.getAllFields();
        Set<FieldDescriptor> objs = commonDump.getAllFields().keySet();
        this.size = fields.size();
        this.fields = new HashMap<String, BytesWritable>(size);
        this.colKeys = new ArrayList<String>(size);
        
        Iterator<FieldDescriptor> items = objs.iterator();
        while (items.hasNext()) {
            FieldDescriptor colKey = items.next();
            Object obj = fields.get(colKey);
            this.fields.put(colKey.getName().toLowerCase(), optType(obj));
            this.colKeys.add(colKey.getName().toLowerCase());
        }
    }
    
    public MyWritable() {
        
        this.fields = new HashMap<String, BytesWritable>();
        this.colKeys = new ArrayList<String>();
    }
    
    public MyWritable(CommonDump commonDump) {
        set(commonDump);
    }
    
    public void write(DataOutput out) throws IOException {
        
        out.writeInt(size);
        for (int index = 0; index < size; index++) {
            
            Text.writeString(out, colKeys.get(index));
            BytesWritable val = fields.get(index);
            val.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        
        int size = in.readInt();
        fields = new HashMap<String, BytesWritable>(size);
        colKeys = new ArrayList<String>(size);
        
        for (int i = 0; i < size; i++) {
            colKeys.add(Text.readString(in));
            BytesWritable val = new BytesWritable();
            val.readFields(in);
            fields.put(colKeys.get(i), val);
        }
    }
    
    public Map<String, BytesWritable> getFields() {
        return fields;
    }

    public void setFields(Map<String, BytesWritable> fields) {
        this.fields = fields;
    }

    public List<String> getColKeys() {
        return colKeys;
    }

    public void setColKeys(List<String> colKeys) {
        this.colKeys = colKeys;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public static final BytesWritable optType (Object obj) {
        
        if (obj instanceof ByteString) {
            
            return new BytesWritable(((ByteString) obj).toByteArray());
        } else {
            
            return new BytesWritable(String.valueOf(obj).getBytes());
        }
    }
}
