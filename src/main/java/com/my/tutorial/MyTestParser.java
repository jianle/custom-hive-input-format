package com.my.tutorial;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.xerial.snappy.SnappyInputStream;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.my.tutorial.MyProtoBuf.CommonDump;

/**
 * @author sabo
 * My parse local snappy file.
 */
public class MyTestParser {
    
    public static void main(String[] args) throws UnsupportedEncodingException, IOException {
        
        InputStream in = new FileInputStream("/tmp/file.dat.sp");
        in = new SnappyInputStream(in);
        
        byte[] header = new byte[4];
        CommonDump commonDump = null;
        long length = 0;
        while (in.read(header, 0, 4) != -1) {
            
            int bodylength = bytesToInt(header);
            byte[] body = new byte[bodylength];
            
            in.read(body, 0, bodylength);
            length++;
            
            if (length > 10) {
                break;
            }
        
            commonDump = CommonDump.parseFrom(body);
            Map<FieldDescriptor, Object> fields = commonDump.getAllFields();
            
            Set<FieldDescriptor> objs = commonDump.getAllFields().keySet();
            Iterator<FieldDescriptor> items = objs.iterator();
            while (items.hasNext()) {
                FieldDescriptor key = items.next();
                Object obj = fields.get(key);
                System.out.println(optType(obj.getClass()) + "\t" + key.getName());
            }
            
            System.out.println(commonDump.getColKey().toStringUtf8() + " => " + commonDump.getTableId()  + " => " + commonDump.getRowKey().toStringUtf8() + " => " + StringEscapeUtils.unescapeJava(commonDump.getValue().toStringUtf8()).replaceAll("\n|\r|\t", "||"));
             
        }
        in.close();
        System.out.println("row counter:" + length);
    }

    public static int bytesToInt(byte[] src) {
        int value;
        value = (int) ((src[3] & 0xFF)
                | ((src[2] & 0xFF) << 8)
                | ((src[1] & 0xFF) << 16)
                | ((src[0] & 0xFF) << 24));
        return value;
    }
    
    public static void print(CommonDump commonDump) {
        Iterator<?> items = commonDump.getAllFields().entrySet().iterator();
        while (items.hasNext()) {
            System.out.print(commonDump.getAllFields().get(items.next()) + "\t");
        }
        System.out.println();
    }
    
    public static String optType (Class<?> type) {
        
        return type.getName().substring(type.getName().lastIndexOf(".") + 1);
    }

}
