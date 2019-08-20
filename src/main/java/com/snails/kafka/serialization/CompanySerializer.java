package com.snails.kafka.serialization;

import com.snails.kafka.entity.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author snails
 * @Create 2019 - 08 - 16 - 16:00
 * @Email snailsone@gmail.com
 * @desc 自定义Company对应的序列化器
 */
public class CompanySerializer implements Serializer<Company> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        byte[] name, address;
        try {
            if (data.getName() != null) {
                name = data.getName().getBytes(encoding);
            } else {
                return new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes(encoding);
            }else{
                return new byte[0];
            }
            ByteBuffer buffer=ByteBuffer.allocate(4+4+name.length+address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
