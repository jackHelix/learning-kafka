package com.snails.kafka.serialization;

import com.snails.kafka.entity.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author snails
 * @Create 2019 - 08 - 16 - 16:13
 * @Email snailsone@gmail.com
 * @desc 自定义Company反序列化器
 */
public class CompanyDeserializer implements Deserializer<Company> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data recevied " + "by CompanyDeserializer is shorter than expected");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;
        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);
        try {
            name = new String(nameBytes, encoding);
            address = new String(addressBytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing");
        }
        return new Company(name, address);
    }

    @Override
    public void close() {

    }
}
