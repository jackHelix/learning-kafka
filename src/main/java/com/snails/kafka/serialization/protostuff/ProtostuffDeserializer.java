package com.snails.kafka.serialization.protostuff;

import com.snails.kafka.entity.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @Author snails
 * @Create 2019 - 09 - 25 - 16:32
 * @Email snailsone@gmail.com
 * @desc
 */
public class ProtostuffDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        Company cmy = new Company();
        ProtostuffIOUtil.mergeFrom(data, cmy, schema);
        return cmy;
    }

    @Override
    public void close() {

    }
}
