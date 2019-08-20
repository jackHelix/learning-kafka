package com.snails.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author snails
 * @Create 2019 - 08 - 16 - 15:51
 * @Email snailsone@gmail.com
 * @desc
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
}
