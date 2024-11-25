package com.job.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lcy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PerJobClientEntity {
    private String userJar;
    private String userJarLib;
    private String flinkDistJar;

}
