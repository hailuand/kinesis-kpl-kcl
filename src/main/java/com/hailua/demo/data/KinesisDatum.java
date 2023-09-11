package com.hailua.demo.data;

import java.io.Serializable;
import java.sql.Timestamp;

public record KinesisDatum(String name, Timestamp timestamp) implements Serializable {
}
