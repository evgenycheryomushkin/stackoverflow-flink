package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TaggedObject {
    String address;
    List<String> organizations;
    boolean isControl = false;
}
