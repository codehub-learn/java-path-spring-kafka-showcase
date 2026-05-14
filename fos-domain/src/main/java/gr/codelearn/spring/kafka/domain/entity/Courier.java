package gr.codelearn.spring.kafka.domain.entity;

import gr.codelearn.spring.kafka.domain.enums.VehicleType;

public record Courier(String courierId, String name, VehicleType vehicleType) {
}
