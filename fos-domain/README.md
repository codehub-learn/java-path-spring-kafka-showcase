# fos-domain

Shared domain classes for the Food Ordering System. Contains the `Order`, `Restaurant`, `Customer`,
and `Courier` entity records, the `OrderStatus` lifecycle enum, and three event records
(`OrderPlacedEvent`, `OrderStatusUpdatedEvent`, `OrderCancelledEvent`) that represent Kafka message
payloads. This module has **no Kafka dependency** — it is a pure Java library consumed by
`fos-producer`, `fos-consumer`, `fos-streams`, and `fos-generator`.

## Package structure

```
gr.codelearn.spring.kafka.domain
├── entity/     Order, Restaurant, Customer, Courier
├── enums/      OrderStatus
└── event/      OrderPlacedEvent, OrderStatusUpdatedEvent, OrderCancelledEvent
```

## Usage in other modules

Add as a Maven dependency:

```xml
<dependency>
    <groupId>gr.codelearn</groupId>
    <artifactId>fos-domain</artifactId>
    <version>${project.version}</version>
</dependency>
```
