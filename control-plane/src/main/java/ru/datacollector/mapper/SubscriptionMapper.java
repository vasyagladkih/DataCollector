package ru.datacollector.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.datacollector.dto.CreateSubscriptionRequest;
import ru.datacollector.dto.CreateSubscriptionResponse;
import ru.datacollector.persistance.entity.SubscriptionEntity;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface SubscriptionMapper {

    CreateSubscriptionResponse toDto(SubscriptionEntity entity);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "status", constant = "CONNECTING")
    @Mapping(target = "errorMessage", ignore = true)
    @Mapping(target = "createdAt", expression = "java(java.time.Instant.now())")
    @Mapping(target = "updatedAt", expression = "java(java.time.Instant.now())")
    SubscriptionEntity toEntity(CreateSubscriptionRequest request);
}
