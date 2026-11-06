package ru.datacollector.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.datacollector.dto.CreateSubscriptionResponse;
import ru.datacollector.persistance.entity.SubscriptionEntity;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface SubscriptionMapper {
    CreateSubscriptionResponse toDto(SubscriptionEntity entity);
}
