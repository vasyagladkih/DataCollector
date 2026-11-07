package ru.datacollector.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.datacollector.dto.SubscriptionResponse;
import ru.datacollector.repository.entity.SubscriptionEntity;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface SubscriptionMapper {
    SubscriptionResponse toDto(SubscriptionEntity entity);
}
