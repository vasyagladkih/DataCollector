package ru.datacollector.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc.InstrumentsServiceBlockingStub;
import ru.ttech.piapi.core.connector.ServiceStubFactory;

@Configuration
@EnableScheduling
public class TinvestConfiguration {

    @Bean
    public InstrumentsServiceBlockingStub instrumentsServiceBlockingStub(ServiceStubFactory stubFactory) {
        return stubFactory
                .newSyncService(InstrumentsServiceGrpc::newBlockingStub)
                .getStub();
    }
}
