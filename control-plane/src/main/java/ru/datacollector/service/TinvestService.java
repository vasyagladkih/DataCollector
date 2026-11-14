package ru.datacollector.service;

import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.datacollector.exception.ExchangeUnavailableException;
import ru.datacollector.exception.SymbolNotFoundException;
import ru.datacollector.mapper.InstrumentEntityMapper;
import ru.datacollector.repository.InstrumentRepository;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.tinkoff.piapi.contract.v1.FindInstrumentRequest;
import ru.tinkoff.piapi.contract.v1.FindInstrumentResponse;
import ru.tinkoff.piapi.contract.v1.InstrumentShort;
import ru.tinkoff.piapi.contract.v1.InstrumentStatus;
import ru.tinkoff.piapi.contract.v1.InstrumentsRequest;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Service
public class TinvestService {

    private static final InstrumentsRequest BASE_REQUEST = InstrumentsRequest.newBuilder()
            .setInstrumentStatus(InstrumentStatus.INSTRUMENT_STATUS_BASE)
            .build();

    private final InstrumentsServiceGrpc.InstrumentsServiceBlockingStub instrumentsServiceBlockingStub;
    private final InstrumentRepository repository;
    private final InstrumentEntityMapper instrumentEntityMapper;

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        if (repository.count() == 0) {
            log.info("Реестр инструментов пуст, выполняется начальная синхронизация...");
            syncAllInstruments();
        }
    }

    @Scheduled(cron = "0 0 3 * * *")
    public void syncAllInstruments() {
        log.info("Запуск синхронизации инструментов с T-Invest API...");
        List<InstrumentEntity> allInstruments = new ArrayList<>();
        Instant now = Instant.now();

        fetchCategory("акций",
                () -> instrumentsServiceBlockingStub.shares(BASE_REQUEST).getInstrumentsList(),
                s -> instrumentEntityMapper.toEntity(s, now),
                allInstruments);

        fetchCategory("фьючерсов",
                () -> instrumentsServiceBlockingStub.futures(BASE_REQUEST).getInstrumentsList(),
                f -> instrumentEntityMapper.toEntity(f, now),
                allInstruments);

        fetchCategory("облигаций",
                () -> instrumentsServiceBlockingStub.bonds(BASE_REQUEST).getInstrumentsList(),
                b -> instrumentEntityMapper.toEntity(b, now),
                allInstruments);

        fetchCategory("валют",
                () -> instrumentsServiceBlockingStub.currencies(BASE_REQUEST).getInstrumentsList(),
                c -> instrumentEntityMapper.toEntity(c, now),
                allInstruments);

        fetchCategory("ETF",
                () -> instrumentsServiceBlockingStub.etfs(BASE_REQUEST).getInstrumentsList(),
                e -> instrumentEntityMapper.toEntity(e, now),
                allInstruments);

        if (!allInstruments.isEmpty()) {
            repository.upsertAll(allInstruments);
            log.info("Синхронизация завершена успешно. Сохранено/обновлено инструментов: {}", allInstruments.size());
        } else {
            log.warn("Синхронизация не вернула ни одного инструмента для сохранения");
        }
    }

    public InstrumentEntity resolve(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Символ инструмента не может быть пустым");
        }
        String cleanSymbol = symbol.trim();

        Optional<InstrumentEntity> fromDb = repository.findByTicker(cleanSymbol)
                .or(() -> repository.findById(cleanSymbol));

        if (fromDb.isPresent()) {
            InstrumentEntity entity = fromDb.get();
            if (!entity.isTradable()) {
                throw new IllegalArgumentException("Торги по инструменту [" + cleanSymbol + "] заблокированы биржей");
            }
            return entity;
        }

        FindInstrumentResponse response;
        try {
            response = instrumentsServiceBlockingStub.findInstrument(
                    FindInstrumentRequest.newBuilder().setQuery(cleanSymbol).build()
            );
        } catch (StatusRuntimeException e) {
            throw new ExchangeUnavailableException("Биржа T-Банка временно недоступна: " + e.getMessage(), e);
        }

        InstrumentShort matched = response.getInstrumentsList().stream()
                .filter(inst -> inst.getTicker().equalsIgnoreCase(cleanSymbol) || inst.getUid().equalsIgnoreCase(cleanSymbol))
                .findFirst()
                .orElseThrow(() -> new SymbolNotFoundException("Инструмент не найден: " + cleanSymbol));

        InstrumentEntity entity = instrumentEntityMapper.toEntity(matched, Instant.now());

        repository.upsert(entity);

        if (!entity.isTradable()) {
            throw new IllegalArgumentException("Торги по инструменту [" + cleanSymbol + "] заблокированы биржей");
        }

        return entity;
    }

    private <T> void fetchCategory(
            String categoryName,
            Supplier<List<T>> fetcher,
            Function<T, InstrumentEntity> mapper,
            List<InstrumentEntity> target
    ) {
        int sizeBefore = target.size();
        try {
            List<T> items = fetcher.get();
            if (items == null || items.isEmpty()) {
                log.warn("Категория [{}] вернула ПУСТОЙ список от T-Invest API (0 инструментов)", categoryName);
                return;
            }

            int skipped = 0;
            for (T item : items) {
                InstrumentEntity entity = mapper.apply(item);
                if (entity != null && !entity.instrumentUid().isBlank()) {
                    target.add(entity);
                } else {skipped++;}
            }
            int added = target.size() - sizeBefore;
            log.info("Категория [{}]: получено из API = {}, добавлено в реестр = {}, пропущено = {}",
                    categoryName, items.size(), added, skipped);

        } catch (StatusRuntimeException e) {
            log.error("gRPC сбой категории [{}]: Code = {}, Description = {}, Trailers = {}",
                    categoryName, e.getStatus().getCode(), e.getStatus().getDescription(), e.getTrailers());
        } catch (Exception e) {
            log.error("Непредвиденная ошибка категории [{}]: {}", categoryName, e.getMessage(), e);
        }
    }
}
