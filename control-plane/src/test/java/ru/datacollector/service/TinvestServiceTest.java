package ru.datacollector.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datacollector.exception.ExchangeUnavailableException;
import ru.datacollector.exception.SymbolNotFoundException;
import ru.datacollector.repository.InstrumentRepository;
import ru.datacollector.repository.entity.InstrumentEntity;
import ru.tinkoff.piapi.contract.v1.Bond;
import ru.tinkoff.piapi.contract.v1.BondsResponse;
import ru.tinkoff.piapi.contract.v1.CurrenciesResponse;
import ru.tinkoff.piapi.contract.v1.Currency;
import ru.tinkoff.piapi.contract.v1.Etf;
import ru.tinkoff.piapi.contract.v1.EtfsResponse;
import ru.tinkoff.piapi.contract.v1.FindInstrumentResponse;
import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.FuturesResponse;
import ru.tinkoff.piapi.contract.v1.InstrumentShort;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc;
import ru.tinkoff.piapi.contract.v1.Share;
import ru.tinkoff.piapi.contract.v1.SharesResponse;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TinvestServiceTest {

    @Mock
    private InstrumentsServiceGrpc.InstrumentsServiceBlockingStub instrumentsServiceBlockingStub;

    @Mock
    private InstrumentRepository repository;

    @InjectMocks
    private TinvestService tinvestService;

    private Share createShare() {
        return Share.newBuilder()
                .setUid("uid-share-1")
                .setTicker("SBER")
                .setClassCode("TQBR")
                .setName("Сбербанк")
                .setLot(10)
                .setCurrency("RUB")
                .setApiTradeAvailableFlag(true)
                .build();
    }

    private Bond createBond() {
        return Bond.newBuilder()
                .setUid("uid-bond-1")
                .setTicker("SU26238RMFS4")
                .setClassCode("TQOB")
                .setName("ОФЗ 26238")
                .setLot(1)
                .setCurrency("RUB")
                .setApiTradeAvailableFlag(true)
                .build();
    }

    private Future createFuture() {
        return Future.newBuilder()
                .setUid("uid-fut-1")
                .setTicker("SiZ4")
                .setClassCode("SPBFUT")
                .setName("Фьючерс Si")
                .setLot(1)
                .setCurrency("RUB")
                .setApiTradeAvailableFlag(true)
                .build();
    }

    private Currency createCurrency() {
        return Currency.newBuilder()
                .setUid("uid-cur-1")
                .setTicker("USDRUB")
                .setClassCode("CETS")
                .setName("Доллар США")
                .setLot(1000)
                .setCurrency("RUB")
                .setApiTradeAvailableFlag(true)
                .build();
    }

    private Etf createEtf() {
        return Etf.newBuilder()
                .setUid("uid-etf-1")
                .setTicker("TMOS")
                .setClassCode("TQTF")
                .setName("Тинькофф iMOEX")
                .setLot(1)
                .setCurrency("RUB")
                .setApiTradeAvailableFlag(true)
                .build();
    }

    private InstrumentShort createShort(String uid, String ticker, boolean tradable) {
        return InstrumentShort.newBuilder()
                .setUid(uid)
                .setTicker(ticker)
                .setClassCode("TQBR")
                .setInstrumentType("share")
                .setName("Тестовый инструмент")
                .setLot(10)
                .setApiTradeAvailableFlag(tradable)
                .build();
    }

    @Test
    @DisplayName("1. onStartup: если база пустая, выполняет синхронизацию всех категорий инструментов")
    void onStartup_whenDatabaseEmpty_syncsAllCategories() {
        when(repository.count()).thenReturn(0L);
        when(instrumentsServiceBlockingStub.shares(any())).thenReturn(
                SharesResponse.newBuilder().addInstruments(createShare()).build()
        );
        when(instrumentsServiceBlockingStub.bonds(any())).thenReturn(
                BondsResponse.newBuilder().addInstruments(createBond()).build()
        );
        when(instrumentsServiceBlockingStub.futures(any())).thenReturn(
                FuturesResponse.newBuilder().addInstruments(createFuture()).build()
        );
        when(instrumentsServiceBlockingStub.currencies(any())).thenReturn(
                CurrenciesResponse.newBuilder().addInstruments(createCurrency()).build()
        );
        when(instrumentsServiceBlockingStub.etfs(any())).thenReturn(
                EtfsResponse.newBuilder().addInstruments(createEtf()).build()
        );

        tinvestService.onStartup();

        verify(repository).count();
        verify(instrumentsServiceBlockingStub).shares(any());
        verify(instrumentsServiceBlockingStub).bonds(any());
        verify(instrumentsServiceBlockingStub).futures(any());
        verify(instrumentsServiceBlockingStub).currencies(any());
        verify(instrumentsServiceBlockingStub).etfs(any());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InstrumentEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(repository, atLeastOnce()).upsertAll(captor.capture());

        List<InstrumentEntity> allUpserted = captor.getAllValues().stream()
                .flatMap(List::stream)
                .toList();

        assertThat(allUpserted)
                .extracting(InstrumentEntity::instrumentUid)
                .contains("uid-share-1", "uid-bond-1", "uid-fut-1", "uid-cur-1", "uid-etf-1");
    }

    @Test
    @DisplayName("2. onStartup: если база уже заполнена, пропускает синхронизацию инструментов")
    void onStartup_whenDatabaseNotEmpty_skipsSync() {
        when(repository.count()).thenReturn(100L);

        tinvestService.onStartup();

        verify(repository).count();
        verifyNoInteractions(instrumentsServiceBlockingStub);
        verify(repository, never()).upsertAll(any());
        verify(repository, never()).upsert(any());
    }

    @Test
    @DisplayName("3. resolve: если тикер найден в локальной БД, возвращает сущность без вызова gRPC")
    void resolve_whenTickerInDatabase_returnsWithoutGrpcCall() {
        InstrumentEntity dbEntity = new InstrumentEntity(
                "uid-sber", "SBER", "TQBR", "share", "Сбербанк", 10, "RUB", true, Instant.now()
        );
        when(repository.findByTicker("SBER")).thenReturn(Optional.of(dbEntity));

        InstrumentEntity resolved = tinvestService.resolve("SBER");

        assertThat(resolved).isEqualTo(dbEntity);
        verify(repository).findByTicker("SBER");
        verifyNoInteractions(instrumentsServiceBlockingStub);
    }

    @Test
    @DisplayName("4. resolve: если инструмент не торгуется через API, выбрасывает IllegalArgumentException")
    void resolve_whenInstrumentNotTradable_throwsIllegalArgumentException() {
        InstrumentEntity nonTradable = new InstrumentEntity(
                "uid-sber", "SBER", "TQBR", "share", "Сбербанк", 10, "RUB", false, Instant.now()
        );
        when(repository.findByTicker("SBER")).thenReturn(Optional.of(nonTradable));

        assertThatThrownBy(() -> tinvestService.resolve("SBER"))
                .isInstanceOf(IllegalArgumentException.class);

        verify(repository).findByTicker("SBER");
        verifyNoInteractions(instrumentsServiceBlockingStub);

        when(repository.findByTicker("NOT_TRADABLE")).thenReturn(Optional.empty());
        when(instrumentsServiceBlockingStub.findInstrument(any())).thenReturn(
                FindInstrumentResponse.newBuilder()
                        .addInstruments(createShort("uid-nt", "NOT_TRADABLE", false))
                        .build()
        );

        assertThatThrownBy(() -> tinvestService.resolve("NOT_TRADABLE"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("5. resolve: если тикера нет в БД, находит его через gRPC и сохраняет в БД")
    void resolve_whenNotInDatabase_callsFindInstrumentAndSaves() {
        when(repository.findByTicker("SBER")).thenReturn(Optional.empty());
        InstrumentShort shortInst = createShort("uid-sber-new", "SBER", true);
        when(instrumentsServiceBlockingStub.findInstrument(any())).thenReturn(
                FindInstrumentResponse.newBuilder().addInstruments(shortInst).build()
        );

        InstrumentEntity resolved = tinvestService.resolve("SBER");

        assertThat(resolved).isNotNull();
        assertThat(resolved.instrumentUid()).isEqualTo("uid-sber-new");
        assertThat(resolved.ticker()).isEqualTo("SBER");
        assertThat(resolved.isTradable()).isTrue();

        ArgumentCaptor<InstrumentEntity> captor = ArgumentCaptor.forClass(InstrumentEntity.class);
        verify(repository).upsert(captor.capture());
        assertThat(captor.getValue().instrumentUid()).isEqualTo("uid-sber-new");
        assertThat(captor.getValue().ticker()).isEqualTo("SBER");
    }

    @Test
    @DisplayName("6. resolve: если инструмента нет ни в БД, ни на бирже, выбрасывает SymbolNotFoundException")
    void resolve_whenNotInDatabaseAndNotInExchange_throwsSymbolNotFound() {
        when(repository.findByTicker("UNKNOWN")).thenReturn(Optional.empty());
        when(instrumentsServiceBlockingStub.findInstrument(any())).thenReturn(
                FindInstrumentResponse.newBuilder().build()
        );

        assertThatThrownBy(() -> tinvestService.resolve("UNKNOWN"))
                .isInstanceOf(SymbolNotFoundException.class);

        verify(repository, never()).upsert(any());
    }

    @Test
    @DisplayName("7. resolve: при сетевой недоступности биржи выбрасывает ExchangeUnavailableException")
    void resolve_whenNotInDatabaseAndGrpcThrowsUnavailable_throwsExchangeUnavailableException() {
        when(repository.findByTicker("SBER")).thenReturn(Optional.empty());
        when(instrumentsServiceBlockingStub.findInstrument(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );

        assertThatThrownBy(() -> tinvestService.resolve("SBER"))
                .isInstanceOf(ExchangeUnavailableException.class);

        verify(repository, never()).upsert(any());
    }

    @Test
    @DisplayName("8. syncAllInstruments: при недоступности gRPC логирует ошибку и не падает с исключением")
    void syncAllInstruments_whenGrpcThrowsUnavailable_handlesGracefully() {
        when(instrumentsServiceBlockingStub.shares(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );
        when(instrumentsServiceBlockingStub.bonds(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );
        when(instrumentsServiceBlockingStub.futures(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );
        when(instrumentsServiceBlockingStub.currencies(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );
        when(instrumentsServiceBlockingStub.etfs(any())).thenThrow(
                new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Service unavailable"))
        );

        assertThatCode(() -> tinvestService.syncAllInstruments()).doesNotThrowAnyException();

        verify(repository, never()).upsertAll(any());
    }

    @Test
    @DisplayName("9. syncAllInstruments: при сбое одной категории продолжает обработку остальных")
    void syncAllInstruments_whenSingleCategoryFails_processesOtherCategories() {
        when(instrumentsServiceBlockingStub.shares(any())).thenThrow(
                new StatusRuntimeException(Status.INTERNAL.withDescription("Internal error in shares"))
        );
        when(instrumentsServiceBlockingStub.bonds(any())).thenReturn(
                BondsResponse.newBuilder().addInstruments(createBond()).build()
        );
        when(instrumentsServiceBlockingStub.futures(any())).thenReturn(
                FuturesResponse.newBuilder().addInstruments(createFuture()).build()
        );
        when(instrumentsServiceBlockingStub.currencies(any())).thenReturn(
                CurrenciesResponse.newBuilder().addInstruments(createCurrency()).build()
        );
        when(instrumentsServiceBlockingStub.etfs(any())).thenReturn(
                EtfsResponse.newBuilder().addInstruments(createEtf()).build()
        );

        assertThatCode(() -> tinvestService.syncAllInstruments()).doesNotThrowAnyException();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<InstrumentEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(repository, atLeastOnce()).upsertAll(captor.capture());

        List<InstrumentEntity> saved = captor.getAllValues().stream()
                .flatMap(List::stream)
                .toList();

        assertThat(saved)
                .extracting(InstrumentEntity::instrumentUid)
                .contains("uid-bond-1", "uid-fut-1", "uid-cur-1", "uid-etf-1")
                .doesNotContain("uid-share-1");
    }
}
