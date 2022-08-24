package com.outprf.batch.service.synchronization;

import com.outprf.batch.service.AsyncService;
import com.outprf.batch.service.future.FutureService;
import com.outprf.core.domain.enumeration.SynchronizationStatus;
import com.outprf.core.domain.enumeration.TradeSector;
import com.outprf.core.domain.screener.ScreenerTableData;
import com.outprf.core.domain.synchronization.SynchronizationRequest;
import com.outprf.core.repository.ScreenerTableDataRepository;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DerivedOptionImpliedVolatilitiesProcess extends AbstractSynchronizationProcess {

    private final DerivedOptionImpliedVolatilitiesService derivedOptionImpliedVolatilitiesService;
    private final ScreenerTableDataRepository screenerTableDataRepository;
    private final AsyncService asyncService;
    private final FutureService futureService;

    private static final Integer THREAD_COUNT = 20;

    public DerivedOptionImpliedVolatilitiesProcess(SynchronizationRequest request,
                                                   DerivedOptionImpliedVolatilitiesService derivedOptionImpliedVolatilitiesService,
                                                   ScreenerTableDataRepository screenerTableDataRepository,
                                                   AsyncService asyncService, FutureService futureService) {
        super(request);
        this.derivedOptionImpliedVolatilitiesService = derivedOptionImpliedVolatilitiesService;
        this.screenerTableDataRepository = screenerTableDataRepository;
        this.asyncService = asyncService;
        this.futureService = futureService;
    }

    @Override
    protected void doRun(SynchronizationRequest request) {
        List<ScreenerTableData> allScreener = screenerTableDataRepository.findAll();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        Collection<Future<?>> futures = new HashSet<>();

        // Downloading all files
        allScreener.forEach(screener -> {
            Runnable proc = new DownloadOptionImpliedVolatilitiesProcess(request,
                    derivedOptionImpliedVolatilitiesService,
                    screener.getTicker(), screener.getmTicker(), errorWriter);
            Future<?> future = executorService.submit(proc);
            futures.add(future);
        });
        futureService.launchThreadsAndWait(executorService, futures, List.of(request));

        // importing all files to db
        Map<String, String> topTickers = allScreener
                .stream()
                .filter(ss -> ss.getMktVal() != null)
                .sorted(Comparator.comparing(ScreenerTableData::getMktVal).reversed())
                .limit(500L)
                .collect(Collectors.toMap(ScreenerTableData::getmTicker, ScreenerTableData::getTicker, (r1, r2) -> r1));
        topTickers.putAll(Arrays.stream(TradeSector.values()).map(Enum::name).collect(Collectors.toMap(Function.identity(), Function.identity())));
        Map<String, String> tickersWithoutTop500Tic = allScreener.stream()
                .filter(screenerTableData -> !topTickers.containsValue(screenerTableData.getTicker()))
                .collect(Collectors.toMap(ScreenerTableData::getmTicker, ScreenerTableData::getTicker, (r1, r2) -> r1));

        try {
            derivedOptionImpliedVolatilitiesService.loadVolToDatabase(request, futureService,
                    asyncService, topTickers, errorWriter);
            derivedOptionImpliedVolatilitiesService.loadVolToDatabase(request, futureService,
                    asyncService, tickersWithoutTop500Tic, errorWriter);

            // delete folder with files
            derivedOptionImpliedVolatilitiesService.removeTempFiles(request);
        } catch (IOException | InterruptedException ex) {
            errorWriter.println(ex.getMessage() + " :: " + Arrays.toString(ex.getStackTrace()));
        }
        request.setStatus(SynchronizationStatus.COMPLETED);
    }
}
