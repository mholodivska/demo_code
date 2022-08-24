package com.outprf.batch.service.synchronization;

import com.outprf.batch.config.ApplicationProperties;
import com.outprf.batch.service.AsyncService;
import com.outprf.batch.service.future.FutureService;
import com.outprf.core.domain.enumeration.SynchronizationStatus;
import com.outprf.core.domain.synchronization.SynchronizationRequest;
import org.apache.commons.io.FileUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service
public class DerivedOptionImpliedVolatilitiesService {

    public static final String NO_HEADER_DIR = "/05_no_header";
    private final ApplicationProperties applicationProperties;
    private final MongoTemplate mongoTemplate;


    private static final String COLLECTION_NAME = "historical_option_vol";
    private static final String FILE_WITH_FIELDS = "historical_option_vol.txt";
    private static final Integer THREAD_COUNT = 20;

    public DerivedOptionImpliedVolatilitiesService(MongoTemplate mongoTemplate,
                                                   ApplicationProperties applicationProperties) {
        this.mongoTemplate = mongoTemplate;
        this.applicationProperties = applicationProperties;
    }

    public String getTickerVolUrlString(String ticker) {
        return String.format(applicationProperties.getQuandl().getDownloadVolApiUrlTemplate(), ticker);
    }

    public void loadVolToDatabase(SynchronizationRequest request,
                                  FutureService futureService,
                                  AsyncService asyncService, Map<String, String> tickerNames,
                                  PrintWriter errorWriter) throws IOException, InterruptedException {
        String loadFileTodbCommandWithoutDrop = applicationProperties.getSynchronization().getLoadFileTodbCommandWithoutDrop();
        String loadCommand;
        String synchDirLinuxPath = "/" + request.getSynchDirPath().replace("\\", "/").replace(":", "");
        String pathToHolder = synchDirLinuxPath + NO_HEADER_DIR;
        Collection<Future<?>> futures = new HashSet<>();

        if (Objects.requireNonNull(new File(pathToHolder).list()).length > 0) {
            List<String> fileNames = Arrays.stream(Objects.requireNonNull(new File(pathToHolder).list())).collect(Collectors.toList());
            ExecutorService executorServiceCalc = Executors.newFixedThreadPool(THREAD_COUNT);

            for (Map.Entry<String, String> tickerInfo : tickerNames.entrySet()) {
                String filename = tickerInfo.getValue().replace('.', '_').replace('-', '_') + ".csv";
                if (fileNames.contains(filename)) {
                    String pathToCsvFile = pathToHolder + "/" + filename;
                    loadCommand = String.format(loadFileTodbCommandWithoutDrop, mongoTemplate.getDb().getName(), COLLECTION_NAME, pathToCsvFile, FILE_WITH_FIELDS);
                    Path workingDirectory = FileSystems.getDefault().getPath(applicationProperties.getSynchronization().getWorkingDir()).toAbsolutePath();
                    Process proc = Runtime.getRuntime().exec(loadCommand, null, workingDirectory.toFile());
                    int exitVal = proc.waitFor();

                    if (0 == exitVal) {
                        Runnable calc = new CalculateOptionImpliedVolatilitiesProcess(request,
                                asyncService, tickerInfo.getKey());
                        Future<?> future = executorServiceCalc.submit(calc);
                        futures.add(future);
                    } else {
                        request.setStatus(SynchronizationStatus.ERROR);
                        errorWriter.println(Instant.now().toString() + " Derived-Option-Implied-Volatilities-Process-error loading file to database; Process exitValue: " + exitVal);
                    }
                }
            }
            futureService.launchThreadsAndWait(executorServiceCalc, futures, List.of(request), FutureService.MAX_TASK_EXPECTED_DURATION_MS);
        }
    }

    public void removeTempFiles(SynchronizationRequest request) throws IOException {
        String synchDirLinuxPath = "/" + request.getSynchDirPath().replace("\\", "/").replace(":", "");
        String pathToHolder = synchDirLinuxPath + NO_HEADER_DIR;
        request.setStatusDetail(String.format("Updated %s tickers", Objects.requireNonNull(new File(pathToHolder).list()).length));
        FileUtils.deleteDirectory(new File(pathToHolder));
    }
}
