package com.outprf.batch.service.synchronization;

import com.outprf.batch.service.errors.BusinessErrorCode;
import com.outprf.batch.service.errors.BusinessException;
import com.outprf.core.domain.synchronization.SynchronizationRequest;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

public class DownloadOptionImpliedVolatilitiesProcess implements Runnable {

    public static final String NO_HEADER_DIR = "/05_no_header";
    private final DerivedOptionImpliedVolatilitiesService derivedOptionImpliedVolatilitiesService;
    private final SynchronizationRequest request;
    private final String ticker;
    private final String mTicker;
    private final PrintWriter errorWriter;

    public DownloadOptionImpliedVolatilitiesProcess(SynchronizationRequest request,
                                                    DerivedOptionImpliedVolatilitiesService derivedOptionImpliedVolatilitiesService,
                                                    String ticker,
                                                    String mTicker,
                                                    PrintWriter errorWriter) {
        this.request = request;
        this.derivedOptionImpliedVolatilitiesService = derivedOptionImpliedVolatilitiesService;
        this.ticker = ticker;
        this.mTicker = mTicker;
        this.errorWriter = errorWriter;
    }

    @Override
    public final void run() {
        try {
            File synchDir = new File(request.getSynchDirPath());
            File noHeaderDir = new File(synchDir + NO_HEADER_DIR);
            String tickerForUrl = ticker.replace('.', '_').replace('-', '_');
            URL url = new URL(getTickerVolUrlString(tickerForUrl)); // create url object for the given string
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            int responseCode = connection.getResponseCode(); //this is http response code
            if (responseCode != 200) {
                System.out.println("Response code while fetching historical volume data: [ " + responseCode + " ], ticker " + ticker);
                throw new BusinessException(BusinessErrorCode.QUANDL_SYNCHRONIZATION_REJECTED, "Export rejected with " + ticker + " HTTP code =" + responseCode);
            }
            File targetFile = new File(noHeaderDir.getPath() + "/" + FilenameUtils.getName(url.getPath()));

            try (InputStream input = connection.getInputStream();
                 BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile))) {
                String separator = System.lineSeparator();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
                String headers = bufferedReader.readLine() + separator;
                if (!headers.startsWith("Date")) {
                    writer.write(headers);
                }

                String next;
                while ((next = bufferedReader.readLine()) != null) {
                    String values = ticker + "," + mTicker + "," + next + separator;
                    writer.write(values);
                }

                writer.flush();
                bufferedReader.close();
            } catch (Exception e) {
                errorWriter.println(ticker + " ticker, responseCode = " + responseCode + ", Exception in inputstream while fetching historical volume data: " + Arrays.toString(e.getStackTrace()));
            }
        } catch (Exception e) {
            errorWriter.println(ticker + " ticker, Exception in url or connection while fetching historical volume data: " + Arrays.toString(e.getStackTrace()));
        }
    }

    private String getTickerVolUrlString(String ticker) {
        return derivedOptionImpliedVolatilitiesService.getTickerVolUrlString(ticker);
    }

}
