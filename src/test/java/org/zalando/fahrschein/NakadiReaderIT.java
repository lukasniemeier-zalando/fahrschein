package org.zalando.fahrschein;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.zalando.fahrschein.domain.Partition;
import org.zalando.fahrschein.salesorder.domain.SalesOrderPlaced;
import org.zalando.jackson.datatype.money.MoneyModule;
import org.zalando.problem.ProblemModule;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NakadiReaderIT {

    @Test(expected = CancellationException.class, timeout = 12000)
    public void shouldNotHangOnCloseBeforeReconnecting()
            throws IOException, URISyntaxException, InterruptedException, ExponentialBackoffException {

        // There has to be some event on the following topic...
        final URI baseUri = new URI("https://nakadi-sandbox-hila.aruha-test.zalan.do");
        final String eventName = "sales-order-service.order-placed";

        final AtomicInteger counter = new AtomicInteger(0);
        final Listener<SalesOrderPlaced> listener = events -> {
            if (counter.incrementAndGet() < 2) {
                throw new IOException("Simulating event processing error...");
            } else {
                throw new CancellationException("Expected: reconnecting successful.");
            }
        };

        final RequestConfig config = RequestConfig.custom()
                .setSocketTimeout(30000)
                .setConnectionRequestTimeout(5000)
                .build();

        final CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionTimeToLive(5, TimeUnit.SECONDS)
                .disableAutomaticRetries()
                .setDefaultRequestConfig(config)
                .disableRedirectHandling()
                .build();

        final ObjectMapper mapper = mapper();
        final InMemoryCursorManager cursorManager = new InMemoryCursorManager();
        final NakadiClient nakadiClient = new NakadiClient(baseUri,
                new AuthorizedClientHttpRequestFactory(
                        new ProblemHandlingClientHttpRequestFactory(
                                new HttpComponentsClientHttpRequestFactory(httpClient),
                                mapper),
                        new ZignAccessTokenProvider()),
                new ExponentialBackoffStrategy(),
                mapper,
                cursorManager);

        final List<Partition> partitions = nakadiClient.getPartitions(eventName);
        cursorManager.fromOldestAvailableOffset(eventName, partitions);
        nakadiClient.listen(eventName, SalesOrderPlaced.class, listener, new StreamParameters().withStreamTimeout(5 * 60 * 1000));
    }

    private static ObjectMapper mapper() {
        final ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.disable(MapperFeature.DEFAULT_VIEW_INCLUSION);

        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new MoneyModule());
        objectMapper.registerModule(new ProblemModule());
        objectMapper.registerModule(new GuavaModule());
        objectMapper.registerModule(new ParameterNamesModule());
        return objectMapper;
    }
}
