package org.zalando.fahrschein;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

public class IOProblem extends IOException implements Problem {
    private final URI type;
    private final String title;
    private final Response.StatusType status;
    private final Optional<String> detail;
    private final Optional<URI> instance;

    @JsonCreator
    public IOProblem(@JsonProperty("type") URI type, @JsonProperty("title") String title, @JsonProperty("status") int status, @JsonProperty("detail") Optional<String> detail, @JsonProperty("instance") Optional<URI> instance) {
        super(formatMessage(type, title, status));
        this.type = type;
        this.title = title;
        this.status = Response.Status.fromStatusCode(status);
        this.detail = detail;
        this.instance = instance;
    }

    public IOProblem(URI type, String title, int status) {
        this(type, title, status, Optional.<String>empty(), Optional.<URI>empty());
    }

    private static String formatMessage(URI type, String title, int status) {
        return String.format("Problem [%s]: Status %d [%s]", type, status, title);
    }

    @Override
    public URI getType() {
        return type;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public Response.StatusType getStatus() {
        return status;
    }

    @Override
    public Optional<String> getDetail() {
        return detail;
    }

    @Override
    public Optional<URI> getInstance() {
        return instance;
    }
}
