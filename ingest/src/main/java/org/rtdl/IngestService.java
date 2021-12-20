package org.rtdl;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/ingest")
public class IngestService {

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Post Received";
    }
}