package org.rtdl;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
public class IngestServiceTest {

    @Test
    public void testHelloEndpoint() {
        given()
          .when().post("/ingest")
          .then()
             .statusCode(200)
             .body(is("Post Received"));
    }

}