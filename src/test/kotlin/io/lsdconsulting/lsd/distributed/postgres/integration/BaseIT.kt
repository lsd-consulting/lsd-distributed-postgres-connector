package io.lsdconsulting.lsd.distributed.postgres.integration

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.TestApplication
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository.TestRepository
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.testcontainers.containers.PostgreSQLContainer
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.MILLIS

private const val POSTGRES_PORT = 5432
private const val POSTGRES_IMAGE = "postgres:15.3-alpine3.18"
private const val TABLE_NAME = "lsd_database"

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [TestApplication::class])
internal open class BaseIT {

    @Autowired
    lateinit var testRepository: TestRepository


    internal fun buildInterceptedInteraction(traceId: String) = InterceptedInteraction(
        traceId = traceId,
        body = RandomStringUtils.randomAlphanumeric(100),
        requestHeaders = mapOf(),
        responseHeaders = mapOf(),
        serviceName = RandomStringUtils.randomAlphanumeric(30),
        target = RandomStringUtils.randomAlphanumeric(30),
        path = RandomStringUtils.randomAlphanumeric(100),
        httpStatus = HttpStatus.values()[RandomUtils.nextInt(0, HttpStatus.values().size - 1)].name,
        httpMethod = HttpMethod.values()[RandomUtils.nextInt(0, HttpMethod.values().size - 1)].name,
        interactionType = InteractionType.values()[RandomUtils.nextInt(0, InteractionType.values().size - 1)],
        profile = RandomStringUtils.randomAlphanumeric(20),
        elapsedTime = RandomUtils.nextLong(),
        createdAt = ZonedDateTime.now(ZoneId.of("UTC")).truncatedTo(MILLIS)
    )

    companion object {
        var postgreSQLContainer: PostgreSQLContainer<*> = PostgreSQLContainer(POSTGRES_IMAGE)
            .withDatabaseName(TABLE_NAME)
            .withUsername("sa")
            .withPassword("sa")
            .withExposedPorts(POSTGRES_PORT)
            .withCreateContainerCmdModifier { cmd ->
                cmd.withHostConfig(
                    HostConfig().withPortBindings(
                        PortBinding(
                            Ports.Binding.bindPort(POSTGRES_PORT), ExposedPort(
                                POSTGRES_PORT
                            )
                        )
                    )
                )
            }

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            postgreSQLContainer.start()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            postgreSQLContainer.stop()
        }
    }
}