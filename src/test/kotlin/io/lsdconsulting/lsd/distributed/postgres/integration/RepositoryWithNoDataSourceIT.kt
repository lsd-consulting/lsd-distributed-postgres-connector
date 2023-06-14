package io.lsdconsulting.lsd.distributed.postgres.integration

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.TestApplication
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository.TestRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.apache.commons.lang3.RandomUtils.nextInt
import org.apache.commons.lang3.RandomUtils.nextLong
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
import java.time.ZonedDateTime.now


@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, classes = [TestApplication::class])
@ActiveProfiles("lsd-datasource")
@Disabled // This test only works with a real instance of the db, eg. in Docker
internal class RepositoryWithNoDataSourceIT {

    @Autowired
    private lateinit var testRepository: TestRepository

    @Value("\${lsd.dist.connectionString}")
    private lateinit var dbConnectionString: String

    @Autowired
    private lateinit var underTest: InterceptedDocumentPostgresRepository

    @BeforeEach
    fun setup() {
        val config = HikariConfig()
        config.jdbcUrl = dbConnectionString
        config.driverClassName = "org.postgresql.Driver"
        testRepository.createTable(HikariDataSource(config))
    }

    @Test
    fun `should save and retrieve from database`() {
        val interceptedInteraction = InterceptedInteraction(
            elapsedTime = 20L,
            httpStatus = "OK",
            serviceName = "service",
            target = "target",
            path = "/path",
            httpMethod = "GET",
            body = "body",
            interactionType = InteractionType.REQUEST,
            traceId = randomAlphanumeric(6),
            createdAt = now(ZoneId.of("UTC"))
        )

        underTest.save(interceptedInteraction)

        val result = underTest.findByTraceIds(interceptedInteraction.traceId)
        assertThat(result, hasSize(1))
        assertThat(result[0].elapsedTime, `is`(20L))
        assertThat(result[0].httpStatus, `is`("OK"))
        assertThat(result[0].path, `is`("/path"))
        assertThat(result[0].httpMethod, `is`("GET"))
        assertThat(result[0].body, `is`("body"))
        assertThat(result[0].interactionType, `is`(InteractionType.REQUEST))
        assertThat(result[0].createdAt, `is`(interceptedInteraction.createdAt))
    }

    @Test
    fun `should save and retrieve random entry from database`() {
        val traceId = randomAlphanumeric(10)
        val interceptedInteraction = buildInterceptedInteraction(traceId)

        underTest.save(interceptedInteraction)

        val result = underTest.findByTraceIds(interceptedInteraction.traceId)
        assertThat(result, hasSize(1))
        assertThat(result[0].elapsedTime, `is`(interceptedInteraction.elapsedTime))
        assertThat(result[0].httpStatus, `is`(interceptedInteraction.httpStatus))
        assertThat(result[0].path, `is`(interceptedInteraction.path))
        assertThat(result[0].httpMethod, `is`(interceptedInteraction.httpMethod))
        assertThat(result[0].body, `is`(interceptedInteraction.body))
        assertThat(result[0].interactionType, `is`(interceptedInteraction.interactionType))
        assertThat(result[0].createdAt, `is`(interceptedInteraction.createdAt))
    }

    @Test
    fun `should save and retrieve in correct order`() {
        val traceId = randomAlphanumeric(10)
        val interceptedInteractions = (1..10)
            .map { _ -> buildInterceptedInteraction(traceId) }
            .sortedByDescending { it.createdAt }

        interceptedInteractions.forEach { underTest.save(it) }

        val result = underTest.findByTraceIds(traceId)
        assertThat(result, hasSize(10))
        (1..10).forEach { assertThat(result[it - 1].createdAt, `is`(interceptedInteractions[10 - it].createdAt)) }
    }

    private fun buildInterceptedInteraction(traceId: String) = InterceptedInteraction(
        traceId = traceId,
        body = randomAlphanumeric(100),
        requestHeaders = mapOf(),
        responseHeaders = mapOf(),
        serviceName = randomAlphanumeric(30),
        target = randomAlphanumeric(30),
        path = randomAlphanumeric(100),
        httpStatus = HttpStatus.values()[nextInt(0, HttpStatus.values().size - 1)].name,
        httpMethod = HttpMethod.values()[nextInt(0, HttpMethod.values().size - 1)].name,
        interactionType = InteractionType.values()[nextInt(0, InteractionType.values().size - 1)],
        profile = randomAlphanumeric(20),
        elapsedTime = nextLong(),
        createdAt = now(ZoneId.of("UTC"))
    )
}
