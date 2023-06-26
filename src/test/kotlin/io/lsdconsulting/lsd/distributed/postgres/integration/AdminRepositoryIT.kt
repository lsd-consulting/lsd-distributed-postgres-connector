package io.lsdconsulting.lsd.distributed.postgres.integration

import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.config.log
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.TestApplication
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository.TestRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import io.zonky.test.db.AutoConfigureEmbeddedDatabase
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.apache.commons.lang3.RandomUtils
import org.apache.commons.lang3.RandomUtils.nextInt
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.ZonedDateTime.now
import java.time.temporal.ChronoUnit
import java.util.*
import javax.sql.DataSource


@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, classes = [TestApplication::class])
@ActiveProfiles("spring-datasource")
@AutoConfigureEmbeddedDatabase(
    provider = AutoConfigureEmbeddedDatabase.DatabaseProvider.ZONKY,
    refresh = AutoConfigureEmbeddedDatabase.RefreshMode.AFTER_EACH_TEST_METHOD,
    type = AutoConfigureEmbeddedDatabase.DatabaseType.POSTGRES
)
internal class AdminRepositoryIT {

    @Autowired
    private lateinit var testRepository: TestRepository

    @Autowired
    private lateinit var dataSource: DataSource

    @Autowired
    private lateinit var interceptedDocumentPostgresRepository: InterceptedDocumentPostgresRepository

    @Autowired
    private lateinit var underTest: InterceptedDocumentPostgresAdminRepository

    private val primaryTraceId = randomAlphanumeric(10)
    private val secondaryTraceId = randomAlphanumeric(10)
    private val sourceName = randomAlphanumeric(10).uppercase(Locale.getDefault())
    private val targetName = randomAlphanumeric(10).uppercase(Locale.getDefault())

    @BeforeEach
    fun setup() {
        testRepository.createTable(dataSource)
    }

    @Test
    fun `should retrieve flows`() {
        val initialInterceptedInteraction = InterceptedInteraction(
            traceId = primaryTraceId,
            httpMethod = "GET",
            path = "/api-listener?message=from_test",
            serviceName = sourceName,
            target = targetName,
            interactionType = InteractionType.REQUEST,
            elapsedTime = 0,
            createdAt = now(ZoneId.of("UTC")).minusSeconds(4).truncatedTo(ChronoUnit.MILLIS)
        )
        interceptedDocumentPostgresRepository.save(initialInterceptedInteraction)

        interceptedDocumentPostgresRepository.save(
            InterceptedInteraction(
                traceId = primaryTraceId,
                serviceName = sourceName,
                target = targetName,
                path = targetName,
                interactionType = InteractionType.RESPONSE,
                elapsedTime = 10L,
                httpStatus = "200 OK",
                createdAt = now(ZoneId.of("UTC")).minusSeconds(3).truncatedTo(ChronoUnit.MILLIS)
            )
        )
        interceptedDocumentPostgresRepository.save(
            InterceptedInteraction(
                traceId = primaryTraceId,
                httpMethod = "POST",
                path = "/external-api?message=from_feign",
                serviceName = "TestApp",
                target = "UNKNOWN_TARGET",
                interactionType = InteractionType.REQUEST,
                elapsedTime = 0,
                createdAt = now(ZoneId.of("UTC")).minusSeconds(2).truncatedTo(ChronoUnit.MILLIS)
            )
        )

        val finalInterceptedInteraction = InterceptedInteraction(
            traceId = primaryTraceId,
            serviceName = "TestApp",
            target = "UNKNOWN_TARGET",
            path = "UNKNOWN_TARGET",
            interactionType = InteractionType.RESPONSE,
            elapsedTime = 20L,
            httpStatus = "200 OK",
            createdAt = now(ZoneId.of("UTC")).minusSeconds(1).truncatedTo(ChronoUnit.MILLIS)
        )
        interceptedDocumentPostgresRepository.save(finalInterceptedInteraction)

        val result = underTest.findRecentFlows(1)

        assertThat(result, hasSize(1))
        assertThat(result[0].initialInteraction, `is`(initialInterceptedInteraction))
        assertThat(result[0].finalInteraction, `is`(finalInterceptedInteraction))
        assertThat(result[0].totalCapturedInteractions, `is`(4))
    }

    @Test
    fun `should distinguish flows`() {
        log().info("primaryTraceId={}", primaryTraceId)
        log().info("secondaryTraceId={}", secondaryTraceId)
        val now = now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MILLIS)
        val primaryFlowInitialInterceptedInteraction = saveInterceptedInteraction(primaryTraceId, now.plusSeconds(1))
        val secondaryFlowInitialInterceptedInteraction = saveInterceptedInteraction(secondaryTraceId, now.plusSeconds(2))
        saveInterceptedInteraction(primaryTraceId, now.plusSeconds(3))
        val secondaryFlowFinalInterceptedInteraction = saveInterceptedInteraction(secondaryTraceId, now.plusSeconds(4))
        saveInterceptedInteraction(primaryTraceId, now.plusSeconds(5))
        val primaryFlowFinalInterceptedInteraction = saveInterceptedInteraction(primaryTraceId, now.plusSeconds(6))

        val result = underTest.findRecentFlows(2)

        assertThat(result, hasSize(2))
        assertThat(result[0].initialInteraction, `is`(primaryFlowInitialInterceptedInteraction))
        assertThat(result[0].finalInteraction, `is`(primaryFlowFinalInterceptedInteraction))
        assertThat(result[0].totalCapturedInteractions, `is`(4))
        assertThat(result[1].initialInteraction, `is`(secondaryFlowInitialInterceptedInteraction))
        assertThat(result[1].finalInteraction, `is`(secondaryFlowFinalInterceptedInteraction))
        assertThat(result[1].totalCapturedInteractions, `is`(2))
    }

    @Test
    fun `should respect the resultSizeLimit`() {
        log().info("primaryTraceId={}", primaryTraceId)
        log().info("secondaryTraceId={}", secondaryTraceId)
        val now = now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MILLIS)
        val primaryFlowInitialInterceptedInteraction = saveInterceptedInteraction(primaryTraceId, now.plusSeconds(1))
        saveInterceptedInteraction(secondaryTraceId, now.plusSeconds(2))
        saveInterceptedInteraction(primaryTraceId, now.plusSeconds(3))
        saveInterceptedInteraction(secondaryTraceId, now.plusSeconds(4))
        saveInterceptedInteraction(primaryTraceId, now.plusSeconds(5))
        val primaryFlowFinalInterceptedInteraction = saveInterceptedInteraction(primaryTraceId, now.plusSeconds(6))

        val result = underTest.findRecentFlows(1)

        assertThat(result, hasSize(1))
        assertThat(result[0].initialInteraction, `is`(primaryFlowInitialInterceptedInteraction))
        assertThat(result[0].finalInteraction, `is`(primaryFlowFinalInterceptedInteraction))
        assertThat(result[0].totalCapturedInteractions, `is`(4))
    }

    @Test
    fun `should retrieve recent flows according to createdAt`() {
        val now = now(ZoneId.of("UTC"))
        (1..5).forEach { flow ->
            saveInterceptedInteraction("flow${flow}traceId", now.plusSeconds(6L - flow))
        }

        val result = underTest.findRecentFlows(2)

        assertThat(result, hasSize(2))
        assertThat(result[0].initialInteraction.traceId, `is`("flow1traceId"))
        assertThat(result[1].initialInteraction.traceId, `is`("flow2traceId"))
    }

    @Test
    fun `should retrieve recent multi-interaction flows according to createdAt`() {
        val now = now(ZoneId.of("UTC"))
        (1..5).forEach { flow ->
            (1..5).forEach { interaction ->
                saveInterceptedInteraction("traceId$flow", now.plusSeconds(10L * (6 - flow) + interaction))
                println("traceId$flow - ${10L * (5 - flow) + interaction}")
            }
        }

        val result = underTest.findRecentFlows(2)

        assertThat(result, hasSize(2))
        assertThat(result[0].initialInteraction.traceId, `is`("traceId1"))
        assertThat(result[0].initialInteraction.createdAt, `is`(now.plusSeconds(51).truncatedTo(ChronoUnit.MILLIS)))
        assertThat(result[0].finalInteraction.createdAt, `is`(now.plusSeconds(55).truncatedTo(ChronoUnit.MILLIS)))
        assertThat(result[1].initialInteraction.traceId, `is`("traceId2"))
        assertThat(result[1].initialInteraction.createdAt, `is`(now.plusSeconds(41).truncatedTo(ChronoUnit.MILLIS)))
        assertThat(result[1].finalInteraction.createdAt, `is`(now.plusSeconds(45).truncatedTo(ChronoUnit.MILLIS)))
    }

    private fun saveInterceptedInteraction(traceId: String, createdAt: ZonedDateTime): InterceptedInteraction {
        val interceptedInteraction = randomInterceptedInteraction()
            .copy(traceId = traceId, createdAt = createdAt.truncatedTo(ChronoUnit.MILLIS))
        interceptedDocumentPostgresRepository.save(interceptedInteraction)
        return interceptedInteraction
    }

    private fun randomInterceptedInteraction() = InterceptedInteraction(
            traceId = randomAlphanumeric(30),
                body = randomAlphanumeric(200),
                requestHeaders = mapOf(randomAlphanumeric(10) to listOf(randomAlphanumeric(20))),
                responseHeaders = mapOf(randomAlphanumeric(10) to listOf(randomAlphanumeric(20))),
                serviceName = randomAlphanumeric(30),
                target = randomAlphanumeric(30),
                path = randomAlphanumeric(100),
                httpStatus = randomAlphanumeric(35),
                httpMethod = randomAlphanumeric(7),
                interactionType = InteractionType.values()[nextInt(0,InteractionType.values().size - 1)],
                profile = randomAlphanumeric(10),
                elapsedTime = RandomUtils.nextLong(),
                createdAt = now(ZoneId.of("UTC")),
    )
}
