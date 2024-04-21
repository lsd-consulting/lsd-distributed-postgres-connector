package io.lsdconsulting.lsd.distributed.postgres.integration

import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import lsd.logging.log
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasSize
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.ZonedDateTime.now
import java.time.temporal.ChronoUnit
import java.util.*
import javax.sql.DataSource

@ActiveProfiles("admin")
internal class AdminRepositoryIT: BaseIT() {

    @Autowired
    lateinit var underTest: InterceptedDocumentPostgresAdminRepository

    @Autowired
    lateinit var interceptedDocumentPostgresRepository: InterceptedDocumentPostgresRepository

    @Autowired
    private lateinit var dataSource: DataSource

    private val primaryTraceId = randomAlphanumeric(10)
    private val secondaryTraceId = randomAlphanumeric(10)
    private val sourceName = randomAlphanumeric(10).uppercase(Locale.getDefault())
    private val targetName = randomAlphanumeric(10).uppercase(Locale.getDefault())

    @BeforeEach
    fun setup() {
        testRepository.createTable(dataSource)
        testRepository.clearTable(dataSource)
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
        val interceptedInteraction = buildInterceptedInteraction(traceId = traceId)
            .copy(createdAt = createdAt.truncatedTo(ChronoUnit.MILLIS))
        interceptedDocumentPostgresRepository.save(interceptedInteraction)
        return interceptedInteraction
    }
}
