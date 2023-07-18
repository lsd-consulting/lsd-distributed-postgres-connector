package io.lsdconsulting.lsd.distributed.postgres.integration

import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
import java.time.ZonedDateTime.now
import java.time.temporal.ChronoUnit.MILLIS
import javax.sql.DataSource

@ActiveProfiles("spring-datasource")
internal class RepositoryWithDataSourceIT: BaseIT() {

    @Autowired
    private lateinit var dataSource: DataSource

    @Autowired
    private lateinit var underTest: InterceptedDocumentPostgresRepository

    @BeforeEach
    fun setup() {
        testRepository.createTable(dataSource)
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
            createdAt = now(ZoneId.of("UTC")).truncatedTo(MILLIS)
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
}
