package io.lsdconsulting.lsd.distributed.postgres.integration

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.trimToSize
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasSize
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
import java.time.ZonedDateTime.now
import java.time.temporal.ChronoUnit

@ActiveProfiles("lsd-datasource")
internal class RepositoryWithNoDataSourceIT: BaseIT() {

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
            createdAt = now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.MILLIS)
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
    fun `should save and retrieve trimmed random entry from database`() {
        val traceId = randomAlphanumeric(10)
        val interceptedInteraction = buildInterceptedInteraction(traceId)

        underTest.save(interceptedInteraction)

        val result = underTest.findByTraceIds(interceptedInteraction.traceId)
        assertThat(result, hasSize(1))
        assertThat(result[0].elapsedTime, `is`(interceptedInteraction.elapsedTime))
        assertThat(result[0].httpStatus, `is`(interceptedInteraction.httpStatus?.trimToSize(10)))
        assertThat(result[0].path, `is`(interceptedInteraction.path.trimToSize(10)))
        assertThat(result[0].httpMethod, `is`(interceptedInteraction.httpMethod?.trimToSize(10)))
        assertThat(result[0].body, `is`(interceptedInteraction.body?.trimToSize(10)))
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
