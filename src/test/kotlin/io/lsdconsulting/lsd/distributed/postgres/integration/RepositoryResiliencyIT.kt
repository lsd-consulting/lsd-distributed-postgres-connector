package io.lsdconsulting.lsd.distributed.postgres.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.apache.commons.lang3.RandomUtils.nextLong
import org.awaitility.Awaitility.await
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Value
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

@ActiveProfiles("resiliency")
internal class RepositoryResiliencyIT : BaseIT() {

    @Value("\${lsd.dist.connectionString}")
    private lateinit var dbConnectionString: String

    @BeforeEach
    fun setup() {
        postgreSQLContainer.start()
        val config = HikariConfig()
        config.jdbcUrl = dbConnectionString
        config.driverClassName = "org.postgresql.Driver"
        testRepository.createTable(HikariDataSource(config))
    }

    @Test
    fun `should handle db being down gracefully on startup`() {
        InterceptedDocumentPostgresRepository(
            dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
            objectMapper = ObjectMapper(),
            traceIdMaxLength = 32,
            bodyMaxLength = 10000,
            requestHeadersMaxLength = 10000,
            responseHeadersMaxLength = 10000,
            serviceNameMaxLength = 200,
            targetMaxLength = 200,
            pathMaxLength = 200,
            httpStatusMaxLength = 35,
            httpMethodMaxLength = 7,
            profileMaxLength = 20,
            maxNumberOfInteractionsToQuery = 100,
        )
    }

    @Test
    fun `should fail on startup when db down and failOnConnectionError set to true`() {
        assertThrows<HikariPool.PoolInitializationException> {
            InterceptedDocumentPostgresRepository(
                dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
                objectMapper = ObjectMapper(),
                failOnConnectionError = true,
                traceIdMaxLength = 32,
                bodyMaxLength = 10000,
                requestHeadersMaxLength = 10000,
                responseHeadersMaxLength = 10000,
                serviceNameMaxLength = 200,
                targetMaxLength = 200,
                pathMaxLength = 200,
                httpStatusMaxLength = 35,
                httpMethodMaxLength = 7,
                profileMaxLength = 20,
                maxNumberOfInteractionsToQuery = 100,
            )
        }
    }

    @Test
    fun `should not slow down startup if db down`() {
        await()
            .atLeast(450, MILLISECONDS)
            .atMost(1250, MILLISECONDS)
            .untilAsserted {
                assertThat(
                    InterceptedDocumentPostgresRepository(
                        dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
                        objectMapper = ObjectMapper(),
                        traceIdMaxLength = 32,
                        bodyMaxLength = 10000,
                        requestHeadersMaxLength = 10000,
                        responseHeadersMaxLength = 10000,
                        serviceNameMaxLength = 200,
                        targetMaxLength = 200,
                        pathMaxLength = 200,
                        httpStatusMaxLength = 35,
                        httpMethodMaxLength = 7,
                        profileMaxLength = 20,
                        maxNumberOfInteractionsToQuery = 100,
                    ), `is`(
                        notNullValue()
                    )
                )
            }
    }

    @Test
    fun `should handle db going down after startup`() {
        val repository = InterceptedDocumentPostgresRepository(
            dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
            objectMapper = ObjectMapper(),
            traceIdMaxLength = 32,
            bodyMaxLength = 10000,
            requestHeadersMaxLength = 10000,
            responseHeadersMaxLength = 10000,
            serviceNameMaxLength = 200,
            targetMaxLength = 200,
            pathMaxLength = 200,
            httpStatusMaxLength = 35,
            httpMethodMaxLength = 7,
            profileMaxLength = 20,
            maxNumberOfInteractionsToQuery = 100,
        )
        val primaryTraceId = randomAlphanumeric(10)
        val interceptedInteraction = buildInterceptedInteraction(primaryTraceId)
        repository.save(interceptedInteraction)
        val initialResult = repository.findByTraceIds("traceId")
        assertThat(initialResult, `is`(notNullValue()))
        postgreSQLContainer.stop()
        val result = repository.findByTraceIds("traceId")
        assertThat(result, `is`(empty()))
    }

    @Test
    fun `should recover from db going down`() {
        val repository = InterceptedDocumentPostgresRepository(
            dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
            objectMapper = ObjectMapper(),
            traceIdMaxLength = 32,
            bodyMaxLength = 10000,
            requestHeadersMaxLength = 10000,
            responseHeadersMaxLength = 10000,
            serviceNameMaxLength = 200,
            targetMaxLength = 200,
            pathMaxLength = 200,
            httpStatusMaxLength = 35,
            httpMethodMaxLength = 7,
            profileMaxLength = 20,
            maxNumberOfInteractionsToQuery = 100,
        )
        val primaryTraceId = randomAlphanumeric(10)
        val interceptedInteraction = buildInterceptedInteraction(primaryTraceId)
        repository.save(interceptedInteraction)
        postgreSQLContainer.stop()
        val result = repository.findByTraceIds("traceId")
        assertThat(result, `is`(empty()))
        postgreSQLContainer.start()
        val initialResult = repository.findByTraceIds("traceId")
        assertThat(initialResult, `is`(notNullValue()))
    }

    @Test
    fun `should not slow down production`() {
        val repository = InterceptedDocumentPostgresRepository(
            dbConnectionString = "jdbc:postgresql://localhost:${nextLong(6000, 7000)}/",
            objectMapper = ObjectMapper(),
            traceIdMaxLength = 32,
            bodyMaxLength = 10000,
            requestHeadersMaxLength = 10000,
            responseHeadersMaxLength = 10000,
            serviceNameMaxLength = 200,
            targetMaxLength = 200,
            pathMaxLength = 200,
            httpStatusMaxLength = 35,
            httpMethodMaxLength = 7,
            profileMaxLength = 20,
            maxNumberOfInteractionsToQuery = 100,
        )
        val primaryTraceId = randomAlphanumeric(10)
        val secondaryTraceId = randomAlphanumeric(10)
        val primaryInterceptedInteraction = buildInterceptedInteraction(primaryTraceId)
        val secondaryInterceptedInteraction = buildInterceptedInteraction(secondaryTraceId)
        repository.save(primaryInterceptedInteraction)
        repository.save(secondaryInterceptedInteraction)
        repository.findByTraceIds(primaryTraceId)

        postgreSQLContainer.stop()

        await()
            .atLeast(50, MILLISECONDS)
            .atMost(600, MILLISECONDS)
            .untilAsserted {
                assertThat(
                    repository.findByTraceIds(secondaryTraceId),
                    `is`(empty())
                )
            }
    }

    @Test
    fun `should not slow down production with large data sets`() {
        val repository = InterceptedDocumentPostgresRepository(
            dbConnectionString = dbConnectionString,
            objectMapper = ObjectMapper(),
            traceIdMaxLength = 32,
            bodyMaxLength = 10000,
            requestHeadersMaxLength = 10000,
            responseHeadersMaxLength = 10000,
            serviceNameMaxLength = 200,
            targetMaxLength = 200,
            pathMaxLength = 200,
            httpStatusMaxLength = 35,
            httpMethodMaxLength = 7,
            profileMaxLength = 20,
            maxNumberOfInteractionsToQuery = 100,
        )
        val traceId = randomAlphanumeric(10)
        val interceptedInteraction = List(100_000) { buildInterceptedInteraction(traceId) }
        interceptedInteraction.forEach(repository::save)

        assertThat(repository.findByTraceIds(traceId), hasSize(100))
    }
}
