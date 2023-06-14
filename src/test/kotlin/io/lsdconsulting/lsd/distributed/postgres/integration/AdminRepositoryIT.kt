package io.lsdconsulting.lsd.distributed.postgres.integration

import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.TestApplication
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository.TestRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import io.zonky.test.db.AutoConfigureEmbeddedDatabase
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment
import org.springframework.test.context.ActiveProfiles
import java.time.ZoneId
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
}
