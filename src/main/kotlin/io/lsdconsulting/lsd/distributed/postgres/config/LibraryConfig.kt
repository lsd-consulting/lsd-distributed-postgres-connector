package io.lsdconsulting.lsd.distributed.postgres.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import lsd.logging.log
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

const val DEFAULT_FAIL_ON_CONNECTION = true
const val DEFAULT_TRACE_ID_MAX_LENGTH = 32
const val DEFAULT_BODY_MAX_LENGTH = 10000
const val DEFAULT_REQUEST_HEADERS_MAX_LENGTH = 10000
const val DEFAULT_RESPONSE_HEADERS_MAX_LENGTH = 10000
const val DEFAULT_SERVICE_NAME_MAX_LENGTH = 200
const val DEFAULT_TARGET_MAX_LENGTH = 200
const val DEFAULT_PATH_MAX_LENGTH = 200
const val DEFAULT_HTTP_STATUS_MAX_LENGTH = 35
const val DEFAULT_HTTP_METHOD_MAX_LENGTH = 7
const val DEFAULT_PROFILE_MAX_LENGTH = 20
const val DEFAULT_MAX_NUMBER_OF_INTERACTIONS_TO_QUERY = 100

@Configuration
open class LibraryConfig {

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('jdbc:postgresql://')}")
    open fun interceptedDocumentRepositoryFromConnectionString(
        @Value("\${lsd.dist.connectionString}") dbConnectionString: String,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{" + DEFAULT_FAIL_ON_CONNECTION + "}}") failOnConnectionError: Boolean,
        @Value("\${lsd.dist.db.traceIdMaxLength:#{" + DEFAULT_TRACE_ID_MAX_LENGTH + "}}") traceIdMaxLength: Int,
        @Value("\${lsd.dist.db.bodyMaxLength:#{" + DEFAULT_BODY_MAX_LENGTH + "}}") bodyMaxLength: Int,
        @Value("\${lsd.dist.db.requestHeadersMaxLength:#{" + DEFAULT_REQUEST_HEADERS_MAX_LENGTH + "}}") requestHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.responseHeadersMaxLength:#{" + DEFAULT_RESPONSE_HEADERS_MAX_LENGTH + "}}") responseHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.serviceNameMaxLength:#{" + DEFAULT_SERVICE_NAME_MAX_LENGTH + "}}") serviceNameMaxLength: Int,
        @Value("\${lsd.dist.db.targetMaxLength:#{" + DEFAULT_TARGET_MAX_LENGTH + "}}") targetMaxLength: Int,
        @Value("\${lsd.dist.db.pathMaxLength:#{" + DEFAULT_PATH_MAX_LENGTH + "}}") pathMaxLength: Int,
        @Value("\${lsd.dist.db.httpStatusMaxLength:#{" + DEFAULT_HTTP_STATUS_MAX_LENGTH + "}}") httpStatusMaxLength: Int,
        @Value("\${lsd.dist.db.httpMethodMaxLength:#{" + DEFAULT_HTTP_METHOD_MAX_LENGTH + "}}") httpMethodMaxLength: Int,
        @Value("\${lsd.dist.db.profileMaxLength:#{" + DEFAULT_PROFILE_MAX_LENGTH + "}}") profileMaxLength: Int,
        @Value("\${lsd.dist.db.maxNumberOfInteractionsToQuery:#{" + DEFAULT_MAX_NUMBER_OF_INTERACTIONS_TO_QUERY + "}}") maxNumberOfInteractionsToQuery: Int,
    ) = InterceptedDocumentPostgresRepository(
        dbConnectionString = dbConnectionString,
        objectMapper = objectMapper,
        failOnConnectionError = failOnConnectionError,
        traceIdMaxLength = traceIdMaxLength,
        bodyMaxLength = bodyMaxLength,
        requestHeadersMaxLength = requestHeadersMaxLength,
        responseHeadersMaxLength = responseHeadersMaxLength,
        serviceNameMaxLength = serviceNameMaxLength,
        targetMaxLength = targetMaxLength,
        pathMaxLength = pathMaxLength,
        httpStatusMaxLength = httpStatusMaxLength,
        httpMethodMaxLength = httpMethodMaxLength,
        profileMaxLength = profileMaxLength,
        maxNumberOfInteractionsToQuery = maxNumberOfInteractionsToQuery,
    )

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('dataSource')}")
    @ConditionalOnMissingBean(value = [InterceptedDocumentPostgresRepository::class])
    open fun interceptedDocumentRepositoryFromDataSource(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{" + DEFAULT_FAIL_ON_CONNECTION + "}}") failOnConnectionError: Boolean,
        @Value("\${lsd.dist.db.traceIdMaxLength:#{" + DEFAULT_TRACE_ID_MAX_LENGTH + "}}") traceIdMaxLength: Int,
        @Value("\${lsd.dist.db.bodyMaxLength:#{" + DEFAULT_BODY_MAX_LENGTH + "}}") bodyMaxLength: Int,
        @Value("\${lsd.dist.db.requestHeadersMaxLength:#{" + DEFAULT_REQUEST_HEADERS_MAX_LENGTH + "}}") requestHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.responseHeadersMaxLength:#{" + DEFAULT_RESPONSE_HEADERS_MAX_LENGTH + "}}") responseHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.serviceNameMaxLength:#{" + DEFAULT_SERVICE_NAME_MAX_LENGTH + "}}") serviceNameMaxLength: Int,
        @Value("\${lsd.dist.db.targetMaxLength:#{" + DEFAULT_TARGET_MAX_LENGTH + "}}") targetMaxLength: Int,
        @Value("\${lsd.dist.db.pathMaxLength:#{" + DEFAULT_PATH_MAX_LENGTH + "}}") pathMaxLength: Int,
        @Value("\${lsd.dist.db.httpStatusMaxLength:#{" + DEFAULT_HTTP_STATUS_MAX_LENGTH + "}}") httpStatusMaxLength: Int,
        @Value("\${lsd.dist.db.httpMethodMaxLength:#{" + DEFAULT_HTTP_METHOD_MAX_LENGTH + "}}") httpMethodMaxLength: Int,
        @Value("\${lsd.dist.db.profileMaxLength:#{" + DEFAULT_PROFILE_MAX_LENGTH + "}}") profileMaxLength: Int,
        @Value("\${lsd.dist.db.maxNumberOfInteractionsToQuery:#{" + DEFAULT_MAX_NUMBER_OF_INTERACTIONS_TO_QUERY + "}}") maxNumberOfInteractionsToQuery: Int,
    ) = InterceptedDocumentPostgresRepository(
        dataSource = dataSource,
        objectMapper = objectMapper,
        traceIdMaxLength = traceIdMaxLength,
        bodyMaxLength = bodyMaxLength,
        requestHeadersMaxLength = requestHeadersMaxLength,
        responseHeadersMaxLength = responseHeadersMaxLength,
        serviceNameMaxLength = serviceNameMaxLength,
        targetMaxLength = targetMaxLength,
        pathMaxLength = pathMaxLength,
        httpStatusMaxLength = httpStatusMaxLength,
        httpMethodMaxLength = httpMethodMaxLength,
        profileMaxLength = profileMaxLength,
        maxNumberOfInteractionsToQuery = maxNumberOfInteractionsToQuery,
    )

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('jdbc:postgresql://')}")
    open fun interceptedDocumentAdminRepositoryFromConnectionString(
        @Value("\${lsd.dist.connectionString}") dbConnectionString: String,
        objectMapper: ObjectMapper,
    ) = InterceptedDocumentPostgresAdminRepository(dbConnectionString, objectMapper)

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('dataSource')}")
    @ConditionalOnMissingBean(value = [InterceptedDocumentPostgresAdminRepository::class])
    open fun interceptedDocumentAdminRepositoryFromDataSource(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
    ): InterceptedDocumentPostgresAdminRepository {
        log().info("Instantiating InterceptedDocumentPostgresAdminRepository")
        return InterceptedDocumentPostgresAdminRepository(dataSource, objectMapper)
    }
}
