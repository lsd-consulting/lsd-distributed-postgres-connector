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

@Configuration
open class LibraryConfig {

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('jdbc:postgresql://')}")
    open fun interceptedDocumentRepositoryFromConnectionString(
        @Value("\${lsd.dist.connectionString}") dbConnectionString: String,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
        @Value("\${lsd.dist.db.traceIdMaxLength:#{32}}") traceIdMaxLength: Int,
        @Value("\${lsd.dist.db.bodyMaxLength:#{10000}}") bodyMaxLength: Int,
        @Value("\${lsd.dist.db.requestHeadersMaxLength:#{10000}}") requestHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.responseHeadersMaxLength:#{10000}}") responseHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.serviceNameMaxLength:#{200}}") serviceNameMaxLength: Int,
        @Value("\${lsd.dist.db.targetMaxLength:#{200}}") targetMaxLength: Int,
        @Value("\${lsd.dist.db.pathMaxLength:#{200}}") pathMaxLength: Int,
        @Value("\${lsd.dist.db.httpStatusMaxLength:#{35}}") httpStatusMaxLength: Int,
        @Value("\${lsd.dist.db.httpMethodMaxLength:#{7}}") httpMethodMaxLength: Int,
        @Value("\${lsd.dist.db.profileMaxLength:#{20}}") profileMaxLength: Int,
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
    )

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('dataSource')}")
    @ConditionalOnMissingBean(value = [InterceptedDocumentPostgresRepository::class])
    open fun interceptedDocumentRepositoryFromDataSource(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
        @Value("\${lsd.dist.db.traceIdMaxLength:#{32}}") traceIdMaxLength: Int,
        @Value("\${lsd.dist.db.bodyMaxLength:#{10000}}") bodyMaxLength: Int,
        @Value("\${lsd.dist.db.requestHeadersMaxLength:#{10000}}") requestHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.responseHeadersMaxLength:#{10000}}") responseHeadersMaxLength: Int,
        @Value("\${lsd.dist.db.serviceNameMaxLength:#{200}}") serviceNameMaxLength: Int,
        @Value("\${lsd.dist.db.targetMaxLength:#{200}}") targetMaxLength: Int,
        @Value("\${lsd.dist.db.pathMaxLength:#{200}}") pathMaxLength: Int,
        @Value("\${lsd.dist.db.httpStatusMaxLength:#{35}}") httpStatusMaxLength: Int,
        @Value("\${lsd.dist.db.httpMethodMaxLength:#{7}}") httpMethodMaxLength: Int,
        @Value("\${lsd.dist.db.profileMaxLength:#{20}}") profileMaxLength: Int,
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
