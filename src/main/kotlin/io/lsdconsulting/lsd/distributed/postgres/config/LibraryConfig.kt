package io.lsdconsulting.lsd.distributed.postgres.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.ApplicationContext
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
    ) = InterceptedDocumentPostgresRepository(dbConnectionString, objectMapper, failOnConnectionError)

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('dataSource')}")
    @ConditionalOnMissingBean(value = [InterceptedDocumentPostgresRepository::class])
    open fun interceptedDocumentRepositoryFromDataSource(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
    ): InterceptedDocumentPostgresRepository {
        return InterceptedDocumentPostgresRepository(dataSource, objectMapper)
    }

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('jdbc:postgresql://')}")
    open fun interceptedDocumentAdminRepositoryFromConnectionString(
        @Value("\${lsd.dist.connectionString}") dbConnectionString: String,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
    ) = InterceptedDocumentPostgresAdminRepository(dbConnectionString, objectMapper, failOnConnectionError)

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('dataSource')}")
    @ConditionalOnMissingBean(value = [InterceptedDocumentPostgresAdminRepository::class])
    open fun interceptedDocumentAdminRepositoryFromDataSource(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
        applicationContext: ApplicationContext,
    ): InterceptedDocumentPostgresAdminRepository {
        log().info("Instantiating InterceptedDocumentPostgresAdminRepository")
        return InterceptedDocumentPostgresAdminRepository(dataSource, objectMapper)
    }
}
