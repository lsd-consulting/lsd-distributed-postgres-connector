package io.lsdconsulting.lsd.distributed.postgres.integration

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.TestApplication
import io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository.TestRepository
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.testcontainers.containers.PostgreSQLContainer

private const val POSTGRES_PORT = 5432
private const val POSTGRES_IMAGE = "postgres:15.3-alpine3.18"
private const val TABLE_NAME = "lsd_database"

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [TestApplication::class])
internal open class BaseIT {

    @Autowired
    lateinit var testRepository: TestRepository

    companion object {
        private var postgreSQLContainer: PostgreSQLContainer<*> = PostgreSQLContainer(POSTGRES_IMAGE)
            .withDatabaseName(TABLE_NAME)
            .withUsername("sa")
            .withPassword("sa")
            .withExposedPorts(POSTGRES_PORT)
            .withCreateContainerCmdModifier { cmd ->
                cmd.withHostConfig(
                    HostConfig().withPortBindings(
                        PortBinding(
                            Ports.Binding.bindPort(POSTGRES_PORT), ExposedPort(
                                POSTGRES_PORT
                            )
                        )
                    )
                )
            }

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            postgreSQLContainer.start()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            postgreSQLContainer.stop()
        }
    }
}