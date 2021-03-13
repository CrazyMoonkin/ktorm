package org.ktorm.logging

import org.junit.Test
import org.ktorm.BaseTest
import org.ktorm.database.Database
import org.ktorm.dsl.from
import org.ktorm.dsl.select

class LoggingTest : BaseTest() {

    private val logger = TestLogger(threshold = LogLevel.TRACE)

    override fun init() {
        database = Database.connect(
            url = "jdbc:h2:mem:ktorm;DB_CLOSE_DELAY=-1",
            driver = "org.h2.Driver",
            loggers = listOf(logger),
            alwaysQuoteIdentifiers = true
        )

        execSqlScript("init-data.sql")
    }

    @Test
    fun testLoggingWithTraceLevel() {
        // it here because can't use @BeforeEach
        logger.reset()
        logger.threshold = LogLevel.TRACE

        database.from(Departments).select().rowSet

        logger.messages[LogLevel.INFO]!!.let { infoLogList ->
            assert(infoLogList.size == 1)
            assert(
                infoLogList[0].first.startsWith("Connected to jdbc:h2:mem:ktorm, productName: H2, productVersion")
            )
        }

        logger.messages[LogLevel.DEBUG]!!.let { debugLogList ->
            assert(debugLogList.size == 3)
            assert(debugLogList[0].first == "SQL: SELECT * FROM \"t_department\" ")
            assert(debugLogList[0].second == null)

            assert(debugLogList[1].first == "Parameters: []")
            assert(debugLogList[0].second == null)

            assert(debugLogList[2].first == "Results: 2")
            assert(debugLogList[0].second == null)
        }

        assert(logger.messages[LogLevel.TRACE]?.size == 0)
        assert(logger.messages[LogLevel.WARN]?.size == 0)
        assert(logger.messages[LogLevel.ERROR]?.size == 0)
    }

    @Test
    fun testLoggingWithInfoLevel() {
        // it here because can't use @BeforeEach
        logger.reset()
        logger.threshold = LogLevel.INFO
        database.from(Departments).select().rowSet

        logger.messages[LogLevel.INFO]!!.let { infoLogList ->
            assert(infoLogList.size == 1)
            assert(
                infoLogList[0].first.startsWith("Connected to jdbc:h2:mem:ktorm, productName: H2, productVersion")
            )
            assert(infoLogList[0].second == null)
        }

        assert(logger.messages[LogLevel.DEBUG]?.size == 0)

        assert(logger.messages[LogLevel.TRACE]?.size == 0)
        assert(logger.messages[LogLevel.WARN]?.size == 0)
        assert(logger.messages[LogLevel.ERROR]?.size == 0)
    }

    inner class TestLogger(var threshold: LogLevel) : Logger {
        val messages: MutableMap<LogLevel, MutableList<Pair<String, Throwable?>>> = reset()

        fun reset(): MutableMap<LogLevel, MutableList<Pair<String, Throwable?>>> = mutableMapOf(
            LogLevel.TRACE to mutableListOf(),
            LogLevel.DEBUG to mutableListOf(),
            LogLevel.INFO to mutableListOf(),
            LogLevel.WARN to mutableListOf(),
            LogLevel.ERROR to mutableListOf()
        )

        override fun isTraceEnabled(): Boolean {
            return LogLevel.TRACE >= threshold
        }

        override fun trace(msg: String, e: Throwable?) {
            log(LogLevel.TRACE, msg, e)
        }

        override fun isDebugEnabled(): Boolean {
            return LogLevel.DEBUG >= threshold
        }

        override fun debug(msg: String, e: Throwable?) {
            log(LogLevel.DEBUG, msg, e)
        }

        override fun isInfoEnabled(): Boolean {
            return LogLevel.INFO >= threshold
        }

        override fun info(msg: String, e: Throwable?) {
            log(LogLevel.INFO, msg, e)
        }

        override fun isWarnEnabled(): Boolean {
            return LogLevel.WARN >= threshold
        }

        override fun warn(msg: String, e: Throwable?) {
            log(LogLevel.WARN, msg, e)
        }

        override fun isErrorEnabled(): Boolean {
            return LogLevel.ERROR >= threshold
        }

        override fun error(msg: String, e: Throwable?) {
            log(LogLevel.ERROR, msg, e)
        }

        private fun log(level: LogLevel, msg: String, e: Throwable?) {
            messages[level]?.add(msg to e)
            if (level >= threshold) {
                val out = if (level >= LogLevel.WARN) System.err else System.out
                out.println("[$level] $msg")
                e?.printStackTrace(out)
            }
        }
    }
}
