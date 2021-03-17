package org.ktorm.logging

import org.junit.Test
import org.ktorm.BaseTest
import org.ktorm.database.Database
import org.ktorm.dsl.from
import org.ktorm.dsl.select

class WithoutLoggerTest : BaseTest() {

    override fun init() {
        database = Database.connect(
            url = "jdbc:h2:mem:ktorm;DB_CLOSE_DELAY=-1",
            driver = "org.h2.Driver",
            loggers = listOf(),
            alwaysQuoteIdentifiers = true
        )

        execSqlScript("init-data.sql")
    }

    @Test
    fun testSelectWithoutLoggers() {
        val query = database.from(Departments).select()
        assert(query.rowSet.size() == 2)

        for (row in query) {
            println(row[Departments.name] + ": " + row[Departments.location])
        }
    }
}
