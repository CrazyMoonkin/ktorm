/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.liuwj.ktorm.support.mysql

import me.liuwj.ktorm.database.Database
import me.liuwj.ktorm.dsl.AssignmentsBuilder
import me.liuwj.ktorm.dsl.KtormDsl
import me.liuwj.ktorm.dsl.batchInsert
import me.liuwj.ktorm.expression.ColumnAssignmentExpression
import me.liuwj.ktorm.expression.FunctionExpression
import me.liuwj.ktorm.expression.SqlExpression
import me.liuwj.ktorm.expression.TableExpression
import me.liuwj.ktorm.schema.BaseTable
import me.liuwj.ktorm.schema.ColumnDeclaring

/**
 * Bulk insert expression, represents a bulk insert statement in MySQL.
 *
 * For example: `insert into table (column1, column2) values (?, ?), (?, ?), (?, ?)...`.
 *
 * @property table the table to be inserted.
 * @property assignments column assignments of the bulk insert statement.
 */
data class BulkInsertExpression(
    val table: TableExpression,
    val assignments: List<List<ColumnAssignmentExpression<*>>>,
    val updateAssignments: List<ColumnAssignmentExpression<*>>,
    override val isLeafNode: Boolean = false,
    override val extraProperties: Map<String, Any> = emptyMap()
) : SqlExpression()

/**
 * Construct a bulk insert expression in the given closure, then execute it and return the effected row count.
 *
 * The usage is almost the same as [batchInsert], but this function is implemented by generating a special SQL
 * using MySQL's bulk insert syntax, instead of based on JDBC batch operations. For this reason, its performance
 * is much better than [batchInsert].
 *
 * The generated SQL is like: `insert into table (column1, column2) values (?, ?), (?, ?), (?, ?)...`.
 *
 * Usage:
 *
 * ```kotlin
 * Employees.bulkInsert {
 *     item {
 *         it.name to "jerry"
 *         it.job to "trainee"
 *         it.managerId to 1
 *         it.hireDate to LocalDate.now()
 *         it.salary to 50
 *         it.departmentId to 1
 *     }
 *     item {
 *         it.name to "linda"
 *         it.job to "assistant"
 *         it.managerId to 3
 *         it.hireDate to LocalDate.now()
 *         it.salary to 100
 *         it.departmentId to 2
 *     }
 * }
 * ```
 *
 * @param block the DSL block, extension function of [BulkInsertStatementBuilder], used to construct the expression.
 * @return the effected row count.
 * @see batchInsert
 */
@Suppress("DEPRECATION")
@Deprecated(
    message = "This function will be removed in the future. Please use database.bulkInsert(table) {...} instead.",
    replaceWith = ReplaceWith("database.bulkInsert(this, block)")
)
fun <T : BaseTable<*>> T.bulkInsert(block: BulkInsertStatementBuilder<T>.() -> Unit): Int {
    return Database.global.bulkInsert(this, block)
}

/**
 * Construct a bulk insert expression in the given closure, then execute it and return the effected row count.
 *
 * The usage is almost the same as [batchInsert], but this function is implemented by generating a special SQL
 * using MySQL's bulk insert syntax, instead of based on JDBC batch operations. For this reason, its performance
 * is much better than [batchInsert].
 *
 * The generated SQL is like: `insert into table (column1, column2) values (?, ?), (?, ?), (?, ?)...`.
 *
 * Usage:
 *
 * ```kotlin
 * database.bulkInsert(Employees) {
 *     item {
 *         it.name to "jerry"
 *         it.job to "trainee"
 *         it.managerId to 1
 *         it.hireDate to LocalDate.now()
 *         it.salary to 50
 *         it.departmentId to 1
 *     }
 *     item {
 *         it.name to "linda"
 *         it.job to "assistant"
 *         it.managerId to 3
 *         it.hireDate to LocalDate.now()
 *         it.salary to 100
 *         it.departmentId to 2
 *     }
 * }
 * ```
 *
 * @since 2.7
 * @param table the table to be inserted.
 * @param block the DSL block, extension function of [BulkInsertStatementBuilder], used to construct the expression.
 * @return the effected row count.
 * @see batchInsert
 */
fun <T : BaseTable<*>> Database.bulkInsert(table: T, block: BulkInsertStatementBuilder<T>.() -> Unit): Int {
    val builder = BulkInsertStatementBuilder(table).apply(block)
    val expression = BulkInsertExpression(table.asExpression(), builder.assignments, builder.updateAssignments)
    return executeUpdate(expression)
}

/**
 * DSL builder for bulk insert statements.
 */
@KtormDsl
class BulkInsertStatementBuilder<T : BaseTable<*>>(internal val table: T) {
    internal val assignments = ArrayList<List<ColumnAssignmentExpression<*>>>()
    internal val updateAssignments = ArrayList<ColumnAssignmentExpression<*>>()

    /**
     * Add the assignments of a new row to the bulk insert.
     */
    fun item(block: BulkInsertAssignmentsBuilder.(T) -> Unit) {
        val itemAssignments = ArrayList<ColumnAssignmentExpression<*>>()
        val builder = BulkInsertAssignmentsBuilder(itemAssignments)
        builder.block(table)

        if (assignments.isEmpty() || assignments[0].map { it.column.name } == itemAssignments.map { it.column.name }) {
            assignments += itemAssignments
        } else {
            throw IllegalArgumentException("Every item in a batch operation must be the same.")
        }
    }

    /**
     * Specify the update assignments while any key conflict exists.
     */
    fun onDuplicateKey(block: BulkInsertAssignmentsBuilder.(T) -> Unit) {
        val updateAssignments = ArrayList<ColumnAssignmentExpression<*>>()
        val builder = BulkInsertAssignmentsBuilder(updateAssignments)
        builder.block(table)
        this.updateAssignments += updateAssignments
    }
}

/**
 * DSL builder for bulk insert assignments.
 */
@KtormDsl
class BulkInsertAssignmentsBuilder(assignments: MutableList<ColumnAssignmentExpression<*>>) :
        AssignmentsBuilder(assignments) {

    /**
     * Use VALUES() function in a ON DUPLICATE KEY UPDATE clause.
     */
    fun <T : Any> values(expr: ColumnDeclaring<T>): FunctionExpression<T> {
        // values(column)
        return FunctionExpression(
            functionName = "values",
            arguments = listOf(expr.asExpression()),
            sqlType = expr.sqlType
        )
    }
}
