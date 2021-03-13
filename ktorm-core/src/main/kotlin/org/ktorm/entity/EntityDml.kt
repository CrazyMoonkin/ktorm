/*
 * Copyright 2018-2021 the original author or authors.
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

package org.ktorm.entity

import org.ktorm.dsl.AliasRemover
import org.ktorm.dsl.combineConditions
import org.ktorm.dsl.delete
import org.ktorm.dsl.deleteAll
import org.ktorm.expression.ArgumentExpression
import org.ktorm.expression.BinaryExpression
import org.ktorm.expression.BinaryExpressionType
import org.ktorm.expression.ColumnAssignmentExpression
import org.ktorm.expression.ColumnExpression
import org.ktorm.expression.DeleteExpression
import org.ktorm.expression.InsertExpression
import org.ktorm.expression.ScalarExpression
import org.ktorm.expression.UpdateExpression
import org.ktorm.schema.BaseTable
import org.ktorm.schema.BooleanSqlType
import org.ktorm.schema.Column
import org.ktorm.schema.ColumnDeclaring
import org.ktorm.schema.NestedBinding
import org.ktorm.schema.ReferenceBinding
import org.ktorm.schema.SqlType
import org.ktorm.schema.Table

/**
 * Insert the given entity into this sequence and return the affected record number. Only non-null properties
 * are inserted.
 *
 * If we use an auto-increment key in our table, we need to tell Ktorm which is the primary key by calling
 * [Table.primaryKey] while registering columns, then this function will obtain the generated key from the
 * database and fill it into the corresponding property after the insertion completes. But this requires us
 * not to set the primary key’s value beforehand, otherwise, if you do that, the given value will be inserted
 * into the database, and no keys generated.
 *
 * Note that after calling this function, the [entity] will **be associated with the current table**.
 *
 * @see Entity.flushChanges
 * @see Entity.delete
 * @since 2.7
 */
@Suppress("UNCHECKED_CAST")
public fun <E : Entity<E>, T : Table<E>> EntitySequence<E, T>.add(entity: E): Int {
    checkIfSequenceModified()
    entity.implementation.checkUnexpectedDiscarding(sourceTable)

    val assignments = entity.findInsertColumns(sourceTable).takeIf { it.isNotEmpty() } ?: return 0

    val expression = AliasRemover.visit(
        expr = InsertExpression(
            table = sourceTable.asExpression(),
            assignments = assignments.map { (col, argument) ->
                ColumnAssignmentExpression(
                    column = col.asExpression() as ColumnExpression<Any>,
                    expression = ArgumentExpression(argument, col.sqlType as SqlType<Any>)
                )
            }
        )
    )

    val primaryKeys = sourceTable.primaryKeys

    val ignoreGeneratedKeys = primaryKeys.size != 1
        || primaryKeys[0].binding == null
        || entity.implementation.getColumnValue(primaryKeys[0].binding!!) != null

    if (ignoreGeneratedKeys) {
        val effects = database.executeUpdate(expression)
        entity.implementation.fromDatabase = database
        entity.implementation.fromTable = sourceTable
        entity.implementation.doDiscardChanges()
        return effects
    } else {
        val (effects, rowSet) = database.executeUpdateAndRetrieveKeys(expression)

        if (rowSet.next()) {
            val generatedKey = primaryKeys[0].sqlType.getResult(rowSet, 1)
            if (generatedKey != null) {
                database.loggers.forEach { logger ->
                    logger.takeIf { it.isDebugEnabled() }?.debug("Generated Key: $generatedKey")
                }

                entity.implementation.setColumnValue(primaryKeys[0].binding!!, generatedKey)
            }
        }

        entity.implementation.fromDatabase = database
        entity.implementation.fromTable = sourceTable
        entity.implementation.doDiscardChanges()
        return effects
    }
}

/**
 * Update the non-null properties of the given entity to the database and return the affected record number.
 *
 * Note that after calling this function, the [entity] will **be associated with the current table**.
 *
 * @see Entity.flushChanges
 * @see Entity.delete
 * @since 3.1.0
 */
@Suppress("UNCHECKED_CAST")
public fun <E : Entity<E>, T : Table<E>> EntitySequence<E, T>.update(entity: E): Int {
    checkIfSequenceModified()
    entity.implementation.checkUnexpectedDiscarding(sourceTable)

    val assignments = entity.findUpdateColumns(sourceTable).takeIf { it.isNotEmpty() } ?: return 0

    val expression = AliasRemover.visit(
        expr = UpdateExpression(
            table = sourceTable.asExpression(),
            assignments = assignments.map { (col, argument) ->
                ColumnAssignmentExpression(
                    column = col.asExpression() as ColumnExpression<Any>,
                    expression = ArgumentExpression(argument, col.sqlType as SqlType<Any>)
                )
            },
            where = entity.implementation.constructIdentityCondition(sourceTable)
        )
    )

    val effects = database.executeUpdate(expression)
    entity.implementation.fromDatabase = database
    entity.implementation.fromTable = sourceTable
    entity.implementation.doDiscardChanges()
    return effects
}

/**
 * Remove all of the elements of this sequence that satisfy the given [predicate].
 *
 * @since 2.7
 */
public fun <E : Any, T : BaseTable<E>> EntitySequence<E, T>.removeIf(
    predicate: (T) -> ColumnDeclaring<Boolean>
): Int {
    checkIfSequenceModified()
    return database.delete(sourceTable, predicate)
}

/**
 * Remove all of the elements of this sequence. The sequence will be empty after this function returns.
 *
 * @since 2.7
 */
public fun <E : Any, T : BaseTable<E>> EntitySequence<E, T>.clear(): Int {
    checkIfSequenceModified()
    return database.deleteAll(sourceTable)
}

@Suppress("UNCHECKED_CAST")
internal fun EntityImplementation.doFlushChanges(): Int {
    check(parent == null) { "The entity is not associated with any database yet." }

    val fromDatabase = fromDatabase ?: error("The entity is not associated with any database yet.")
    val fromTable = fromTable ?: error("The entity is not associated with any table yet.")
    checkUnexpectedDiscarding(fromTable)

    val assignments = findChangedColumns(fromTable).takeIf { it.isNotEmpty() } ?: return 0

    val expression = AliasRemover.visit(
        expr = UpdateExpression(
            table = fromTable.asExpression(),
            assignments = assignments.map { (col, argument) ->
                ColumnAssignmentExpression(
                    column = col.asExpression() as ColumnExpression<Any>,
                    expression = ArgumentExpression(argument, col.sqlType as SqlType<Any>)
                )
            },
            where = constructIdentityCondition(fromTable)
        )
    )

    return fromDatabase.executeUpdate(expression).also { doDiscardChanges() }
}

@Suppress("UNCHECKED_CAST")
internal fun EntityImplementation.doDelete(): Int {
    check(parent == null) { "The entity is not associated with any database yet." }

    val fromDatabase = fromDatabase ?: error("The entity is not associated with any database yet.")
    val fromTable = fromTable ?: error("The entity is not associated with any table yet.")

    val expression = AliasRemover.visit(
        expr = DeleteExpression(
            table = fromTable.asExpression(),
            where = constructIdentityCondition(fromTable)
        )
    )

    return fromDatabase.executeUpdate(expression)
}

private fun EntitySequence<*, *>.checkIfSequenceModified() {
    val isModified = expression.where != null
        || expression.groupBy.isNotEmpty()
        || expression.having != null
        || expression.isDistinct
        || expression.orderBy.isNotEmpty()
        || expression.offset != null
        || expression.limit != null

    if (isModified) {
        throw UnsupportedOperationException(
            "Entity manipulation functions are not supported by this sequence object. " +
                "Please call on the origin sequence returned from database.sequenceOf(table)"
        )
    }
}

private fun Entity<*>.findInsertColumns(table: Table<*>): Map<Column<*>, Any?> {
    val assignments = LinkedHashMap<Column<*>, Any?>()

    for (column in table.columns) {
        if (column.binding != null) {
            val value = implementation.getColumnValue(column.binding)
            if (value != null) {
                assignments[column] = value
            }
        }
    }

    return assignments
}

private fun Entity<*>.findUpdateColumns(table: Table<*>): Map<Column<*>, Any?> {
    val assignments = LinkedHashMap<Column<*>, Any?>()

    for (column in table.columns - table.primaryKeys) {
        if (column.binding != null) {
            val value = implementation.getColumnValue(column.binding)
            if (value != null) {
                assignments[column] = value
            }
        }
    }

    return assignments
}

private fun EntityImplementation.findChangedColumns(fromTable: Table<*>): Map<Column<*>, Any?> {
    val assignments = LinkedHashMap<Column<*>, Any?>()

    for (column in fromTable.columns) {
        val binding = column.binding ?: continue

        when (binding) {
            is ReferenceBinding -> {
                if (binding.onProperty.name in changedProperties) {
                    val child = this.getProperty(binding.onProperty.name) as Entity<*>?
                    assignments[column] = child?.implementation?.getPrimaryKeyValue(binding.referenceTable as Table<*>)
                }
            }
            is NestedBinding -> {
                var anyChanged = false
                var curr: Any? = this

                for (prop in binding.properties) {
                    if (curr is Entity<*>) {
                        curr = curr.implementation
                    }

                    check(curr is EntityImplementation?)

                    if (curr != null && prop.name in curr.changedProperties) {
                        anyChanged = true
                    }

                    curr = curr?.getProperty(prop.name)
                }

                if (anyChanged) {
                    assignments[column] = curr
                }
            }
        }
    }

    return assignments
}

internal fun EntityImplementation.doDiscardChanges() {
    check(parent == null) { "The entity is not associated with any database yet." }
    val fromTable = fromTable ?: error("The entity is not associated with any table yet.")

    for (column in fromTable.columns) {
        val binding = column.binding ?: continue

        when (binding) {
            is ReferenceBinding -> {
                changedProperties.remove(binding.onProperty.name)
            }
            is NestedBinding -> {
                var curr: Any? = this

                for (prop in binding.properties) {
                    if (curr == null) {
                        break
                    }
                    if (curr is Entity<*>) {
                        curr = curr.implementation
                    }

                    check(curr is EntityImplementation)
                    curr.changedProperties.remove(prop.name)
                    curr = curr.getProperty(prop.name)
                }
            }
        }
    }
}

// Add check to avoid bug #10
private fun EntityImplementation.checkUnexpectedDiscarding(fromTable: Table<*>) {
    for (column in fromTable.columns) {
        if (column.binding !is NestedBinding) continue

        var curr: Any? = this
        for ((i, prop) in column.binding.properties.withIndex()) {
            if (curr == null) {
                break
            }
            if (curr is Entity<*>) {
                curr = curr.implementation
            }

            check(curr is EntityImplementation)

            if (i > 0 && prop.name in curr.changedProperties) {
                val isExternalEntity = curr.fromTable != null && curr.getRoot() != this
                if (isExternalEntity) {
                    val propPath = column.binding.properties.subList(0, i + 1).joinToString(separator = ".") { it.name }
                    val msg = "this.$propPath may be unexpectedly discarded, please save it to database first."
                    throw IllegalStateException(msg)
                }
            }

            curr = curr.getProperty(prop.name)
        }
    }
}

private tailrec fun EntityImplementation.getRoot(): EntityImplementation {
    val parent = this.parent
    if (parent == null) {
        return this
    } else {
        return parent.getRoot()
    }
}

internal fun Entity<*>.clearChangesRecursively() {
    implementation.changedProperties.clear()

    for ((_, value) in properties) {
        if (value is Entity<*>) {
            value.clearChangesRecursively()
        }
    }
}

@Suppress("UNCHECKED_CAST")
private fun EntityImplementation.constructIdentityCondition(fromTable: Table<*>): ScalarExpression<Boolean> {
    val primaryKeys = fromTable.primaryKeys
    if (primaryKeys.isEmpty()) {
        error("Table '$fromTable' doesn't have a primary key.")
    }

    val conditions = primaryKeys.map { pk ->
        if (pk.binding == null) {
            error("Primary column $pk has no bindings to any entity field.")
        }

        val pkValue = getColumnValue(pk.binding) ?: error("The value of primary key column $pk is null.")

        BinaryExpression(
            type = BinaryExpressionType.EQUAL,
            left = pk.asExpression(),
            right = ArgumentExpression(pkValue, pk.sqlType as SqlType<Any>),
            sqlType = BooleanSqlType
        )
    }

    return conditions.combineConditions().asExpression()
}
