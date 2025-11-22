/*
 * Copyright (C) 2023 Ctrip.com.
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

package com.ctrip.sqllin.driver

import java.sql.ResultSet
import java.sql.SQLException
import kotlin.IllegalArgumentException

/**
 * JDBC implementation of [CommonCursor] backed by a ResultSet.
 *
 * @author Yuang Qiao
 */
internal class JdbcCursor(private val resultSet: ResultSet) : CommonCursor {

    override fun getInt(columnIndex: Int): Int {
        val result = resultSet.getInt(columnIndex + 1)
        if (resultSet.wasNull())
            throw SQLiteException("The value of column $columnIndex is NULL")
        return result
    }

    override fun getLong(columnIndex: Int): Long {
        val result = resultSet.getLong(columnIndex + 1)
        if (resultSet.wasNull())
            throw SQLiteException("The value of column $columnIndex is NULL")
        return result
    }

    override fun getFloat(columnIndex: Int): Float {
        val result = resultSet.getFloat(columnIndex + 1)
        if (resultSet.wasNull())
            throw SQLiteException("The value of column $columnIndex is NULL")
        return result
    }

    override fun getDouble(columnIndex: Int): Double {
        val result = resultSet.getDouble(columnIndex + 1)
        if (resultSet.wasNull())
            throw SQLiteException("The value of column $columnIndex is NULL")
        return result
    }

    override fun getString(columnIndex: Int): String? = resultSet.getString(columnIndex + 1)

    override fun getString(columnName: String): String? {
        return getString(getColumnIndex(columnName))
    }

    override fun getByteArray(columnIndex: Int): ByteArray? = resultSet.getBytes(columnIndex + 1)

    override fun getColumnIndex(columnName: String): Int {
        try{
            return resultSet.findColumn(columnName) - 1
        } catch (e: SQLException){
            throw IllegalArgumentException("Col for $columnName not found")
        }
    }

    override fun getColumnCount(): Int {
        return resultSet.metaData.columnCount
    }

    override fun getColumnName(columnIndex: Int): String {
        val columnCount = resultSet.metaData.columnCount
        if (columnIndex >= columnCount)
            throw SQLiteException("$columnIndex exceeds the total number of columns")
        if (columnIndex < 0)
            throw SQLiteException("The column index is less than 0")

        return resultSet.metaData.getColumnName(columnIndex + 1)
    }

    override fun forEachRow(block: (Int) -> Unit) {
        var index = 0
        while (next())
            block(index++)
    }

    override fun next(): Boolean = resultSet.next()

    override fun isNull(columnIndex: Int): Boolean {
        resultSet.getObject(columnIndex + 1)
        return resultSet.wasNull()
    }

    override fun close() {
        resultSet.close()
        resultSet.statement.close()
    }
}