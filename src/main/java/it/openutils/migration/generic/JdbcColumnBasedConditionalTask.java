/*
 * Copyright Openmind http://www.openmindonline.it
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
package it.openutils.migration.generic;

import it.openutils.migration.task.setup.BaseConditionalTask;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Base conditional task that operates on conditions related to a specific column. This base task takes care of
 * retrieving the column metadata, so that subclasses only need to override <code>checkColumnMetadata()</code>.
 * @author fgiust
 * @version $Id$
 */
public abstract class JdbcColumnBasedConditionalTask extends BaseConditionalTask
{

    /**
     * Column name
     */
    protected String column;

    /**
     * Catalog name
     */
    protected String catalog;

    /**
     * Schema name
     */
    protected String schema;

    /**
     * Sets the catalog.
     * @param catalog the catalog to set
     */
    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    /**
     * Sets the schema.
     * @param schema the schema to set
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Sets the column.
     * @param column the column to set
     */
    public void setColumn(String column)
    {
        this.column = column;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean check(SimpleJdbcTemplate jdbcTemplate)
    {

        String columnTrim = StringUtils.trim(column);

        final String tableName = StringUtils.substringBefore(columnTrim, ".");
        final String columnName = StringUtils.substringAfter(columnTrim, ".");
        return (Boolean) jdbcTemplate.getJdbcOperations().execute(new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                DatabaseMetaData dbMetadata = con.getMetaData();
                ResultSet rs = dbMetadata.getColumns(catalog, schema, tableName, columnName);
                boolean conditionMet = rs.next();
                if (conditionMet)
                {
                    ResultSetMetaData rsmeta = rs.getMetaData();
                    int colcount = rsmeta.getColumnCount();
                    Map<String, Object> params = new HashMap<String, Object>();
                    for (int j = 1; j <= colcount; j++)
                    {
                        params.put(rsmeta.getColumnName(j), rs.getObject(j));
                    }
                    conditionMet = checkColumnMetadata(params);
                }
                rs.close();
                return conditionMet;
            }
        });
    }

    /**
     * <p>
     * Check if a specific condition is met depending on column metadata.
     * </p>
     * Column attributes included in the Map are:
     * <ol>
     * <li><strong>TABLE_CAT</strong> String => table catalog (may be <code>null</code>)</li>
     * <li><strong>TABLE_SCHEM</strong> String => table schema (may be <code>null</code>)</li>
     * <li><strong>TABLE_NAME</strong> String => table name</li>
     * <li><strong>COLUMN_NAME</strong> String => column name</li>
     * <li><strong>DATA_TYPE</strong> int => SQL type from java.sql.Types</li>
     * <li><strong>TYPE_NAME</strong> String => Data source dependent type name, for a UDT the type name is fully
     * qualified</li>
     * <li><strong>COLUMN_SIZE</strong> int => column size.</li>
     * <li><strong>BUFFER_LENGTH</strong> is not used.</li>
     * <li><strong>DECIMAL_DIGITS</strong> int => the number of fractional digits. Null is returned for data types
     * where DECIMAL_DIGITS is not applicable.</li>
     * <li><strong>NUM_PREC_RADIX</strong> int => Radix (typically either 10 or 2)</li>
     * <li><strong>NULLABLE</strong> int => is NULL allowed.</li>
     * <ul>
     * <li> columnNoNulls - might not allow <code>NULL</code> values</li>
     * <li> columnNullable - definitely allows <code>NULL</code> values</li>
     * <li> columnNullableUnknown - nullability unknown</li>
     * </ul>
     * <li><strong>REMARKS</strong> String => comment describing column (may be <code>null</code>)</li>
     * <li><strong>COLUMN_DEF</strong> String => default value for the column, which should be interpreted as a string
     * when the value is enclosed in single quotes (may be <code>null</code>)</li>
     * <li><strong>SQL_DATA_TYPE</strong> int => unused</li>
     * <li><strong>SQL_DATETIME_SUB</strong> int => unused</li>
     * <li><strong>CHAR_OCTET_LENGTH</strong> int => for char types the maximum number of bytes in the column</li>
     * <li><strong>ORDINAL_POSITION</strong> int => index of column in table (starting at 1)</li>
     * <li><strong>IS_NULLABLE</strong> String => ISO rules are used to determine the nullability for a column.</li>
     * <ul>
     * <li> YES --- if the parameter can include NULLs</li>
     * <li> NO --- if the parameter cannot include NULLs</li>
     * <li> empty string --- if the nullability for the parameter is unknown</li>
     * </ul>
     * <li><strong>SCOPE_CATLOG</strong> String => catalog of table that is the scope of a reference attribute (<code>null</code>
     * if DATA_TYPE isn't REF)</li>
     * <li><strong>SCOPE_SCHEMA</strong> String => schema of table that is the scope of a reference attribute (<code>null</code>
     * if the DATA_TYPE isn't REF)</li>
     * <li><strong>SCOPE_TABLE</strong> String => table name that this the scope of a reference attribure (<code>null</code>
     * if the DATA_TYPE isn't REF)</li>
     * <li><strong>SOURCE_DATA_TYPE</strong> short => source type of a distinct type or user-generated Ref type, SQL
     * type from java.sql.Types (<code>null</code> if DATA_TYPE isn't DISTINCT or user-generated REF)</li>
     * <li><strong>IS_AUTOINCREMENT</strong> String => Indicates whether this column is auto incremented</li>
     * <ul>
     * <li> YES --- if the column is auto incremented</li>
     * <li> NO --- if the column is not auto incremented</li>
     * <li> empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown</li>
     * </ul>
     * </ol>
     * @param metadata column metadata
     * @return <code>true</code> if the condition is met
     */
    protected abstract boolean checkColumnMetadata(Map<String, Object> metadata);

}
