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
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Task that executes if a given column exists.
 * @author fgiust
 * @version $Id: $
 */
public class JdbcIfColumnExistsConditionalTask extends BaseConditionalTask
{

    /**
     * Column name.
     */
    protected String column;

    /**
     * Catalog.
     */
    protected String catalog;

    /**
     * Schema.
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
                boolean tableExists = rs.next();
                rs.close();

                return tableExists;
            }
        });
    }

}
