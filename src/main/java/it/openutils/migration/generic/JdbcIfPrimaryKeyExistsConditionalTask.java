/**
 *
 * openutils db migration (http://www.openmindlab.com/lab/products/dbmigration.html)
 * Copyright(C) 2007-2010, Openmind S.r.l. http://www.openmindonline.it
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package it.openutils.migration.generic;

import it.openutils.migration.task.setup.BaseConditionalTask;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Task that executes if a given column have at least one index.
 * @author Antonio Gagliardi
 * @version $Id$
 */
public class JdbcIfPrimaryKeyExistsConditionalTask extends BaseConditionalTask
{

    protected String catalog;

    protected String schema;

    protected String table;

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(JdbcIfPrimaryKeyExistsConditionalTask.class);

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean check(SimpleJdbcTemplate jdbcTemplate)
    {
        checkInputs();

        ConnectionCallback action = new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                DatabaseMetaData dbMetadata = con.getMetaData();
                ResultSet rs = dbMetadata.getPrimaryKeys(catalog, schema, table);

                Map<Integer, String> primaryKey = new TreeMap<Integer, String>();
                while (rs.next())
                {
                    String actualColumnName = rs.getString("COLUMN_NAME");
                    Integer position = rs.getInt("KEY_SEQ");
                    primaryKey.put(position, actualColumnName);
                }
                rs.close();
                return primaryKey.values();
            }
        };
        Collection<String> primaryKeyActual = (Collection<String>) jdbcTemplate.getJdbcOperations().execute(action);

        log.debug("Actual:{}", asString(primaryKeyActual));
        if (primaryKeyActual.isEmpty())
        {
            return false;
        }
        else
        {
            return true;
        }

    }

    private void checkInputs()
    {
        if (StringUtils.isBlank(table))
        {
            throw new IllegalArgumentException("table is mandatory");
        }
    }

    private final static String asString(Collection<String> primaryKey)
    {
        StringBuilder sb = new StringBuilder();
        int position = 0;
        for (String pkColumn : primaryKey)
        {
            sb.append("" + position + "|" + pkColumn);
            position++;
        }
        return sb.toString();
    }

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
     * Sets the table.
     * @param table the table to set
     */
    public void setTable(String table)
    {
        this.table = table;
    }

}
