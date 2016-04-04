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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.BaseConditionalTask;


/**
 * Task that executes if a named foreign key eists.
 * @author fgiust
 * @version $Id$
 */
public class JdbcIfForeignKeyExistsConditionalTask extends BaseConditionalTask
{

    private String fkName;

    private String catalog;

    private String schema;

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
     * Sets the fkName (TABLE.FKNAME).
     * @param fkName the fkName to set
     */
    public void setFkName(String fkName)
    {
        this.fkName = fkName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean check(JdbcTemplate jdbcTemplate)
    {

        String fkNameTrim = StringUtils.trim(fkName);

        final String tableName = StringUtils.substringBefore(fkNameTrim, ".");
        final String fkName = StringUtils.substringAfter(fkNameTrim, ".");
        return (Boolean) jdbcTemplate.execute(new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                boolean fkExists = false;
                DatabaseMetaData dbMetadata = con.getMetaData();
                ResultSet rs = dbMetadata.getExportedKeys(catalog, schema, tableName);
                while (rs.next())
                {
                    if (StringUtils.equals(fkName, rs.getString("FK_NAME")))
                    {
                        fkExists = true;
                    }
                }
                rs.close();

                return fkExists;
            }
        });
    }

}
