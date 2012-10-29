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

import it.openutils.migration.task.setup.GenericConditionalTask;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author fgiust
 * @version $Id:SqlServerObjCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public abstract class JdbcObjectCreationTask extends GenericConditionalTask
{

    /**
     * Catalog.
     */
    protected String catalog;

    /**
     * Schema.
     */
    protected String schema;

    abstract String getObjectType();

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
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        for (Resource script : scripts)
        {

            if (script == null || !script.exists())
            {
                log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
                return;
            }

            String fqTableName = this.objectNameFromFileName(script);
            String tmptableName = null;
            String tmpschema = schema;

            if (StringUtils.contains(fqTableName, "."))
            {
                String[] tokens = StringUtils.split(fqTableName, ".");
                tmptableName = tokens[1];
                tmpschema = tokens[0];
            }
            else
            {
                tmptableName = fqTableName;
            }

            final String tableName = tmptableName;
            final String schema = tmpschema;

            boolean result = (Boolean) new JdbcTemplate(dataSource).execute(new ConnectionCallback()
            {

                public Object doInConnection(Connection con) throws SQLException, DataAccessException
                {

                    DatabaseMetaData dbMetadata = con.getMetaData();
                    ResultSet rs = dbMetadata.getTables(catalog, schema, tableName, new String[]{getObjectType() });
                    boolean tableExists = rs.next();
                    rs.close();

                    return tableExists;
                }
            });

            if (!result)
            {
                String scriptContent;
                InputStream is = null;

                try
                {
                    is = script.getInputStream();
                    scriptContent = IOUtils.toString(is, "UTF8");
                    scriptContent = performSubstitution(scriptContent);
                }
                catch (IOException e)
                {
                    log.error(
                        "Unable to execute db task \"{}\", script \"{}\" can't be read.",
                        getDescription(),
                        script);
                    return;
                }
                finally
                {
                    IOUtils.closeQuietly(is);
                }

                String[] ddls = StringUtils.split(scriptContent, ";");

                log.info("Creating new {} {}", getObjectType(), tableName);

                for (String ddl : ddls)
                {
                    if (StringUtils.isNotBlank(ddl))
                    {
                        log.debug("Executing:\n{}", ddl);
                        jdbcTemplate.update(ddl);
                    }
                }
            }
            else
            {
                log.debug("{} {} already existing", getObjectType(), tableName);
            }
        }
    }
}