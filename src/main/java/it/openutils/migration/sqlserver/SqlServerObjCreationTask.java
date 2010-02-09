/**
 *
 * openutils db migration (http://www.openmindlab.com/lab/products/dbmigration.html)
 * Copyright(C) null-2010, Openmind S.r.l. http://www.openmindonline.it
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

package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.GenericConditionalTask;

import java.io.IOException;
import java.io.InputStream;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author fgiust
 * @version $Id:SqlServerObjCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerObjCreationTask extends GenericConditionalTask
{

    /**
     * Query to check with standard objects' name.
     */
    private String unqualifiedObjQuery;

    /**
     * Query to check with full database objects' hierarchy.
     */
    private String qualifiedObjQuery;

    /**
     * Returns the qualifiedObjQuery.
     * @return the qualifiedObjQuery
     */
    public String getQualifiedObjQuery()
    {
        return qualifiedObjQuery;
    }

    /**
     * Sets the qualifiedObjQuery.
     * @param qualifiedObjQuery the qualifiedObjQuery to set
     */
    public void setQualifiedObjQuery(String qualifiedObjQuery)
    {
        this.qualifiedObjQuery = qualifiedObjQuery;
    }

    /**
     * Returns the unqualifiedObjQuery.
     * @return the unqualifiedObjQuery
     */
    public String getUnqualifiedObjQuery()
    {
        return unqualifiedObjQuery;
    }

    /**
     * Sets the unqualifiedObjQuery.
     * @param unqualifiedObjQuery the unqualifiedObjQuery to set
     */
    public void setUnqualifiedObjQuery(String unqualifiedObjQuery)
    {
        this.unqualifiedObjQuery = unqualifiedObjQuery;
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
            String fqTableName = this.objectNameFromFileName(script);

            int result = 0;
            if (StringUtils.contains(fqTableName, "."))
            {
                String[] tokens = StringUtils.split(fqTableName, ".");
                result = jdbcTemplate.queryForInt(qualifiedObjQuery, new Object[]{tokens[1], tokens[0] });
            }
            else
            {
                result = jdbcTemplate.queryForInt(unqualifiedObjQuery, fqTableName);
            }

            if (result == 0)
            {
                if (script == null || !script.exists())
                {
                    log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
                    return;
                }

                String scriptContent;
                InputStream is = null;

                try
                {
                    is = script.getInputStream();
                    scriptContent = IOUtils.toString(is, "UTF8");
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

                for (String ddl : ddls)
                {
                    if (StringUtils.isNotBlank(ddl))
                    {
                        String ddlReplaced = performSubstitution(ddl);
                        log.debug("Executing:\n{}", ddlReplaced);
                        jdbcTemplate.update(ddlReplaced);
                    }
                }
            }
        }
    }
}