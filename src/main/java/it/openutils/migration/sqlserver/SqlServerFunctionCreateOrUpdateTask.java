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

package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.GenericConditionalTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Compares the function script with the one stored in the db and drops and recreates it if the scripts differs. Be
 * aware that tabs chars are ALWAYS considered different, so remove them from your scripts! This works ONLY for SQL
 * Server 2005.
 * @author Danilo Ghirardelli
 * @version $Id$
 */
public class SqlServerFunctionCreateOrUpdateTask extends GenericConditionalTask
{

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void execute(DataSource dataSource)
    {
        String checkQuery = "select count(*) from dbo.sysobjects where id = object_id(?) and xtype in (N'FN', N'IF', N'TF')";

        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        for (Resource script : scripts)
        {
            String functionName = this.objectNameFromFileName(script);
            int result = jdbcTemplate.queryForInt(checkQuery, functionName);
            String scriptContent = readFully(script);
            scriptContent = StringUtils.replace(scriptContent, "\t", " ");
            if (StringUtils.isBlank(scriptContent))
            {
                continue;
            }
            if (result == 0)
            {
                log.info("Function {} not existing. Creating new function", functionName);
                createFunction(jdbcTemplate, scriptContent);
            }
            else
            { // If the script is too long a list will be returned, and it must be joined to get the original script.
                List<String> previousDDlList = jdbcTemplate.getJdbcOperations().queryForList(
                    "exec sp_helptext ?",
                    new Object[]{functionName },
                    String.class);
                String previousDDl = StringUtils.join(previousDDlList.toArray(new String[previousDDlList.size()]));
                if (!StringUtils.equals(previousDDl, scriptContent))
                {
                    log.info(
                        "Previous definition of function {} differs from actual. Dropping and recreating function",
                        new Object[]{functionName });
                    jdbcTemplate.update("DROP FUNCTION [" + functionName + "]");
                    createFunction(jdbcTemplate, scriptContent);
                }
            }
        }

    }

    /**
     * Creates a function executing the given script.
     * @param jdbcTemplate Jdbc connection.
     * @param script Function script.
     * @return
     */
    private void createFunction(SimpleJdbcTemplate jdbcTemplate, String script)
    {
        String[] ddls = StringUtils.split(script, ";");
        for (String ddl : ddls)
        {
            if (StringUtils.isNotBlank(ddl))
            {
                log.debug("Executing:\n{}", ddl);
                jdbcTemplate.update(ddl);
            }
        }
    }

    /**
     * Reads the script from the given resource and convert it to a string suitable for update query.
     * @param script The script file
     * @return The script content
     */
    private String readFully(Resource script)
    {
        if (script == null || !script.exists())
        {
            log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
            return null;
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
            log.error("Unable to execute db task \"{}\", script \"{}\" can't be read.", getDescription(), script);
            return null;
        }
        finally
        {
            IOUtils.closeQuietly(is);
        }
        return StringUtils.stripEnd(StringUtils.trim(performSubstitution(scriptContent)), ";");
    }
}