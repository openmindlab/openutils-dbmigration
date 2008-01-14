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
package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.GenericScriptBasedConditionalTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Compares the procedure script with the one stored in the db and drops and recreates it if the scripts differs. Be
 * aware that tabs chars are ALWAYS considered different, so remove them from your scripts! This works ONLY for SQL
 * Server 2005.
 * @author Danilo Ghirardelli
 * @version $Id$
 */
public class SqlServerProcedureCreateOrUpdateTask extends GenericScriptBasedConditionalTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(SqlServerObjCreationTask.class);

    /**
     * The db with the objects, may differ from the current.
     */
    private String sourceDb;

    /**
     * Sets the sourceDb.
     * @param sourceDb the sourceDb to set
     */
    public void setSourceDb(String sourceDb)
    {
        this.sourceDb = sourceDb;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void execute(DataSource dataSource)
    {
        String checkQuery = "select count(*) from dbo.sysobjects where id = object_id(?) and (OBJECTPROPERTY(id, N'IsProcedure') = 1)";
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        for (Resource script : scripts)
        {
            String procedureName = this.objectNameFromFileName(script);
            int result = jdbcTemplate.queryForInt(checkQuery, procedureName);
            String scriptContent = readFully(script);
            scriptContent = StringUtils.replace(scriptContent, "\t", " ");
            if (StringUtils.isBlank(scriptContent))
            {
                continue;
            }
            if (result == 0)
            {
                log.info("Procedure {} not existing. Creating new procedure", procedureName);
                createProcedure(jdbcTemplate, scriptContent);
            }
            else
            { // If the script is too long a list will be returned, and it must be joined to get the original script.
                List<String> previousDDlList = jdbcTemplate.getJdbcOperations().queryForList(
                    "exec sp_helptext ?",
                    new Object[]{procedureName },
                    String.class);
                String previousDDl = StringUtils.join(previousDDlList.toArray(new String[previousDDlList.size()]));
                if (!StringUtils.equals(previousDDl, scriptContent))
                {
                    log.info(
                        "Previous definition of procedure {} differs from actual. Dropping and recreating procedure",
                        new Object[]{procedureName });
                    jdbcTemplate.update("DROP PROCEDURE [" + procedureName + "]");
                    createProcedure(jdbcTemplate, scriptContent);
                }
            }
        }

    }

    /**
     * Creates a stored procedure executing the given script.
     * @param jdbcTemplate Jdbc connection.
     * @param script Stored procedure script.
     * @return
     */
    private void createProcedure(SimpleJdbcTemplate jdbcTemplate, String script)
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
        return StringUtils.stripEnd(
            StringUtils.trim(StringUtils.replace(scriptContent, "${sourceDb}", this.sourceDb)),
            ";");
    }
}