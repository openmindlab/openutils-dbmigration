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
 * @author Danilo Ghirardelli
 * @version $Id: SqlServerViewCreateOrUpdateTask.java 391 2007-07-10 17:25:42Z fgiust $
 */
public class SqlServerViewCreateOrUpdateTask extends GenericConditionalTask
{

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void execute(DataSource dataSource)
    {

        String checkQuery = "select count(*) from dbo.sysobjects where id = object_id(?) and OBJECTPROPERTY(id, N'IsView') = 1";

        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        for (Resource script : scripts)
        {
            String viewName = this.objectNameFromFileName(script);

            int result = jdbcTemplate.queryForInt(checkQuery, viewName);

            String scriptContent = readFully(script);
            scriptContent = StringUtils.replace(scriptContent, "\t", " ");

            if (scriptContent == null)
            {
                continue;
            }

            if (result == 0)
            {
                log.info("View {} not existing. Creating new view", viewName);

                createView(jdbcTemplate, scriptContent);
            }
            else
            {

                List<String> previousDDlList = jdbcTemplate.getJdbcOperations().queryForList(
                    "exec sp_helptext ?",
                    new Object[]{viewName },
                    String.class);

                String previousDDl = StringUtils.join(previousDDlList.toArray(new String[previousDDlList.size()]));

                if (!StringUtils.equals(previousDDl, scriptContent))
                {
                    log.info(
                        "Previous definition of view {} differs from actual. Dropping and recreating view",
                        new Object[]{viewName });

                    jdbcTemplate.update("DROP VIEW [dbo].[" + viewName + "]");

                    createView(jdbcTemplate, scriptContent);
                }
            }
        }

    }

    /**
     * @param jdbcTemplate
     * @param script
     * @return
     */
    private void createView(SimpleJdbcTemplate jdbcTemplate, String script)
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
     * @param script
     * @return
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