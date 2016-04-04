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

package it.openutils.migration.oracle;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.DbTask;


/**
 * <p>
 * Db tasks that handles the initial setup of views.
 * </p>
 * <p>
 * Limitations:
 * </p>
 * <ol>
 * <li>* not supported in field list</li>
 * <li>fields must be enclosed in quotes</li>
 * </ol>
 * @author albertoq
 * @version $Id$
 */
public class OracleViewCreateOrUpdateTask implements DbTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(OracleViewCreateOrUpdateTask.class);

    /**
     * Script list to execute
     */
    private List<Resource> scripts;

    /**
     * Query to verify view existence
     */
    private String selectUserViewExistence;

    /**
     * Query to retrieve view ddl
     */
    private String selectUserViewDDL;

    /**
     * Statement to drop a view
     */
    private String dropView;

    public String getDescription()
    {
        return "Checking Views";
    }

    public void setScripts(List<Resource> scripts)
    {
        this.scripts = scripts;
    }

    public void setSelectUserViewExistence(String selectUserViewExistence)
    {
        this.selectUserViewExistence = selectUserViewExistence;
    }

    public void setSelectUserViewDDL(String selectUserViewDDL)
    {
        this.selectUserViewDDL = selectUserViewDDL;
    }

    public void setDropView(String dropView)
    {
        this.dropView = dropView;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (Resource script : scripts)
        {
            String viewName = this.objectNameFromFileName(script);

            int result = jdbcTemplate.queryForObject(this.selectUserViewExistence, Integer.class, viewName);

            String scriptContent = readFully(script);

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
                String scriptBody = extractViewBody(scriptContent);
                if (scriptBody == null)
                {
                    continue;
                }

                String previousDDl = jdbcTemplate
                    .queryForObject(this.selectUserViewDDL, new Object[]{viewName }, String.class);

                if (!StringUtils.equals(previousDDl.trim(), scriptBody.trim()))
                {
                    log.info(
                        "Previous definition of view {} differs from actual. Dropping and recreating view",
                        new Object[]{viewName });

                    jdbcTemplate.update(MessageFormat.format(this.dropView, new Object[]{viewName }));

                    createView(jdbcTemplate, scriptContent);
                }
            }
        }

    }

    /**
     * @param scriptContent
     * @return
     */
    private String extractViewBody(String scriptContent)
    {
        Pattern pattern = Pattern.compile(".*?\\s+AS\\s+(.*);", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(scriptContent);
        boolean bodyFound = matcher.find();
        if (bodyFound)
        {
            return matcher.group(1);
        }
        else
        {
            return null;
        }
    }

    /**
     * @param jdbcTemplate
     * @param script
     * @return
     */
    private void createView(JdbcTemplate jdbcTemplate, String script)
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
     * @param script The script resource
     * @return The script name
     */
    protected String objectNameFromFileName(Resource script)
    {
        return StringUtils.substringBeforeLast(script.getFilename(), ".");
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
        return scriptContent;
    }

}
