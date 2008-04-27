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
package it.openutils.migration.task.setup;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * A base conditional task that executes a task only if an expected condition is met.
 * @author fgiust
 * @version $Id:SqlServerObjCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public abstract class BaseConditionalTask extends BaseDbTask
{

    /**
     * Logger.
     */
    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     * DDL to run when the condition is met.
     */
    protected String ddl;

    /**
     * If <code>true</code> executes only if check returned <code>false</code>
     */
    protected boolean not;

    /**
     * Map of key-value that will be replaced in ddl.
     */
    protected Map<String, String> variables;

    /**
     * Script list to execute
     */
    protected List<Resource> scripts;

    /**
     * Sets the scripts.
     * @param scripts the scripts to set
     */
    public void setScripts(List<Resource> scripts)
    {
        this.scripts = scripts;
    }

    /**
     * {@inheritDoc}
     */
    public final void setDdl(String ddls)
    {
        this.ddl = ddls;
    }

    /**
     * Sets the not.
     * @param not the not to set
     */
    public final void setNot(boolean not)
    {
        this.not = not;
    }

    /**
     * Sets the ddlScript.
     * @param ddlScript the ddlScript to set
     * @deprecated use the "scripts" property
     */
    @Deprecated
    public final void setDdlScript(Resource ddlScript)
    {
        log.warn("ddlScript is deprecated, please use \"scripts\"");
        if (scripts == null)
        {
            scripts = new ArrayList<Resource>(1);
        }
        scripts.add(ddlScript);
    }

    /**
     * Subclasses need to override this method and provide specific checks.
     * @param jdbcTemplate SimpleJdbcTemplate
     * @return true if the condition is met
     */
    public abstract boolean check(SimpleJdbcTemplate jdbcTemplate);

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {

        if (StringUtils.isNotEmpty(super.getDescription()))
        {
            return super.getDescription();
        }

        if (scripts != null && !scripts.isEmpty())
        {
            StringBuffer result = new StringBuffer();
            result.append("Checking :\n");
            for (Resource script : scripts)
            {
                result.append("  - " + objectNameFromFileName(script) + "\n");
            }
            return result.toString();

        }

        return getClass().getName();
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
     * Perform sobstitution in the given string.
     * @param string Original String
     * @return processed string
     */
    protected String performSubstitution(String string)
    {
        if (variables == null || variables.isEmpty())
        {
            return string;
        }

        String stringReplaced = string;
        for (String key : variables.keySet())
        {
            stringReplaced = StringUtils.replace(stringReplaced, "${" + key + "}", variables.get(key));
        }

        return stringReplaced;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        if (check(jdbcTemplate) ^ !not)
        {
            log.info("Executing Alter Task: {}", getDescription());

            if (StringUtils.isNotBlank(ddl))
            {
                executeSingle(jdbcTemplate, ddl);
            }
            else
            {
                if (scripts == null || scripts.isEmpty())
                {
                    log.error("Unable to execute db task \"{}\", no ddl or scripts configured.", getDescription());
                    return;
                }

                for (Resource script : scripts)
                {
                    String scriptContent = ddl;
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
                    executeSingle(jdbcTemplate, scriptContent);
                }
            }
        }
    }

    /**
     * @param jdbcTemplate
     * @param scriptContent
     */
    protected void executeSingle(SimpleJdbcTemplate jdbcTemplate, String scriptContent)
    {
        String[] ddls = StringUtils.split(performSubstitution(scriptContent), ';');
        for (String statement : ddls)
        {
            if (StringUtils.isNotBlank(statement))
            {
                log.debug("Executing:\n{}", statement);
                jdbcTemplate.update(statement);
            }
        }
    }

}
