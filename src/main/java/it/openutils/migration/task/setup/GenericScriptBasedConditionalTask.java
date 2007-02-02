/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

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
 * @author fgiust
 * @version $Id$
 */
public class GenericScriptBasedConditionalTask implements DbTask
{

    /**
     * Logger.
     */
    private static Logger log = LoggerFactory.getLogger(GenericScriptBasedConditionalTask.class);

    /**
     * Script list to execute
     */
    protected List<Resource> scripts;

    private String check;

    /**
     * Sets the scripts.
     * @param scripts the scripts to set
     */
    public void setScripts(List<Resource> scripts)
    {
        this.scripts = scripts;
    }

    /**
     * Sets the check.
     * @param check the check to set
     */
    public void setCheck(String check)
    {
        this.check = check;
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
     * {@inheritDoc}
     */
    public String getDescription()
    {
        // @todo add better description and avoid NPEs
        return "Checking " + objectNameFromFileName(scripts.get(0));
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {

        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        for (Resource script : scripts)
        {
            int result = jdbcTemplate.queryForInt(check, this.objectNameFromFileName(script));
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
                        log.debug("Executing:\n{}", ddl);
                        jdbcTemplate.update(ddl);
                    }
                }
            }
        }

    }
}
