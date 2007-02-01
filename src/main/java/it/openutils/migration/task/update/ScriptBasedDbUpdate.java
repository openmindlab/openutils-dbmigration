/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.update;

import it.openutils.migration.task.setup.ScriptBasedUnconditionalTask;

import java.io.IOException;
import java.io.InputStream;

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
public class ScriptBasedDbUpdate implements DbUpdate
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(ScriptBasedUnconditionalTask.class);

    private Resource script;

    private int version;

    private String description;

    /**
     * Sets the script.
     * @param script the script to set
     */
    public void setScript(Resource script)
    {
        this.script = script;
    }

    /**
     * {@inheritDoc}
     */
    public int getVersion()
    {
        return version;
    }

    /**
     * Sets the version.
     * @param version the version to set
     */
    public void setVersion(int version)
    {
        this.version = version;
    }

    /**
     * {@inheritDoc}
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description.
     * @param description the description to set
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
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
            log.error("Unable to execute db task \"{}\", script \"{}\" can't be read.", getDescription(), script);
            return;
        }
        finally
        {
            IOUtils.closeQuietly(is);
        }

        String[] ddls = StringUtils.split(scriptContent, ";");
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

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
