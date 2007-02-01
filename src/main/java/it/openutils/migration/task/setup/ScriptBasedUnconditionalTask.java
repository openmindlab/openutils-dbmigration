/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

import java.io.IOException;
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
public class ScriptBasedUnconditionalTask extends BaseDbTask implements DbTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(ScriptBasedUnconditionalTask.class);

    private List<Resource> scripts;

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
    public void execute(DataSource dataSource)
    {

        for (Resource script : scripts)
        {
            if (script == null || !script.exists())
            {
                log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
                return;
            }

            String scriptContent;

            try
            {
                // @todo we should read line by line, avoiding to cache all the script in memory
                scriptContent = IOUtils.toString(script.getInputStream(), "UTF8");
            }
            catch (IOException e)
            {
                log.error("Unable to execute db task \"{}\", script \"{}\" can't be read.", getDescription(), script);
                return;
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

}
