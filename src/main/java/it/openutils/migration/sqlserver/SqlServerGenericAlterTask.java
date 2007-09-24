/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.GenericConditionalTask;

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
 * This class is used for any alter task needed. The triggerValue is the value (only numeric) of result query that
 * activates the alter script. Default is zero. Both the condition query and the alter query can be script or embedded
 * in the xml.
 * @author Danilo Ghirardelli
 * @version $Id:SqlServerGenericAlterTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerGenericAlterTask extends GenericConditionalTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(SqlServerGenericAlterTask.class);

    /**
     * The condition can be in a string or in a script file.
     */
    private Resource checkScript;

    /**
     * Override of the corresponding string.
     */
    private String ddl;

    /**
     * The alter script can be embedded in the xml file or in a script file.
     */
    private Resource ddlScript;

    /**
     * The value that will let the ddl script start. Default is zero.
     */
    private Integer triggerValue = 0;

    /**
     * The db with the objects, synonyms will point to this db.
     */
    private String sourceDb;

    /**
     * Sets the checkScript.
     * @param checkScript the checkScript to set
     */
    public void setCheckScript(Resource checkScript)
    {
        this.checkScript = checkScript;
    }

    /**
     * Sets the ddl.
     * @param ddl the ddl to set
     */
    @Override
    public void setDdl(String ddl)
    {
        this.ddl = ddl;
    }

    /**
     * Sets the ddlScript.
     * @param ddlScript the ddlScript to set
     */
    public void setDdlScript(Resource ddlScript)
    {
        this.ddlScript = ddlScript;
    }

    /**
     * Sets the triggerValue.
     * @param triggerValue the triggerValue to set
     */
    public void setTriggerValue(Integer triggerValue)
    {
        this.triggerValue = triggerValue;
    }

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
    @Override
    public String getDescription()
    {

        if (StringUtils.isNotEmpty(super.getDescription()))
        {
            return super.getDescription();
        }

        StringBuffer result = new StringBuffer();
        if (StringUtils.isNotBlank(getCheck()))
        {
            result.append("Checking alter task condition:\n" + getCheck());
        }
        else
        {
            result.append("Checking alter task condition in script:\n" + checkScript);
        }
        return result.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        String resultQuery;
        if (StringUtils.isNotBlank(getCheck()))
        {
            // Simple query embedded in xml.
            resultQuery = getCheck();
            resultQuery = StringUtils.isNotBlank(this.sourceDb) ? StringUtils.replace(
                resultQuery,
                "${sourceDb}",
                this.sourceDb) : resultQuery;
        }
        else
        {
            // Check query in a script file.
            if (checkScript == null || !checkScript.exists())
            {
                log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), checkScript);
                return;
            }
            InputStream is = null;
            String scriptContent;
            try
            {
                is = checkScript.getInputStream();
                scriptContent = IOUtils.toString(is, "UTF8");
            }
            catch (IOException e)
            {
                log.error(
                    "Unable to execute db task \"{}\", script \"{}\" can't be read.",
                    getDescription(),
                    checkScript);
                return;
            }
            finally
            {
                IOUtils.closeQuietly(is);
            }
            resultQuery = scriptContent;
            resultQuery = StringUtils.isNotBlank(this.sourceDb) ? StringUtils.replace(
                resultQuery,
                "${sourceDb}",
                this.sourceDb) : resultQuery;
        }

        if ((triggerValue != null) && (jdbcTemplate.queryForInt(resultQuery) == triggerValue))
        {
            log.info("Executing Alter Task: {}", getDescription());
            String scriptContent;

            if (StringUtils.isNotBlank(ddl))
            {
                scriptContent = ddl;
            }
            else
            {
                if (ddlScript == null || !ddlScript.exists())
                {
                    log
                        .error(
                            "Unable to execute db task \"{}\", script \"{}\" not found.",
                            getDescription(),
                            ddlScript);
                    return;
                }
                InputStream is = null;
                try
                {
                    is = ddlScript.getInputStream();
                    scriptContent = IOUtils.toString(is, "UTF8");
                }
                catch (IOException e)
                {
                    log.error(
                        "Unable to execute db task \"{}\", script \"{}\" can't be read.",
                        getDescription(),
                        ddlScript);
                    return;
                }
                finally
                {
                    IOUtils.closeQuietly(is);
                }
            }

            String[] ddls = StringUtils.split(scriptContent, ";");

            for (String ddl : ddls)
            {
                if (StringUtils.isNotBlank(ddl))
                {
                    String ddlReplaced = ddl;
                    ddlReplaced = StringUtils.isNotBlank(this.sourceDb) ? StringUtils.replace(
                        ddlReplaced,
                        "${sourceDb}",
                        this.sourceDb) : ddlReplaced;
                    log.debug("Executing:\n{}", ddlReplaced);
                    jdbcTemplate.update(ddlReplaced);
                }
            }
        }
    }
}