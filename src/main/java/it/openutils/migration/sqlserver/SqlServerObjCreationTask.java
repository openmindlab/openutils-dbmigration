/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.GenericScriptBasedConditionalTask;

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
 * @version $Id:SqlServerObjCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerObjCreationTask extends GenericScriptBasedConditionalTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(SqlServerObjCreationTask.class);

    /**
     * Query to check with standard objects' name.
     */
    private String unqualifiedObjQuery;

    /**
     * Query to check with full database objects' hierarchy.
     */
    private String qualifiedObjQuery;

    /**
     * The db with the objects, synonyms will point to this db.
     */
    private String sourceDb;

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
     * Sets the sourceDb.
     * @param sourceDb the sourceDb to set
     */
    public void setSourceDb(String sourceDb)
    {
        this.sourceDb = sourceDb;
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
}