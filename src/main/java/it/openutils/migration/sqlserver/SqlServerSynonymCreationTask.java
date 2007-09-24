/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.sqlserver;

import it.openutils.migration.task.setup.DbTask;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author Danilo Ghirardelli
 * @version $Id:SqlServerSynonymCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerSynonymCreationTask implements DbTask
{

    private String source;

    private List<String> objects;

    /**
     * Sets the source.
     * @param source the source to set
     */
    public void setSource(String source)
    {
        this.source = source;
    }

    /**
     * Sets the objects.
     * @param objects the objects to set
     */
    public void setObjects(List<String> objects)
    {
        this.objects = objects;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {

        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        for (String objectName : objects)
        {
            int result = jdbcTemplate.queryForInt(
                "select count(*) from dbo.sysobjects where id = object_id(?) and xtype = N'SN'",
                objectName);
            if (result == 0)
            {
                jdbcTemplate.update("CREATE SYNONYM [dbo].["
                    + objectName
                    + "] FOR ["
                    + source
                    + "].[dbo].["
                    + objectName
                    + "]");
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    public String getDescription()
    {
        return "Creating synonyms from " + source;
    }
}