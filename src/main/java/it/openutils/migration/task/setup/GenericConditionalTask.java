/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author fgiust
 * @version $Id$
 */
public class GenericConditionalTask extends BaseDbTask implements DbTask
{

    private String check;

    private String ddl;

    /**
     * {@inheritDoc}
     */
    public void setDdl(String ddls)
    {
        this.ddl = ddls;
    }

    /**
     * {@inheritDoc}
     */
    public void setCheck(String name)
    {
        this.check = name;
    }

    /**
     * Returns the check.
     * @return the check
     */
    public String getCheck()
    {
        return check;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        int result = jdbcTemplate.queryForInt(getCheck());
        if (result == 0)
        {
            String[] ddls = StringUtils.split(ddl, ';');
            for (String statement : ddls)
            {
                if (StringUtils.isNotBlank(statement))
                {
                    jdbcTemplate.update(statement);
                }
            }
        }
    }

}
