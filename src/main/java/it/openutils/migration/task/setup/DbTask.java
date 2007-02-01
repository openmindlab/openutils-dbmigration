/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

import javax.sql.DataSource;


/**
 * @author fgiust
 * @version $Id$
 */
public interface DbTask
{

    /**
     * Returns a description for this task
     * @return task description
     */
    String getDescription();

    /**
     * Execute this db task.
     * @param dataSource javax.sql.datasource
     */
    void execute(DataSource dataSource);
}
