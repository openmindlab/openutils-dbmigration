/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

/**
 * @author fgiust
 * @version $Id$
 */
public abstract class BaseDbTask implements DbTask
{

    private String description;

    /**
     * {@inheritDoc}
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * {@inheritDoc}
     */
    public void setDescription(String description)
    {
        this.description = description;

    }

}
