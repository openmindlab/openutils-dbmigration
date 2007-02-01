/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.update;

import it.openutils.migration.task.setup.DbTask;


/**
 * @author fgiust
 * @version $Id$
 */
public interface DbUpdate extends DbTask
{

    /**
     * Returns the version for this update. THe task will be executed only if the current db version is lower than this.
     * @return target version for this upgrade.
     */
    int getVersion();

}
