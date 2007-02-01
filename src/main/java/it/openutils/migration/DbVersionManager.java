/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration;

/**
 * @author fgiust
 * @version $Id$
 */
public interface DbVersionManager
{

    /**
     * Returns the current version for the db, usually reading from a configuration table. This should also handle table
     * initialization.
     * @return current db version
     */
    int getCurrentVersion();

    /**
     * Saves a new version for the db.
     * @param version new version
     */
    void setNewVersion(int version);
}
