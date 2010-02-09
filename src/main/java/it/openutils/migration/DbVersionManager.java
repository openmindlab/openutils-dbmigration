/**
 *
 * openutils db migration (http://www.openmindlab.com/lab/products/dbmigration.html)
 * Copyright(C) null-2010, Openmind S.r.l. http://www.openmindonline.it
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
