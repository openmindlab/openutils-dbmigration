/*
 * Copyright Openmind http://www.openmindonline.it
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.openutils.migration;

import javax.sql.DataSource;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * <pre>
 * &lt;bean class="it.openutils.migration.DefaultDbVersionManagerImpl">
 *   &lt;property name="dataSource" ref="dataSource" />
 *   &lt;property name="versionQuery">
 *     &lt;value>SELECT DBVERSION FROM DBVERSION&lt;/value>
 *   &lt;/property>
 *   &lt;property name="versionUpdate">
 *     &lt;value>UPDATE DBVERSION SET DBVERSION = ?&lt;/value>
 *   &lt;/property>
 *   &lt;property name="versionCreate">
 *     &lt;value>INSERT INTO DBVERSION(DBVERSION) VALUES (0)&lt;/value>
 *   &lt;/property>
 * &lt;/bean>
 * </pre>
 *
 * @author fgiust
 * @version $Id$
 */
public class DefaultDbVersionManagerImpl implements DbVersionManager
{

    private String versionQuery;

    private String versionUpdate;

    private String versionCreate;

    private DataSource dataSource;

    /**
     * Sets the versionCreate.
     * @param versionCreate the versionCreate to set
     */
    public void setVersionCreate(String versionCreate)
    {
        this.versionCreate = versionCreate;
    }

    /**
     * Sets the versionQuery.
     * @param versionQuery the versionQuery to set
     */
    public void setVersionQuery(String versionQuery)
    {
        this.versionQuery = versionQuery;
    }

    /**
     * Sets the versionUpdate.
     * @param versionUpdate the versionUpdate to set
     */
    public void setVersionUpdate(String versionUpdate)
    {
        this.versionUpdate = versionUpdate;
    }

    /**
     * Sets the dataSource.
     * @param dataSource the dataSource to set
     */
    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    /**
     * {@inheritDoc}
     */
    public int getCurrentVersion()
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        int initialVersion = 0;
        try
        {
            initialVersion = jdbcTemplate.queryForInt(versionQuery);
        }
        catch (EmptyResultDataAccessException e)
        {
            jdbcTemplate.update(versionCreate);
        }
        return initialVersion;
    }

    /**
     * {@inheritDoc}
     */
    public void setNewVersion(int version)
    {
        new SimpleJdbcTemplate(dataSource).update(versionUpdate, version);
    }

}
