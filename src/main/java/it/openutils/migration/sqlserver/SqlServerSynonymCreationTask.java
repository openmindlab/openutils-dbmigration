/**
 *
 * openutils db migration (http://www.openmindlab.com/lab/products/dbmigration.html)
 * Copyright(C) 2007-2010, Openmind S.r.l. http://www.openmindonline.it
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

package it.openutils.migration.sqlserver;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.DbTask;


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

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        for (String objectName : objects)
        {
            int result = jdbcTemplate.queryForObject(
                "select count(*) from dbo.sysobjects where id = object_id(?) and xtype = N'SN'",
                Integer.class,
                objectName);
            if (result == 0)
            {
                jdbcTemplate.update(
                    "CREATE SYNONYM [dbo].[" + objectName + "] FOR [" + source + "].[dbo].[" + objectName + "]");
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