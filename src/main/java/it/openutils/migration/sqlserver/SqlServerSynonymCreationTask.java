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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.DbTask;


/**
 * @author Danilo Ghirardelli
 * 
 */
public class SqlServerSynonymCreationTask implements DbTask
{

    private List<String> objects;

    private Logger log = LoggerFactory.getLogger(SqlServerSynonymCreationTask.class);

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
    @Override
    public void execute(DataSource dataSource)
    {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        for (String objectName : objects)
        {

            String originalobj = objectName;
            String synonym = objectName;

            if (StringUtils.contains(objectName, "="))
            {
                synonym = StringUtils.substringBefore(originalobj, "=");
                originalobj = StringUtils.substringAfter(originalobj, "=");
            }

            int result = jdbcTemplate.queryForObject(
                "select count(*) from dbo.sysobjects where id = object_id(?) and xtype = N'SN'",
                new Object[]{synonym },
                Integer.class);

            if (result > 0)
            {
                // existing synonym, nothing to do
                continue;
            }

            result = jdbcTemplate.queryForObject(
                "select count(*) from dbo.sysobjects where id = object_id(?)",
                new Object[]{synonym },
                Integer.class);

            if (result > 0)
            {
                log.warn("An existing object with name {} has been found, but it's not a synonym as expected", synonym);
                continue;
            }

            jdbcTemplate.update("CREATE SYNONYM " + synonym + " FOR " + originalobj + "");

        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {
        return "Creating synonyms from " + ArrayUtils.toString(objects);
    }
}