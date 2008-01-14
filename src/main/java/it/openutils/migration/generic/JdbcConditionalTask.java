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
package it.openutils.migration.generic;

import it.openutils.migration.task.setup.BaseDbTask;
import it.openutils.migration.task.setup.DbTask;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author fgiust
 * @version $Id:SqlServerObjCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public abstract class JdbcConditionalTask extends BaseDbTask implements DbTask
{

    private String ddl;

    private boolean not;

    /**
     * {@inheritDoc}
     */
    public void setDdl(String ddls)
    {
        this.ddl = ddls;
    }

    /**
     * Sets the not.
     * @param not the not to set
     */
    public void setNot(boolean not)
    {
        this.not = not;
    }

    public abstract boolean check(SimpleJdbcTemplate jdbcTemplate);

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        if (check(jdbcTemplate) ^ !not)
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
