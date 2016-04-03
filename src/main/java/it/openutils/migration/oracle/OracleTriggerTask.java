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

package it.openutils.migration.oracle;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.GenericConditionalTask;


public class OracleTriggerTask extends GenericConditionalTask
{
    @Override
    protected void executeSingle(JdbcTemplate jdbcTemplate, final String scriptContent)
    {
        if (StringUtils.isNotBlank(scriptContent))
        {
            jdbcTemplate.update(scriptContent.trim());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {
        return "triggers";
    }
}
