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

package it.openutils.migration.task.setup;

import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * A siple update task that executes a query and apply a DDL only if the query retuns a certain value (by default
 * <code>0</code>)
 * @author fgiust
 * @version $Id$
 */
public class GenericConditionalTask extends BaseConditionalTask
{

    /**
     * Query for the check condition.
     */
    protected String check;

    /**
     * The value that will let the ddl script start. Default is <code>0</code>.
     */
    protected Integer triggerValue = 0;

    /**
     * {@inheritDoc}
     */
    public final void setCheck(String name)
    {
        this.check = name;
    }

    /**
     * Returns the check.
     * @return the check
     */
    public String getCheck()
    {
        return check;
    }

    /**
     * Sets the triggerValue.
     * @param triggerValue the triggerValue to set
     */
    public final void setTriggerValue(Integer triggerValue)
    {
        this.triggerValue = triggerValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {
        String supDesc = super.getDescription();
        if (StringUtils.isNotEmpty(supDesc) && !StringUtils.equals(supDesc, getClass().getName()))
        {
            return super.getDescription();
        }

        if (StringUtils.isNotBlank(getCheck()))
        {
            return "Checking alter task condition: " + getCheck();
        }

        return getClass().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean check(SimpleJdbcTemplate jdbcTemplate)
    {
        int result = jdbcTemplate.queryForInt(performSubstitution(getCheck()));
        return result == triggerValue;
    }

    /**
     * Sets the sourceDb.
     * @param sourceDb the sourceDb to set
     * @deprecated
     */
    @Deprecated
    public final void setSourceDb(String sourceDb)
    {
        log.warn("sourceDb in "
            + getClass().getName()
            + " is deprecated, please use the more generic \"variables\" property");

        if (this.variables == null)
        {
            variables = new HashMap<String, String>(1);
        }
        variables.put("sourceDb", sourceDb);
    }

}
