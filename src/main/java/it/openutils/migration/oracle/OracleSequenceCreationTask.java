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

import java.text.MessageFormat;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.DbTask;


/**
 * Db tasks that handles the initial setup of sequences.
 * @author fgiust
 * @version $Id$
 */
public class OracleSequenceCreationTask implements DbTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(OracleSequenceCreationTask.class);

    private List<String> sequences;

    private String creationQuery = "CREATE SEQUENCE {0} INCREMENT BY 1 START WITH {1} MAXVALUE 1E28 MINVALUE 1 NOCACHE NOCYCLE ORDER";

    private String selectUserSequences = "SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ?";

    private String selectAllSequences = "SELECT COUNT(*) FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = ? AND SEQUENCE_OWNER = ?";

    private int startsWith = 1;

    /**
     * Sets the sequences.
     * @param sequences the sequences to set
     */
    public void setSequences(List<String> sequences)
    {
        this.sequences = sequences;
    }

    /**
     * Sets the creationQuery.
     * @param creationQuery the creationQuery to set
     */
    public void setCreationQuery(String creationQuery)
    {
        this.creationQuery = creationQuery;
    }

    /**
     * Sets the selectAllSequences.
     * @param selectAllSequences the selectAllSequences to set
     */
    public void setSelectAllSequences(String selectAllSequences)
    {
        this.selectAllSequences = selectAllSequences;
    }

    /**
     * Sets the startsWith.
     * @param startsWith the startsWith to set
     */
    public void setStartsWith(int startsWith)
    {
        this.startsWith = startsWith;
    }

    /**
     * Sets the selectUserSequences.
     * @param selectUserSequences the selectUserSequences to set
     */
    public void setSelectUserSequences(String selectUserSequences)
    {
        this.selectUserSequences = selectUserSequences;
    }

    /**
     * {@inheritDoc}
     */
    public String getDescription()
    {
        return "Checking Sequences";
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (String sequenceName : sequences)
        {
            int result = 0;

            if (StringUtils.contains(sequenceName, "."))
            {
                String[] tokens = StringUtils.split(sequenceName, ".");
                result = jdbcTemplate
                    .queryForObject(selectAllSequences, Integer.class, new Object[]{tokens[1], tokens[0] });
            }
            else
            {
                result = jdbcTemplate.queryForObject(selectUserSequences, Integer.class, sequenceName);
            }

            if (result <= 0)
            {
                log.info("Creating new {}", sequenceName);
                jdbcTemplate.update(
                    MessageFormat.format(creationQuery, new Object[]{sequenceName, String.valueOf(startsWith) }));
            }
        }
    }
}