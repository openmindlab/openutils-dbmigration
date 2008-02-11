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
package it.openutils.migration.oracle;

import it.openutils.migration.task.setup.DbTask;

import java.text.MessageFormat;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


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

    private String creationQuery;

    private String selectUserSequences;

    private String selectAllSequences;

    private int startsWith;

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
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        for (String sequenceName : sequences)
        {
            int result = 0;

            if (StringUtils.contains(sequenceName, "."))
            {
                String[] tokens = StringUtils.split(sequenceName, ".");
                result = jdbcTemplate.queryForInt(selectAllSequences, new Object[]{tokens[1], tokens[0]});
            }
            else
            {
                result = jdbcTemplate.queryForInt(selectUserSequences, sequenceName);
            }

            if (result <= 0)
            {
                log.info("Creating new {}", sequenceName);
                jdbcTemplate.update(MessageFormat.format(creationQuery, new Object[]{sequenceName, startsWith}));
            }
        }
    }
}