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

package it.openutils.migration.task.setup;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * Executes the specified task list only if database vendor info matches both product name and version stated, otherwise
 * ignores the tasks. Name and version string comparison is case insensitive. If databaseProductName or version are not
 * specified in configuration (empty or null), those are treated as matching values. If both are missing, the task list
 * is executed always. Example, to execute a specific task only on MySQL: <code>
      <bean class="it.openutils.migration.task.setup.DatabaseConditionalTaskList">
          <property name="databaseProductName" value="MySQL" />
          <property name="description" value="Mysql db creation" />
          <property name="tasks">
            <list>
             <bean class="MyTask">...
             </bean>
            </list>
          </property>
        </bean><code>
 * @author dfghi
 */
public class DatabaseConditionalTaskList extends BaseDbTask {

    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Database vendor name. Usual names are "Apache Derby", "PostgreSQL", "MySQL"...
     */
    private String databaseProductName;

    /**
     * Database version. This is not the current dbMigration version, but the db product version: for Derby should be
     * something like "10.x.x"
     */
    private String databaseProductVersion;

    /**
     * Task to execute if product and version matches.
     */
    private List<DbTask> tasks;

    /**
     * @param databaseProductName
     *            the databaseProductName to set
     */
    public void setDatabaseProductName(String databaseProductName) {
        this.databaseProductName = databaseProductName;
    }

    /**
     * @param databaseProductVersion
     *            the databaseProductVersion to set
     */
    public void setDatabaseProductVersion(String databaseProductVersion) {
        this.databaseProductVersion = databaseProductVersion;
    }

    /**
     * @param tasks
     *            the tasks to set
     */
    public void setTasks(List<DbTask> tasks) {
        this.tasks = tasks;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String databaseRealProductName = (String) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                return con.getMetaData().getDatabaseProductName();
            }
        });
        String databaseRealProductVersion = (String) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                return con.getMetaData().getDatabaseProductVersion();
            }
        });
        if (StringUtils.isNotEmpty(this.databaseProductName) && (!StringUtils.equalsIgnoreCase(databaseRealProductName, this.databaseProductName))) {
            log.info("Skipping tasks for specific database: " + this.databaseProductName + " Database found: " + databaseRealProductName);
            return;
        }
        if (StringUtils.isNotEmpty(this.databaseProductVersion) && (!StringUtils.equalsIgnoreCase(databaseRealProductVersion, this.databaseProductVersion))) {
            log.info("Skipping tasks for specific database version: " + this.databaseProductVersion + " Database version found: " + databaseRealProductVersion);
            return;
        }
        for (DbTask task : tasks) {
            log.debug(task.getDescription());
            task.execute(dataSource);
        }
    }
}
