/**
 *
 * openutils dbmigration (http://www.openmindlab.com/lab/products/dbmigration.html)
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

import it.openutils.migration.task.setup.DbTask;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * This class can be used to remove anonymous unique constraint from sql server db specifying the table name and the
 * column name that have the constraint.
 * @author gcast
 * 
 */
public class SqlServerDefaultValueConstraintDelete implements DbTask // NO_UCD (unused code)
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(SqlServerDefaultValueConstraintDelete.class);

    /**
     * name of the table with the unique constraint to remove.
     */
    private String tableName;

    /**
     * name of the column with the unique constraint to remove.
     */
    private String columnName;

    /**
     * name of the column with the unique constraint to remove.
     */
    private String description;

    private static final String QUERY_TO_FIND_NAME_OF_UNIQUE_CONSTRAINT_INDEX = "SELECT NAME "
        + " FROM SYSOBJECTS SO "
        + " JOIN SYSCONSTRAINTS SC ON SO.ID = SC.CONSTID "
        + " WHERE OBJECT_NAME(SO.PARENT_OBJ) = ? "
        + " AND SO.XTYPE = 'D' AND SC.COLID = "
        + " (SELECT COLID FROM SYSCOLUMNS WHERE ID = OBJECT_ID(?) AND NAME = ?)";

    /**
     * Sets the tableName.
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Sets the columnName.
     * @param columnName the columnName to set
     */
    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(columnName))
        {
            log.error(
                "Both table name and column name must be specificed. table name: \"{}\", column name \"{}\".",
                tableName,
                columnName);
            return;
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        List<String> query = jdbcTemplate.query(QUERY_TO_FIND_NAME_OF_UNIQUE_CONSTRAINT_INDEX, new RowMapper<String>()
        {

            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException
            {
                return rs.getString("name");
            }
        }, tableName, tableName, columnName);
        if (query.size() == 1)
        {
            String alterTableDropConstraint = "ALTER TABLE " + tableName + " DROP CONSTRAINT " + query.get(0);
            log.debug("Executing:\n{}", alterTableDropConstraint);
            jdbcTemplate.update(alterTableDropConstraint);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description.
     * @param description the description to set
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

}
