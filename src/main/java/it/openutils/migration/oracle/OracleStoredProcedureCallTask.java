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

import it.openutils.migration.task.setup.GenericConditionalTask;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;


public class OracleStoredProcedureCallTask extends GenericConditionalTask
{
    @Override
    protected void executeSingle(SimpleJdbcTemplate jdbcTemplate, final String scriptContent)
    {
        jdbcTemplate.getJdbcOperations().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                CallableStatement cs = null;
                try
                {
                    cs = con.prepareCall("{call " + scriptContent.trim() + "}");
                    cs.execute();
                }
                finally
                {
                    JdbcUtils.closeStatement(cs);
                }
                return null;
            }
        });
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription()
    {
        return "calling stored procedures";
    }
}
