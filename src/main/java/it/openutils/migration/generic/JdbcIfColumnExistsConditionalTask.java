package it.openutils.migration.generic;

import it.openutils.migration.task.setup.BaseConditionalTask;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * @author fgiust
 * @version $Id: $
 */
public class JdbcIfColumnExistsConditionalTask extends BaseConditionalTask
{

    private String column;

    /**
     * Sets the column.
     * @param column the column to set
     */
    public void setColumn(String column)
    {
        this.column = column;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean check(SimpleJdbcTemplate jdbcTemplate)
    {
        final String catalog = null;
        final String schema = null;

        String columnTrim = StringUtils.trim(column);

        final String tableName = StringUtils.substringBefore(columnTrim, ".");
        final String columnName = StringUtils.substringAfter(columnTrim, ".");
        return (Boolean) jdbcTemplate.getJdbcOperations().execute(new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {

                DatabaseMetaData dbMetadata = con.getMetaData();
                ResultSet rs = dbMetadata.getColumns(catalog, schema, tableName, columnName);
                boolean tableExists = rs.first();
                rs.close();

                return !tableExists;
            }
        });
    }

}
