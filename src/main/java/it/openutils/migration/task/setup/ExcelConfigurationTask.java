/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;


/**
 * @author fgiust
 * @version $Id$
 */
public class ExcelConfigurationTask extends BaseDbTask implements DbTask
{

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(ScriptBasedUnconditionalTask.class);

    private Resource script;

    private Map<String, ExcelConfigurationTask.QueryConfig> config;

    /**
     * Sets the script.
     * @param script the script to set
     */
    public void setScript(Resource script)
    {
        this.script = script;
    }

    /**
     * Sets the config.
     * @param config the config to set
     */
    public void setConfig(Map<String, ExcelConfigurationTask.QueryConfig> config)
    {
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        if (script == null || !script.exists())
        {
            log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
            return;
        }

        InputStream is = null;
        try
        {
            is = script.getInputStream();
            POIFSFileSystem fs = new POIFSFileSystem(is);
            HSSFWorkbook hssfworkbook = new HSSFWorkbook(fs);
            int sheetNums = hssfworkbook.getNumberOfSheets();
            for (int j = 0; j < sheetNums; j++)
            {
                HSSFSheet sheet = hssfworkbook.getSheetAt(j);
                String tableName = hssfworkbook.getSheetName(j);

                QueryConfig conf = config.get(tableName);
                if (conf == null)
                {
                    log.error("Unable to handle table {}", tableName);
                    continue;
                }
                processSheet(sheet, tableName, conf, dataSource);

            }

        }
        catch (IOException e)
        {
            log.error(e.getMessage(), e);
        }
        finally
        {
            IOUtils.closeQuietly(is);
        }

    }

    /**
     * @param sheet
     * @param tableName
     */
    private void processSheet(HSSFSheet sheet, final String tableName, QueryConfig con, DataSource dataSource)
    {
        final List<String> columns = new ArrayList<String>();

        HSSFRow row = sheet.getRow(0);
        for (short k = 0; k < row.getLastCellNum(); k++)
        {
            HSSFCell cell = row.getCell(k);
            if (cell != null)
            {
                String columnName = cell.getStringCellValue();
                if (StringUtils.isNotBlank(columnName))
                {
                    columns.add(StringUtils.trim(columnName));
                }
                else
                {
                    break;
                }
            }
        }

        log.debug("Table: {}, Columns: {}", tableName, columns);

        final List<Integer> types = new ArrayList<Integer>();

        boolean result = (Boolean) new JdbcTemplate(dataSource).execute(new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                for (String column : columns)
                {
                    ResultSet res = con.getMetaData().getColumns(null, null, tableName, column);
                    if (res.next())
                    {
                        types.add(res.getInt("DATA_TYPE"));
                    }
                    else
                    {
                        log.warn("Unable to determine type for column '{}' in table '{}'", column, tableName);
                        return false;
                    }
                    res.close();
                }
                return true;
            }
        });

        if (!result)
        {
            log.warn("Skipping sheet {} ", tableName);
        }

        String checkStatement = StringUtils.remove(StringUtils.trim(con.getCheckQuery()), "\n");
        String insertStatement = StringUtils.remove(StringUtils.trim(con.getInsertQuery()), "\n");
        String selectStatement = StringUtils.remove(StringUtils.trim(con.getSelectQuery()), "\n");
        String updateStatement = StringUtils.remove(StringUtils.trim(con.getUpdateQuery()), "\n");

        processRecords(
            sheet,
            columns,
            ArrayUtils.toPrimitive(types.toArray(new Integer[types.size()]), Types.NULL),
            checkStatement,
            insertStatement,
            selectStatement,
            updateStatement,
            dataSource,
            tableName);
    }

    /**
     * @param sheet
     * @param columns
     * @param checkStatement
     * @param insertStatement
     * @param updateStatement
     * @param selectStatement
     */
    private void processRecords(HSSFSheet sheet, List<String> columns, int[] types, String checkStatement,
        String insertStatement, String selectStatement, String updateStatement, DataSource dataSource, String tableName)
    {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        int checkNum = StringUtils.countMatches(checkStatement, "?");
        int insertNum = StringUtils.countMatches(insertStatement, "?");
        int selectNum = StringUtils.countMatches(selectStatement, "?");
        int updateNum = StringUtils.countMatches(updateStatement, "?");

        HSSFRow row;
        for (short rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++)
        {
            row = sheet.getRow(rowNum);
            if (row == null)
            {
                return;
            }

            List<String> values = new ArrayList<String>();

            for (short k = 0; k < columns.size() && k <= row.getLastCellNum(); k++)
            {
                HSSFCell cell = row.getCell(k);
                String value = null;

                if (cell == null)
                {
                    value = StringUtils.EMPTY;
                }
                else if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING)
                {
                    value = cell.getStringCellValue();
                }
                else if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC)
                {
                    double valueDouble = cell.getNumericCellValue();
                    // when need to really check if it is a double or an int
                    double fraction = valueDouble % 1;
                    if (fraction == 0)
                    {
                        value = Integer.toString((int) valueDouble);
                    }
                    else
                    {
                        value = Double.toString(valueDouble);
                    }
                }

                if (StringUtils.isEmpty(value))
                {
                    value = StringUtils.EMPTY;
                }

                if ("<NULL>".equalsIgnoreCase(value))
                {
                    value = null;
                }

                values.add(value);
            }

            Object[] checkParams = ArrayUtils.subarray(values.toArray(), 0, checkNum);
            for (int i = 0; i < checkParams.length; i++)
            {
                if (StringUtils.isEmpty((String) checkParams[i]))
                {
                    return;
                }
            }

            int existing;
            try
            {
                existing = jdbcTemplate.queryForInt(checkStatement, checkParams);
            }
            catch (BadSqlGrammarException bsge)
            {
                log.error("Error executing check query, current sheet will be skipped. {} Query in error: {}", bsge
                    .getMessage(), checkStatement);
                return;
            }

            if (existing == 0)
            {
                Object[] insertParams = ArrayUtils.subarray(values.toArray(), 0, insertNum);
                int[] insertTypes = ArrayUtils.subarray(types, 0, insertNum);
                if (log.isDebugEnabled())
                {
                    log.debug("Missing record with key {}; inserting {}", ArrayUtils.toString(checkParams), ArrayUtils
                        .toString(insertParams));
                }

                if (insertParams.length != insertTypes.length)
                {
                    log.warn("Invalid number of param/type for table {}. Params: {}, types: {}", new Object[]{
                        tableName,
                        insertParams.length,
                        insertTypes.length });
                }

                try
                {
                    jdbcTemplate.update(updateStatement, insertParams, insertTypes);
                }
                catch (DataIntegrityViolationException bsge)
                {
                    log
                        .error(
                            "Error executing update, record at {}:{} will be skipped. Query in error: '{}', values: {}. Error message: {}",
                            new Object[]{
                                tableName,
                                rowNum + 1,
                                insertStatement,
                                ArrayUtils.toString(insertParams),
                                bsge.getMessage() });
                    continue;
                }
            }
            else if (StringUtils.isNotBlank(updateStatement) && StringUtils.isNotBlank(selectStatement))
            {
                try
                {
                    RowMapper rowMapper = new ColumnMapRowMapper();
                    Object[] selectParams = ArrayUtils.subarray(values.toArray(), 0, selectNum);
                    List selectResult = jdbcTemplate.query(selectStatement, selectParams, rowMapper);
                    Map<String, Object> fetchedColumns = (Map<String, Object>) selectResult.get(0);
                    int i = 0;
                    boolean updateNeeded = false;
                    for (String columnName : columns)
                    {
                        Object columnObject = fetchedColumns.get(columnName);
                        if (columnObject == null)
                        {
                            continue;
                        }
                        String columnValue = ObjectUtils.toString(fetchedColumns.get(columnName));
                        if (!StringUtils.equals(columnValue, values.get(i)))
                        {
                            updateNeeded = true;
                            break;
                        }
                        i++;
                    }
                    if (updateNeeded)
                    {
                        Object[] updateParams = ArrayUtils.subarray(values.toArray(), 0, updateNum);
                        int[] insertTypes = ArrayUtils.subarray(types, 0, insertNum);
                        if (log.isDebugEnabled())
                        {
                            log.debug(
                                "Missing record with key {}; updating {}",
                                ArrayUtils.toString(checkParams),
                                ArrayUtils.toString(updateParams));
                        }

                        if (updateParams.length != insertTypes.length)
                        {
                            log.warn("Invalid number of param/type for table {}. Params: {}, types: {}", new Object[]{
                                tableName,
                                updateParams.length,
                                insertTypes.length });
                        }

                        try
                        {
                            Object[] compoundUpdateParams = new Object[checkParams.length + updateParams.length];
                            System.arraycopy(updateParams, 0, compoundUpdateParams, 0, updateParams.length);
                            System.arraycopy(
                                checkParams,
                                0,
                                compoundUpdateParams,
                                compoundUpdateParams.length - 1,
                                checkParams.length);
                            jdbcTemplate.update(updateStatement, compoundUpdateParams);
                        }
                        catch (DataIntegrityViolationException bsge)
                        {
                            log
                                .error(
                                    "Error executing insert, record at {}:{} will be skipped. Query in error: '{}', values: {}. Error message: {}",
                                    new Object[]{
                                        tableName,
                                        rowNum + 1,
                                        insertStatement,
                                        ArrayUtils.toString(updateParams),
                                        bsge.getMessage() });
                            continue;
                        }
                    }
                }
                catch (BadSqlGrammarException bsge)
                {
                    log
                        .error(
                            "Error executing query to load row values, current possible update of row will be skipped. {} Query in error: {}",
                            bsge.getMessage(),
                            checkStatement);
                    return;
                }
                // 1 check if it is the same
                // 2 update only if they differ
            }

        }
    }

    /**
     * @author fgiust
     * @version $Id$
     */
    public static class QueryConfig
    {

        private String checkQuery;

        private String insertQuery;

        private String selectQuery;

        private String updateQuery;

        /**
         * Returns the selectQuery.
         * @return the selectQuery
         */
        public String getSelectQuery()
        {
            return selectQuery;
        }

        /**
         * Sets the selectQuery.
         * @param selectQuery the selectQuery to set
         */
        public void setSelectQuery(String selectQuery)
        {
            this.selectQuery = selectQuery;
        }

        /**
         * Returns the checkQuery.
         * @return the checkQuery
         */
        public String getCheckQuery()
        {
            return checkQuery;
        }

        /**
         * Sets the checkQuery.
         * @param checkQuery the checkQuery to set
         */
        public void setCheckQuery(String checkQuery)
        {
            this.checkQuery = checkQuery;
        }

        /**
         * Returns the insertQuery.
         * @return the insertQuery
         */
        public String getInsertQuery()
        {
            return insertQuery;
        }

        /**
         * Sets the insertQuery.
         * @param insertQuery the insertQuery to set
         */
        public void setInsertQuery(String insertQuery)
        {
            this.insertQuery = insertQuery;
        }

        /**
         * Returns the updateQuery.
         * @return the updateQuery
         */
        public String getUpdateQuery()
        {
            return updateQuery;
        }

        /**
         * Sets the updateQuery.
         * @param updateQuery the updateQuery to set
         */
        public void setUpdateQuery(String updateQuery)
        {
            this.updateQuery = updateQuery;
        }
    }
}
