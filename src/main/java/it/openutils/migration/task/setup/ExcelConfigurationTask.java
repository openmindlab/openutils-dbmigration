/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.task.setup;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


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
    private void processSheet(HSSFSheet sheet, String tableName, QueryConfig con, DataSource dataSource)
    {
        List<String> columns = new ArrayList<String>();

        HSSFRow row = sheet.getRow(0);
        for (short k = 0; k < row.getLastCellNum(); k++)
        {
            String columnName = row.getCell(k).getStringCellValue();
            if (StringUtils.isNotBlank(columnName))
            {
                columns.add(columnName);
            }
            else
            {
                break;
            }
        }

        log.debug("Table: {}, Columns: {}", tableName, columns);

        String checkStatement = con.getCheckQuery();
        String insertStatement = con.getInsertQuery();

        processRecords(sheet, columns, checkStatement, insertStatement, dataSource);
    }

    /**
     * @param sheet
     * @param columns
     * @param checkStatement
     * @param insertStatement
     */
    private void processRecords(HSSFSheet sheet, List<String> columns, String checkStatement, String insertStatement,
        DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        int checkNum = StringUtils.countMatches(checkStatement, "?");
        int insertNum = StringUtils.countMatches(insertStatement, "?");

        HSSFRow row;
        for (short u = 1; u <= sheet.getLastRowNum(); u++)
        {
            row = sheet.getRow(u);
            if (row == null)
            {
                return;
            }

            List<String> values = new ArrayList<String>();

            for (short k = 0; k < columns.size() && k <= row.getLastCellNum(); k++)
            {
                HSSFCell cell = row.getCell(k);
                if (cell == null)
                {
                    return;
                }

                String value = null;
                if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING)
                {
                    value = cell.getStringCellValue();
                }
                else if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC)
                {
                    double valueDouble = cell.getNumericCellValue();
                    value = Double.toString(valueDouble);
                }

                if (StringUtils.isEmpty(value))
                {
                    return;
                }
                values.add(value);
            }

            Object[] checkParams = ArrayUtils.subarray(values.toArray(), 0, checkNum);

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
                log.debug("Missing record with key {}; inserting {}", ArrayUtils.toString(checkParams), ArrayUtils
                    .toString(insertParams));
                jdbcTemplate.update(insertStatement, insertParams);
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
    }
}
