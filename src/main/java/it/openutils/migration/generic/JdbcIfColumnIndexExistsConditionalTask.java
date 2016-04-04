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

package it.openutils.migration.generic;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import it.openutils.migration.task.setup.BaseConditionalTask;


/**
 * Task that executes if a given column have at least one index.
 * @author Antonio Gagliardi
 * @version $Id$
 */
public class JdbcIfColumnIndexExistsConditionalTask extends BaseConditionalTask
{

    protected String catalog;

    protected String schema;

    protected String table;

    protected String[] columns;

    /**
     * Logger.
     */
    private Logger log = LoggerFactory.getLogger(JdbcIfColumnIndexExistsConditionalTask.class);

    class IndexItem implements Comparable<IndexItem>
    {

        private String columnName;

        private int position;

        private IndexItem(String columnName, int position)
        {
            super();
            this.columnName = columnName;
            this.position = position;
        }

        public int compareTo(IndexItem o)
        {
            return position - o.position;
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(1066590407, 744017859)
                .append(this.position)
                .append(this.columnName)
                .toHashCode();
        }

        @Override
        public boolean equals(Object object)
        {
            if (!(object instanceof IndexItem))
            {
                return false;
            }
            IndexItem rhs = (IndexItem) object;
            return new EqualsBuilder()
                .append(this.position, rhs.position)
                .append(this.columnName, rhs.columnName)
                .isEquals();
        }

    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean check(JdbcTemplate jdbcTemplate)
    {
        Set<IndexItem> indexExpected = new TreeSet<IndexItem>();

        checkInputs(indexExpected);

        ConnectionCallback action = new ConnectionCallback()
        {

            public Object doInConnection(Connection con) throws SQLException, DataAccessException
            {
                DatabaseMetaData dbMetadata = con.getMetaData();
                ResultSet rs = dbMetadata.getIndexInfo(catalog, schema, table, false, false);

                Map<String, Set<IndexItem>> indexs = new HashMap<String, Set<IndexItem>>();
                while (rs.next())
                {
                    String indexName = rs.getString("INDEX_NAME");
                    String actualColumnName = rs.getString("COLUMN_NAME");
                    int position = rs.getInt("ORDINAL_POSITION");
                    Set<IndexItem> indexItems = indexs.get(indexName);
                    if (indexItems == null)
                    {
                        indexItems = new TreeSet<IndexItem>();
                        indexs.put(indexName, indexItems);
                    }
                    indexItems.add(new IndexItem(actualColumnName, position));
                }
                rs.close();
                return indexs.values();
            }
        };
        Collection<Set<IndexItem>> indexs = (Collection<Set<IndexItem>>) jdbcTemplate.execute(action);

        log.debug("Expected:{}", asString(indexExpected));
        for (Set<IndexItem> index : indexs)
        {
            log.debug("Actual:{}", asString(index));
            if (index.size() == indexExpected.size())
            {
                if (asString(index).equalsIgnoreCase(asString(indexExpected)))
                {
                    return true;
                }
            }
        }
        return false;

    }

    private void checkInputs(Set<IndexItem> indexExpected)
    {
        if (StringUtils.isBlank(table))
        {
            throw new IllegalArgumentException("table is mandatory");
        }
        if (ArrayUtils.isEmpty(columns))
        {
            throw new IllegalArgumentException("columns are mandatory");
        }

        for (int i = 0; i < columns.length; i++)
        {
            String columnName = columns[i];
            if (StringUtils.isBlank(columnName))
            {
                throw new IllegalArgumentException("columnName can not be blank");
            }
            indexExpected.add(new IndexItem(columnName, i + 1));
        }
    }

    private final static String asString(Set<IndexItem> index)
    {
        StringBuilder sb = new StringBuilder();
        for (IndexItem indexItem : index)
        {
            sb.append("" + indexItem.position + "|" + indexItem.columnName);
        }
        return sb.toString();
    }

    /**
     * Sets the catalog.
     * @param catalog the catalog to set
     */
    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    /**
     * Sets the schema.
     * @param schema the schema to set
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Sets the table.
     * @param table the table to set
     */
    public void setTable(String table)
    {
        this.table = table;
    }

    /**
     * Sets the columns.
     * @param columns the columns to set
     */
    public void setColumns(String[] columns)
    {
        this.columns = columns;
    }

}
