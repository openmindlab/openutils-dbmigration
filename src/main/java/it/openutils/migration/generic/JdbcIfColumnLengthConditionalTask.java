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

import java.util.Map;


/**
 * Task that executes if a given column has a specific length.
 * @author fgiust
 * @version $Id$
 */
public class JdbcIfColumnLengthConditionalTask extends JdbcColumnBasedConditionalTask
{

    private int length;

    /**
     * Sets the length.
     * @param length the length to set
     */
    public void setLength(int length)
    {
        this.length = length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean checkColumnMetadata(Map<String, Object> metadata)
    {

        Number columnLength = (Number) metadata.get("COLUMN_SIZE");
        return length == (columnLength != null ? columnLength.intValue() : 0);
    }
}
