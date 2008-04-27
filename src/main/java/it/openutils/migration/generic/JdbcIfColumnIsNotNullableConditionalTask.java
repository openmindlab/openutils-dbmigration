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
package it.openutils.migration.generic;

import java.util.Map;


/**
 * Task that executes if a given column is not nullable (IS_NULLABLE = NO).
 * @author fgiust
 * @version $Id$
 */
public class JdbcIfColumnIsNotNullableConditionalTask extends JdbcColumnBasedConditionalTask
{

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean checkColumnMetadata(Map<String, Object> metadata)
    {

        String isNullable = (String) metadata.get("IS_NULLABLE");
        return "NO".equals(isNullable);
    }
}
