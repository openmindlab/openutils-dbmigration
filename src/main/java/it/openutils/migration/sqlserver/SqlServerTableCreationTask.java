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
package it.openutils.migration.sqlserver;

import javax.sql.DataSource;


/**
 * @author fgiust
 * @version $Id:SqlServerTableCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerTableCreationTask extends SqlServerObjCreationTask
{

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        setQualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and OBJECTPROPERTY(id, N'IsUserTable') = 1");
        setUnqualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and OBJECTPROPERTY(id, N'IsUserTable') = 1");
        super.execute(dataSource);
    }
}