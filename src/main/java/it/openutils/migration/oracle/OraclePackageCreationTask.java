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

package it.openutils.migration.oracle;

import it.openutils.migration.task.setup.DbTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Db tasks that handles the initial setup of packages.
 * @author quario
 * @version $Id$
 */
public class OraclePackageCreationTask implements DbTask
{

    private Logger log = LoggerFactory.getLogger(OraclePackageCreationTask.class);

    private List<Resource> scripts;

    private String selectUserPackages = "select COUNT(*) FROM USER_OBJECTS WHERE OBJECT_NAME = ? and OBJECT_TYPE='PACKAGE'";

    private String selectAllPackages = "SELECT COUNT(*) from ALL_OBJECTS where OBJECT_NAME = ? AND OBJECT_TYPE='PACKAGE' AND OWNER = ?";

    protected Map<String, String> variables;

    /**
     * Sets the packages.
     * @param packages the packages to set
     */
    public void setScripts(List<Resource> packages)
    {
        this.scripts = packages;
    }

    /**
     * Sets the selectAllPackages.
     * @param selectAllPackages the selectAllPackages to set
     */
    public void setSelectAllPackages(String selectAllPackages)
    {
        this.selectAllPackages = selectAllPackages;
    }

    /**
     * Sets the selectUserPackages.
     * @param selectUserPackages the selectUserPackages to set
     */
    public void setSelectUserPackages(String selectUserPackages)
    {
        this.selectUserPackages = selectUserPackages;
    }

    /**
     * Map of key-value that will be replaced in ddl.
     */
    public void setVariables(Map<String, String> variables)
    {
        this.variables = variables;
    }

    /**
     * {@inheritDoc}
     */
    public String getDescription()
    {
        return "Checking Packages";
    }

    /**
     * @param script The script resource
     * @return The script name
     */
    protected String objectNameFromFileName(Resource script)
    {
        return StringUtils.substringBeforeLast(script.getFilename(), ".");
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DataSource dataSource)
    {
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(dataSource);

        for (Resource script : scripts)
        {

            if (script == null || !script.exists())
            {
                log.error("Unable to execute db task \"{}\", script \"{}\" not found.", getDescription(), script);
                return;
            }

            String fqPackageName = this.objectNameFromFileName(script);
            String tmpPackageName = null;
            String tmpowner = null;

            if (StringUtils.contains(fqPackageName, "."))
            {
                String[] tokens = StringUtils.split(fqPackageName, ".");
                tmpPackageName = tokens[1];
                tmpowner = tokens[0];
            }
            else
            {
                tmpPackageName = fqPackageName;
            }

            final String packageName = tmpPackageName;
            final String owner = tmpowner;

            int result = 0;

            if (StringUtils.isNotBlank(owner))
            {
                result = jdbcTemplate.queryForInt(selectAllPackages, new Object[]{packageName, owner });
            }
            else
            {
                result = jdbcTemplate.queryForInt(selectUserPackages, packageName);
            }

            if (result <= 0)
            {
                String scriptContent;
                InputStream is = null;

                try
                {
                    is = script.getInputStream();
                    scriptContent = IOUtils.toString(is, "UTF8");
                    scriptContent = scriptContent.replaceAll("\\s*\n\\s*", "\n");
                }
                catch (IOException e)
                {
                    log.error(
                        "Unable to execute db task \"{}\", script \"{}\" can't be read.",
                        getDescription(),
                        script);
                    return;
                }
                finally
                {
                    IOUtils.closeQuietly(is);
                }

                String[] scriptSections = scriptContent.split("/");
                String packageHeader = performSubstitution(scriptSections[0].trim());
                String packageBody = performSubstitution(scriptSections[1].trim());

                if (StringUtils.isNotBlank(packageHeader) && StringUtils.isNotBlank(packageBody))
                {
                    log.info("Creating new package header for {}", packageName);
                    jdbcTemplate.update(packageHeader);
                    log.info("Creating new package body for {}", packageName);
                    jdbcTemplate.update(packageBody);
                }
            }
            else
            {
                log.debug("Package {} already existing", packageName);
            }
        }
    }

    /**
     * Perform sobstitution in the given string.
     * @param string Original String
     * @return processed string
     */
    protected String performSubstitution(String string)
    {
        if (variables == null || variables.isEmpty())
        {
            return string;
        }

        String stringReplaced = string;
        for (String key : variables.keySet())
        {
            stringReplaced = StringUtils.replace(stringReplaced, "${" + key + "}", variables.get(key));
        }

        return stringReplaced;
    }
}
