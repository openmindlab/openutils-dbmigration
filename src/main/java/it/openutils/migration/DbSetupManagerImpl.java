/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration;

import it.openutils.migration.task.setup.DbTask;
import it.openutils.migration.task.update.DbUpdate;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;


/**
 * DB setup manager: handles db setup and upgrade.
 * @author fgiust
 * @version $Id$
 */
public class DbSetupManagerImpl implements DbSetupManager
{

    /**
     * Logger.
     */
    private static Logger log = LoggerFactory.getLogger(DbSetupManagerImpl.class);

    private List<DbTask> setupTasks;

    private List<DbUpdate> updateTasks;

    private DataSource dataSource;

    private DbVersionManager versionManager;

    private TransactionTemplate transactionTemplate;

    private boolean enabled = true;

    /**
     * Sets the enabled.
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    /**
     * Sets the versionManager.
     * @param versionManager the versionManager to set
     */
    public void setVersionManager(DbVersionManager versionManager)
    {
        this.versionManager = versionManager;
    }

    /**
     * Sets the transactionTemplate.
     * @param transactionTemplate the transactionTemplate to set
     */
    public void setTransactionTemplate(TransactionTemplate transactionTemplate)
    {
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Setter for <code>dataSource</code>.
     * @param dataSource The dataSource to set.
     */
    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    /**
     * Sets the tasks.
     * @param setupTasks the tasks to set
     */
    public void setSetupTasks(List<DbTask> setupTasks)
    {
        this.setupTasks = setupTasks;
    }

    /**
     * Sets the updateTasks.
     * @param updateTasks the updateTasks to set
     */
    public void setUpdateTasks(List<DbUpdate> updateTasks)
    {
        this.updateTasks = updateTasks;
    }

    /**
     * @see it.openutils.dbupdate.DbSetupManager#updateDDL()
     */
    public void updateDDL()
    {

        if (!enabled)
        {
            log.info("DB migration is disabled, not running tasks.");
            return;
        }

        transactionTemplate.execute(new TransactionCallbackWithoutResult()
        {

            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status)
            {
                if (setupTasks != null)
                {
                    executeSetupTasks();
                }
                if (updateTasks != null)
                {
                    executeUpdateTasks();
                }
            }
        });

    }

    /**
     *
     */
    private void executeSetupTasks()
    {
        log.info("Preparing db, checking {} setup tasks.", setupTasks.size());
        for (DbTask task : setupTasks)
        {
            log.info(task.getDescription());
            task.execute(dataSource);
        }

    }

    /**
     *
     */
    private void executeUpdateTasks()
    {

        int initialVersion = versionManager.getCurrentVersion();
        int currentVersion = initialVersion;

        Set<DbUpdate> sortedMigrations = new TreeSet<DbUpdate>(new Comparator<DbUpdate>()
        {

            public int compare(DbUpdate o1, DbUpdate o2)
            {
                return o1.getVersion() - o2.getVersion();
            }

        });
        sortedMigrations.addAll(updateTasks);

        log.info("Found {} migrations, looking for updates to run...", updateTasks.size());
        for (DbUpdate update : sortedMigrations)
        {
            if (update.getVersion() > currentVersion)
            {
                currentVersion = update.getVersion();

                log.info("Preparing migration to version {}. {}", update.getVersion(), update.getDescription());
                try
                {
                    update.execute(dataSource);
                }
                catch (DataAccessException e)
                {
                    log.error("\n***********\n\n\nDatabase upgrade from version "
                        + initialVersion
                        + " to version "
                        + currentVersion
                        + " FAILED!\n\n\n***********\n", e);
                }
                versionManager.setNewVersion(currentVersion);
            }
        }
        if (currentVersion != initialVersion)
        {
            log.info("Database upgraded from version {} to version {}", initialVersion, currentVersion);
        }
        else
        {
            log.info("No Database upgrade is needed. Current version is {} ", initialVersion);
        }
        // org.springframework.jdbc.BadSqlGrammarException:
        // java.sql.SQLException: ORA-00959: tablespace 'XDM_DATA' inesistente

    }
}
