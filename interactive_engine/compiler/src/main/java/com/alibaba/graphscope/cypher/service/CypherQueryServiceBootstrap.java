/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.service;

import com.alibaba.graphscope.common.antlr4.Antlr4Parser;
import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.cypher.executor.GraphQueryExecutor;
import com.alibaba.graphscope.gremlin.Utils;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricStatementLifecycles;
import org.neo4j.fabric.pipeline.SignatureResolver;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.FabricTransactionMonitor;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.neo4j.scheduler.Group.CYPHER_CACHE;
import static org.neo4j.scheduler.Group.FABRIC_WORKER;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

public class CypherQueryServiceBootstrap extends FabricServicesBootstrap.Community {
    private final Config config;
    private final FabricConfig fabricConfig;
    private final LogService logService;
    private final AvailabilityGuard availabilityGuard;
    private final AbstractSecurityLog securityLog;

    public CypherQueryServiceBootstrap(
            LifeSupport lifeSupport,
            Dependencies dependencies,
            LogService logService) {
        super(lifeSupport, dependencies, logService);
        this.logService = logService;
        this.config = Utils.getFieldValue(FabricServicesBootstrap.class, this, "config");
        this.fabricConfig =
                Utils.getFieldValue(FabricServicesBootstrap.class, this, "fabricConfig");
        this.availabilityGuard =
                Utils.getFieldValue(FabricServicesBootstrap.class, this, "availabilityGuard");
        this.securityLog =
                Utils.getFieldValue(FabricServicesBootstrap.class, this, "securityLog");
    }

    @Override
    public void bootstrapServices() {
        LogProvider internalLogProvider = logService.getInternalLogProvider();

        @SuppressWarnings( "unchecked" )
        var databaseManager = (DatabaseManager<DatabaseContext>) resolve( DatabaseManager.class );
        var fabricDatabaseManager = register( createFabricDatabaseManager( fabricConfig ), FabricDatabaseManager.class );

        var jobScheduler = resolve( JobScheduler.class );
        var monitors = resolve( Monitors.class );

        var databaseAccess = createFabricDatabaseAccess();
        var remoteExecutor = bootstrapRemoteStack();
        var localExecutor = register( new FabricLocalExecutor( fabricConfig, fabricDatabaseManager, databaseAccess ), FabricLocalExecutor.class );

        var systemNanoClock = resolve( SystemNanoClock.class );
        var transactionMonitor = new FabricTransactionMonitor( systemNanoClock, logService, fabricConfig );

        var transactionCheckInterval = config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis();
        register( new TransactionMonitorScheduler( transactionMonitor, jobScheduler, transactionCheckInterval, null ), TransactionMonitorScheduler.class );

        var errorReporter = new ErrorReporter( logService );
        register( new TransactionManager( remoteExecutor, localExecutor, errorReporter, fabricConfig, transactionMonitor, securityLog, systemNanoClock, config,
                availabilityGuard ), TransactionManager.class );

        var cypherConfig = CypherConfiguration.fromConfig( config );

        Supplier<GlobalProcedures> proceduresSupplier = () -> resolve( GlobalProcedures.class );
        var catalogManager = register( createCatalogManger( databaseManager.databaseIdRepository() ), CatalogManager.class );
        var signatureResolver = new SignatureResolver( proceduresSupplier );
        var statementLifecycles = new FabricStatementLifecycles( databaseManager, monitors, config, systemNanoClock );
        var monitoredExecutor = jobScheduler.monitoredJobExecutor( CYPHER_CACHE );
        var cacheFactory = new ExecutorBasedCaffeineCacheFactory( job -> monitoredExecutor.execute( systemJob( "Query plan cache maintenance" ), job ) );
        var planner = register( new FabricPlanner( fabricConfig, cypherConfig, monitors, cacheFactory, signatureResolver ), FabricPlanner.class );
        var useEvaluation = register( new UseEvaluation( catalogManager, proceduresSupplier, signatureResolver ), UseEvaluation.class );

        Executor fabricWorkerExecutor = jobScheduler.executor( FABRIC_WORKER );
        // gie config
        var graphConfig = (Configs) resolve(Configs.class);
        var antlr4Parser = (Antlr4Parser) resolve(Antlr4Parser.class);
        var graphPlanner = (GraphPlanner) resolve(GraphPlanner.class);
        var metaQueryCallback = (IrMetaQueryCallback) resolve(IrMetaQueryCallback.class);
        var executionClient = (ExecutionClient) resolve(ExecutionClient.class);
        var fabricExecutor =
                new GraphQueryExecutor(
                        fabricConfig,
                        planner, useEvaluation, catalogManager, internalLogProvider, statementLifecycles, fabricWorkerExecutor,
                        graphConfig,
                        antlr4Parser,
                        graphPlanner,
                        metaQueryCallback,
                        executionClient);
        register( fabricExecutor, FabricExecutor.class );

        register( new TransactionBookmarkManagerFactory( fabricDatabaseManager ), TransactionBookmarkManagerFactory.class );
    }
}
