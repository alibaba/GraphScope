LOG_NAME=store0 MAXGRAPH_CONF_FILE=conf/store0.config ./bin/store.sh
LOG_NAME=store1 MAXGRAPH_CONF_FILE=conf/store1.config ./bin/store.sh
LOG_NAME=coordinator MAXGRAPH_CONF_FILE=conf/coordinator.config ./bin/coordinator.sh
LOG_NAME=ingestor0 MAXGRAPH_CONF_FILE=conf/ingestor0.config ./bin/ingestor.sh
LOG_NAME=ingestor1 MAXGRAPH_CONF_FILE=conf/ingestor1.config ./bin/ingestor.sh
LOG_NAME=frontend0 MAXGRAPH_CONF_FILE=conf/frontend0.config ./bin/frontend.sh
