# GraphScope Monitor
GraphScope Monitor is a monitoring system for GraphScope. It is based on [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/) Stack.

To our work, it consists of two parts:   
- GIE Exporter : a web exporter for exporting GIE metrics to Prometheus.
- Coordinator Exporter : a web exporter for exporting coordinator metrics to Prometheus.


## GIE Exporter
GIE Exporter is a python script `GraphScope/k8s/prometheus/monitor.py` which should runs in each GIE pod.  
It collects GIE metrics from a metric log `/var/log/graphscope/<?>/frontend/metric.log` and exports them via HTTP.  

### Usage
1. Install dependencies  
   ```bash
   # install prometheus_client
   $ cd GraphScope/k8s/prometheus && pip install prometheus-client==0.14.1
   ```
2. Start exporter  
   ```bash
   $ cd GraphScope/k8s/prometheus
   
   # default linstening on 0.0.0.0:9969
   $ python3 monitor.py
   
   # or specify the host and port
   $ python3 monitor.py 0.0.0.0:9969
   ```
3. Start a prometheus server and scrape metrics from GIE exporter.  
   You just need to add a scrape config for GIE exporter in prometheus.yml and then start a prometheus server.  
   Config example:  
   ```yaml
   scrape_configs:
     # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
     - job_name: "scope"
       scrape_interval: 5s
       # metrics_path defaults to '/metrics'
       # scheme defaults to 'http'.
   
       static_configs:
         - targets: ["127.0.0.1:9969"]
   ```
   
   See [prometheus documentation](https://prometheus.io/docs/introduction/first_steps/) for more details.  

4. Start a grafana server and create a monitor dashboard.  
   See [grafana documentation](https://grafana.com/docs/grafana/v9.0/getting-started/get-started-grafana-prometheus/) for more details.  

   There are some query tips for displaying GIE metrics on grafana dashboard:  
   - Session State  
   ```promql
   session_state
   ```
   - Total requests  
   ```promql
   interactive_request_total + analytical_request_total
   ```
   - Analytical requests  
   ```promql
   # total analytical requests
   analytical_request_total
   
   # last 5 minutes analytical requests
   increase(analytical_request_total[5m])
   ```
   - Interactive requests  
   ```promql
   # total interactive requests
   interactive_request_total
   
   # last 5 minutes interactive requests  
   increase(interactive_request_total[5m])
   ```
   - Analytical requests latency (Analytical OP time)  
   ```promql
   analytical_request_time{op_name=~".+"}
   ```
   - Interactive requests latency (Interactive OP time)  
   ```promql
   interactive_request_time{op_name=~".+"}
   ```
   - Analytical performance  
   ```promql
   # where "$app" and "$graph" is the variable you defined in grafana dashboard
   analytical_performance{app=~"$app",graph=~"$graph"}
   ```
   To define grafana variables, see [Templates and variables](https://grafana.com/docs/grafana/v9.0/variables/).  
   Here is the regex syntax to query values of `$app` and `$graph` when you define them in grafana dashboard:  
   
   ```
   /.*app="([^"]*).*/
   
   /.*graph="([^"]*).*/
   ```
   
## Coordinator Exporter
Coordinator Exporter is a web exporter for exporting coordinator metrics to prometheus, which is embedded in coordinator package.  

### Usage
1. Add `--monitor True`  option to enable the exporter while starting coordinator.  
   To specify a listening port, use `--monitor_port 9968`.   
   
   ```bash 
   $ cd GraphScope/coordinator
   
   # enable monitor. default listening on 0.0.0.0:9968.
   $ python3 -m gscoordinator --num_workers 1 --hosts localhost --log_level INFO --timeout_seconds 600 --port 50254 --cluster_type hosts --instance_id svuifn --vineyard_shared_mem 4G --monitor True --monitor_port 9968
   ```
2. Start a prometheus server and scrape metrics from coordinator exporter.  
   See [prometheus documentation](https://prometheus.io/docs/introduction/first_steps/) for more details.  

   To collect node metrics, see [node-exporter](https://prometheus.io/docs/guides/node-exporter/).  

3. Start a grafana server and create a monitor dashboard.  
   See [grafana documentation](https://grafana.com/docs/grafana/v9.0/getting-started/get-started-grafana-prometheus/) for more details.  

   There are some query tips for displaying coordinator metrics on grafana dashboard:   
   - Succeed queries in last 24 hours  
   ```promql
   sum(increase(gie_request_count{success="true"}[24h]))
   
   # to display all succeed queries in life time, using
   sum(gie_request_count{success="true"})
   ```
   - Failed queries in last 24 hours  
   ```promql
   sum(increase(gie_request_count{success="false"}[24h]))
   ```
   - Failed query percentage in last 24 hours  
   ```promql
   100* sum(increase(gie_request_count{success="false"}[24h])) / sum(increase(gie_request_count[24h]))
   ```
   - Succeed QPS
   ```promql
   # 5m means it's a statistic result in 5 miniutes.
   # you may change it to 10m, 30s, 10s or other time range to get a different statistic result.  
   sum(rate(gie_request_count{success="true"}[5m])) 
   ```
   - Failed QPS
   ```promql
   sum(rate(gie_request_count{success="false"}[5m]))
   ```
   - Average executing time of succeed queries in 5 minutes
   ```promql
   sum(rate(gie_request_sum{success="true"}[5m]))/sum(rate(gie_request_count{success="true"}[5m]))
   ```
   - Average executing time of failed queries in 5 minutes
   ```promql
   sum(rate(gie_request_sum{success="false"}[5m])) / sum(rate(gie_request_count{success="false"}[5m]))
   ```