# GraphScope Monitor
GraphScope Monitor is a monitoring system for GraphScope. It is based on [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/) Stack.

To our work, it consists of two parts:   
- Gie Exporter: a web exporter for exporting gie metrics to Prometheus.
- Coordinator Exporter: a web exporter for exporting coordinator metrics to Prometheus.


## Gie Exporter
Gie Exporter is python script `GraphScope/k8s/prometheus/monitor.py` that should runs in each gie pod.  
It collects gie metrics from a metric log `/var/log/graphscope/<?>/frontend/metric.log` and exports them via HTTP.  

### Usage
1. install dependencies  
   ```bash
   # install prometheus_client
   $ cd GraphScope/k8s/prometheus && pip install -r requirements.txt
   ```
2. start exporter  
   ```bash
   $ cd GraphScope/k8s/prometheus

   # default linstening on 0.0.0.0:9969
   $ python3 monitor.py

   # or specify the host and port
   $ python3 monitor.py 127.0.0.1:12345
   ``` 
3. start a prometheus server and scrape metrics from gie exporter.  
   <br/>
   You just need to add a scrape config for gie exporter in prometheus.yml and then start a prometheus server.  
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

4. start a grafana server and create a monitor dashboard.  
   <br/>
   Here [gie-grafana.json](https://gist.githubusercontent.com/VincentFF/23613b8f833a7751ce39483ce6643b6a/raw/d40f4e22bb460b4ba04331da15d8129170935340/gie-grafana.json) is a dashboard example for gie monitor. You can import it on grafana dashboard to easily create a gie monitor dashboard.  
	<br/>
   See [grafana documentation](https://grafana.com/docs/grafana/v9.0/getting-started/get-started-grafana-prometheus/) for more details.  
   

## Coordinator Exporter
Coordinator Exporter is a web exporter for exporting coordinator metrics to Prometheus, which is embedded in coordinator package.  

### Usage
1. start a coordinator will start exporter automatically.  
   <br>
   To specify the host and port of exporter, use `--monitor_port 12345` and `--monitor_host 0.0.0.0`. To disable exporter, use `--monitor False`.  
   ```bash 
   $ cd GraphScope/coordinator

   # default linstening on 127.0.0.1:9968
   $ python3 -m gscoordinator --num_workers 1 --hosts localhost --log_level INFO --timeout_seconds 600 --port 50254 --cluster_type hosts --instance_id svuifn --vineyard_shared_mem 4G

   # specify the host to 0.0.0.0 and port to 12345
   $ python3 -m gscoordinator --num_workers 1 --hosts localst --log_level INFO --timeout_seconds 600 --port 50254 --cluster_type hosts --instance_id svuifn --vineyard_shared_mem 4G --monitor_port 12345 --monitor_host 0.0.0.0
   ```
2. start a prometheus server and scrape metrics from coordinator exporter.  
   <br> 
   Here [coordinator-grafana.json](https://gist.githubusercontent.com/VincentFF/23613b8f833a7751ce39483ce6643b6a/raw/d40f4e22bb460b4ba04331da15d8129170935340/coordinator-grafana.json) is a dashboard example for gie monitor. You can import it on grafana dashboard to easily create a gie monitor dashboard.  
   This dashboard integrates node metrics. To collect node metrics, see [node-exporter](https://prometheus.io/docs/guides/node-exporter/).  
   <br/>
   See [prometheus documentation](https://prometheus.io/docs/introduction/first_steps/) for more details.  

3. start a grafana server and make a monitor dashboard.  
   <br/>
   See [grafana documentation](https://grafana.com/docs/grafana/v9.0/getting-started/get-started-grafana-prometheus/) for more details.  
