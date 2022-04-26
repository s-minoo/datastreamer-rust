# Data-streamer 

In order to support the evaluation of stream processing engine a data
source that is able to mimic different streaming data characteristics is 
desirable. Evaluating the engines with different data stream characteristics
would provide more insights into the performance of the engine. 

This data streaming component is inspired by the works of Van Dongen et al
on [streaming engines benchmark](https://ieeexplore.ieee.org/document/9025240).
In the reference paper, Apache Kafa was used as a data source for a 
more reliable measurement of latency. However, distributed messaging 
brokers like Kafka have an overhead of I/O operations since they write 
the data first to disk before reading and streaming it. 

In order to reduce the overhead of the I/O operations, we implemented 
an in-memory data streaming component which will store the necessary data 
in memory first before streaming. The data streamer could also be  
generalized to stream any kinds of data for evaluation purposes without
being bound to a specific dataset.  


# Configuration of data streamer

The data streamer can be configured with the
`config.toml` file. Currently, only two mode 
of data streaming characteristics are supported;
* Constant: streams the data at a constant stream data rate determined by the `volume` parameter 
* Periodic: streams the data at a periodic
rate, a burst of data followed by low data 
stream rate. 
The amount of data streamed during the burst 
is determined by the `volume` parameter. 


Multiple data sources can be streamed by 
by defining more array of tables with the key `[[streamconfigs]]` 

The config file looks as follows: 
```toml
log_level="info" 

[[streamconfigs]]
ip = "0.0.0.0"
port = 9000
mode = "Constant"
volume = 1
input_format = "JSON"
output_format = "JSON"
data_folder = "./data/flow"

[[streamconfigs]]
ip = "0.0.0.0"
port = 9001
mode = "Periodic"
volume = 20
input_format = "JSON"
output_format = "JSON"
data_folder = "./data/speed"
```


# Running 

To start the data streamer, you could run the following command: 

`./start.sh`

This will start the docker for the data streamer component and listen for
client connections at the specified ports. 


# Stopping

Stopping the data streamer is as simple as running: 

`./stop.sh` 