# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1
 
# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = ${sourceDir}
a1.sources.r1.fileHeader = true
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = timestamp

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = ${sinkDir}
a1.sinks.k1.hdfs.writeFormat = Text
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 0
a1.sinks.k1.hdfs.rollCount = 0
a1.sinks.k1.hdfs.filePrefix = %Y-%m-%d-%H-%M-%S
a1.sinks.k1.hdfs.useLocalTimeStamp = true
 
# Use a channel which buffers events in file
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = ${channelCheckDir}
a1.channels.c1.dataDirs = ${channelDataDir}
 
# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1