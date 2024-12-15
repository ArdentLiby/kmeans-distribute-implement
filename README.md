# 基于Hadoop集群的MapReduce、Spark与Flink在K-means聚类中的性能对比

## 任务说明：

### 任务背景：

K-means聚类是一种广泛应用的无监督学习算法，用于将数据集划分为不同的簇。在大规模数据处理环境中，K-means聚类的效率和性能对数据分析任务至关重要。为了评估MapReduce、Spark和Flink在Hadoop集群下的执行效果，本任务将利用这些框架对K-means聚类算法进行性能比较和优化。

### 数据集：

本次实验使用的数据集是大小为1k，小数点位数大于>10的二维数据。

![image-20241215153947534](/img/数据集.png)

### 任务目标：

在Hadoop集群环境下，使用MapReduce、Spark和Flink分别实现K-means聚类算法，迭代次数为30。

## 环境配置：

### 安装windows版本Docker

![image-20241213143319088](/img/windows版本Docker.png)

### 拉取docker镜像，初始化hadoop集群

![image-20241213143416486](/img/初始化hadoop集群.png)

### 建立使用桥接模式的docker子网,使用桥接模式创建了一个172.19.0.0/16的子网出来

![image-20241213143529676](/img/桥接.png)

### 使用拉取的镜像，启动三个容器，分别是Master,Slave1,Slave2作为集群的三个节点

![image-20241213144914248](/img/启动容器1.png)

![image-20241213144936298](/img/启动容器2.png)

![image-20241213144951207](/img/启动容器3.png)

### 添加国内镜像源

![image-20241213145243861](/img/添加国内镜像源.png)

### 对三台容器的/etc/host进行修改,对主机名进行映射

### 实现容器之间还有容器与宿主机的通信

![image-20241213151402647](/img/实现容器之间还有容器与宿主机的通信1.png)

![image-20241213151415546](/img/实现容器之间还有容器与宿主机的通信2.png)

### 拉取的镜像的环境变量默认配置在/etc/profile中

![image-20241213151822519](/img/配置etcprofile.png)

### 启用容器的时候默认不会source /etc/profile,因此需要在bash的配置文件中添加语句 source /etc/profile,ssh 服务默认也是不会启用的，还需要在 bash的配置文件中添加service ssh start

![image-20241213152127095](/img/添加service%20ssh%20start.png)

### Spark集群搭建

### 解压SPARK

![image-20241213160710671](/img/解压SPARK.png)

### 配置worker工作节点

![image-20241213161427537](/img/配置worker工作节点.png)

### 将spark文件文件传输到其他节点上

![image-20241213163522783](/img/将spark文件文件传输到其他节点上1.png)

![image-20241213163613794](/img/将spark文件文件传输到其他节点上2.png)

## 实验具体流程：

### 打开DockerDesktop：

![image-20241215131454296](/img/打开DockerDesktop.png)

### 启动容器：

![image-20241215131534760](/img/启动容器.png)

### 分别进入容器的bash：

![image-20241215131645146](/img/分别进入容器的bash1.png)

![image-20241215131657432](/img/分别进入容器的bash2.png)

![image-20241215131711265](/img/分别进入容器的bash3.png)

### 启动Hadoop集群：

![image-20241215131851222](/img/启动Hadoop集群1.png)

![image-20241215131945146](/img/启动Hadoop集群2.png)

![image-20241215132033551](/img/启动Hadoop集群3.png)

## MapReduce

### 启动MapReduce任务脚本：
### 启动MapReduce任务脚本：

```
(base) root@Master:/# cd /usr/local/hadoop

(base) root@Master:/usr/local/hadoop# ./bin/hadoop jar ./hadoop_kmeans-1.0-SNAPSHOT\(1\).jar /input /output 4
```

![image-20241215133554654](/img/启动MapReduce任务脚本.png)

### MapReduce总用时：

![image-20241215133632742](/img/MapReduce总用时.png)

### MapReduce任务执行过程中Nodes的状态：

![image-20241215133809401](/img/MapReduce任务执行过程中Nodes的状态.png)

### MapReduce任务输出结果：

![image-20241215134302246](/img/MapReduce任务输出结果.png)

### 初始质心：

![image-20241215134637521](/img/初始质心.png)

### Kmeans_cluster_result结果:

![image-20241215134033758](/img/Kmeans_cluster_result结果.png)

### 聚类结果可视化：

![image-20241214173349514](/img/聚类结果可视化.png)

## Spark

### 启动spark：

![image-20241215135323198](/img/启动spark1.png)

![image-20241215135401550](/img/启动spark2.png)

![image-20241215135425170](/img/启动spark3.png)

### 激活已创建的pytho3.8虚拟环境：

![image-20241215135529631](/img/激活已创建的pytho3.8虚拟环境1.png)

![image-20241215135602369](/img/激活已创建的pytho3.8虚拟环境1.png)

![image-20241215135613768](/img/激活已创建的pytho3.8虚拟环境1.png)

### 启动Spark任务脚本：

先进入Spark环境，再运行

```
/usr/local/spark/bin/spark-submit --master spark://localhost:7077 /usr/local/spark/kmeans.py 4 0.01 /usr/local/spark/data.txt
```


![image-20241215141154247](/img/启动Spark任务脚本.png)

### Spark运行总时间：

![image-20241215141238954](/img/Spark运行总时间.png)

### Spark任务执行过程中Nodes的状态：

![image-20241215142054142](/img/Spark任务执行过程中Nodes的状态.png)

## Flink

### Master启动 Flink 集群

![image-20241215142659438](/img/Master启动%20Flink%20集群.png)

### Slave1和Slave2启动 Flink 集群中的 **TaskManager** 进程

具体来说，**TaskManager** 是 Flink 集群的计算节点，负责执行实际的任务和处理数据流。Flink 采用 master-slave 架构，通常有一个 **JobManager** 和多个 **TaskManager**。JobManager 负责管理作业的调度和资源分配，而 TaskManager 则执行实际的数据处理任务。

![image-20241215142924988](/img/Slave1启动%20Flink%20集群中的%20TaskManager%20进程.png)

![image-20241215143026811](/img/Slave2启动%20Flink%20集群中的%20TaskManager%20进程.png)

### 启动Flink任务脚本：

```
(base) root@Master:/usr/local/flink# flink run ./flink-kmeans-example-1.0-SNAPSHOT1.jar --points /usr/local/flink/flink_data.txt --centroids /usr/local/flink/centralid.txt --output /output --iterations 30
```

### Flink运行总时间：

![image-20241215153105173](/img/Flink运行总时间.png)

## 实验总结：

在本次实验中，我们对K-means聚类算法在Hadoop集群环境下进行了基于MapReduce、Spark和Flink三种框架的实现，并分别测量了它们的执行时间。通过实验结果发现，MapReduce的总执行时间为141662 ms，Spark为14519 ms，而Flink则为3576 ms。这个实验结果揭示了不同分布式框架在处理相同任务时的显著性能差异。

### 为什么Spark比MapReduce快？

**数据处理模型**：

**MapReduce**采用的是基于磁盘的存储方式，所有的中间数据都需要在磁盘上进行读写。这种方式使得MapReduce的计算非常依赖磁盘I/O，导致性能瓶颈。

**Spark**则采用内存计算模型。Spark在执行计算时尽可能将数据存储在内存中，减少了磁盘I/O的开销，极大提高了计算速度。此外，Spark支持数据缓存和RDD（弹性分布式数据集）优化，使得重复计算时可以从内存中获取数据，进一步提高了效率。

### 为什么Flink最快？

### **流处理和批处理结合**：

**Flink**的最大优势在于其流处理和批处理的统一模型。Flink本身设计为一个流处理引擎，即使是在处理批数据时，它也采用流式计算的思想。这使得Flink能够更高效地进行数据传输和计算，避免了传统批处理框架中大量的中间数据存储和磁盘I/O开销。
