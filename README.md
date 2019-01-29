# spark_example
在这里面包含Spark Core和SparkStreaming的例子。


# Spark Core Example

* [WordCountMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/wordcount/WordCountMain.java) : 对单词个数进行统计，然后按照出现次数由大到小输出。
* [RemoveMaxOccurrenceWordMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/wordcount/RemoveMaxOccurrenceWordMain.java) : 需求： 读取一个文件，统计单词出现的次数。并且把出现次数多的单词移除掉，然后将剩下的单词按照出现次数降序排序。
* [MonitorStatusMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/MonitorStatusMain.java) : 
需求： 统计分析监控点和摄像头的状态（正常工作/异常） 。 
目的：如果摄像头异常，那么，就需要对这些摄像头进行维修或者更换。
* [Random10VehicleTracingMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/Random10VehicleTracingMain.java) : 
需求： 在所有监控点里面，随机抽取10辆车，然后计算出这些车的车辆轨迹。 
目的： 对公交车轨迹分析，可以知道该公交车是否有改道行为。对可疑车辆进行轨迹追踪，协助有关部门破案。
* [Top10MonitorMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/Top10MonitorMain.java) : 
需求: 在所有监控点里面，通过车辆最多的10个监控点是什么？
目的： 可以对这些监控点所在的路段采取一些措施来缓解车流量。
* [Top10OverSpeedMonitorMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/Top10OverSpeedMonitorMain.java) : 
需求: 在所有监控点里面，超速（max speed: 250）车辆中车速最大的10个监控点是什么？
目的： 超速行驶很危险，需要对这些监控点所在的路段采取措施，比如设置减速带，加大处罚力度等一系列措施来限制车辆超速。
* [Top10OverSpeedVehicleNumberMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/Top10OverSpeedVehicleNumberMain.java) : 
需求： 在所有监控点里面，超速（max speed: 250）车辆最多的10个监控点是什么？
目的： 知道了结果以后，相关人员可以对这些监控点所在的路段进行分析，并采取相关措施来限制车辆超速。比如：加设减速带等
* [Top10VehicleNumberHourMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/Top10VehicleNumberHourMain.java) : 
需求： 在所有监控点里面，在24小时内，查询出每个小时里面车流量，取前10个车流量最大的时段。
目的： 了解各个小时内的车流量情况，若果出现车流高峰，应该采取什么样的措施。
* [MonitorConversionRateMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/traffic/MonitorConversionRateMain.java) : 
需求： 在所有监控点里面，这些监控点直接的转化率是多少？
目的： 检测给出的两个监控点的设计是否合理。如果转化率太低，说明这样的设计不是很合理，那么可以通过采取一些措施，使得资源得到充分利用。


# SparkStreaming Example

![Example](https://github.com/Hongten/RootMemory/blob/master/image/contact.png)

![alt text](https://github.com/Hongten/spark_example/blob/master/output/realTimeVehicleSpeedMonitor.gif)


* [RealTimeVehicleSpeedMonitorMain](https://github.com/Hongten/spark_example/blob/master/src/com/hongten/spark/example/streaming/RealTimeVehicleSpeedMonitorMain.java) : 
需求：使用SparkStreaming，并且结合Kafka，获取实时道路交通拥堵情况信息。
目的： 对监控点平均车速进行监控，可以实时获取交通拥堵情况信息。相关部门可以对交通拥堵情况采取措施。
e.g.1.通过广播方式，让司机改道。 
    2.通过实时交通拥堵情况数据，反映在一些APP上面，形成实时交通拥堵情况地图，方便用户查询。

![Example](https://github.com/Hongten/spark_example/tree/master/output/realTimeVehicleSpeedMonitor.gif)


# More Information

* Author            : Hongten
* E-mail            : [hongtenzone@foxmail.com](mailto:hongtenzone@foxmail.com)
* Home Page         : [http://www.cnblogs.com/hongten](http://www.cnblogs.com/hongten)