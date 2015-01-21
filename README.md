                    主从切换控制


  依赖于zookeeper一致性协调服务，轻松实现Server一主多从的主从切换功能，只需要简单嵌入即可完成


  需要做主从的Server或者Server内部的模块只需要实现Instance的三个接口
  用户的Instance主从必须使用同一个key，key在多个Instance下必须唯一
  InstanceManager用以管理用户的实例

  每一个Instance的主从控制由一个一个RunningMonitor控制，RunningMonitor必须有一个监听器，DefaultServerRunningListener是一个默认的监听器，监听对应的实例，控制InstanceManager存放的Instance进行start、stop动作。如有复杂的实现，需要用户可自定义Listener,实现RunningListener接口
  
  代码中有一个简单的测试示例
