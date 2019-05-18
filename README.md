# TopK-Log-Map-Reduce
**基于MapReduce方法统计服务器日志topk数据**
**基于MapReduce方法统计服务器日志黑名单数据**
## data_producer.py:日志数据生成器
## mapper.py:Map过程（多个map函数）
## reducer.py:Reducer过程（多个reduce函数）
## GetTopK.py:主函数（将数据生成，MapReduce过程组合到一起，并输出topk数据）
## BlackList.py:主函数（生成数据，MapReduce过程组合到一起，并输出黑名单数据）

### 过程说明
*topk*
* first_map:ip  access_time --> ip  1   access_time
* first_reduce:ip  1    access_time   -->  ip  n    access_time1 accesstime2 ...
* second_map:ip  n    access_time1 accesstime2 ...  --> n   ip  access_time1 accesstime2 ...
* second_reduce:对n排序得到topk

*黑名单*
* first_map:ip  access_time --> access_time ip  1
* first_reduce:按access_time排序
* second_map:access_time ip  1  --> ip  access_time access_time 1
* second_reduce:ip  access_time access_time 1   --> ip  access_time1 access_time2 m1,access_time3 access_time4 m2,
* first_map:ip  access_time1 access_time2 m1,access_time3 access_time4 m2,分解每一个时间段得到    m1  ip access_time1 access_time2
* first_reduce:对访问次数m1进行排序，得到在时间段内访问次数最多的ip


#### 使用说明：
* 1、创建data_files(随机生成的数据)，process_files(mapreduce过程文档)
* 2、时间和域名的随机区间要稍小一些，这样能看到效果
* 3、为了空间效率使用了生成器，只能使用迭代方式顺序访问