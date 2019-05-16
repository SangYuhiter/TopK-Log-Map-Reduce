# TopK-Log-Map-Reduce
**基于MapReduce方法统计服务器日志topk数据**
## data_producer.py:日志数据生成器
## mapper.py:Map过程（多个map函数）
## reducer.py:Reducer过程（多个reduce函数）
## GetTopK.py:主函数（将数据生成，MapReduce过程组合到一起，并输出topk数据）
#### 使用说明：
* 1、创建data_files(随机生成的数据)，process_files(mapreduce过程文档)
* 2、时间和域名的随机区间要稍小一些，这样能看到效果
* 3、为了空间效率使用了生成器，只能使用迭代方式顺序访问