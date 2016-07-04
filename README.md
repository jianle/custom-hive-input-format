Custom HiveInputFormat
===========================

文件通过snappy解压后格式如下：

```
[4字节数据长度（大字节序）][数据内容（pb序列化）][4字节数据长度（大字节序）][数据内容（pb序列化）]...
```

### 使用Hive表读取原始文件信息

* 可选字段（字段名/类型必须保持一致）
  * tableid（`int`）
  * colspace（`int`）
  * rowkey（`string`）
  * colkey（`string`）
  * value（`string/binary`）
  * score（`bigint`）
  * ttl（`int`）


* 创建Hive表

```sql  
add jar /user/hadoop/udf/UDHiveInputFormat.jar;
CREATE EXTERNAL TABLE my_test(
  tableid int,
  colkey string,
  score bigint,
  rowkey string,
  value string)
STORED BY 'com.my.tutorial.MyStorageHandler';
```  

* 使用（查询等操作）  

```sql
add jar /user/hadoop/udf/UDHiveInputFormat.jar;
SELECT * FROM my_test LIMIT 10;
or
SELECT * FROM my_test WHERE tableid = 1 LIMIT 10;
...
```    

### 注意事项：  
* 字段名必须属于（`tableid`，`colspace`，`rowkey`，`colkey`，`value`，`score`，`ttl`）， 字段类型也保持一致  
* 字段类型  
* `STORED BY`必须指定相应class  
* 如果要从`my_test`表中写数据到别的表，许对`value`做进一步处理，`value`中有可能包含`\r`、`\n`、`\t`等特殊字符，需要regexp_replace处理等  


### Run & Developer

```
$ mvn elipse:eclipse
$ mvn package
```