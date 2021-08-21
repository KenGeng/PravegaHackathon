# 运行说明

## 前期准备

### Pravega准备

下载pravega压缩包并解压

```bash
$ wget https://github.com/pravega/pravega/releases/download/v0.9.0/pravega-0.9.0.tgz
$ tar zxvf pravega-0.9.0.tgz
```

设置上述解压后的路径为`PRAVEGA_HOME` 

启动Pravega集群

```bash
$ ${PRAVEGA_HOME}/bin/pravega-standalone
```

创建新的终端窗口，在其中运行Pravega CLI，创建所需Scope与Stream

```bash
$ ${PRAVEGA_HOME}/bin/pravega-cli
> scope create my-scope
> stream create my-scope/input-stream
> stream create my-scope/input-stream
```

### Pravega Flink Connector准备

下载connector的jar包：https://github.com/pravega/flink-connectors/releases/download/v0.9.1/pravega-connectors-flink-1.11_2.12-0.9.1.jar

将jar包放在`/root/`目录下

### 数据准备

在python文件同级目录下创建`input_data.txt`，在其中写入任意行内容。

## 运行过程

新开两个窗口，分别运行`pravega-pravega.py`和`pravega-print.py`，先后顺序随意

```bash
$ python pravega-pravega.py
```

```bash
$ python pravega-print.py
```

再新开一个窗口，运行`file-pravega.py`

```bash
$ python file-pravega.py
```

顺利的话，可以在`pravega-print.py`对应的窗口中看到`input_data.txt`中的内容被打印出来。

## 运行过程解读

- `file-pravega.py`从`input_data.txt`中读取数据，写入pravega集群的`my-scope/input-stream`中
- `pravega-pravega.py`从`my-scope/input-stream`中读取数据，写入`my-scope/output-stream`中
- `pravega-print.py`从`my-scope/output-stream`中读取数据，打印至终端。

## 相关文档

- [Pravega Flink Connector Table API](https://github.com/pravega/flink-connectors/blob/master/documentation/src/docs/table-api.md)
- [Pravega Quick Start Guide](https://github.com/pravega/pravega/blob/master/documentation/src/docs/getting-started/quick-start.md)