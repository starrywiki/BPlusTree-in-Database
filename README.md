# Project4 : B+ tree

这是 CS1959(2023 - 2024 - 2) 程序设计与数据结构课程的第 4 个 project: 实现数据库中的 B+ 树。

在这次 project 中， 我们会为 bustub 关系型数据库编写 B+ 树索引。完成本次 project 不需要具备额外的数据库相关知识。

## 基础知识

在开始这个 project 之前， 我们需要了解一些基础知识。 由于课上已学习过 B 树与 B+ 树，这里没有对 B 树与 B+ 树进行介绍， 如有需要请查阅相关课程 PPT。如果你对 B+ 树进行操作后的结构有疑惑， 请在 https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html 网站上进行尝试。

### 数据库的简单介绍(optional)

我们所说的数据库通常指数据库管理系统(DBMS, 即 Database Management System)。 我们可以简单理解为组织管理庞大数据的一个软件。

数据库可以分成很多类型， 这里我们所关心的 bustub 数据库是"关系型数据库"。 我们可以简单理解成， 这种数据库里存储的是一张张表格， 这些表格根据数据之间的关系建立起来。 例如， 下图就是一个表格示例。

![](https://notes.sjtu.edu.cn/uploads/upload_d74ed6ea471f51aa663eeb281bae90b9.png)


对于数据库架构， 我从网络上找到了一个非常形象的图：(版权归"小林 coding"公众号所有， 该图为知名数据库 mysql 的架构示意图)

![](https://notes.sjtu.edu.cn/uploads/upload_2f43366c45fa12ba230efff3b21c3da4.png)

我们的数据库管理系统通常分为 Server 层和存储引擎层。 Server 层需要解决网络通信、 SQL 语句解析、 执行计划生成与优化等问题。 Server 层决定了用户输入的 SQL 查询语句是如何转化成优化后的执行计划。存储引擎层则负责数据的存储与提取。 不同存储引擎所使用的数据结构和实现方式可能并不相同。

如果你并不能看懂上图也没有关系。对于本次 project， 我们只需要关心 "存储引擎" 部分。下面我将介绍 bustub 数据库的存储引擎。

### 存储引擎与 B+ 树

我们的 bustub 数据库的存储引擎将数据存储在磁盘上， 实现数据的持久化。我们知道， 磁盘的一大特征便是空间大但访问速度非常慢， 因此， 我们希望能减少对于磁盘的交互访问 (以下称为磁盘 IO) 次数。

为什么我们采取 B+ 树作为这个存储引擎的数据结构？ 为了便于查询，我们需要给我们的表格建立目录， 即选取表格中的某一列作为 "索引"。这样， 通过索引便可建立一个有序的数据结构， 如二叉搜索树， 我们查询时只需要先找到索引便可找到我们所需的数据行。 但二叉搜索树的深度太大， 导致对其进行查询（或插入删除）操作时磁盘 IO 次数太多。 因此， 我们可以考虑选取 B 树， B 树的深度往往远小于数据数量， 通常可维持在 3-4 层左右。 但 B 树每个结点都存储索引和数据行， 导致 B 树的单个节点所占空间太大。 另一方面， B 树不支持按照索引进行顺序查找。 因此我们可以将 B 树升级为 B+ 树， 只在叶子结点存储真正的数据行， 非叶子节点只存储索引。

![](https://notes.sjtu.edu.cn/uploads/upload_bfde29d73741b26103fce71094eae7e4.png)


Tips: 以上所说的是主键索引的情况， 实际上数据库还有其他多种索引方式， 但与本次 project 无关， 有兴趣的同学可以自行了解。

### 存储引擎与数据页(optional)


根据上方的表格图像，我们每行都存储了一条数据信息。但如果用户执行查询操作， 我们并不能以 "行" 为单位读取数据， 否则一次磁盘 IO 只能处理一行， 执行效率过低。 我们这里采取的策略是以 "page"（数据页）为单位进行磁盘 IO。 我们可以简单地将一个 page 视为是固定大小的一块存储空间。 通过打包一系列数据行进入同一个 page， 可以实现减少磁盘 IO 的效果。这里我们并不需要了解 page 的细节，与磁盘的 IO 操作已被封装为以下几个函数: `ReadPageGuard`，`WritePageGuard`。 

![](https://notes.sjtu.edu.cn/uploads/upload_5879015cd51b787ca781b64ac3e5e7b2.png)


### B+ 树加锁方法

B+ 树加锁采用 "螃蟹法则"。

## 主体任务

请你修改 src/include/storage/index/b_plus_tree.h 和 src/storage/index/b_plus_tree.cpp, 实现 b+ 树的查找、插入和删除函数。 在此之后， 请你完善 b+ 树的查找、插入、删除函数， 使其线程安全。

## 测试方法

请根目录执行

```shell
sudo build_support/packages.sh #Linux 环境请执行这个
build_support/packages.sh # macOS 可以直接这样执行
#windows 环境请使用 wsl 或者虚拟机

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
```

之后， 每次测试需要重新 make, 命令如下:

```shell
cd build #进入 build 目录， 如果已经在 build 目录请忽略
make b_plus_tree_insert_test b_plus_tree_delete_test b_plus_tree_contention_test b_plus_tree_concurrent_test -j$(nproc) #并行编译所有测试单元。 如果你暂时只想执行一部分测试程序， 请只 make 对应的 b_plus_tree_*_test。
```

待编译好之后， 可以这样测试:

```shell
cd build #进入 build 目录， 如果已经在 build 目录请忽略
./test/b_plus_tree_insert_test
./test/b_plus_tree_delete_test
./test/b_plus_tree_contention_test
./test/b_plus_tree_concurrent_test
```

## 需要阅读的代码



Acknowledgement : CMU15445 Database System.
