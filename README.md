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

## 熟悉项目代码

请跟着我一步一步熟悉那些本次 project 需要用到的代码。 让我们从 IO 的基本单元 page 开始。

#### page

首先， 请看 `src/include/storage/page/page.h `:

我们的 page 类包含以下成员:

```cpp
  /** The actual data that is stored within a page. */
  // Usually this should be stored as `char data_[BUSTUB_PAGE_SIZE]{};`. But to
  // enable ASAN to detect page overflow, we store it as a ptr.
  char* data_;
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding
   * page on disk. */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
```

所以， 我们为每个 page 都实现了一个读写锁 `rwlatch_`, 我们之后的加锁是为 page 加锁。 其次， 我们的 page 包含一个 char * 区域存储 page 内部包含的数据。 其他成员你可以忽略。

#### b_plus_tree_page

其次, 请看 `src/include/storage/page/b_plus_tree_page.h`:

我们的 B+ 树的 page 存储在上方原始 page 的 data 区域。 你可以认为， 上方的 page 包裹着这里的 b_plus_tree_page。

```cpp
/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
class BPlusTreePage
{
  public:
    //...

  private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_;
  int size_;
  int max_size_;
};
```

此外， 请见 `src/include/storage/page/b_plus_tree_header_page.h`, 我们在这里定义了一个特殊的 header page 类型， 它存储着 B+ 树的根节点。 特殊定义一个 header page 有助于提升并发表现。

```cpp
class BPlusTreeHeaderPage
{
  public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeHeaderPage() = delete;
  BPlusTreeHeaderPage(const BPlusTreeHeaderPage& other) = delete;

  page_id_t root_page_id_;
};
```

请注意， 我们在 page 内部存储指向另一个 page 的 "指针" 时, 我们都是存储 page_id, 因为这些 page 在磁盘上。

然后， 请见 `src/include/storage/page/b_plus_tree_internal_page.h`, `src/include/storage/page/b_plus_tree_leaf_page.h`. 我们的 internal page 和 leaf page 类都继承自 BPlusTreePage. internal page 对应 B+ 树的内部结点， leaf page 对应 B+ 树的叶子结点。对于 internal page, 它存储着 n 个 索引的 key值 和 n + 1 个指向 children page 的指针。 对于 LeafPage, 它存储着 n 个 索引的 key 值和 n 个对应的数据行 ID。 这里的 "KeyAt", "SetKeyAt", "ValueAt", "SetValueAt" 可用于键值对的查询与更新， 会在 B+ 树的编写中用到。

#### page_guard

然后， 请见 `src/include/storage/page/page_guard.h`。 我们在 page.h 中可以看到， 每个 page 都附带了一个读写锁。 但是我们通常会遗忘掉释放锁， 为了解决这一问题， 我们使用 RAII 思想为 page 写了一个封装起来的类: page_guard。 page_guard 即在构造函数里获取锁， 在析构函数里释放锁。 我们对应设计了 `ReadPageGuard` 和 `WritePageGuard`. 在这个 .h 文件里你还需要关注 page_guard 的 `As` 函数， 可以获取 page_guard 封装起来的 page 的 data 区域, 将这块区域重新解释为某一类型。此外， `Drop` 成员函数相当于手动调用析构函数： 它会释放对于 page 的所有权

例如，请看我在 `src/storage/index/b_plus_tree.cpp` 给出的示例函数 `IsEmpty`:

```cpp
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const  ->  bool
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page -> root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
}
```

这里的 `guard.template As<BPlusTreeHeaderPage>();` 即为获取我们读到的 `ReadPageGuard`, 获取它封装的 `page` 的 data 区域， 将这块区域重新解释为 `BPlusTreeHeaderPage` 类型。 也就是， 我们获取了一个用来读的 `page`， 然后把这个 `page` 里面的数据解释为 `BPlusTreeHeaderPage`， 读取 header page 里的 root_page_id 信息， 检查是否是 INVALID.

上方有一个很奇怪的函数， 叫做 `bpm_ -> FetchPageRead`, 这是什么呢？ 请接着看。

#### FetchPage by buffer pool manager

请见 `src/include/buffer/buffer_pool_manager.h `. 这份代码实际上是用于实现缓存池， 具体内容你不需要理解。我们只需要用到其中两个函数：

```cpp
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;
```

请你在本次 project 中将它们视为一个黑盒，`bpm_ -> FetchPageRead` 和 `bpm_ -> FetchPageWrite` 用于通过磁盘 IO ， 根据 page id 得到一个用于读的 `ReadPageGuard` 或者一个用于写 `WritePageGuard`. 如果你好奇其中的细节(如缓存池)， 请私信我。 `ReadPageGuard` 会自动获取该 page 的读锁， 并在析构时释放读锁。 `WritePageGuard` 会自动获取该 page 的写锁， 并在析构时释放写锁。


下面是一些使用 `FetchPageRead / FetchPageWrite` 的例子：

```cpp
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key)  ->  INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);
    //读到 header page

  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ == INVALID_PAGE_ID)
  {
    return End();
  }
    //如果 header page 存的 root_page_id 是 INVALID, 说明树空， 返回 End()

  ReadPageGuard guard = bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
    //读到 root page, 即 B+ 树的根节点

  head_guard.Drop();
  //析构 header page

  auto tmp_page = guard.template As<BPlusTreePage>();
    //下面需要一步步寻找参数 key， 先把 guard 的 data 部分解释为 BPlusTreePage. 这一步实际上是我们这个 project 的惯例 : 拿到 page guard, 然后用 As 成员函数拿到 b_plus_tree_page 的指针。

  while (!tmp_page -> IsLeafPage())
  { 
    //如果不是叶子结点，我就一直找

    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    //这里是内部结点， 那就把它 cast 成 InternalPage. InternalPage 是 BPlusTreeInternalPage 的别名。请注意， 只有我们的指针类型正确时候， 我们才能拿到这个类的数据成员和成员函数。

    int slot_num = BinaryFind(internal, key);
    //然后调用辅助函数 BinaryFind 在 page 内部二分查找这个 key， 找到该向下走哪个指针

    if (slot_num == -1)
    {
      return End();
    }
    //异常处理

    guard = bpm_ -> FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    //现在向下走， 根据上方得到的 page id 拿到新的 page guard。

    tmp_page = guard.template As<BPlusTreePage>();
    //然后再用相同方式把 page guard 的数据部分解释为 BPlusTreePage, 继续循环。
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);
  //最后跳出循环， 说明找到了叶子结点。

  int slot_num = BinaryFind(leaf_page, key);
    //在叶子节点内部二分查找，找到对应的 key

  if (slot_num != -1)
  {
    //如果找到了， 构造对应迭代器。
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  } 
  return End();
}
  ```

#### B+ 树核心代码

请见 `src/include/storage/index/b_plus_tree.h` 与 `src/storage/index/b_plus_tree.cpp`

#### Context

请见 `src/include/storage/index/b_plus_tree.h`.

Context 类可用于编写 B+ 树的螃蟹法则， 存储一条链上的锁。

```cpp
/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};
```


## 测试方法

### 本地测试

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

### 提交测试

由于本次项目过大， ACMOJ 不具备相关功能。 因此， 请通过本地测试后将代码压缩发给我， 我会尽快在我的本机上进行测试。

## 评分标准

./test/b_plus_tree_insert_test              45分
./test/b_plus_tree_delete_test              45分
./test/b_plus_tree_contention_test          20分
./test/b_plus_tree_concurrent_test          20分
Code Review                                 10分

满分上限为 120 分， 溢出 100 分的部分抵消之前大作业所扣分数。


Acknowledgement : CMU15445 Database System.
