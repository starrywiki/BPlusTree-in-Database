#include "storage/index/b_plus_tree.h"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
                          BufferPoolManager* buffer_pool_manager,
                          const KeyComparator& comparator, int leaf_max_size,
                          int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id)
{
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  // In the original bpt, I fetch the header page
  // thus there's at least one page now
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  // reinterprete the data of the page into "HeaderPage"
  root_header_page->root_page_id_ = INVALID_PAGE_ID;
  // set the root_id to INVALID
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool
{
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page->root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType& key,
                              std::vector<ValueType>* result, Transaction* txn)
    -> bool
{
  if (IsEmpty())
    return false;
  page_id_t pid;
  {
    auto header_guard = bpm_->FetchPageRead(header_page_id_);
    pid = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  }

  while (true)
  {
    auto guard = bpm_->FetchPageRead(pid);
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage())
    {
      auto leaf = reinterpret_cast<const LeafPage*>(page);
      int sz = leaf->GetSize();
      for (int i = 0; i < sz; i++)
      {
        if (comparator_(leaf->KeyAt(i), key) == 0)
        {
          result->push_back(leaf->ValueAt(i));
          return true;
        }
      }
      return false;
    }
    else
    {
      auto internal = reinterpret_cast<const InternalPage*>(page);
      int idx = BinaryFind(internal, key);
      pid = internal->ValueAt(idx);
    }
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DropAllGuards(Context& ctx)
{
  if (ctx.header_page_)
  {
    ctx.header_page_->Drop();
    ctx.header_page_ = std::nullopt;
  }
  for (auto& w : ctx.write_set_)
  {
    w.Drop();
  }
  ctx.write_set_.clear();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternalPage(InternalPage *parent_page,
                                       const KeyType &key,
                                       page_id_t new_page_id,
                                       KeyType &mid_key,
                                       page_id_t &new_internal_id) {
  //collect keys & values
  int old_size = parent_page->GetSize();
  std::vector<KeyType> all_keys;
  std::vector<page_id_t> all_values;

  for (int i = 0; i < old_size; ++i) {
    all_values.push_back(parent_page->ValueAt(i));
    if (i != 0) {
      all_keys.push_back(parent_page->KeyAt(i));
    }
  }

  //insert target
  int insert_pos = BinaryFind(parent_page, key)+1;
  all_keys.insert(all_keys.begin() + insert_pos - 1, key);
  all_values.insert(all_values.begin() + insert_pos, new_page_id);

  auto new_guard = bpm_->NewPageGuarded(&new_internal_id);
  WritePageGuard new_write_guard = new_guard.UpgradeWrite();
  InternalPage *new_page = new_write_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);

  // split
  int total = all_values.size(); 
  int mid = total / 2;
  mid_key = all_keys[mid-1];

  parent_page->SetSize(mid);
  for (int i = 0; i < mid; ++i) {
    parent_page->SetValueAt(i, all_values[i]);
  }
  for (int i = 1; i < mid; ++i) {
    parent_page->SetKeyAt(i, all_keys[i - 1]);
  }

  new_page->SetSize(total - mid);
  for (int i = 0; i < new_page->GetSize(); ++i) {
    new_page->SetValueAt(i, all_values[i + mid]);
  }
  for (int i = 1; i < new_page->GetSize(); ++i) {
    new_page->SetKeyAt(i, all_keys[i + mid - 1]);
  }

  new_write_guard.Drop();
  new_guard.Drop();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const KeyType& key, page_id_t new_page_id,
                                      Context& ctx, int index)
{
  // std::cout << "enter insertintopa,index= " << index << std::endl;
  if (index < 0)
  {
    // Root split, create new root
    page_id_t new_root_id;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
    WritePageGuard write_root_guard = new_root_guard.UpgradeWrite();
    auto new_root = write_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    new_root->SetSize(2);
    // std::cout << "wow: " << ctx.write_set_[0].PageId() << std::endl;
    new_root->SetValueAt(0, ctx.write_set_[0].PageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, new_page_id);
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_id;
    return;
  }
  // Insert into parent internal page
  // auto parent_index = ctx.write_set_.size() - 2;
  // auto parent_guard = ctx.write_set_[parent_index];
  auto parent_page = ctx.write_set_[index].AsMut<InternalPage>();

  int pos = BinaryFind(parent_page, key) + 1;
  // std::cout << "pos: " << pos << std::endl;
  if (parent_page->GetSize() < internal_max_size_)
  {
    parent_page->IncreaseSize(1);
    for (int i = parent_page->GetSize() - 1; i > pos; --i)
    {
      parent_page->SetKeyAt(i, parent_page->KeyAt(i - 1));
      parent_page->SetValueAt(i, parent_page->ValueAt(i - 1));
    }
    parent_page->SetKeyAt(pos, key);
    parent_page->SetValueAt(pos, new_page_id);
    return;
  }

  // std::cout << "aaa" << std::endl;
  // std::cout << "index= " << index << std::endl;
  page_id_t new_internal_id;
  KeyType mid_key;

  SplitInternalPage(parent_page, key, new_page_id, mid_key, new_internal_id);

  // auto new_internal_guard = bpm_->NewPageGuarded(&new_internal_id);
  // WritePageGuard inter_write_guard = new_internal_guard.UpgradeWrite();
  // auto new_internal = inter_write_guard.AsMut<InternalPage>();
  // new_internal->Init(internal_max_size_);
  // int mid = parent_page->GetMinSize();
  // new_internal->SetSize(internal_max_size_ - mid + 1);
  // // for (int i = 1; i < new_internal->GetSize(); ++i)
  // // {
  // //   new_internal->SetKeyAt(i, parent_page->KeyAt(i + mid-1));
  // //   new_internal->SetValueAt(i, parent_page->ValueAt(i + mid-1));
  // // }
  // // parent_page->SetSize(mid);
  // KeyType mid_key;
  // if (pos < mid)
  // {
  //   for (int i = 1; i < new_internal->GetSize(); ++i)
  //   {
  //     new_internal->SetKeyAt(i, parent_page->KeyAt(i + mid - 1));
  //     new_internal->SetValueAt(i, parent_page->ValueAt(i + mid - 1));
  //   }
  //   // parent_page->SetSize(mid);
  //   // parent_page->IncreaseSize(1);
  //   for (int i = parent_page->GetSize() - 1; i > pos; --i)
  //   {
  //     parent_page->SetKeyAt(i, parent_page->KeyAt(i - 1));
  //     parent_page->SetValueAt(i, parent_page->ValueAt(i - 1));
  //   }
  //   parent_page->SetKeyAt(pos, key);
  //   parent_page->SetValueAt(pos, new_page_id);
  //   mid_key = parent_page->KeyAt(parent_page->GetSize() - 1);
  //   new_internal->SetValueAt(0,
  //                            parent_page->ValueAt(parent_page->GetSize() - 1));
  //   parent_page->SetSize(parent_page->GetMinSize());
  // }
  // else if (pos == mid)
  // {
  //   for (int i = 1; i < new_internal->GetSize(); ++i)
  //   {
  //     new_internal->SetKeyAt(i, parent_page->KeyAt(i + mid - 1));
  //     new_internal->SetValueAt(i, parent_page->ValueAt(i + mid - 1));
  //   }
  //   new_internal->SetValueAt(0, new_page_id);
  //   mid_key = key;
  //   parent_page->SetSize(parent_page->GetMinSize());
  // }
  // else
  // {
  //   pos -= mid;
  //   // new_internal->IncreaseSize(1);
  //   new_internal->SetValueAt(0, parent_page->ValueAt(mid));
  //   for (int i = 1; i < new_internal->GetSize(); ++i)
  //   {
  //     new_internal->SetKeyAt(i, parent_page->KeyAt(i + mid));
  //     new_internal->SetValueAt(i, parent_page->ValueAt(i + mid));
  //   }
  //   for (int i = new_internal->GetSize() - 1; i > pos; --i)
  //   {
  //     new_internal->SetKeyAt(i, new_internal->KeyAt(i - 1));
  //     new_internal->SetValueAt(i, new_internal->ValueAt(i - 1));
  //   }
  //   new_internal->SetKeyAt(pos, key);
  //   new_internal->SetValueAt(pos, new_page_id);
  //   mid_key = parent_page->KeyAt(mid);
  //   parent_page->SetSize(parent_page->GetMinSize());
  // }
  index--;
  InsertIntoParent(mid_key, new_internal_id, ctx, index);
  return;
}

int totalInsert = 0;
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value,
                            Transaction* txn) -> bool
{
  totalInsert++;
  // std::cout << "tree: " << totalInsert << std::endl;
            // << DrawBPlusTree() << std::endl;
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID)
  {
    page_id_t new_root_id;
    auto root_guard = bpm_->NewPageGuarded(&new_root_id);
    WritePageGuard write_guard_l = root_guard.UpgradeWrite();
    auto leaf_page = write_guard_l.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->SetSize(1);
    leaf_page->SetAt(0, key, value);

    header_page->root_page_id_ = new_root_id;
    ctx.header_page_->Drop();
    ctx.header_page_ = std::nullopt;
    return true;
  }

  ctx.root_page_id_ = header_page->root_page_id_;
  page_id_t curIdx = header_page->root_page_id_;
  while (true)
  {
    WritePageGuard curGuard = bpm_->FetchPageWrite(curIdx);
    auto curPage = curGuard.AsMut<BPlusTreePage>();
    ctx.write_set_.emplace_back(std::move(curGuard));
    if (curPage->IsLeafPage())
    {
      break;
    }
    auto inter_page = reinterpret_cast<InternalPage*>(curPage);
    int child_index = BinaryFind(inter_page, key);
    curIdx = inter_page->ValueAt(child_index);
  }

  auto& leaf_guard = ctx.write_set_.back();
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  int index = BinaryFind(leaf_page, key);
  if (index >= 0 && comparator_(leaf_page->KeyAt(index), key) == 0)
  {
    DropAllGuards(ctx);
    return false;  // duplicate keys
  }
  index += 1;
  if (leaf_page->GetSize() < leaf_max_size_)
  {
    leaf_page->IncreaseSize(1);
    for (int i = leaf_page->GetSize() - 1; i > index; --i)
    {
      leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetAt(index, key, value);
    DropAllGuards(ctx);
    return true;
  }

  //split leaf
  page_id_t new_leaf_id;
  auto new_leaf_guard = bpm_->NewPageGuarded(&new_leaf_id);
  auto new_write_guard = new_leaf_guard.UpgradeWrite();
  auto new_leaf = new_write_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetSize(leaf_page->GetSize() - leaf_page->GetMinSize());

  for (int i = 0; i < new_leaf->GetSize(); ++i)
  {
    new_leaf->SetAt(i, leaf_page->KeyAt(i + leaf_page->GetMinSize()),
                    leaf_page->ValueAt(i + leaf_page->GetMinSize()));
  }
  // insert
  leaf_page->SetSize(leaf_page->GetMinSize());
  if (index < leaf_page->GetMinSize())
  {
    leaf_page->IncreaseSize(1);
    for (int i = leaf_page->GetSize() - 1; i > index; --i)
    {
      leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetAt(index, key, value);
  }
  else
  {
    index -= leaf_page->GetMinSize();
    new_leaf->IncreaseSize(1);
    for (int i = new_leaf->GetSize() - 1; i > index; --i)
    {
      new_leaf->SetAt(i, new_leaf->KeyAt(i - 1), new_leaf->ValueAt(i - 1));
    }
    new_leaf->SetAt(index, key, value);
  }
  new_leaf->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_id);

  KeyType push_up_key = new_leaf->KeyAt(0);
  int idx = ctx.write_set_.size() - 2;
  InsertIntoParent(push_up_key, new_leaf_id, ctx, idx);
  new_write_guard.Drop();
  DropAllGuards(ctx);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* txn)
{
  // Your code here
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::BinaryFind(const BPlusTreePage* page, const KeyType& key)
{
  if (page->IsLeafPage())
  {
    auto leaf = reinterpret_cast<const LeafPage*>(page);
    int low = 0, high = leaf->GetSize() - 1;
    while (low <= high)
    {
      int mid = low + (high - low) / 2;
      int cmp = comparator_(leaf->KeyAt(mid), key);
      if (cmp == 0)
        return mid;
      if (cmp < 0)
        low = mid + 1;
      else
        high = mid - 1;
    }
    return high;  // 返回最后一个小于key的位zhi
  }
  else
  {
    auto internal = reinterpret_cast<const InternalPage*>(page);
    int l = 1;
    int r = internal->GetSize() - 1;
    while (l < r)
    {
      int mid = (l + r + 1) >> 1;
      if (comparator_(internal->KeyAt(mid), key) != 1)
      {
        l = mid;
      }
      else
      {
        r = mid - 1;
      }
    }

    if (r == -1 || comparator_(internal->KeyAt(r), key) == 1)
    {
      r = 0;
    }

    return r;
    // int size = internal->GetSize();
    // int idx = 1;
    // while (idx < size && comparator_(internal->KeyAt(idx), key) <= 0)
    // {
    //   idx++;
    // }
    // return idx - 1;
  }
}
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::BinaryFind(const LeafPage* leaf_page, const KeyType&
// key)
//      ->  int
// {
//   int l = 0;
//   int r = leaf_page -> GetSize() - 1;
//   while (l < r)
//   {
//     int mid = (l + r + 1) >> 1;
//     if (comparator_(leaf_page -> KeyAt(mid), key) != 1)
//     {
//       l = mid;
//     }
//     else
//     {
//       r = mid - 1;
//     }
//   }

//   if (r >= 0 && comparator_(leaf_page -> KeyAt(r), key) == 1)
//   {
//     r = -1;
//   }

//   return r;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::BinaryFind(const InternalPage* internal_page,
//                                 const KeyType& key)  ->  int
// {
//   int l = 1;
//   int r = internal_page -> GetSize() - 1;
//   while (l < r)
//   {
//     int mid = (l + r + 1) >> 1;
//     if (comparator_(internal_page -> KeyAt(mid), key) != 1)
//     {
//       l = mid;
//     }
//     else
//     {
//       r = mid - 1;
//     }
//   }

//   if (r == -1 || comparator_(internal_page -> KeyAt(r), key) == 1)
//   {
//     r = 0;
//   }

//   return r;
// }

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE
// Just go left forever
{
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();

  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page->IsLeafPage())
  {
    int slot_num = 0;
    guard = bpm_->FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  int slot_num = 0;
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
  }
  return End();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key) -> INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);

  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();
  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page->IsLeafPage())
  {
    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    int slot_num = BinaryFind(internal, key);
    if (slot_num == -1)
    {
      return End();
    }
    guard = bpm_->FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

  int slot_num = BinaryFind(leaf_page, key);
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE
{
  return INDEXITERATOR_TYPE(bpm_, -1, -1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t
{
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page->root_page_id_;
  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string& file_name,
                                      Transaction* txn)
{
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input)
  {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction)
    {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager* bpm)
{
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage* page)
{
  if (page->IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId()
              << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else
  {
    auto* internal = reinterpret_cast<const InternalPage*>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++)
    {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++)
    {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager* bpm, const std::string& outf)
{
  if (IsEmpty())
  {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage* page,
                             std::ofstream& out)
{
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize()
        << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID)
    {
      out << leaf_prefix << page_id << "   ->   " << leaf_prefix
          << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix
          << leaf->GetNextPageId() << "};\n";
    }
  }
  else
  {
    auto* inner = reinterpret_cast<const InternalPage*>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize()
        << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++)
    {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner->KeyAt(i) << "  " << inner->ValueAt(i);
      // } else {
      // out << inner  ->  ValueAt(0);
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++)
    {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0)
      {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage())
        {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId()
              << " " << internal_prefix << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId()
          << "   ->   ";
      if (child_page->IsLeafPage())
      {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      }
      else
      {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string
{
  if (IsEmpty())
  {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id)
    -> PrintableBPlusTree
{
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage())
  {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++)
  {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub