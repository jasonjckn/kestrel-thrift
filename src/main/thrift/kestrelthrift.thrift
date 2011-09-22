namespace java com.twitter.kestrelthrift.thrift
namespace rb Kestrelthrift

struct Item {
  /* the actual data */
  1: binary data

  /* transaction ID, to be used in the `confirm` call */
  2: i32 xid
}


/**
 * A simple memcache-like service, which stores strings by key/value.
 * You should replace this with your actual service.
 */
service KestrelthriftService {
  list<Item> get(1: string queue_name, 2: i32 max_items, 3: bool transaction)  // max items always = 1.
  void put(1: string queue_name, 2: list<binary> items)
  void ack(1: string queue_name, 2: set<i32> xids)
  void fail(1: string queue_name, 2: set<i32> xids)
  void flush(1: string queue_name)
}
