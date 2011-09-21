namespace java com.twitter.kestrelthrift.thrift
namespace rb Kestrelthrift

/**
 * It's considered good form to declare an exception type for your service.
 * Thrift will serialize and transmit them transparently.
 */
exception KestrelthriftException {
  1: string description
}

/**
 * A simple memcache-like service, which stores strings by key/value.
 * You should replace this with your actual service.
 */
service KestrelthriftService {
  string get(1: string key) throws(1: KestrelthriftException ex)

  void put(1: string key, 2: string value)
}
