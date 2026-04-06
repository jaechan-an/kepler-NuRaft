#include <rocksdb/db.h>

#include <iostream>
#include <libnuraft/nuraft.hxx>

int main() {
    rocksdb::DB* db;
    rocksdb::Options opts;
    opts.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(opts, "/tmp/testdb", &db);
    std::cout << s.ToString() << std::endl;
    delete db;
}

