add_executable(kepler_logging
    nuraft_logging.cxx
    logger.cc
    nl_log_store.cxx
)
target_link_libraries(kepler_logging nuraft rocksdb)
