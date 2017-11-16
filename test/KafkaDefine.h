#ifndef _KAFKA_DEFINE_H_
#define _KAFKA_DEFINE_H_

#include "CommonInclude.h"

//无打印
//#define NORMAL_STATE
//可打印性能信息
#define TEST_PERFORMANCE
//可打印提交信息
//#define TEST_POLL_STATE

#ifdef TEST_PERFORMANCE

typedef int					Int32;
typedef unsigned int		UInt32;
typedef long long			Int64;
typedef unsigned long long	UInt64;

#endif


namespace librdkafka
{

enum KafkaHandleStatType
{
    FAILED_TO_OPEN_CONFIG_FILE,
    UNKNOWN_CONFIG_KEY,
    INVALID_CONFIG_VALUE,
    FAILED_TO_CREATE_KAFKA_HANDLE,

    SUCCESS_TO_INIT_KAFKA,
};

enum InitKafkaProducerStatType
{
    FAILED_TO_OPEN_TOPIC_CONFIG_FILE,
    UNKNOWN_TOPIC_CONFIG_KEY,
    INVALID_TOPIC_CONFIG_VALUE,
    FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE,

    FAILED_TO_ACQUIRE_METADATA,

    SUCCESS_TO_INIT_PRODUCER,
};

enum SendMsgStatType
{
    UNKNOWN_TOPIC_OR_PARTITION,
    MSG_SIZE_TOO_LARGE,
    SUCCESS_TO_SEND_MSG_TO_KAFKA,
};

#ifdef TEST_PERFORMANCE

UInt64 getCurrentTime();
void print_state();

#endif

}// end of namespace librdkafka

#endif
