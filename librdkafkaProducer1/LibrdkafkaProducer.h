#ifndef __LIBRDKAFKAPRODUCER_H__
#define __LIBRDKAFKAPRODUCER_H__

#include "rdkafka.h"
//#include "gameplatform_types.h"
#include "LibrdkafkaConfig.h"

#include <list>

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

//返回状态
enum InitKafkaStatType
{
	INIT_SUCCESS,

    FAILED_TO_READ_CONFIGFILE,
    UNKNOWN_CONFIG_NAME,
    INVALID_CONFIG_VALUE,

    FAILED_TO_READ_TOPIC_CONFIGFILE,
    UNKNOWN_TOPIC_CONFIG_NAME,
    INVALID_TOPIC_CONFIG_VALUE,

    FAILED_TO_CREATE_PRODUCE,
    FAILED_TO_CREATE_TOPIC,

    FAILED_TO_ACQUIRE_METADATA,
};

enum SendMsgStatType
{
    UNKNOWN_TOPIC_OR_PARTITION,
    MSG_SIZE_TOO_LARGE,
    SUCCESS_TO_SEND_MSG_TO_KAFKA,
};


class LibrdkafkaProducer
{
public:
	LibrdkafkaProducer(const std::string & confFilePath, const std::string & topicConfFilePath, const std::string & topic);
	~LibrdkafkaProducer();

	InitKafkaStatType InitProducer();

	SendMsgStatType SendMessageToKafka(const char * msg, int32_t msglen);
	SendMsgStatType SendMessageToKafka(const std::string & msg);

    //用来将SendMessageToKafka失败的msg重发至kafak
    SendMsgStatType OnTick();

    //将队列中剩余msg发到kafka
    void WaitMsgDeliver();

#ifdef TEST_PERFORMANCE
public:
    static int32_t GetCurrentTime();
    void Print_state();
#endif

private:
    //设置全局配置属性
	InitKafkaStatType _ConfPropertiesSet();
    //设置topic配置属性
	InitKafkaStatType _TopicConfPropertiesSet();
    //初始化元数据对象
	InitKafkaStatType _MetadataInit();
    //创建rdkafka对象
	InitKafkaStatType _RdkafkaObjectCreate();
    //注册回调函数
	void _CallbackSet();

    //kafka回调函数
	static void _Logger(const rd_kafka_t *m_Rk, int32_t level, const char *fac, const char *buf);
	static void _Msg_delivered(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

	//自定义分区
    const std::string _GetRandomKeyStr();
	static int32_t _Partitioner(const rd_kafka_topic_t *rkt, const void *key, size_t leylen, int32_t partition_cnt, void *rkt_opaque, void *msg_opaque);
	static uint32_t _DJBHash(const char *str, size_t len);

    //初始化错误信息数组
	void _MemsetErrstrZero();

    //回收资源
	void _Destroy();

private:
    //全局配置文件
	const std::string m_ConfFilePath;
    //topic配置文件
	const std::string m_TopicConfFilePath;
    //topic名称
	const std::string m_Topic;

    //读取配置文件类
	LibrdkafkaConfig m_LibrdkafkaConfig;

    //设置全局配置属性集
	rd_kafka_conf_t			*m_Conf;
    //设置topic配置属性集
	rd_kafka_topic_conf_t	*m_Topic_conf;
    //属性集返回类型
	rd_kafka_conf_res_t		m_Res;

    //属性结构体
	const struct rd_kafka_metadata *m_Metadata;

    //kafka对象
	rd_kafka_t			*m_Rk;
    //topic对象
	rd_kafka_topic_t	*m_Rkt;

    //错误信息数组
	char m_Errstr[512];

    //因外部队列达到界值而生产失败的msg
    std::list<std::string> m_FaildMsgs;
};

}// end of namespace librdkafka

#endif
