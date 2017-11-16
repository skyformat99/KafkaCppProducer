#ifndef _COMMON_HANDLE_H_
#define _COMMON_HANDLE_H_

#include "rdkafka.h"
#include "CommonInclude.h"
#include "KafkaConfig.h"


namespace librdkafka
{

class CommonHandle
{
protected:
    //注册回调函数
	virtual void _SetCallback()=0;

protected:
    //配置文件路径
	std::string m_ConfFilePath;

    //读取配置文件类
	KafkaConfig m_KafkaConfig;

    //属性集返回类型
	rd_kafka_conf_res_t	 m_Res;

    //错误信息数组
	char m_Errstr[512];
};

}// end of namespace librdkafka

#endif
