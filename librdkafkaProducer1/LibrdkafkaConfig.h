#ifndef __LIBRDKAFKACONFIG_H__
#define __LIBRDKAFKACONFIG_H__


#include <map>
#include <string>

namespace librdkafka
{

class LibrdkafkaConfig
{
public:
	LibrdkafkaConfig();
	~LibrdkafkaConfig();

	bool ReadFromConfig(const std::string & configFilePath);

    const std::map<std::string, std::string> & GetConfigMap() const
    {   return m_ConfigMap;   }

private:
    std::map<std::string, std::string> m_ConfigMap;
};

}//end of namespace librdkafka

#endif
