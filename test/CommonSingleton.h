#ifndef _COMMON_SINGLETON_H_
#define _COMMON_SINGLETON_H_

#include "Nocopyable.h"


template<typename T>
class Singleton : public Nocopyable
{
public:
    static T* Instance()
    {
        if(m_pInstance == 0)
        {
            m_pInstance = new T;
        }
        return m_pInstance;
    }

    static void Destroy()
    {
        delete m_pInstance;
        m_pInstance = 0;
    }

private:
    static T* m_pInstance;

    class DestroyHelper
    {
        ~DestroyHelper()
        {
            Singleton<T>::Destroy();
        }
    };
    static DestroyHelper m_Helper;
};

template<typename T>
T* Singleton<T>::m_pInstance = 0;

template<typename T>
typename Singleton<T>::DestroyHelper Singleton<T>::m_Helper;

#endif
