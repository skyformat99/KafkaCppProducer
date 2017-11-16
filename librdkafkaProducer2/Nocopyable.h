#ifndef _NOCOPYABLE_H_
#define _NOCOPYABLE_H_

class Nocopyable
{
protected:
    Nocopyable() {}
    ~Nocopyable() {}

private:
    Nocopyable(const Nocopyable&);
    const Nocopyable& operator=(const Nocopyable&);
};

#endif
