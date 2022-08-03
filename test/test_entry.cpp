#include "include/index.hpp"
#include "include/engine.hpp"

using Id = int64_t;
using Uid = UserString;
using Name = UserString;
using Salary = int64_t;

int main() {
    printf("sizeof(Entry<Id>) = %ld\n", sizeof(Entry<Id>));
    printf("sizeof(Entry<Uid>) = %ld\n", sizeof(Entry<Uid>));
    printf("sizeof(Entry<Name>) = %ld\n", sizeof(Entry<Name>));
    printf("sizeof(Entry<Salary>) = %ld\n", sizeof(Entry<Salary>));
    
    char *addr = (char *)4096;

    printf("addr = %p\n", addr);
    printf("addr + %d * Entry<Id> = %p\n", 1, reinterpret_cast<Entry<Id> *>(addr + sizeof(Entry<Id>)));
    printf("addr + %d * Entry<Id> = %p\n", 2, reinterpret_cast<Entry<Id> *>(addr + 2 * sizeof(Entry<Id>)));
    printf("addr + %d * Entry<Id> = %p\n", 3, reinterpret_cast<Entry<Id> *>(addr + 3 * sizeof(Entry<Id>)));
    printf("addr + %d * Entry<Uid> = %p\n", 1, reinterpret_cast<Entry<Uid> *>(addr + sizeof(Entry<Uid>)));
    printf("addr + %d * Entry<Uid> = %p\n", 2, reinterpret_cast<Entry<Uid> *>(addr + 2 * sizeof(Entry<Uid>)));
    printf("addr + %d * Entry<Uid> = %p\n", 3, reinterpret_cast<Entry<Uid> *>(addr + 3 * sizeof(Entry<Uid>)));
    printf("addr + %d * Entry<Name> = %p\n", 1, reinterpret_cast<Entry<Name> *>(addr + sizeof(Entry<Name>)));
    printf("addr + %d * Entry<Name> = %p\n", 2, reinterpret_cast<Entry<Name> *>(addr + 2 * sizeof(Entry<Name>)));
    printf("addr + %d * Entry<Name> = %p\n", 3, reinterpret_cast<Entry<Name> *>(addr + 3 * sizeof(Entry<Name>)));
    printf("addr + %d * Entry<Salary> = %p\n", 1, reinterpret_cast<Entry<Salary> *>(addr + sizeof(Entry<Salary>)));
    printf("addr + %d * Entry<Salary> = %p\n", 2, reinterpret_cast<Entry<Salary> *>(addr + 2 * sizeof(Entry<Salary>)));
    printf("addr + %d * Entry<Salary> = %p\n", 3, reinterpret_cast<Entry<Salary> *>(addr + 3 * sizeof(Entry<Salary>)));
}