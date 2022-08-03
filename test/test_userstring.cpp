

#include "include/engine.hpp"
#include <unordered_map>

#define BUF_SIZE 255

uint64_t ushash(const UserString &us) {
    return std::hash<std::string>()(std::string(us.ptr, 128));
}


int main() {

    auto userstring_size = sizeof(UserString);

    printf("UserString size = %ld\n", userstring_size);

    char buf1[BUF_SIZE + 1];
    char buf2[BUF_SIZE + 1];

    memset(buf1, 'a', userstring_size);
    memset(buf2, 'a', userstring_size);
    memset(buf1 + userstring_size, 'x', BUF_SIZE - userstring_size);
    memset(buf2 + userstring_size, 'y', BUF_SIZE - userstring_size);
    buf1[BUF_SIZE] = 0;
    buf2[BUF_SIZE] = 0;
    printf("buf1: %s\n", buf1);
    printf("buf2: %s\n", buf2);


    const UserString *us1 = reinterpret_cast<const UserString *>(buf1);
    const UserString *us2 = reinterpret_cast<const UserString *>(buf2);

    printf("Origin Hash UserString1: %ld\n", std::hash<UserString>()(*us1));
    printf("Origin Hash UserString2: %ld\n", std::hash<UserString>()(*us2));

    printf("New Hash UserString1: %ld\n", ushash(*us1));
    printf("New Hash UserString2: %ld\n", ushash(*us2));

}