
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <sys/epoll.h>

#define SERVER_PORT (7778)
#define EPOLL_MAX_NUM (2048)
#define BUFFER_MAX_LEN (4096)

char buffer[BUFFER_MAX_LEN];

void str_toupper(char *str)
{
    int i;
    for (i = 0; i < strlen(str); i++)
    {
        str[i] = toupper(str[i]);
    }
}

int main(int argc, char **argv)
{
    struct epoll_event events[5];
    /*epoll_create 在内核开辟一块空间，原来存放 epoll 中 fd 的数据结构（红黑树）*/
    int epfd = epoll_create(10);
    // ...

    // 模拟5个客户端
    for (int i = 0; i < 5; i++)
    {
        /*epoll 中 fd 的数据结构和 poll 的差不多，只是没有了 revents*/
        static struct epoll_event ev;
        memset(&client, 0, sizeof(client));
        addrlen = sizeof(client);
        ev.data.fd = accept(sockfd, (struct sockaddr *)&client, &addrlen);
        /*epoll_ctl 把每一个 socket 的 fd 数据结构放到 epoll_create 创建的内存空间中（加入到红黑树）*/
        epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev);
    }

    while (1)
    {
        puts("round again");
        /*epoll_wait 阻塞，只有当 epoll_create 中创建的内存空间中的 fd 有事件发生，才会把这些 fd 放在就绪列表中，返回就绪 fd 的个数*/
        nfds = epoll_wait(epfd, events, 5, 10000);

        /*遍历就绪列表，读取数据*/
        for (int i = 0; i < nfds; i++)
        {
            memset(buffer, 0, MAXBUF);
            read(events[i].data.fd, buffer, MAXBUF);
            puts(buffer);
        }
    }

    // // 创建一个 epoll句柄
    // int epfd = epoll_create(1000);

    // // 将 listen_fd 添加进 epoll 中
    // epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &listen_event);

    // while (1)
    // {
    //     // 阻塞等待 epoll 中 的fd 触发
    //     int active_cnt = epoll_wait(epfd, events, 1000, -1);

    //     for (i = 0; i < active_cnt; i++)
    //     {
    //         if (evnets[i].data.fd == listen_fd)
    //         {
    //             // accept. 并且将新accept 的fd 加进epoll中.
    //         }
    //         else if (events[i].events & EPOLLIN)
    //         {
    //             // 对此fd 进行读操作
    //         }
    //         else if (events[i].events & EPOLLOUT)
    //         {
    //             // 对此fd 进行写操作
    //         }
    //     }
    // }
}