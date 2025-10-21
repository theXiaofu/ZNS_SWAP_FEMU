#ifndef __FEMU_ZFTL_H
#define __FEMU_ZFTL_H

#include "../nvme.h"

/*
 * zftl.h
 * -----
 * 简体中文说明：
 * 该头文件定义了 ZNS FTL (Flash Translation Layer) 相关的辅助宏和
 * 初始化接口。此处只添加注释以帮助理解代码结构与用途。
 *
 * 重要说明：仅包含轻量级调试/错误打印宏和 zftl_init() 原型，
 * 不应在此处实现复杂逻辑。
 */

#define INVALID_PPA     (~(0ULL))
#define INVALID_LPN     (~(0ULL))
#define UNMAPPED_PPA    (~(0ULL))

/*
 * zftl_init - 启动 FTL 后台线程并完成 FTL 子系统的初始化。
 * 参数：
 *   n - 指向控制器主结构 FemuCtrl 的指针，FTL 将在该控制器上下文内工作。
 * 返回：无（内部通过线程运行并使用控制器回调和队列通信）。
 */
void zftl_init(FemuCtrl *n);

#ifdef FEMU_DEBUG_ZFTL
#define ftl_debug(fmt, ...) \
    do { printf("[Misao] ZFTL-Dbg: " fmt, ## __VA_ARGS__); } while (0)
#else
#define ftl_debug(fmt, ...) \
    do { } while (0)
#endif

#define ftl_err(fmt, ...) \
    do { fprintf(stderr, "[Misao] ZFTL-Err: " fmt, ## __VA_ARGS__); } while (0)

#define ftl_log(fmt, ...) \
    do { printf("[Misao] ZFTL-Log: " fmt, ## __VA_ARGS__); } while (0)


/* FEMU assert() */
#ifdef FEMU_DEBUG_FTL
#define ftl_assert(expression) assert(expression)
#else
#define ftl_assert(expression)
#endif

#endif
