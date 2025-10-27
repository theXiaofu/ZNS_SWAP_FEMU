#include "./zns.h"

#define MIN_DISCARD_GRANULARITY     (4 * KiB)
#define NVME_DEFAULT_ZONE_SIZE      (128 * MiB)
#define NVME_DEFAULT_MAX_AZ_SIZE    (128 * KiB)

/*
 * zns.c - 简体中文概述
 * 该文件实现了 ZNS（Zoned Namespace）相关的核心逻辑：
 * - Zone 几何与描述符的初始化与校验
 * - Zone 状态机（open/close/finish/reset/offline）以及 AOR（Active/Open Resources）管理
 * - Zone IO（读/写/append）路径与 Zone 管理命令（SEND/RECV）的处理
 * - ZNS 设备参数与内部模拟结构（通道/平面/块/页）的初始化
 *
 * 注：本文件内仅添加注释以提高可读性，未修改任何功能性代码。
 */

static inline uint32_t zns_zone_idx(NvmeNamespace *ns, uint64_t slba)
{
    FemuCtrl *n = ns->ctrl;

    return (n->zone_size_log2 > 0 ? slba >> n->zone_size_log2 : slba / n->zone_size);
}

/*
 * zns_zone_idx
 * 将逻辑块地址 (slba) 转换为 zone 索引。
 * 优先使用位移运算（当 zone 大小为 2 的幂时），否则使用除法。
 */

static inline NvmeZone *zns_get_zone_by_slba(NvmeNamespace *ns, uint64_t slba)
{
    // FemuCtrl *n = ns->ctrl;
    // uint32_t zone_idx = zns_zone_idx(ns, slba);

    // assert(zone_idx < n->num_zones);
    // return &n->zone_array[zone_idx];
    // 1. 从SLBA计算出逻辑Zone的索引
    uint32_t logical_zone_idx = zns_zone_idx(ns, slba);
    assert(logical_zone_idx < n->num_zones);

    // 2. 使用映射表找到对应的物理Zone索引
    uint32_t physical_zone_idx = n->zns->logical_to_physical_zone_map[logical_zone_idx];
    assert(physical_zone_idx < n->num_zones);

    // 3. 返回物理Zone数组中对应的结构
    return &n->zone_array[physical_zone_idx];
}

/*
 * zns_get_zone_by_slba
 * 根据给定的 slba 获取对应的 NvmeZone 结构体指针（调用者需保证 slba 有效）。
 */

static int zns_init_zone_geometry(NvmeNamespace *ns, Error **errp)
{
    FemuCtrl *n = ns->ctrl;
    uint64_t zone_size, zone_cap;
    uint32_t lbasz = 1 << zns_ns_lbads(ns);

    if (n->zone_size_bs) {
        zone_size = n->zone_size_bs;
    } else {
        zone_size = NVME_DEFAULT_ZONE_SIZE;
    }

    if (n->zone_cap_bs) {
        zone_cap = n->zone_cap_bs;
    } else {
        zone_cap = zone_size;
    }

    if (zone_cap > zone_size) {
        femu_err("zone capacity %luB > zone size %luB", zone_cap, zone_size);
        return -1;
    }
    if (zone_size < lbasz) {
        femu_err("zone size %luB too small, must >= %uB", zone_size, lbasz);
        return -1;
    }
    if (zone_cap < lbasz) {
        femu_err("zone capacity %luB too small, must >= %uB", zone_cap, lbasz);
        return -1;
    }

    n->zone_size = zone_size / lbasz;
    n->zone_capacity = zone_cap / lbasz;
    n->num_zones = ns->size / lbasz / n->zone_size;

    if (n->max_open_zones > n->num_zones) {
        femu_err("max_open_zones value %u exceeds the number of zones %u",
                 n->max_open_zones, n->num_zones);
        return -1;
    }
    if (n->max_active_zones > n->num_zones) {
        femu_err("max_active_zones value %u exceeds the number of zones %u",
                 n->max_active_zones, n->num_zones);
        return -1;
    }

    if (n->zd_extension_size) {
        if (n->zd_extension_size & 0x3f) {
            femu_err("zone descriptor extension size must be multiples of 64B");
            return -1;
        }
        if ((n->zd_extension_size >> 6) > 0xff) {
            femu_err("zone descriptor extension size is too large");
            return -1;
        }
    }

    return 0;
}

/*
 * zns_init_zone_geometry
 * 初始化与 zone 大小/容量/数量相关的几何参数，并进行输入值校验。
 * 例如：zone cap 必须不大于 zone size，zd_extension 必须是 64B 的倍数等。
 */

static void zns_init_zoned_state(NvmeNamespace *ns)
{
    FemuCtrl *n = ns->ctrl;
    uint64_t start = 0, zone_size = n->zone_size;
    uint64_t capacity = n->num_zones * zone_size;
    NvmeZone *zone;
    int i;

    //分配物理zone数组
    n->zone_array = g_new0(NvmeZone, n->num_zones);
    if (n->zd_extension_size) {
        n->zd_extensions = g_malloc0(n->zd_extension_size * n->num_zones);
    }

    /*
     * 新增：分配并初始化逻辑到物理Zone的映射表。
     * 初始状态下，逻辑Zone i 指向 物理Zone i。
     * Added: Allocate and initialize the logical-to-physical zone map.
     * Initially, logical zone i maps to physical zone i.
    */
    n->zns->logical_to_physical_zone_map = g_new0(uint32_t, n->num_zones);
    for (i = 0; i < n->num_zones; i++) {
        n->zns->logical_to_physical_zone_map[i] = i;
    }

    QTAILQ_INIT(&n->exp_open_zones);
    QTAILQ_INIT(&n->imp_open_zones);
    QTAILQ_INIT(&n->closed_zones);
    QTAILQ_INIT(&n->full_zones);

    zone = n->zone_array;
    for (i = 0; i < n->num_zones; i++, zone++) {
        if (start + zone_size > capacity) {
            zone_size = capacity - start;
        }
        zone->d.zt = NVME_ZONE_TYPE_SEQ_WRITE;
        zns_set_zone_state(zone, NVME_ZONE_STATE_EMPTY);
        zone->d.za = 0;
        zone->d.zcap = n->zone_capacity;
        zone->d.zslba = start;
        zone->d.wp = start;
        zone->w_ptr = start;
        start += zone_size;
    }

    n->zone_size_log2 = 0;
    if (is_power_of_2(n->zone_size)) {
        n->zone_size_log2 = 63 - clz64(n->zone_size);
    }
}

/*
 * zns_init_zoned_state
 * 为命名空间分配 zone 数组并初始化每个 zone 的描述符（类型、容量、
 * 起始 LBA、写指针等）。如果存在 zone descriptor extension，则为
 * 每个 zone 分配扩展区。
 */

static void zns_init_zone_identify(FemuCtrl *n, NvmeNamespace *ns, int lba_index)
{
    NvmeIdNsZoned *id_ns_z;

    zns_init_zoned_state(ns);

    id_ns_z = g_malloc0(sizeof(NvmeIdNsZoned));

    /* MAR/MOR are zeroes-based, 0xffffffff means no limit */
    id_ns_z->mar = cpu_to_le32(n->max_active_zones - 1);
    id_ns_z->mor = cpu_to_le32(n->max_open_zones - 1);
    id_ns_z->zoc = 0;
    id_ns_z->ozcs = n->cross_zone_read ? 0x01 : 0x00;

    id_ns_z->lbafe[lba_index].zsze = cpu_to_le64(n->zone_size);
    id_ns_z->lbafe[lba_index].zdes = n->zd_extension_size >> 6; /* Units of 64B */

    n->csi = NVME_CSI_ZONED;
    ns->id_ns.nsze = cpu_to_le64(n->num_zones * n->zone_size);
    ns->id_ns.ncap = ns->id_ns.nsze;
    ns->id_ns.nuse = ns->id_ns.ncap;

    ns->id_ns.noiob = 1;
    /* NvmeIdNs */
    /*
     * The device uses the BDRV_BLOCK_ZERO flag to determine the "deallocated"
     * status of logical blocks. Since the spec defines that logical blocks
     * SHALL be deallocated when then zone is in the Empty or Offline states,
     * we can only support DULBE if the zone size is a multiple of the
     * calculated NPDG.
     */
    if (n->zone_size % (ns->id_ns.npdg + 1)) {
        femu_err("the zone size (%"PRIu64" blocks) is not a multiple of the"
                 "calculated deallocation granularity (%"PRIu16" blocks); DULBE"
                 "support disabled", n->zone_size, ns->id_ns.npdg + 1);
        ns->id_ns.nsfeat &= ~0x4;
    }

    n->id_ns_zoned = id_ns_z;
}

/*
 * zns_init_zone_identify
 * 构建 Identify Namespace 的 zoned 扩展结构，设置 MAR/MOR、ZOC/ OZCS 等
 * 字段以及 LBA 格式中 zone 大小和 zd_extension 的单元信息。
 */

static void zns_clear_zone(NvmeNamespace *ns, NvmeZone *zone)
{
    FemuCtrl *n = ns->ctrl;
    uint8_t state;

    zone->w_ptr = zone->d.wp;
    state = zns_get_zone_state(zone);
    if (zone->d.wp != zone->d.zslba || (zone->d.za & NVME_ZA_ZD_EXT_VALID)) {
        if (state != NVME_ZONE_STATE_CLOSED) {
            zns_set_zone_state(zone, NVME_ZONE_STATE_CLOSED);
        }
        zns_aor_inc_active(ns);
        QTAILQ_INSERT_HEAD(&n->closed_zones, zone, entry);
    } else {
        zns_set_zone_state(zone, NVME_ZONE_STATE_EMPTY);
    }
}

/*
 * zns_clear_zone
 * 将单个 zone 恢复到 Empty 或 Closed 状态（依据描述符中 wp 与 zslba 的值），
 * 并在必要时更新 AOR 计数器与队列结构。
 */

static void zns_zoned_ns_shutdown(NvmeNamespace *ns)
{
    FemuCtrl *n = ns->ctrl;
    NvmeZone *zone, *next;

    QTAILQ_FOREACH_SAFE(zone, &n->closed_zones, entry, next) {
        QTAILQ_REMOVE(&n->closed_zones, zone, entry);
        zns_aor_dec_active(ns);
        zns_clear_zone(ns, zone);
    }
    QTAILQ_FOREACH_SAFE(zone, &n->imp_open_zones, entry, next) {
        QTAILQ_REMOVE(&n->imp_open_zones, zone, entry);
        zns_aor_dec_open(ns);
        zns_aor_dec_active(ns);
        zns_clear_zone(ns, zone);
    }
    QTAILQ_FOREACH_SAFE(zone, &n->exp_open_zones, entry, next) {
        QTAILQ_REMOVE(&n->exp_open_zones, zone, entry);
        zns_aor_dec_open(ns);
        zns_aor_dec_active(ns);
        zns_clear_zone(ns, zone);
    }

    assert(n->nr_open_zones == 0);
}

void zns_ns_shutdown(NvmeNamespace *ns)
{
    FemuCtrl *n = ns->ctrl;
    if (n->zoned) {
        zns_zoned_ns_shutdown(ns);
    }
}

void zns_ns_cleanup(NvmeNamespace *ns)
{
    FemuCtrl *n = ns->ctrl;
    if (n->zoned) {
        g_free(n->id_ns_zoned);
        g_free(n->zone_array);
        g_free(n->zd_extensions);
    }
}

static void zns_assign_zone_state(NvmeNamespace *ns, NvmeZone *zone, NvmeZoneState state)
{
    FemuCtrl *n = ns->ctrl;

    if (QTAILQ_IN_USE(zone, entry)) {
        switch (zns_get_zone_state(zone)) {
        case NVME_ZONE_STATE_EXPLICITLY_OPEN:
            QTAILQ_REMOVE(&n->exp_open_zones, zone, entry);
            break;
        case NVME_ZONE_STATE_IMPLICITLY_OPEN:
            QTAILQ_REMOVE(&n->imp_open_zones, zone, entry);
            break;
        case NVME_ZONE_STATE_CLOSED:
            QTAILQ_REMOVE(&n->closed_zones, zone, entry);
            break;
        case NVME_ZONE_STATE_FULL:
            QTAILQ_REMOVE(&n->full_zones, zone, entry);
        default:
            ;
        }
    }

    zns_set_zone_state(zone, state);

    switch (state) {
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        QTAILQ_INSERT_TAIL(&n->exp_open_zones, zone, entry);
        break;
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        QTAILQ_INSERT_TAIL(&n->imp_open_zones, zone, entry);
        break;
    case NVME_ZONE_STATE_CLOSED:
        QTAILQ_INSERT_TAIL(&n->closed_zones, zone, entry);
        break;
    case NVME_ZONE_STATE_FULL:
        QTAILQ_INSERT_TAIL(&n->full_zones, zone, entry);
    case NVME_ZONE_STATE_READ_ONLY:
        break;
    default:
        zone->d.za = 0;
    }
}

/*
 * zns_assign_zone_state
 * 将 zone 从当前状态迁移到目标状态：
 * - 从原链表中移除（若在链表中）
 * - 设置 zs 字段并插入到目标链表
 * - 在需要时维护 nr_open_zones / nr_active_zones 等计数器
 */

/*
 * Check if we can open a zone without exceeding open/active limits.
 * AOR stands for "Active and Open Resources" (see TP 4053 section 2.5).
 */
static int zns_aor_check(NvmeNamespace *ns, uint32_t act, uint32_t opn)
{
    FemuCtrl *n = ns->ctrl;
    if (n->max_active_zones != 0 &&
        n->nr_active_zones + act > n->max_active_zones) {
        return NVME_ZONE_TOO_MANY_ACTIVE | NVME_DNR;
    }
    if (n->max_open_zones != 0 &&
        n->nr_open_zones + opn > n->max_open_zones) {
        return NVME_ZONE_TOO_MANY_OPEN | NVME_DNR;
    }

    return NVME_SUCCESS;
}

/*
 * zns_aor_check
 * 检查执行某个操作后是否会超出最大允许的 active/open zones，
 * 用于拒绝会导致资源超额的命令。
 */

static uint16_t zns_check_zone_state_for_write(NvmeZone *zone)
{
    uint16_t status;

    switch (zns_get_zone_state(zone)) {
    case NVME_ZONE_STATE_EMPTY:
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
    case NVME_ZONE_STATE_CLOSED:
        status = NVME_SUCCESS;
        break;
    case NVME_ZONE_STATE_FULL:
        status = NVME_ZONE_FULL;
        break;
    case NVME_ZONE_STATE_OFFLINE:
        status = NVME_ZONE_OFFLINE;
        break;
    case NVME_ZONE_STATE_READ_ONLY:
        status = NVME_ZONE_READ_ONLY;
        break;
    default:
        assert(false);
    }

    return status;
}

/*
 * zns_check_zone_state_for_write
 * 基于 zone 的当前状态判断是否允许写入，并返回对应的 NVMe 错误码。
 */

static uint16_t zns_check_zone_write(FemuCtrl *n, NvmeNamespace *ns,
                                     NvmeZone *zone, uint64_t slba,
                                     uint32_t nlb, bool append)
{
    uint16_t status;

    if (unlikely((slba + nlb) > zns_zone_wr_boundary(zone))) {
        status = NVME_ZONE_BOUNDARY_ERROR;
    } else {
        status = zns_check_zone_state_for_write(zone);
    }

    if (status != NVME_SUCCESS) {
    } else {
        assert(zns_wp_is_valid(zone));
        if (append) {
            if (unlikely(slba != zone->d.zslba)) {
                status = NVME_INVALID_FIELD;
            }
            if (zns_l2b(ns, nlb) > (n->page_size << n->zasl)) {
                status = NVME_INVALID_FIELD;
            }
        } else if (unlikely(slba != zone->w_ptr)) {
            status = NVME_ZONE_INVALID_WRITE;
        }
    }

    return status;
}

/*
 * zns_check_zone_write
 * 进行写请求级别的校验：边界检查、状态检查、append 模式下的特定
 * 约束（例如写入长度不超过 page size * 2^zasl）以及写指针一致性。
 */

static uint16_t zns_check_zone_state_for_read(NvmeZone *zone)
{
    uint16_t status;

    switch (zns_get_zone_state(zone)) {
    case NVME_ZONE_STATE_EMPTY:
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
    case NVME_ZONE_STATE_FULL:
    case NVME_ZONE_STATE_CLOSED:
    case NVME_ZONE_STATE_READ_ONLY:
        status = NVME_SUCCESS;
        break;
    case NVME_ZONE_STATE_OFFLINE:
        status = NVME_ZONE_OFFLINE;
        break;
    default:
        assert(false);
    }

    return status;
}

static uint16_t zns_check_zone_read(NvmeNamespace *ns, uint64_t slba, uint32_t nlb)
{
    FemuCtrl *n = ns->ctrl;
    NvmeZone *zone = zns_get_zone_by_slba(ns, slba);
    uint64_t bndry = zns_zone_rd_boundary(ns, zone);
    uint64_t end = slba + nlb;
    uint16_t status;

    status = zns_check_zone_state_for_read(zone);
    if (status != NVME_SUCCESS) {
        ;
    } else if (unlikely(end > bndry)) {
        if (!n->cross_zone_read) {
            status = NVME_ZONE_BOUNDARY_ERROR;
        } else {
            /*
             * Read across zone boundary - check that all subsequent
             * zones that are being read have an appropriate state.
             */
            do {
                zone++;
                status = zns_check_zone_state_for_read(zone);
                if (status != NVME_SUCCESS) {
                    break;
                }
            } while (end > zns_zone_rd_boundary(ns, zone));
        }
    }

    return status;
}

/*
 * zns_check_zone_read
 * 校验读请求是否合法，若跨 zone 读则依据 cross_zone_read 标志决定是否允许，
 * 并对跨越的每个 zone 检查其读权限。
 */

static void zns_auto_transition_zone(NvmeNamespace *ns)
{
    FemuCtrl *n = ns->ctrl;
    NvmeZone *zone;

    if (n->max_open_zones &&
        n->nr_open_zones == n->max_open_zones) {
        zone = QTAILQ_FIRST(&n->imp_open_zones);
        if (zone) {
             /* Automatically close this implicitly open zone */
            QTAILQ_REMOVE(&n->imp_open_zones, zone, entry);
            zns_aor_dec_open(ns);
            zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_CLOSED);
        }
    }
}

/*
 * zns_auto_transition_zone
 * 当隐式打开区数量达到控制器限制时，自动关闭最旧的隐式打开区以
 * 释放资源（实现 AOR 策略的一部分）。
 */

static uint16_t zns_auto_open_zone(NvmeNamespace *ns, NvmeZone *zone)
{
    uint16_t status = NVME_SUCCESS;
    uint8_t zs = zns_get_zone_state(zone);

    if (zs == NVME_ZONE_STATE_EMPTY) {
        zns_auto_transition_zone(ns);
        status = zns_aor_check(ns, 1, 1);
    } else if (zs == NVME_ZONE_STATE_CLOSED) {
        zns_auto_transition_zone(ns);
        status = zns_aor_check(ns, 0, 1);
    }

    return status;
}

/*
 * zns_auto_open_zone
 * 在需要隐式打开 zone 的场景（如 append）中调用，执行 AOR 检查并
 * 将 zone 状态从 Empty/Closed 转换为 Implicitly Open（或进一步转换）。
 */

static void zns_finalize_zoned_write(NvmeNamespace *ns, NvmeRequest *req, bool failed)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)&req->cmd;
    NvmeZone *zone;
    NvmeZonedResult *res = (NvmeZonedResult *)&req->cqe;
    uint64_t slba;
    uint32_t nlb;

    slba = le64_to_cpu(rw->slba);
    nlb = le16_to_cpu(rw->nlb) + 1;
    zone = zns_get_zone_by_slba(ns, slba);

    zone->d.wp += nlb;

    if (failed) {
        res->slba = 0;
    }

    if (zone->d.wp == zns_zone_wr_boundary(zone)) {
        switch (zns_get_zone_state(zone)) {
        case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        case NVME_ZONE_STATE_EXPLICITLY_OPEN:
            zns_aor_dec_open(ns);
            /* fall through */
        case NVME_ZONE_STATE_CLOSED:
            zns_aor_dec_active(ns);
            /* fall through */
        case NVME_ZONE_STATE_EMPTY:
            zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_FULL);
            /* fall through */
        case NVME_ZONE_STATE_FULL:
            break;
        default:
            assert(false);
        }
    }
}

/*
 * zns_finalize_zoned_write
 * 写请求完成后的收尾处理：更新 zone 的描述符 wp 字段，根据是否写满
 * 执行状态转换并维护 AOR 计数器。若写失败，会在返回的 cqe 中写入 0。
 */

// Add some function
// --------------------------------

static inline uint64_t zone_slba(FemuCtrl *n, uint32_t zone_idx)
{
    return (zone_idx) * n->zone_size;
}

static uint64_t zns_advance_zone_wp(NvmeNamespace *ns, NvmeZone *zone, uint32_t nlb)
{
    uint64_t result = zone->w_ptr;
    uint8_t zs;

    zone->w_ptr += nlb;

    if (zone->w_ptr < zns_zone_wr_boundary(zone)) {
        zs = zns_get_zone_state(zone);
        switch (zs) {
        case NVME_ZONE_STATE_EMPTY:
            zns_aor_inc_active(ns);
            /* fall through */
        case NVME_ZONE_STATE_CLOSED:
            zns_aor_inc_open(ns);
            zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_IMPLICITLY_OPEN);
        }
    }

    return result;
}

/*
 * zns_advance_zone_wp
 * 将 zone 的 w_ptr 推进 nlb 个 LBA，并在第一次写入该 zone 时更新
 * active/open 计数和状态（可能从 Empty/Cosed -> Implicitly Open）。
 */

struct zns_zone_reset_ctx {
    NvmeRequest *req;
    NvmeZone    *zone;
};

static void zns_aio_zone_reset_cb(NvmeRequest *req, NvmeZone *zone)
{
    NvmeNamespace *ns = req->ns;
    /*
     * 新增：每次物理Zone被重置时，增加其重置计数器。
     * Added: Increment the reset counter each time a physical zone is reset.
    */
    zone->reset_count++;
    /* FIXME, We always assume reset SUCCESS */
    switch (zns_get_zone_state(zone)) {
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        /* fall through */
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        zns_aor_dec_open(ns);
        /* fall through */
    case NVME_ZONE_STATE_CLOSED:
        zns_aor_dec_active(ns);
        /* fall through */
    case NVME_ZONE_STATE_FULL:
        zone->w_ptr = zone->d.zslba;
        zone->d.wp = zone->w_ptr;
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_EMPTY);
    default:
        break;
    }

#if 0
    FemuCtrl *n = ns->ctrl;
    int ch, lun;
    struct zns_ssd *zns = n->zns;
    uint64_t num_ch = zns->num_ch;
    uint64_t num_lun = zns->num_lun;

    struct ppa ppa;
    for (ch = 0; ch < num_ch; ch++) {
        for (lun = 0; lun < num_lun; lun++) {
            ppa.g.ch = ch;
            ppa.g.fc = lun;
            ppa.g.blk = zns_zone_idx(ns, zone->d.zslba);
            //FIXME: no erase
        }
    }
#endif
}

/*
 * zns_aio_zone_reset_cb
 * 异步 reset 操作的回调实现：将 zone 写指针回退并把 zone 标记为 Empty，
 * 同时调整 AOR 计数器。该函数内现在假定 erase 一定成功。
 */

typedef uint16_t (*op_handler_t)(NvmeNamespace *, NvmeZone *, NvmeZoneState,
                                 NvmeRequest *);

enum NvmeZoneProcessingMask {
    NVME_PROC_CURRENT_ZONE    = 0,
    NVME_PROC_OPENED_ZONES    = 1 << 0,
    NVME_PROC_CLOSED_ZONES    = 1 << 1,
    NVME_PROC_READ_ONLY_ZONES = 1 << 2,
    NVME_PROC_FULL_ZONES      = 1 << 3,
};

static uint16_t zns_open_zone(NvmeNamespace *ns, NvmeZone *zone,
                              NvmeZoneState state, NvmeRequest *req)
{
    uint16_t status;

    switch (state) {
    case NVME_ZONE_STATE_EMPTY:
        status = zns_aor_check(ns, 1, 0);
        if (status != NVME_SUCCESS) {
            return status;
        }
        zns_aor_inc_active(ns);
        /* fall through */
    case NVME_ZONE_STATE_CLOSED:
        status = zns_aor_check(ns, 0, 1);
        if (status != NVME_SUCCESS) {
            if (state == NVME_ZONE_STATE_EMPTY) {
                zns_aor_dec_active(ns);
            }
            return status;
        }
        zns_aor_inc_open(ns);
        /* fall through */
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_EXPLICITLY_OPEN);
        /* fall through */
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        return NVME_SUCCESS;
    default:
        return NVME_ZONE_INVAL_TRANSITION;
    }
}

static uint16_t zns_close_zone(NvmeNamespace *ns, NvmeZone *zone,
                               NvmeZoneState state, NvmeRequest *req)
{
    switch (state) {
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        /* fall through */
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        zns_aor_dec_open(ns);
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_CLOSED);
        /* fall through */
    case NVME_ZONE_STATE_CLOSED:
        return NVME_SUCCESS;
    default:
        return NVME_ZONE_INVAL_TRANSITION;
    }
}

static uint16_t zns_finish_zone(NvmeNamespace *ns, NvmeZone *zone,
                                NvmeZoneState state, NvmeRequest *req)
{
    switch (state) {
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        /* fall through */
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
        zns_aor_dec_open(ns);
        /* fall through */
    case NVME_ZONE_STATE_CLOSED:
        zns_aor_dec_active(ns);
        /* fall through */
    case NVME_ZONE_STATE_EMPTY:
        zone->w_ptr = zns_zone_wr_boundary(zone);
        zone->d.wp = zone->w_ptr;
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_FULL);
        /* fall through */
    case NVME_ZONE_STATE_FULL:
        return NVME_SUCCESS;
    default:
        return NVME_ZONE_INVAL_TRANSITION;
    }
}

static uint16_t zns_reset_zone(NvmeNamespace *ns, NvmeZone *zone,
                               NvmeZoneState state, NvmeRequest *req)
{
    switch (state) {
    case NVME_ZONE_STATE_EMPTY:
        return NVME_SUCCESS;
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
    case NVME_ZONE_STATE_CLOSED:
    case NVME_ZONE_STATE_FULL:
        break;
    default:
        return NVME_ZONE_INVAL_TRANSITION;
    }

    zns_aio_zone_reset_cb(req, zone);

    return NVME_SUCCESS;
}

static uint16_t zns_offline_zone(NvmeNamespace *ns, NvmeZone *zone,
                                 NvmeZoneState state, NvmeRequest *req)
{
    switch (state) {
    case NVME_ZONE_STATE_READ_ONLY:
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_OFFLINE);
        /* fall through */
    case NVME_ZONE_STATE_OFFLINE:
        return NVME_SUCCESS;
    default:
        return NVME_ZONE_INVAL_TRANSITION;
    }
}

static uint16_t zns_set_zd_ext(NvmeNamespace *ns, NvmeZone *zone)
{
    uint16_t status;
    uint8_t state = zns_get_zone_state(zone);

    if (state == NVME_ZONE_STATE_EMPTY) {
        status = zns_aor_check(ns, 1, 0);
        if (status != NVME_SUCCESS) {
            return status;
        }
        zns_aor_inc_active(ns);
        zone->d.za |= NVME_ZA_ZD_EXT_VALID;
        zns_assign_zone_state(ns, zone, NVME_ZONE_STATE_CLOSED);
        return NVME_SUCCESS;
    }

    return NVME_ZONE_INVAL_TRANSITION;
}

static uint16_t zns_bulk_proc_zone(NvmeNamespace *ns, NvmeZone *zone,
                                   enum NvmeZoneProcessingMask proc_mask,
                                   op_handler_t op_hndlr, NvmeRequest *req)
{
    uint16_t status = NVME_SUCCESS;
    NvmeZoneState zs = zns_get_zone_state(zone);
    bool proc_zone;

    switch (zs) {
    case NVME_ZONE_STATE_IMPLICITLY_OPEN:
    case NVME_ZONE_STATE_EXPLICITLY_OPEN:
        proc_zone = proc_mask & NVME_PROC_OPENED_ZONES;
        break;
    case NVME_ZONE_STATE_CLOSED:
        proc_zone = proc_mask & NVME_PROC_CLOSED_ZONES;
        break;
    case NVME_ZONE_STATE_READ_ONLY:
        proc_zone = proc_mask & NVME_PROC_READ_ONLY_ZONES;
        break;
    case NVME_ZONE_STATE_FULL:
        proc_zone = proc_mask & NVME_PROC_FULL_ZONES;
        break;
    default:
        proc_zone = false;
    }

    if (proc_zone) {
        status = op_hndlr(ns, zone, zs, req);
    }

    return status;
}

static uint16_t zns_do_zone_op(NvmeNamespace *ns, NvmeZone *zone,
                               enum NvmeZoneProcessingMask proc_mask,
                               op_handler_t op_hndlr, NvmeRequest *req)
{
    FemuCtrl *n = ns->ctrl;
    NvmeZone *next;
    uint16_t status = NVME_SUCCESS;
    int i;

    if (!proc_mask) {
        status = op_hndlr(ns, zone, zns_get_zone_state(zone), req);
    } else {
        if (proc_mask & NVME_PROC_CLOSED_ZONES) {
            QTAILQ_FOREACH_SAFE(zone, &n->closed_zones, entry, next) {
                status = zns_bulk_proc_zone(ns, zone, proc_mask, op_hndlr, req);
                if (status && status != NVME_NO_COMPLETE) {
                    goto out;
                }
            }
        }
        if (proc_mask & NVME_PROC_OPENED_ZONES) {
            QTAILQ_FOREACH_SAFE(zone, &n->imp_open_zones, entry, next) {
                status = zns_bulk_proc_zone(ns, zone, proc_mask, op_hndlr,
                                             req);
                if (status && status != NVME_NO_COMPLETE) {
                    goto out;
                }
            }

            QTAILQ_FOREACH_SAFE(zone, &n->exp_open_zones, entry, next) {
                status = zns_bulk_proc_zone(ns, zone, proc_mask, op_hndlr,
                                             req);
                if (status && status != NVME_NO_COMPLETE) {
                    goto out;
                }
            }
        }
        if (proc_mask & NVME_PROC_FULL_ZONES) {
            QTAILQ_FOREACH_SAFE(zone, &n->full_zones, entry, next) {
                status = zns_bulk_proc_zone(ns, zone, proc_mask, op_hndlr, req);
                if (status && status != NVME_NO_COMPLETE) {
                    goto out;
                }
            }
        }

        if (proc_mask & NVME_PROC_READ_ONLY_ZONES) {
            for (i = 0; i < n->num_zones; i++, zone++) {
                status = zns_bulk_proc_zone(ns, zone, proc_mask, op_hndlr,
                                             req);
                if (status && status != NVME_NO_COMPLETE) {
                    goto out;
                }
            }
        }
    }

out:
    return status;
}

static uint16_t zns_get_mgmt_zone_slba_idx(FemuCtrl *n, NvmeCmd *c,
                                           uint64_t *slba, uint32_t *zone_idx)
{
    NvmeNamespace *ns = &n->namespaces[0];
    uint32_t dw10 = le32_to_cpu(c->cdw10);
    uint32_t dw11 = le32_to_cpu(c->cdw11);

    if (!n->zoned) {
        return NVME_INVALID_OPCODE | NVME_DNR;
    }

    *slba = ((uint64_t)dw11) << 32 | dw10;
    if (unlikely(*slba >= ns->id_ns.nsze)) {
        *slba = 0;
        return NVME_LBA_RANGE | NVME_DNR;
    }

    *zone_idx = zns_zone_idx(ns, *slba);
    assert(*zone_idx < n->num_zones);

    return NVME_SUCCESS;
}

static inline uint16_t zns_check_bounds(NvmeNamespace *ns, uint64_t slba,
                                        uint32_t nlb)
{
    uint64_t nsze = le64_to_cpu(ns->id_ns.nsze);

    if (unlikely(UINT64_MAX - slba < nlb || slba + nlb > nsze)) {
        return NVME_LBA_RANGE | NVME_DNR;
    }

    return NVME_SUCCESS;
}

static uint16_t zns_check_dulbe(NvmeNamespace *ns, uint64_t slba, uint32_t nlb)
{
    return NVME_SUCCESS;
}

static uint16_t zns_map_dptr(FemuCtrl *n, size_t len, NvmeRequest *req)
{
    uint64_t prp1, prp2;

    switch (req->cmd.psdt) {
    case NVME_PSDT_PRP:
        prp1 = le64_to_cpu(req->cmd.dptr.prp1);
        prp2 = le64_to_cpu(req->cmd.dptr.prp2);

        return nvme_map_prp(&req->qsg, &req->iov, prp1, prp2, len, n);
    default:
        return NVME_INVALID_FIELD;
    }
}

/*
 * zns_map_dptr
 * 将请求中的 PRP（或其他 PSDT）映射到 req->iov，准备进行后端 IO。
 * 目前仅支持 PRP 模式。
 */

/*Misao: backend read/write without latency emulation*/
static uint16_t zns_nvme_rw(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req,bool append)
{
    NvmeRwCmd *rw = (NvmeRwCmd *)&req->cmd; 
    uint64_t slba = le64_to_cpu(rw->slba);
    uint32_t nlb = (uint32_t)le16_to_cpu(rw->nlb) + 1;
    uint64_t data_size = zns_l2b(ns, nlb);
    uint64_t data_offset;
    uint16_t status;

    NvmeZone *zone;
    NvmeZonedResult *res = (NvmeZonedResult *)&req->cqe;
    assert(n->zoned);
    // Fix zone append not working as expected
    req->is_write = ((rw->opcode == NVME_CMD_WRITE) || (rw->opcode == NVME_CMD_ZONE_APPEND)) ? 1 : 0;

    status = nvme_check_mdts(n, data_size);
    if (status) {
        goto err;
    }

    status = zns_check_bounds(ns, slba, nlb);
    if (status) {
        goto err;
    }

    if(req->is_write)
    {
        zone = zns_get_zone_by_slba(ns, slba);
        status = zns_check_zone_write(n, ns, zone, slba, nlb, append);
        if (status) {
            femu_err("Misao check zone write failed with status (%u)\n",status);
            goto err;
        }
        if(append)
        {
             status = zns_auto_open_zone(ns, zone);
             if(status)
             {
                goto err;
             }
             slba = zone->w_ptr;
        }
        res->slba = zns_advance_zone_wp(ns, zone, nlb);
    }
    else
    {
        status = zns_check_zone_read(ns, slba, nlb);
        if (status) {
            goto err;
        }

        /* Misao
           Deallocated or Unwritten Logical Block Error (DULBE) is an option on
           NVMe drives that allows a storage array to deallocate blocks that are
           part of a volume. Deallocating blocks on a drive can greatly reduce
           the time it takes to initialize volumes. In addition, hosts can
           deallocate logical blocks in the volume using the NVMe Dataset
           Management command.
        */
        if (NVME_ERR_REC_DULBE(n->features.err_rec)) { status =
            zns_check_dulbe(ns, slba, nlb); if (status) { goto err; } } }

    data_offset = zns_l2b(ns, slba);
    status = zns_map_dptr(n, data_size, req);
    if (status) {
        goto err;
    }

    req->slba = slba;
    req->status = NVME_SUCCESS;
    req->nlb = nlb;

    backend_rw(n->mbe, &req->qsg, &data_offset, req->is_write);

    if(req->is_write)
    {
        zns_finalize_zoned_write(ns, req, false);
    }

    n->zns->active_zone = zns_zone_idx(ns,slba);
    return NVME_SUCCESS;
err:
    return status | NVME_DNR;
}

/*
 * zns_nvme_rw
 * NVMe IO（Read/Write/Zone Append）的统一入口：
 * - 校验 MDTS/边界/zone 状态
 * - 对 append 做自动打开与返回旧写地址的处理
 * - 将主机内存映射到 request 并调用 backend_rw 执行实际数据传输
 * - 写操作完成后调用 zns_finalize_zoned_write 做收尾工作
 */

static uint16_t zns_zone_mgmt_send(FemuCtrl *n, NvmeRequest *req)
{
    NvmeCmd *cmd = (NvmeCmd *)&req->cmd;
    NvmeNamespace *ns = req->ns;
    uint64_t prp1 = le64_to_cpu(cmd->dptr.prp1);
    uint64_t prp2 = le64_to_cpu(cmd->dptr.prp2);
    NvmeZone *zone;
    uintptr_t *resets;
    uint8_t *zd_ext;
    uint32_t dw13 = le32_to_cpu(cmd->cdw13);
    uint64_t slba = 0;
    uint32_t zone_idx = 0;
    uint16_t status;
    uint8_t action;
    bool all;
    enum NvmeZoneProcessingMask proc_mask = NVME_PROC_CURRENT_ZONE;

    action = dw13 & 0xff;
    all = dw13 & 0x100;

    req->status = NVME_SUCCESS;

    if (!all) {
        status = zns_get_mgmt_zone_slba_idx(n, cmd, &slba, &zone_idx);
        if (status) {
            return status;
        }
    }

    zone = &n->zone_array[zone_idx];
    if (slba != zone->d.zslba) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }

    switch (action) {
    case NVME_ZONE_ACTION_OPEN:
        if (all) {
            proc_mask = NVME_PROC_CLOSED_ZONES;
        }
        status = zns_do_zone_op(ns, zone, proc_mask, zns_open_zone, req);
        break;
    case NVME_ZONE_ACTION_CLOSE:
        if (all) {
            proc_mask = NVME_PROC_OPENED_ZONES;
        }
        status = zns_do_zone_op(ns, zone, proc_mask, zns_close_zone, req);
        break;
    case NVME_ZONE_ACTION_FINISH:
        if (all) {
            proc_mask = NVME_PROC_OPENED_ZONES | NVME_PROC_CLOSED_ZONES;
        }
        status = zns_do_zone_op(ns, zone, proc_mask, zns_finish_zone, req);
        break;
    case NVME_ZONE_ACTION_RESET:
        resets = (uintptr_t *)&req->opaque;

        if (all) {
            proc_mask = NVME_PROC_OPENED_ZONES | NVME_PROC_CLOSED_ZONES |
                NVME_PROC_FULL_ZONES;
        }
        *resets = 1;
        status = zns_do_zone_op(ns, zone, proc_mask, zns_reset_zone, req);
        (*resets)--;
        return NVME_SUCCESS;
    case NVME_ZONE_ACTION_OFFLINE:
        if (all) {
            proc_mask = NVME_PROC_READ_ONLY_ZONES;
        }
        status = zns_do_zone_op(ns, zone, proc_mask, zns_offline_zone, req);
        break;
    case NVME_ZONE_ACTION_SET_ZD_EXT:
        if (all || !n->zd_extension_size) {
            return NVME_INVALID_FIELD | NVME_DNR;
        }
        zd_ext = zns_get_zd_extension(ns, zone_idx);
        status = dma_write_prp(n, (uint8_t *)zd_ext, n->zd_extension_size, prp1,
                               prp2);
        if (status) {
            return status;
        }
        status = zns_set_zd_ext(ns, zone);
        if (status == NVME_SUCCESS) {
            return status;
        }
        break;
    default:
        status = NVME_INVALID_FIELD;
    }

    if (status) {
        status |= NVME_DNR;
    }

    return status;
}

/*
 * zns_zone_mgmt_send
 * 处理 Zone Management Send 命令：根据 action（open/close/finish/reset/offline/set_zd_ext）
 * 调用对应的状态机处理函数，可选择对单个 zone 或所有 zone 执行操作（all 标志）。
 */

static bool zns_zone_matches_filter(uint32_t zafs, NvmeZone *zl)
{
    NvmeZoneState zs = zns_get_zone_state(zl);

    switch (zafs) {
    case NVME_ZONE_REPORT_ALL:
        return true;
    case NVME_ZONE_REPORT_EMPTY:
        return zs == NVME_ZONE_STATE_EMPTY;
    case NVME_ZONE_REPORT_IMPLICITLY_OPEN:
        return zs == NVME_ZONE_STATE_IMPLICITLY_OPEN;
    case NVME_ZONE_REPORT_EXPLICITLY_OPEN:
        return zs == NVME_ZONE_STATE_EXPLICITLY_OPEN;
    case NVME_ZONE_REPORT_CLOSED:
        return zs == NVME_ZONE_STATE_CLOSED;
    case NVME_ZONE_REPORT_FULL:
        return zs == NVME_ZONE_STATE_FULL;
    case NVME_ZONE_REPORT_READ_ONLY:
        return zs == NVME_ZONE_STATE_READ_ONLY;
    case NVME_ZONE_REPORT_OFFLINE:
        return zs == NVME_ZONE_STATE_OFFLINE;
    default:
        return false;
    }
}

static uint16_t zns_zone_mgmt_recv(FemuCtrl *n, NvmeRequest *req)
{
    NvmeCmd *cmd = (NvmeCmd *)&req->cmd;
    NvmeNamespace *ns = req->ns;
    uint64_t prp1 = le64_to_cpu(cmd->dptr.prp1);
    uint64_t prp2 = le64_to_cpu(cmd->dptr.prp2);
    /* cdw12 is zero-based number of dwords to return. Convert to bytes */
    uint32_t data_size = (le32_to_cpu(cmd->cdw12) + 1) << 2;
    uint32_t dw13 = le32_to_cpu(cmd->cdw13);
    uint32_t logical_zone_idx, zra, zrasf, partial; // 注意：这里是逻辑索引 Note: this is the logical index
    // uint32_t zone_idx, zra, zrasf, partial;
    uint64_t max_zones, nr_zones = 0;
    uint16_t status;
    uint64_t slba, capacity = zns_ns_nlbas(ns);
    NvmeZoneDescr *z;
    NvmeZone *zone;
    NvmeZoneReportHeader *header;
    void *buf, *buf_p;
    size_t zone_entry_sz;

    req->status = NVME_SUCCESS;

    // zns_get_mgmt_zone_slba_idx 返回的是逻辑Zone索引
    // zns_get_mgmt_zone_slba_idx returns the logical zone index
    status = zns_get_mgmt_zone_slba_idx(n, cmd, &slba, &logical_zone_idx);
    if (status) {
        return status;
    }

    zra = dw13 & 0xff;
    if (zra != NVME_ZONE_REPORT && zra != NVME_ZONE_REPORT_EXTENDED) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }
    if (zra == NVME_ZONE_REPORT_EXTENDED && !n->zd_extension_size) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }

    zrasf = (dw13 >> 8) & 0xff;
    if (zrasf > NVME_ZONE_REPORT_OFFLINE) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }

    if (data_size < sizeof(NvmeZoneReportHeader)) {
        return NVME_INVALID_FIELD | NVME_DNR;
    }

    status = nvme_check_mdts(n, data_size);
    if (status) {
        return status;
    }

    partial = (dw13 >> 16) & 0x01;

    zone_entry_sz = sizeof(NvmeZoneDescr);
    if (zra == NVME_ZONE_REPORT_EXTENDED) {
        zone_entry_sz += n->zd_extension_size;
    }

    max_zones = (data_size - sizeof(NvmeZoneReportHeader)) / zone_entry_sz;
    buf = g_malloc0(data_size);
    
    // 新增
    // 循环计算符合条件的逻辑Zone数量
    for (uint32_t i = logical_zone_idx; i < n->num_zones; i++) {
        if(partial && nr_zones >= max_zones) {
            break;
        }
        // 通过映射获取真实的物理Zone
        uint32_t physical_zone_idx = n->zns->logical_to_physical_zone_map[i];
        NvmeZone *physical_zone = &n->zone_array[physical_zone_idx];
        if (zns_zone_matches_filter(zrasf, physical_zone)) {
            nr_zones++;
        }
    }




    // zone = &n->zone_array[zone_idx];
    // for (; slba < capacity; slba += n->zone_size) {
    //     if (partial && nr_zones >= max_zones) {
    //         break;
    //     }
    //     if (zns_zone_matches_filter(zrasf, zone++)) {
    //         nr_zones++;
    //     }
    // }
    header = (NvmeZoneReportHeader *)buf;
    header->nr_zones = cpu_to_le64(nr_zones);

    buf_p = buf + sizeof(NvmeZoneReportHeader);

    // 循环填充报告
    for (uint32_t i = logical_zone_idx; i < n->num_zones && max_zones > 0; i++) {
        uint32_t physical_zone_idx = n->zns->logical_to_physical_zone_map[i];
        NvmeZone *physical_zone = &n->zone_array[physical_zone_idx];

        if (zns_zone_matches_filter(zrasf, physical_zone)) {
            z = (NvmeZoneDescr *)buf_p;
            buf_p += sizeof(NvmeZoneDescr);

            z->zt = physical_zone->d.zt;
            z->zs = physical_zone->d.zs;
            z->zcap = cpu_to_le64(physical_zone->d.zcap);
            /*
             * 关键：报告给主机的zslba必须是逻辑Zone的zslba！
             * 逻辑Zone i 的zslba就是 i * zone_size。
             * CRITICAL: The zslba reported to the host MUST be the logical zone's zslba!
             * The zslba for logical zone i is simply i * zone_size.
            */
            z->zslba = cpu_to_le64((uint64_t)i * n->zone_size);
            z->za = physical_zone->d.za;

            if (zns_wp_is_valid(physical_zone)) {
                z->wp = cpu_to_le64(physical_zone->d.wp);
            } else {
                z->wp = cpu_to_le64(~0ULL);
            }

            if (zra == NVME_ZONE_REPORT_EXTENDED) {
                if (physical_zone->d.za & NVME_ZA_ZD_EXT_VALID) {
                    memcpy(buf_p, zns_get_zd_extension(ns, physical_zone_idx), n->zd_extension_size);
                }
                buf_p += n->zd_extension_size;
            }

            max_zones--;
        }
    }

    /*
    for (; zone_idx < n->num_zones && max_zones > 0; zone_idx++) {
        zone = &n->zone_array[zone_idx];
        if (zns_zone_matches_filter(zrasf, zone)) {
            z = (NvmeZoneDescr *)buf_p;
            buf_p += sizeof(NvmeZoneDescr);

            z->zt = zone->d.zt;
            z->zs = zone->d.zs;
            z->zcap = cpu_to_le64(zone->d.zcap);
            z->zslba = cpu_to_le64(zone->d.zslba);
            z->za = zone->d.za;

            if (zns_wp_is_valid(zone)) {
                z->wp = cpu_to_le64(zone->d.wp);
            } else {
                z->wp = cpu_to_le64(~0ULL);
            }

            if (zra == NVME_ZONE_REPORT_EXTENDED) {
                if (zone->d.za & NVME_ZA_ZD_EXT_VALID) {
                    memcpy(buf_p, zns_get_zd_extension(ns, zone_idx),
                           n->zd_extension_size);
                }
                buf_p += n->zd_extension_size;
            }

            max_zones--;
        }
    }
    */
    status = dma_read_prp(n, (uint8_t *)buf, data_size, prp1, prp2);

    g_free(buf);

    return status;
}

/*
 * zns_zone_mgmt_recv
 * 实现 Zone Management Receive（Report Zones）：根据请求参数生成 zone 列表
 * 并写回到主机缓冲区，支持扩展描述符（如果控制器配置了 zd_extension）。
 */

static inline bool nvme_csi_has_nvm_support(NvmeNamespace *ns)
{
    switch (ns->ctrl->csi) {
    case NVME_CSI_NVM:
    case NVME_CSI_ZONED:
        return true;
    }

    return false;
}

static uint16_t zns_admin_cmd(FemuCtrl *n, NvmeCmd *cmd)
{
    switch (cmd->opcode) {
    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

static uint16_t zns_io_cmd(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req)
{
    switch (cmd->opcode) {
    case NVME_CMD_READ:
    case NVME_CMD_WRITE:
        return zns_nvme_rw(n, ns, cmd, req,false);
    case NVME_CMD_ZONE_APPEND:
        return zns_nvme_rw(n, ns, cmd, req,true);
    case NVME_CMD_ZONE_MGMT_SEND:
        return zns_zone_mgmt_send(n, req);
    case NVME_CMD_ZONE_MGMT_RECV:
        return zns_zone_mgmt_recv(n, req);
    }

    return NVME_INVALID_OPCODE | NVME_DNR;
}

static void zns_set_ctrl_str(FemuCtrl *n)
{
    static int fsid_zns = 0;
    const char *zns_mn = "FEMU ZMS-SSD Controller [by Misao]";
    const char *zns_sn = "vZNSSD";

    nvme_set_ctrl_name(n, zns_mn, zns_sn, &fsid_zns);
}

/*
 * zns_set_ctrl_str
 * 设置控制器的制造商字符串和序列号，便于区分该模拟设备。
 */

static void zns_set_ctrl(FemuCtrl *n)
{
    uint8_t *pci_conf = n->parent_obj.config;

    zns_set_ctrl_str(n);
    pci_config_set_vendor_id(pci_conf, PCI_VENDOR_ID_INTEL);
    pci_config_set_device_id(pci_conf, 0x5845);
}

/*
 * zns_set_ctrl
 * 设置控制器的 PCI vendor/device id 等基础信息，并调用 zns_set_ctrl_str。
 */

// Add zns init ch, zns init flash and zns init block
// ----------------------------
static void zns_init_blk(struct zns_blk *blk,int num_blk,int blkidx,int flash_type)
{
    blk->nand_type = flash_type;
    blk->next_blk_avail_time = 0;
    blk->page_wp = 0;
}

static void zns_init_plane(struct zns_plane *plane,int num_blk,int flash_type)
{
    plane->blk = g_malloc0(sizeof(struct zns_blk) * num_blk);
    for (int i = 0; i < num_blk; i++) {
        zns_init_blk(&plane->blk[i],num_blk,i,flash_type);
    }
    plane->next_plane_avail_time = 0;
}

static void zns_init_fc(struct zns_fc *fc,uint8_t num_plane,uint8_t num_blk,int flash_type)
{
    fc->plane = g_malloc0(sizeof(struct zns_plane) * num_plane);
    for(int i = 0;i < num_plane;i++)
    {
        zns_init_plane(&fc->plane[i],num_blk,flash_type);
    }
    fc->next_fc_avail_time = 0;
}

static void zns_init_ch(struct zns_ch *ch, uint8_t num_lun,uint8_t num_plane, uint8_t num_blk,int flash_type)
{
    ch->fc = g_malloc0(sizeof(struct zns_fc) * num_lun);
    for (int i = 0; i < num_lun; i++) {
        zns_init_fc(&ch->fc[i],num_plane,num_blk,flash_type);
    }
    ch->next_ch_avail_time = 0;
}

static void zns_init_params(FemuCtrl *n)
{
    struct zns_ssd *id_zns;
    int i;
    uint64_t rev_map_sz; // 新增：用于计算反向映射表大小 Added: for calculating reverse map size

    id_zns = g_malloc0(sizeof(struct zns_ssd));
    id_zns->num_ch = n->zns_params.zns_num_ch;
    id_zns->num_lun = n->zns_params.zns_num_lun;
    id_zns->num_plane = n->zns_params.zns_num_plane;
    id_zns->num_blk = n->zns_params.zns_num_blk;
    id_zns->num_page = n->ns_size/ZNS_PAGE_SIZE/(id_zns->num_ch*id_zns->num_lun*id_zns->num_blk);
    id_zns->lbasz = 1 << zns_ns_lbads(&n->namespaces[0]);
    id_zns->flash_type = n->zns_params.zns_flash_type;

    /*
     * 新增：初始化超级设备(Super Device)数量。
     * Added: Initialize the number of super devices.
    */
    id_zns->num_sd = 2; // 固定为2个超级设备 Fixed to 2 super devices

    id_zns->ch = g_malloc0(sizeof(struct zns_ch) * id_zns->num_ch);
    for (i =0; i < id_zns->num_ch; i++) {
        zns_init_ch(&id_zns->ch[i], id_zns->num_lun,id_zns->num_plane,id_zns->num_blk,id_zns->flash_type);
    }
    /*
     * 修改：为每个超级设备的写指针进行初始化。
     * Modified: Initialize write pointers for each super device.
    */
    for (i = 0; i < id_zns->num_sd; i++) {
        id_zns->wp[i].ch = i * (id_zns->num_ch / id_zns->num_sd); // 每个SD的起始通道 Each SD's starting channel
        id_zns->wp[i].lun = 0;
    }

    // id_zns->wp.ch = 0;
    // id_zns->wp.lun = 0;

    //Misao: init mapping table
    id_zns->l2p_sz = n->ns_size/LOGICAL_PAGE_SIZE;
    id_zns->maptbl = g_malloc0(sizeof(struct ppa) * id_zns->l2p_sz);
    for (i = 0; i < id_zns->l2p_sz; i++) {
        id_zns->maptbl[i].ppa = UNMAPPED_PPA;
    }


    /*
     * 新增：初始化PPA->LPN反向映射表。
     * 表的大小由物理地址空间决定，所有条目初始化为无效LPN。
     * Added: Initialize the PPA->LPN reverse mapping table.
     * The table size is determined by the physical address space, and all entries are initialized to an invalid LPN.
    */
    rev_map_sz = id_zns->num_ch * id_zns->num_lun * id_zns->num_plane * \
                 id_zns->num_blk * id_zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE);
    id_zns->rev_maptbl = g_malloc0(sizeof(uint64_t) * rev_map_sz);
    for (i = 0; i < rev_map_sz; i++) {
        id_zns->rev_maptbl[i] = INVALID_LPN;
    }
    
    /*
     * 根据用户要求，移除SRAM写缓冲区相关的初始化代码。
     * Removed SRAM write buffer initialization code as per user request.
    */
    // //Misao: init sram
    // id_zns->program_unit = ZNS_PAGE_SIZE*id_zns->flash_type*2; //PAGE_SIZE*flash_type*2 planes
    // id_zns->stripe_unit = id_zns->program_unit*id_zns->num_ch*id_zns->num_lun;
    // id_zns->cache.num_wc = ZNS_DEFAULT_NUM_WRITE_CACHE;
    // id_zns->cache.write_cache = g_malloc0(sizeof(struct zns_write_cache) * id_zns->cache.num_wc);
    // for(i =0; i < id_zns->cache.num_wc; i++)
    // {
    //     id_zns->cache.write_cache[i].sblk = i;
    //     id_zns->cache.write_cache[i].used = 0;
    //     id_zns->cache.write_cache[i].cap = (id_zns->stripe_unit/LOGICAL_PAGE_SIZE);
    //     id_zns->cache.write_cache[i].lpns = g_malloc0(sizeof(uint64_t) * id_zns->cache.write_cache[i].cap);
    // }

    femu_log("===========================================\n");
    femu_log("|        ZMS HW Configuration()           |\n");      
    femu_log("===========================================\n");
    femu_log("|\t# Super Devices\t: %u\t\t|\n", id_zns->num_sd); // 新增日志输出 Added log output
    femu_log("|\tnchnl\t: %lu\t|\tchips per chnl\t: %lu\t|\tplanes per chip\t: %lu\t|\tblks per plane\t: %lu\t|\tpages per blk\t: %lu\t|\n",id_zns->num_ch,id_zns->num_lun,id_zns->num_plane,id_zns->num_blk,id_zns->num_page);
    //femu_log("|\tl2p sz\t: %lu\t|\tl2p cache sz\t: %u\t|\n",id_zns->l2p_sz,id_zns->cache.num_l2p_ent);
    femu_log("|\tprogram unit\t: %lu KiB\t|\tstripe unit\t: %lu KiB\t|\t# of write caches\t: %u\t|\t size of write caches (4KiB)\t: %lu\t|\n",id_zns->program_unit/(KiB),id_zns->stripe_unit/(KiB),id_zns->cache.num_wc,(id_zns->stripe_unit/LOGICAL_PAGE_SIZE));
    femu_log("===========================================\n"); 

    //Misao: use average read latency
    id_zns->timing.pg_rd_lat[SLC] = SLC_READ_LATENCY_NS;
    id_zns->timing.pg_rd_lat[TLC] = TLC_READ_LATENCY_NS;
    id_zns->timing.pg_rd_lat[QLC] = QLC_READ_LATENCY_NS;

    //Misao: do not suppirt partial programing
    id_zns->timing.pg_wr_lat[SLC] = SLC_PROGRAM_LATENCY_NS;
    id_zns->timing.pg_wr_lat[TLC] = TLC_PROGRAM_LATENCY_NS;
    id_zns->timing.pg_wr_lat[QLC] = QLC_PROGRAM_LATENCY_NS;

    //Misao: copy from nand.h
    id_zns->timing.blk_er_lat[SLC] = SLC_BLOCK_ERASE_LATENCY_NS;
    id_zns->timing.blk_er_lat[TLC] = TLC_BLOCK_ERASE_LATENCY_NS;
    id_zns->timing.blk_er_lat[QLC] = QLC_BLOCK_ERASE_LATENCY_NS;

    id_zns->dataplane_started_ptr = &n->dataplane_started;

    n->zns = id_zns;

    //Misao: init ftl
    zftl_init(n);
}

/*
 * zns_init_blk/plane/fc/ch/params
 * 负责层级化分配存储层对象并初始化其字段（nand 类型、page write pointer、
 * 各层下一可用时间等），同时为 L2P 映射表和写缓存分配内存。
 */

static int zns_init_zone_cap(FemuCtrl *n)
{
    assert(n->zns);
    struct zns_ssd* zns  = n->zns;
    n->zoned = true;
    n->zasl_bs = NVME_DEFAULT_MAX_AZ_SIZE;
    n->zone_size_bs = zns->num_ch*zns->num_lun*zns->num_plane*zns->num_page*ZNS_PAGE_SIZE;
    n->zone_cap_bs = 0;
    n->cross_zone_read = false;
    n->max_active_zones = 0;
    n->max_open_zones = 0;
    n->zd_extension_size = 0;

    return 0;
}

/*
 * zns_init_zone_cap
 * 将命名空间配置为 Zoned 模式，并计算与 zone 大小相关的字节数参数，
 * 初始化 cross_zone_read 与其他能力标志。
 */

static int zns_start_ctrl(FemuCtrl *n)
{
    /* Coperd: let's fail early before anything crazy happens */
    assert(n->page_size == 4096);

    if (!n->zasl_bs) {
        n->zasl = n->mdts;
    } else {
        if (n->zasl_bs < n->page_size) {
            femu_err("ZASL too small (%dB), must >= 1 page (4K)\n", n->zasl_bs);
            return -1;
        }
        n->zasl = 31 - clz32(n->zasl_bs / n->page_size);
    }

    return 0;
}

static void zns_init(FemuCtrl *n, Error **errp)
{
    NvmeNamespace *ns = &n->namespaces[0];

    zns_set_ctrl(n);
    zns_init_params(n);

    zns_init_zone_cap(n);

    if (zns_init_zone_geometry(ns, errp) != 0) {
        return;
    }

    zns_init_zone_identify(n, ns, 0);
}

/*
 * zns_init
 * ZNS 扩展的入口点：设置控制器属性、初始化 zns 参数并完成 zone 相关
 * 的几何与 identify 初始化。
 */

static void zns_exit(FemuCtrl *n)
{
    /*
     * Release any extra resource (zones) allocated for ZNS mode
     */
}

int nvme_register_znssd(FemuCtrl *n)
{
    n->ext_ops = (FemuExtCtrlOps) {
        .state            = NULL,
        .init             = zns_init,
        .exit             = zns_exit,
        .rw_check_req     = NULL,
        .start_ctrl       = zns_start_ctrl,
        .admin_cmd        = zns_admin_cmd,
        .io_cmd           = zns_io_cmd,
        .get_log          = NULL,
    };

    return 0;
}

/*
 * nvme_register_znssd
 * 将 ZNS 的扩展函数集合注册到控制器的 ext_ops 中，使得 Femu 在
 * 创建命名空间时能够使用这些扩展功能。
 */
