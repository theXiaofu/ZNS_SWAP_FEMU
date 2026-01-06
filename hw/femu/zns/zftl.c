#include "zns.h"

/*
 * zftl.c
 * -----
 * 简体中文说明：
 * 本文件实现了一个非常精简的 FTL 后台线程，用于处理来自数据平面
 * 的读写请求（通过 lockless ring 进行通信）。注释仅用于说明各个
 * 函数的职责、并行性和延时计算方法，不修改原有实现逻辑。
 */

//#define FEMU_DEBUG_ZFTL

/*
 * 新增：定义与冷热Zone均衡相关的阈值。
 * ZONE_RESET_THRESHOLD: 用于区分冷热Zone的重置次数阈值。
 * CRITICAL_THRESHOLD_PERCENT: 超级设备中冷Zone数量占总Zone数的最大百分比，超过则触发均衡。
 * Added: Define thresholds for hot/cold zone balancing.
 * ZONE_RESET_THRESHOLD: Threshold for reset count to distinguish hot/cold zones.
 * CRITICAL_THRESHOLD_PERCENT: Maximum percentage of cold zones in a super device before balancing is triggered.
*/
#define ZONE_RESET_THRESHOLD 5
#define CRITICAL_THRESHOLD_PERCENT 30
#define MAX_ZONES_TO_BALANCE_PER_CYCLE 256 // 新增：定义一次均衡操作中最多迁移的Zone数量
/*
 * 新增：定义每个Zone可能包含的最大LPN数量。
 * 用于静态数组的大小定义。基于 128 MiB Zone / 4 KiB LPN 计算。
 * Added: Define the maximum possible number of LPNs per Zone.
 * Used for static array size definition. Calculated based on 128 MiB Zone / 4 KiB LPN.
*/
#define MAX_LPNS_PER_ZONE 262145

static void *ftl_thread(void *arg);

/*
 * 新增：根据PPA计算其在反向映射表中的索引。
 * Added: Helper function to calculate the index in the reverse map table from a PPA.
*/
static uint64_t ppa_to_idx(struct zns_ssd *zns, struct ppa *ppa)
{
    uint64_t idx = 0;
    // 计算公式依赖于设备几何结构
    // Formula depends on device geometry
    idx += ppa->g.ch * (zns->num_lun * zns->num_plane * zns->num_blk * zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE));
    idx += ppa->g.fc * (zns->num_plane * zns->num_blk * zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE));
    idx += ppa->g.pl * (zns->num_blk * zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE));
    idx += ppa->g.blk * (zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE));
    idx += ppa->g.pg * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE);
    idx += ppa->g.spg;
    return idx;
}

static inline struct ppa get_maptbl_ent(struct zns_ssd *zns, uint64_t lpn)
{
    ftl_assert(lpn < zns->l2p_sz);
    return zns->maptbl[lpn];
}

// static inline void set_maptbl_ent(struct zns_ssd *zns, uint64_t lpn, struct ppa *ppa)
// {
//     ftl_assert(lpn < zns->l2p_sz);
//     zns->maptbl[lpn] = *ppa;
// }


/*
 * valid_ppa / mapped_ppa
 * 校验 ppa 是否落在设备几何范围内，以及是否已被映射（判定为 UNMAPPED_PPA）。
 */

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}




/*
 * 修改：更新映射表条目时，同时更新反向映射表。
 * Modified: When updating a map table entry, also update the reverse map table.
*/
static inline void set_maptbl_ent(struct zns_ssd *zns, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < zns->l2p_sz);
    struct ppa old_ppa = zns->maptbl[lpn];
    // 如果之前存在映射，需要将旧的PPA在反向映射表中置为无效
    if (mapped_ppa(&old_ppa)) {
        uint64_t old_idx = ppa_to_idx(zns, &old_ppa);
        if (old_idx < zns->num_ch * zns->num_lun * zns->num_plane * zns->num_blk * zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE)) {
             zns->rev_maptbl[old_idx] = INVALID_LPN;
        }
    }

    zns->maptbl[lpn] = *ppa;
    // 设置新的PPA到LPN的反向映射
    uint64_t new_idx = ppa_to_idx(zns, ppa);
    if (new_idx < zns->num_ch * zns->num_lun * zns->num_plane * zns->num_blk * zns->num_page * (ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE)) {
        zns->rev_maptbl[new_idx] = lpn;
    }
}

void zftl_init(FemuCtrl *n)
{
    struct zns_ssd *ssd = n->zns;

    qemu_thread_create(&ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n,
                       QEMU_THREAD_JOINABLE);
}

/*
 * zftl_init
 * 创建并启动 FTL 后台线程（可 join）。线程负责从 to_ftl 队列读取请求、
 * 执行读写逻辑并将请求返回到 to_poller 队列。
 */

static inline struct zns_ch *get_ch(struct zns_ssd *zns, struct ppa *ppa)
{
    return &(zns->ch[ppa->g.ch]);
}

static inline struct zns_fc *get_fc(struct zns_ssd *zns, struct ppa *ppa)
{
    struct zns_ch *ch = get_ch(zns, ppa);
    return &(ch->fc[ppa->g.fc]);
}

static inline struct zns_plane *get_plane(struct zns_ssd *zns, struct ppa *ppa)
{
    struct zns_fc *fc = get_fc(zns, ppa);
    return &(fc->plane[ppa->g.pl]);
}

static inline struct zns_blk *get_blk(struct zns_ssd *zns, struct ppa *ppa)
{
    struct zns_plane *pl = get_plane(zns, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline void check_addr(int a, int max)
{
   assert(a >= 0 && a < max);
}





/*
 * zns_advance_status
 * 基于命令类型和目标 plane 的可用时间计算并更新延迟（纳秒）。
 * 返回子操作引入的延迟（sublat）。该函数模拟了 NAND 的并行性：
 * 同一 plane 上的命令必须串行化，下一可用时间基于 plane 级别维护。
 */

/* 修改后的 zns_advance_status：使用 Chip (FC) 粒度进行时序模拟 */
static uint64_t zns_advance_status(struct zns_ssd *zns, struct ppa *ppa, struct nand_cmd *ncmd)
{
    int c = ncmd->cmd;

    uint64_t nand_stime;
    uint64_t req_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;

    /* * 关键修改：获取 Chip (FC) 结构指针，而非 Plane。
     * 对应您的 "并行粒度修改为 Chips 级并行" 要求。
     * 同一个 FC 下的所有 Plane 将共享同一个时间锁 (next_fc_avail_time)。
     */
    struct zns_fc *fc = get_fc(zns, ppa);

    uint64_t lat = 0;
    // 获取 NAND 类型对应的延迟参数
    int nand_type = get_blk(zns, ppa)->nand_type; 

    uint64_t read_delay = zns->timing.pg_rd_lat[nand_type];
    uint64_t write_delay = zns->timing.pg_wr_lat[nand_type];
    uint64_t erase_delay = zns->timing.blk_er_lat[nand_type];

    switch (c) {
    case NAND_READ:
        // 检查 FC (Chip) 及其通道何时可用
        nand_stime = (fc->next_fc_avail_time < req_stime) ? req_stime : \
                     fc->next_fc_avail_time;
        fc->next_fc_avail_time = nand_stime + read_delay;
        lat = fc->next_fc_avail_time - req_stime;
        break;

    case NAND_WRITE:
        nand_stime = (fc->next_fc_avail_time < req_stime) ? req_stime : \
                     fc->next_fc_avail_time;
        fc->next_fc_avail_time = nand_stime + write_delay;
        lat = fc->next_fc_avail_time - req_stime;
        break;

    case NAND_ERASE:
        nand_stime = (fc->next_fc_avail_time < req_stime) ? req_stime : \
                     fc->next_fc_avail_time;
        fc->next_fc_avail_time = nand_stime + erase_delay;
        lat = fc->next_fc_avail_time - req_stime;
        break;

    default:
        /* To silent warnings */
        ;
    }

    return lat;
}


static inline bool valid_ppa(struct zns_ssd *zns, struct ppa *ppa)
{
    int ch = ppa->g.ch;
    int lun = ppa->g.fc;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sub_pg = ppa->g.spg;

    if (ch >= 0 && ch < zns->num_ch && lun >= 0 && lun < zns->num_lun && pl >=
        0 && pl < zns->num_plane && blk >= 0 && blk < zns->num_blk && pg>=0 && pg < zns->num_page && sub_pg >= 0 && sub_pg < ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE)
        return true;

    return false;
}




/*
 * zns_get_wcidx
 * 在写缓存数组中查找与当前活动超级块 (active_zone) 对应的写缓存索引。
 * 未找到时返回 -1（表示需要分配或触发 flush）。
 */

static uint64_t zns_read(struct zns_ssd *zns, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    uint32_t nlb = req->nlb;
    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE/zns->lbasz;
    uint64_t start_lpn = lba / secs_per_pg;
    uint64_t end_lpn = (lba + nlb - 1) / secs_per_pg;
    //int wcidx = zns_get_wcidx(zns);
    struct ppa ppa;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(zns, lpn);
        if (!mapped_ppa(&ppa) || !valid_ppa(zns, &ppa)) {

            continue;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;

        sublat = zns_advance_status(zns, &ppa, &srd);
        femu_log("[R] lpn:\t%lu\t<--ch:\t%u\tlun:\t%u\tpl:\t%u\tblk:\t%u\tpg:\t%u\tsubpg:\t%u\tlat\t%lu\n",lpn,ppa.g.ch,ppa.g.fc,ppa.g.pl,ppa.g.blk,ppa.g.pg,ppa.g.spg,sublat);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}





static struct ppa lpn_2_ppa(struct zns_ssd *zns, uint64_t lpn, uint64_t logical_zone_idx) {
    struct ppa ppa_v = {0};
    uint64_t channel_per_sd = zns->num_ch / zns->num_sd;
    uint64_t tt_page = zns->num_plane * zns->num_lun * channel_per_sd;
    // uint64_t page_per_ch = tt_page / channel_per_sd;
    // uint64_t page_per_lun = page_per_ch / zns->num_lun;
    // uint64_t page_per_lun = tt_page / zns->num_lun;
    uint32_t physical_zone_idx = zns->logical_to_physical_zone_map[logical_zone_idx];
    int sd_idx = physical_zone_idx % zns->num_sd;
    ppa_v.g.blk = physical_zone_idx;
    ppa_v.g.spg = 0;
    ppa_v.g.pg = (lpn / tt_page)% zns->num_page;
    lpn %= tt_page;
    // ppa_v.g.ch = lpn / page_per_ch;
    // lpn %= page_per_ch;
    // ppa_v.g.fc = lpn / page_per_lun;
    // lpn %= page_per_lun;
    // ppa_v.g.pl = lpn;
    // femu_log("zns->num_lun * zns->num_plane:%lu zns->num_plane:%lu\n", zns->num_lun * zns->num_plane, zns->num_plane);
    ppa_v.g.ch = lpn / (zns->num_lun * zns->num_plane) + sd_idx * channel_per_sd;
    // femu_log("lpn:%lu ppa_v.g.ch:%u\n", lpn, ppa_v.g.ch);
    lpn %= (zns->num_lun * zns->num_plane);
    ppa_v.g.fc = lpn / zns->num_plane;
    lpn %= zns->num_plane;
    ppa_v.g.pl = lpn;
    return ppa_v;
}

static uint64_t zns_write(struct zns_ssd *zns, NvmeRequest *req)
{
    NvmeNamespace *ns = req->ns;
    FemuCtrl *n = ns->ctrl;
    uint64_t lba = req->slba;
    uint32_t nlb = req->nlb;
    // uint64_t secs_per_pg = LOGICAL_PAGE_SIZE/zns->lbasz;//8
    uint64_t secs_per_pg = ZNS_PAGE_SIZE/zns->lbasz;//8
    uint64_t start_lpn;
    uint64_t end_lpn;
    uint64_t lpn;
    struct ppa ppa_v;
    uint64_t sublat = 0, maxlat = 0;
    uint32_t zone_idx;
    uint64_t mask = (1ULL << n->zone_size_log2) - 1;

    // femu_log("slba:%lu\tstart_lpn:%lu", lba, start_lpn);
    zone_idx = zns_zone_idx(ns, lba);
    lba &= mask;
    // femu_log("slba:%lu nlb:%u zone_idx:%lu\n", lba, nlb, zone_idx);
    // femu_log("slba:%lX start_lpn:%lX zone_idx:%u\n", lba, start_lpn, zone_idx);
    // lba &= n->zone_size_log2;
    start_lpn = lba / secs_per_pg;
    end_lpn = (lba + nlb - 1) / secs_per_pg;
    // for (lpn = start_lpn / 4; lpn <= (end_lpn / 4 + (end_lpn % 4 != 0)); lpn++) {
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        // zone_idx = zns_zone_idx(req->ns, (lpn * secs_per_pg));
        ppa_v = lpn_2_ppa(zns, lpn, zone_idx);
        // ppa = lpn_to_ppa(req->ns, lpn);
        // advance_read_pointer(n);
        struct nand_cmd write;
        write.cmd = NAND_WRITE;
        write.stime = req->stime;
        sublat = zns_advance_status(zns, &ppa_v, &write);
        // femu_log("[R] lpn:\t%lu\t<--ch:\t%u\tlun:\t%u\tpl:\t%u\tblk:\t%u\tpg:\t%u\tsubpg:\t%u\tlat\t%lu\n",lpn,ppa_v.g.ch,ppa_v.g.fc,ppa_v.g.pl,ppa_v.g.blk,ppa_v.g.pg,ppa_v.g.spg,sublat);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
        set_maptbl_ent(zns, lpn, &ppa_v);
    }
    return maxlat;
}


// 202501-10 修改到这里




// /*
//  * 新增：辅助函数，用于获取NvmeZoneState的字符串表示 (用于日志)
//  */
// static const char* nvme_zone_state_str(NvmeZoneState state) {
//     switch (state) {
//         case NVME_ZONE_STATE_EMPTY: return "EMPTY";
//         case NVME_ZONE_STATE_IMPLICITLY_OPEN: return "IOPEN";
//         case NVME_ZONE_STATE_EXPLICITLY_OPEN: return "EOPEN";
//         case NVME_ZONE_STATE_CLOSED: return "CLOSED";
//         case NVME_ZONE_STATE_FULL: return "FULL";
//         case NVME_ZONE_STATE_READ_ONLY: return "RO";
//         case NVME_ZONE_STATE_OFFLINE: return "OFFLINE";
//         default: return "UNKNOWN";
//     }
// }



/* =========================================================================
 * 2. BaseLine 迁移逻辑
 * 流程：
 * 循环 Plane (0..15):
 * -> 找出该 Plane 所有 LPN 放入数组
 * -> 读数组 -> 写数组
 * 循环结束:
 * -> 擦除整个 Zone
 * ========================================================================= */
static uint64_t zns_move_zone_data_batched(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx, uint64_t requested_start_time)
{
    struct zns_ssd *zns = n->zns;
    NvmeNamespace *ns = n->namespaces;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    /* --- [Start] 健壮性检查与元数据准备 --- */
    NvmeZoneState original_source_state = zns_get_zone_state(physical_src_zone);
    NvmeZoneState original_target_state = zns_get_zone_state(physical_dst_zone);
    uint32_t original_source_reset_count = physical_src_zone->reset_count;
    uint32_t original_target_reset_count = physical_dst_zone->reset_count; 

    // 反向查找 logical_dst_idx (指向 physical_dst_idx 的那个逻辑 Zone)
    uint32_t logical_dst_idx = -1;
    for (uint32_t i = 0; i < n->num_zones; i++) {
        if (zns->logical_to_physical_zone_map[i] == physical_dst_idx) {
            logical_dst_idx = i;
            break;
        }
    }

    if (logical_dst_idx == -1 || logical_dst_idx == logical_src_idx) {
        ftl_err("Batched Move Error: Invalid logical mapping (Dst: %d, Src: %d)\n", logical_dst_idx, logical_src_idx);
        printf("Batched Move Error: Invalid logical mapping (Dst: %d, Src: %d)\n", logical_dst_idx, logical_src_idx);
        return 0; 
    }

    if (original_target_state != NVME_ZONE_STATE_EMPTY) {
        ftl_err("Batched Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        printf("Batched Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0; 
    }

    // 计算有效数据量
    uint64_t secs_per_pg = ZNS_PAGE_SIZE / zns->lbasz;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_valid_lpns = (valid_lba_count + secs_per_pg - 1) / secs_per_pg;

    // 快速路径：无数据，直接交换元数据
    if (num_valid_lpns == 0) {
        ftl_log("Batched Move: No valid data. Swapping metadata only.\n");
        // 交换映射
        zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
        zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;
        
        // 更新 Target (原 Source 的状态)
        physical_dst_zone->w_ptr = physical_dst_zone->d.zslba;
        physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
        physical_dst_zone->reset_count = original_source_reset_count;
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
            zns_aor_inc_active(ns);
            if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
                 zns_aor_inc_open(ns);
            }
        }
        zns_assign_zone_state(ns, physical_dst_zone, original_source_state); 

        // 重置 Source (变为空闲)
        physical_src_zone->reset_count = original_target_reset_count;
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
             zns_aor_dec_open(ns); 
        }
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
             zns_aor_dec_active(ns); 
        }
        physical_src_zone->w_ptr = physical_src_zone->d.zslba; 
        physical_src_zone->d.wp = physical_src_zone->d.zslba;
        zns_assign_zone_state(ns, physical_src_zone, original_target_state); 
        return 0; 
    }
    /* --- [End] 健壮性检查 --- */


    /* --- [Start] 数据迁移 (Plane Batched) --- */
    uint64_t start_time = (requested_start_time > qemu_clock_get_ns(QEMU_CLOCK_REALTIME)) ? 
                           requested_start_time : qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t current_time = start_time;
    static uint64_t lpn_buffer[MAX_LPNS_PER_ZONE];

    // 1. 遍历 Plane (串行粒度)
    for (int plane_idx = 0; plane_idx < zns->num_plane; plane_idx++) {
        
        // A. 初始化数组：收集该 Plane 所有的有效 LPN
        int buf_count = 0;
        for (uint64_t lpn = 0; lpn < num_valid_lpns; lpn++) {
            struct ppa temp_ppa = lpn_2_ppa(zns, lpn, logical_src_idx);
            if (temp_ppa.g.pl == plane_idx) {
                // 使用 MAX_LPNS_PER_ZONE 保证数组足够大
                if (buf_count >= MAX_LPNS_PER_ZONE) {
                    ftl_err("batched Move: Zone LPN count (%d) exceeds static limit (%d)!\n", buf_count, MAX_LPNS_PER_ZONE);
                    printf("batched Move: Zone LPN count (%d) exceeds static limit (%d)!\n", buf_count, MAX_LPNS_PER_ZONE);
                    return 0; // 失败
                }
                lpn_buffer[buf_count++] = lpn;
            }
        }

        if (buf_count > 0) {
            // B. 读阶段
            uint64_t max_read_finish = current_time;
            for (int k = 0; k < buf_count; k++) {
                struct ppa ppa = lpn_2_ppa(zns, lpn_buffer[k], logical_src_idx);
                struct nand_cmd cmd = { .cmd = NAND_READ, .type = GC_IO, .stime = current_time };
                zns_advance_status(zns, &ppa, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &ppa);
                if (fc->next_fc_avail_time > max_read_finish) max_read_finish = fc->next_fc_avail_time;
            }
            current_time = max_read_finish;

            // C. 写阶段
            // 注意：此时 logical_dst_idx 指向 physical_dst_idx，可以直接用来计算目标 PPA
            uint64_t max_write_finish = current_time;
            for (int k = 0; k < buf_count; k++) {
                struct ppa ppa = lpn_2_ppa(zns, lpn_buffer[k], logical_dst_idx); // 使用 logical_dst 计算目标位置
                struct nand_cmd cmd = { .cmd = NAND_WRITE, .type = GC_IO, .stime = current_time };
                zns_advance_status(zns, &ppa, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &ppa);
                if (fc->next_fc_avail_time > max_write_finish) max_write_finish = fc->next_fc_avail_time;
            }
            current_time = max_write_finish;
        }
    }

    // 2. 擦除阶段 (BaseLine: 擦除整个 Source Zone)
    uint64_t max_erase_finish = current_time;
    for (int ch = 0; ch < zns->num_ch; ch++) {
        for (int lun = 0; lun < zns->num_lun; lun++) {
            for (int pl = 0; pl < zns->num_plane; pl++) {
                struct ppa p;
                p.g.ch = ch; p.g.fc = lun; p.g.pl = pl;
                p.g.blk = physical_src_idx;

                struct nand_cmd cmd = { .cmd = NAND_ERASE, .type = GC_IO, .stime = current_time };
                zns_advance_status(zns, &p, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &p);
                if (fc->next_fc_avail_time > max_erase_finish) max_erase_finish = fc->next_fc_avail_time;
            }
        }
    }
    current_time = max_erase_finish;
    /* --- [End] 数据迁移 --- */


    /* --- [Start] 元数据更新与资源释放 --- */
    // 交换映射
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
    zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;

    // 更新 Dst Zone (继承 Src 属性)
    physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (num_valid_lpns * secs_per_pg);
    physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
    physical_dst_zone->reset_count = original_source_reset_count;

    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
        zns_aor_inc_active(ns);
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
            zns_aor_inc_open(ns);
        }
    }
    zns_assign_zone_state(ns, physical_dst_zone, original_source_state);

    // 释放 Src 资源 (AOR Dec)
    if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
        zns_aor_dec_open(ns);
    }
    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
         zns_aor_dec_active(ns);
    }

    // 重置 Src Zone
    physical_src_zone->reset_count = original_target_reset_count;
    physical_src_zone->w_ptr = physical_src_zone->d.zslba;
    physical_src_zone->d.wp = physical_src_zone->d.zslba;
    zns_assign_zone_state(ns, physical_src_zone, original_target_state); // Empty

    uint64_t return_latency = (current_time > requested_start_time) ? (current_time - requested_start_time) : 0;
    return return_latency;
    /* --- [End] 元数据更新 --- */
}

 
/* =========================================================================
 * 3. Optimized (Pipeline) 迁移逻辑
 * 流程：
 * 循环 Plane (0..15):
 * -> 找出该 Plane 所有 LPN 放入数组
 * -> 读数组
 * -> 写数组 (同时擦除上一个有效 Plane)
 * -> 记录当前 Plane 为“待擦除”
 * 循环结束:
 * -> 擦除最后一个 Plane
 * ========================================================================= */
static uint64_t zns_move_zone_data_pipelined(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx, uint64_t requested_start_time)
{
    struct zns_ssd *zns = n->zns;
    NvmeNamespace *ns = n->namespaces;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    /* --- [Start] 健壮性检查 --- */
    NvmeZoneState original_source_state = zns_get_zone_state(physical_src_zone);
    NvmeZoneState original_target_state = zns_get_zone_state(physical_dst_zone);
    uint32_t original_source_reset_count = physical_src_zone->reset_count;
    uint32_t original_target_reset_count = physical_dst_zone->reset_count; 

    uint32_t logical_dst_idx = -1;
    for (uint32_t i = 0; i < n->num_zones; i++) {
        if (zns->logical_to_physical_zone_map[i] == physical_dst_idx) {
            logical_dst_idx = i;
            break;
        }
    }

    if (logical_dst_idx == -1 || logical_dst_idx == logical_src_idx) {
         ftl_err("Pipelined Move Error: Invalid logical mapping (Dst: %d, Src: %d)\n", logical_dst_idx, logical_src_idx);
         printf("Pipelined Move Error: Invalid logical mapping (Dst: %d, Src: %d)\n", logical_dst_idx, logical_src_idx);
        return 0; 
    }

    if (original_target_state != NVME_ZONE_STATE_EMPTY) {
        ftl_err("Pipelined Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        printf("Pipelined Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0; 
    }

    uint64_t secs_per_pg = ZNS_PAGE_SIZE / zns->lbasz;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_valid_lpns = (valid_lba_count + secs_per_pg - 1) / secs_per_pg;

    if (num_valid_lpns == 0) {
        ftl_log("Pipelined Move: No valid data. Swapping metadata only.\n");
        // 交换
        zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
        zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;

        physical_dst_zone->w_ptr = physical_dst_zone->d.zslba;
        physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
        physical_dst_zone->reset_count = original_source_reset_count;
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
            zns_aor_inc_active(ns);
            if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
                 zns_aor_inc_open(ns);
            }
        }
        zns_assign_zone_state(ns, physical_dst_zone, original_source_state); 

        physical_src_zone->reset_count = original_target_reset_count;
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
             zns_aor_dec_open(ns); 
        }
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
             zns_aor_dec_active(ns); 
        }
        physical_src_zone->w_ptr = physical_src_zone->d.zslba; 
        physical_src_zone->d.wp = physical_src_zone->d.zslba;
        zns_assign_zone_state(ns, physical_src_zone, original_target_state); 
        return 0; 
    }
    /* --- [End] 健壮性检查 --- */


    /* --- [Start] 数据迁移 (Pipeline) --- */
    uint64_t start_time = (requested_start_time > qemu_clock_get_ns(QEMU_CLOCK_REALTIME)) ? 
                           requested_start_time : qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t t_src = start_time;
    uint64_t t_dst = start_time;
    static uint64_t lpn_buffer[MAX_LPNS_PER_ZONE];
    int prev_processed_plane = -1;

    for (int plane_idx = 0; plane_idx < zns->num_plane; plane_idx++) {
        // A. 初始化数组
        int buf_count = 0;
        for (uint64_t lpn = 0; lpn < num_valid_lpns; lpn++) {
            struct ppa temp_ppa = lpn_2_ppa(zns, lpn, logical_src_idx);
            if (temp_ppa.g.pl == plane_idx) {
                // 使用 MAX_LPNS_PER_ZONE 保证数组足够大
                if (buf_count >= MAX_LPNS_PER_ZONE) {
                    ftl_err("Pipelined Move: Zone LPN count (%d) exceeds static limit (%d)!\n", buf_count, MAX_LPNS_PER_ZONE);
                    printf("Pipelined Move: Zone LPN count (%d) exceeds static limit (%d)!\n", buf_count, MAX_LPNS_PER_ZONE);
                    return 0; // 失败
                }
                lpn_buffer[buf_count++] = lpn;
            }
        }

        if (buf_count > 0) {
            // B. 读 (Source)
            uint64_t read_finish = t_src;
            for (int k = 0; k < buf_count; k++) {
                struct ppa ppa = lpn_2_ppa(zns, lpn_buffer[k], logical_src_idx);
                struct nand_cmd cmd = { .cmd = NAND_READ, .type = GC_IO, .stime = t_src };
                zns_advance_status(zns, &ppa, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &ppa);
                if (fc->next_fc_avail_time > read_finish) read_finish = fc->next_fc_avail_time;
            }
            t_src = read_finish; 

            // C. 写 (Dst)
            uint64_t write_start = (t_src > t_dst) ? t_src : t_dst;
            uint64_t write_finish = write_start;
            for (int k = 0; k < buf_count; k++) {
                struct ppa ppa = lpn_2_ppa(zns, lpn_buffer[k], logical_dst_idx); // 使用 logical_dst
                struct nand_cmd cmd = { .cmd = NAND_WRITE, .type = GC_IO, .stime = write_start };
                zns_advance_status(zns, &ppa, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &ppa);
                if (fc->next_fc_avail_time > write_finish) write_finish = fc->next_fc_avail_time;
            }
            t_dst = write_finish;

            // D. 擦除上一个有效 Plane (Source) - 隐藏在写操作中
            // 擦除指令发给 Source，开始时间可以是 t_src (即读完后)
            if (prev_processed_plane != -1) {
                uint64_t erase_finish = t_src;
                // 遍历 Source 设备中该 Plane 对应的所有 Chip
                for (int ch = 0; ch < zns->num_ch; ch++) {
                    for (int lun = 0; lun < zns->num_lun; lun++) {
                        struct ppa p;
                        p.g.ch = ch; p.g.fc = lun; p.g.pl = prev_processed_plane;
                        p.g.blk = physical_src_idx;

                        struct nand_cmd cmd = { .cmd = NAND_ERASE, .type = GC_IO, .stime = t_src };
                        zns_advance_status(zns, &p, &cmd);
                        
                        struct zns_fc *fc = get_fc(zns, &p);
                        if (fc->next_fc_avail_time > erase_finish) erase_finish = fc->next_fc_avail_time;
                    }
                }
                t_src = erase_finish; 
            }
            prev_processed_plane = plane_idx;
        }
    }

    // 2. 擦除最后一个 Plane
    if (prev_processed_plane != -1) {
        uint64_t last_erase_finish = t_src;
        for (int ch = 0; ch < zns->num_ch; ch++) {
            for (int lun = 0; lun < zns->num_lun; lun++) {
                struct ppa p;
                p.g.ch = ch; p.g.fc = lun; p.g.pl = prev_processed_plane;
                p.g.blk = physical_src_idx;

                struct nand_cmd cmd = { .cmd = NAND_ERASE, .type = GC_IO, .stime = t_src };
                zns_advance_status(zns, &p, &cmd);
                
                struct zns_fc *fc = get_fc(zns, &p);
                if (fc->next_fc_avail_time > last_erase_finish) last_erase_finish = fc->next_fc_avail_time;
            }
        }
        t_src = last_erase_finish;
    }
    uint64_t max_finish_time = (t_src > t_dst) ? t_src : t_dst;
    /* --- [End] 数据迁移 --- */


    /* --- [Start] 元数据更新 --- */
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
    zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;

    physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (num_valid_lpns * secs_per_pg);
    physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
    physical_dst_zone->reset_count = original_source_reset_count;
    
    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
        zns_aor_inc_active(ns);
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
            zns_aor_inc_open(ns);
        }
    }
    zns_assign_zone_state(ns, physical_dst_zone, original_source_state);
    
    if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
        zns_aor_dec_open(ns);
    }
    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
         zns_aor_dec_active(ns);
    }

    physical_src_zone->reset_count = original_target_reset_count;
    physical_src_zone->w_ptr = physical_src_zone->d.zslba;
    physical_src_zone->d.wp = physical_src_zone->d.zslba;
    zns_assign_zone_state(ns, physical_src_zone, original_target_state); 
    
    uint64_t return_latency = (max_finish_time > requested_start_time) ? (max_finish_time - requested_start_time) : 0;
    return return_latency;
    /* --- [End] 元数据更新 --- */
}

/*
 * (最终版 - 混合策略)：zns_check_and_balance_super_devices 函数。
 * * 实现了基于“阈值触发” + “均值目标” + “最小迁移量”的均衡策略：
 * 1. 统计两个SD上的冷Zone数量 (cold_zone_counts[0] 和 cold_zone_counts[1])。
 * 2. 检查是否有SD的冷Zone比例超过 CRITICAL_THRESHOLD_PERCENT (sd_above_thresh)。
 * 3. [触发条件1]：如果没有任何SD超过阈值，则不均衡。
 * 4. 如果触发，计算总冷Zone数 (total_cold_zones) 和 平均冷Zone数 (average_cold_zones)。
 * 5. 找出源SD (sd_source = sd_above_thresh) 和目标SD (sd_target = sd_below_thresh)。
 * 6. 计算需要迁移的数量 (num_to_migrate_needed) = 源SD冷Zone数 - 平均冷Zone数。
 * 7. [触发条件2]：如果 num_to_migrate_needed <= 1，则不均衡。
 * 8. 如果两个条件都满足，则查找所有可用的源Zone和目标Zone。
 * 9. 确定最终迁移量 (num_to_migrate)，取以下三者的最小值：
 * (num_to_migrate_needed, 可用的源Zone数, 可用的目标Zone数)
 * 10. 批量执行迁移。
 */
static void zns_check_and_balance_super_devices(FemuCtrl *n)
{
    struct zns_ssd *zns = n->zns;
    bool use_batch = false; // 仍然可以在这里切换批处理/流水线模式
    
    // 健壮性检查
    if (!zns || n->num_zones == 0 || zns->num_sd == 0) return;
    uint32_t zones_per_sd = n->num_zones / zns->num_sd;
    if (zones_per_sd == 0) return;
    
    // 该策略目前仅支持2个SD
    if (zns->num_sd != 2) {
        ftl_err("Balancing Error: Strategy requires exactly 2 Super Devices (num_sd = %u)\n", zns->num_sd);
        return;
    }

    uint32_t cold_zone_counts[2];
    int sd_above_thresh = -1, sd_below_thresh = -1;
    uint32_t total_cold_zones = 0;
    uint32_t average_cold_zones = 0;
    uint32_t num_to_migrate_needed = 0;

    memset(cold_zone_counts, 0, sizeof(cold_zone_counts));

    // 1. 统计每个物理SD上的冷Zone数量
    for (uint32_t i = 0; i < n->num_zones; i++) {
        NvmeZone *p_zone = &n->zone_array[i];
        int sd_idx = i % zns->num_sd;
        // 判定为冷Zone：reset_count 低于阈值且 Zone 非空
        if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
            cold_zone_counts[sd_idx]++;
        }
    }

    // 2. 寻找超载 (sd_above_thresh) 和轻载 (sd_below_thresh) 的SD
    for (int i = 0; i < zns->num_sd; i++) {
        uint32_t threshold = (zones_per_sd * CRITICAL_THRESHOLD_PERCENT) / 100;
        threshold = MAX(threshold, 1);
        
        if (cold_zone_counts[i] > threshold) sd_above_thresh = i;
        else if (sd_below_thresh == -1 || cold_zone_counts[i] < cold_zone_counts[sd_below_thresh]) sd_below_thresh = i;
    }

    // 3. [触发条件1] 检查：是否触发了阈值？
    if (sd_above_thresh != -1 && sd_below_thresh != -1 && sd_above_thresh != sd_below_thresh) {
        
        int sd_source = sd_above_thresh; // 源SD是超过阈值的那个
        int sd_target = sd_below_thresh; // 目标SD是冷Zone最少的那个
        
        // 4. 计算均值
        total_cold_zones = cold_zone_counts[0] + cold_zone_counts[1];
        average_cold_zones = total_cold_zones / 2; // (zns->num_sd == 2)
        
        // 5. 计算需要迁移多少个Zone才能达到平均值
        if (cold_zone_counts[sd_source] > average_cold_zones) {
            num_to_migrate_needed = cold_zone_counts[sd_source] - average_cold_zones;
        } else {
            num_to_migrate_needed = 0;
        }

        // 6. [触发条件2] 检查：需要迁移的Zone数是否大于1？
        if (num_to_migrate_needed > 1) {
            
            // 7. 寻找所有可迁移的Zone
            // 使用静态数组存储待迁移的Zone列表
            uint32_t source_zones[MAX_ZONES_TO_BALANCE_PER_CYCLE];
            uint32_t target_zones[MAX_ZONES_TO_BALANCE_PER_CYCLE];
            uint32_t source_count = 0;
            uint32_t target_count = 0;

            // 7a. 寻找源 (来自 sd_source 的逻辑冷Zone)
            for (uint32_t i = 0; i < n->num_zones; i++) {
                if (source_count >= MAX_ZONES_TO_BALANCE_PER_CYCLE) break; // 防止数组溢出

                uint32_t p_idx = zns->logical_to_physical_zone_map[i];
                if ((p_idx % zns->num_sd) == sd_source) {
                     NvmeZone *p_zone = &n->zone_array[p_idx];
                     // 必须是冷Zone且非空
                    if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
                        source_zones[source_count++] = i;
                    }
                }
            }

            // 7b. 寻找目标 (来自 sd_target 的物理空Zone)
            for (uint32_t i = 0; i < n->num_zones; i++) {
                if (target_count >= MAX_ZONES_TO_BALANCE_PER_CYCLE) break; // 防止数组溢出

                if ((i % zns->num_sd) == sd_target) {
                     if (zns_get_zone_state(&n->zone_array[i]) == NVME_ZONE_STATE_EMPTY) {
                        target_zones[target_count++] = i;
                    }
                }
            }

            // 8. 确定最终迁移数量
            uint32_t num_to_migrate = MIN(num_to_migrate_needed, source_count);
            num_to_migrate = MIN(num_to_migrate, target_count);

            // 9. 批量执行迁移
            if (num_to_migrate > 0) {
                
                uint64_t current_sim_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
                uint64_t start_time_for_next_op; // 用于批处理模式
                // uint64_t start_time_for_all_ops; // 用于流水线模式
                uint64_t total_accumulated_latency = 0; // 批处理模式：延迟总和
                // uint64_t max_parallel_latency = 0;      // 流水线模式：最大延迟
                
                if (use_batch) {
                    start_time_for_next_op = current_sim_time;
                    ftl_log("Balancing triggered (Batch Mode, Threshold+Avg): Need %u, Migrating %u zones serially from SD %d (has %u) to SD %d (has %u). Start: %lu ns\n",
                            num_to_migrate_needed, num_to_migrate, sd_source, cold_zone_counts[sd_source], sd_target, cold_zone_counts[sd_target], start_time_for_next_op);
                } else {
                    start_time_for_next_op = current_sim_time;
                    ftl_log("Balancing triggered (Pipeline Mode, Threshold+Avg): Need %u, Migrating %u zones in parallel from SD %d (has %u) to SD %d (has %u). Start: %lu ns\n",
                            num_to_migrate_needed, num_to_migrate, sd_source, cold_zone_counts[sd_source], sd_target, cold_zone_counts[sd_target], start_time_for_next_op);
                }

                for (uint32_t i = 0; i < num_to_migrate; i++) {
                    uint32_t logical_src_idx = source_zones[i];
                    uint32_t physical_dst_idx = target_zones[i];

                    ftl_log("  -> Migrating logical %u (phy %u) to phy %u\n",
                            logical_src_idx, zns->logical_to_physical_zone_map[logical_src_idx], physical_dst_idx);

                    uint64_t duration; // 存储单次迁移的 *持续时间*
                    
                    if(use_batch){
                        // 1. 调用批处理，传入 *下一个* 可用时间
                        duration = zns_move_zone_data_batched(n, logical_src_idx, physical_dst_idx, start_time_for_next_op);
                        // 2. 累加总延迟 (串行)
                        total_accumulated_latency += duration;
                        // 3. 更新下一个操作的开始时间
                        start_time_for_next_op += duration;
                    }else{
                        // 1. 调用流水线，传入 *同一个* 开始时间
                        duration = zns_move_zone_data_pipelined(n, logical_src_idx, physical_dst_idx, start_time_for_next_op);

                        // 2. 累加总延迟 (串行)
                        total_accumulated_latency += duration;
                        // 3. 更新下一个操作的开始时间
                        start_time_for_next_op += duration;
                        // // 2. 记录最大延迟 (并行)
                        // max_parallel_latency = MAX(max_parallel_latency, duration);
                    }
                }

                // 循环外，根据模式打印总报告
                if (use_batch) {
                     printf("Balancing completed %u migrations (Batch Mode). Total accumulated (serial) latency: %lu ns\n",
                           num_to_migrate, total_accumulated_latency);
                } else {
                    //  // 流水线模式下，总延迟等于所有并行操作中，耗时最长的那个操作的延迟
                    //  printf("Balancing completed %u migrations (Pipeline Mode). Total operation (parallel) latency: %lu ns\n",
                    //        num_to_migrate, max_parallel_latency);
                     printf("Balancing completed %u migrations (Pipeline Mode). Total operation (parallel) latency: %lu ns\n",
                           num_to_migrate, total_accumulated_latency);
                }

            } else {
                 ftl_log("Balancing check (Threshold+Avg): Need %u. Found %u sources and %u targets. No migration possible.\n",
                         num_to_migrate_needed, source_count, target_count);
            }
        } else {
            // [触发条件2] 未满足
            ftl_log("Balancing check (Threshold+Avg): Triggered (SD%d > %d%%), but migration need (%u) is not > 1. No balancing required.\n",
                    sd_source, CRITICAL_THRESHOLD_PERCENT, num_to_migrate_needed);
        }
    } else {
        // [触发条件1] 未满足
        // (可选日志)
        // ftl_log("Balancing check (Threshold+Avg): System is below critical threshold (SD0: %u, SD1: %u cold zones). No balancing required.\n",
        //         cold_zone_counts[0], cold_zone_counts[1]);
    }
}
// FTL 主线程
static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct zns_ssd *zns = n->zns;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(zns->dataplane_started_ptr)) {
        usleep(100000);
    }

    zns->to_ftl = n->to_ftl;
    zns->to_poller = n->to_poller;

    while (1) {
        bool io_processed = false;
        for (i = 1; i <= n->nr_pollers; i++) {
            if (!zns->to_ftl[i] || !femu_ring_count(zns->to_ftl[i]))
                continue;

            io_processed = true;
            rc = femu_ring_dequeue(zns->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FEMU: FTL to_ftl dequeue failed\n");
                continue; // 处理下一个队列或循环
            }

            ftl_assert(req);
            switch (req->cmd.opcode) {
                case NVME_CMD_ZONE_APPEND:
                case NVME_CMD_WRITE:
                    lat = zns_write(zns, req);
                    break;
                case NVME_CMD_READ:
                    lat = zns_read(zns, req);
                    break;
                case NVME_CMD_DSM:
                    lat = 0; // DSM 通常认为是元数据操作，延迟较低
                    break;
                default:
                    lat = 0; // 其他未知命令，暂不计算延迟
                    ;
            }

            // 如果zns_write失败，lat可能为-1，需要处理
            if (lat == -1) {
                 ftl_err("IO command processing failed for LBA %lu\n", req->slba);
                 req->status = NVME_INTERNAL_DEV_ERROR; // 设置错误状态
                 lat = 0; // 不增加延迟
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(zns->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed for req cid %u\n", req->cmd.cid);
                // 严重错误，可能需要停止模拟或采取恢复措施
            }
        }

        // 在处理完一批IO请求后，检查是否需要进行超级设备均衡
        // After processing a batch of IO requests, check if super device balancing is needed
        if (io_processed) {
            zns_check_and_balance_super_devices(n);
        } else {
            // 如果没有IO处理，可以短暂休眠避免CPU空转
             usleep(10); // 休眠10微秒
        }
    }

    return NULL;
}



/*
static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct zns_ssd *zns = n->zns;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(zns->dataplane_started_ptr)) {
        usleep(100000);
    }

    // FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully 
    zns->to_ftl = n->to_ftl;
    zns->to_poller = n->to_poller;

    while (1) {
        for (i = 1; i <= n->nr_pollers; i++) {
            if (!zns->to_ftl[i] || !femu_ring_count(zns->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(zns->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);
            switch (req->cmd.opcode) {
                // Fix bug: zone append not respecting configured delay
                case NVME_CMD_ZONE_APPEND:
                    // Fall through 
                case NVME_CMD_WRITE:
                    lat = zns_write(zns, req);
                    break;
                case NVME_CMD_READ:
                    lat = zns_read(zns, req);
                    break;
                case NVME_CMD_DSM:
                    lat = 0;
                    break;
                default:
                    //ftl_err("FTL received unkown request type, ERROR\n");
                    ;
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(zns->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

        }
    }

    return NULL;
}
*/

/*
 * ftl_thread
 * FTL 后台线程主循环：等待数据平面启动后读取来自多个 poller 的请求队列，
 * 根据命令类型调用 zns_read/zns_write 等处理函数，设置 req->reqlat 和 expire_time，
 * 并将请求放回到 to_poller 队列供上层轮询线程继续完成请求生命周期。
 */
