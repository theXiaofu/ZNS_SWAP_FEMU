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
#define CRITICAL_THRESHOLD_PERCENT 70
#define MAX_LPNS_PER_ZONE 32768
#define MAX_ZONES_TO_BALANCE_PER_CYCLE 256 // 新增：定义一次均衡操作中最多迁移的Zone数量
/*
 * 新增：定义每个Zone可能包含的最大LPN数量。
 * 用于静态数组的大小定义。基于 128 MiB Zone / 4 KiB LPN 计算。
 * Added: Define the maximum possible number of LPNs per Zone.
 * Used for static array size definition. Calculated based on 128 MiB Zone / 4 KiB LPN.
*/
#define MAX_LPNS_PER_ZONE 32768

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
 * get_ch/get_fc/get_plane/get_blk
 * 层级访问辅助函数，返回对应层次的数据结构指针。这些函数通过
 * ppa 的位域值索引数组，请确保 ppa 合法性以避免越界。
 */

/*
static void zns_advance_write_pointer(struct zns_ssd *zns)
{
    struct write_pointer *wpp = &zns->wp;

    check_addr(wpp->ch, zns->num_ch);
    wpp->ch++;
    if (wpp->ch == zns->num_ch) {
        wpp->ch = 0;
        check_addr(wpp->lun, zns->num_lun);
        wpp->lun++;
        // in this case, we should go to next lun
        if (wpp->lun == zns->num_lun) {
            wpp->lun = 0;
        }
    }
}
*/


/*
 * zns_advance_write_pointer
 * 将写指针按通道轮询前进；当当前通道耗尽，会进位到下一个 lun，再循环。
 * 此逻辑用于实现多通道/多 lun 的带宽分布（round-robin allocation）。
 */

/*
 * 修改：zns_advance_write_pointer
 * 根据当前活动的逻辑Zone，找到其映射的物理Zone，再确定其所属的超级设备，最后推进该超级设备的写指针。
 * Modified: zns_advance_write_pointer
 * Based on the current active logical zone, find its mapped physical zone, determine its super device, and then advance that super device's write pointer.
*/
static void zns_advance_write_pointer(struct zns_ssd *zns, FemuCtrl *n)
{
    uint32_t logical_zone_idx = zns->active_zone;
    // 健壮性检查: 确保逻辑索引有效
    // Robustness check: ensure logical index is valid
    if (logical_zone_idx >= n->num_zones) {
        ftl_err("Invalid active_zone index %u\n", logical_zone_idx);
        return; // 或者采取其他错误处理
    }
    uint32_t physical_zone_idx = zns->logical_to_physical_zone_map[logical_zone_idx];
    int sd_idx = physical_zone_idx % zns->num_sd;

    struct write_pointer *wpp = &zns->wp[sd_idx];
    uint64_t ch_per_sd = zns->num_ch / zns->num_sd;
    uint64_t start_ch = sd_idx * ch_per_sd;
    uint64_t end_ch = start_ch + ch_per_sd;

    wpp->ch++;
    if (wpp->ch >= end_ch) {
        wpp->ch = start_ch;
        wpp->lun++;
        if (wpp->lun >= zns->num_lun) {
            wpp->lun = 0;
        }
    }
}



static uint64_t zns_advance_status(struct zns_ssd *zns, struct ppa *ppa,struct nand_cmd *ncmd)
{
    int c = ncmd->cmd;

    uint64_t nand_stime;
    uint64_t req_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;

    //plane level parallism
    struct zns_plane *pl = get_plane(zns, ppa);

    uint64_t lat = 0;
    int nand_type = get_blk(zns,ppa)->nand_type;

    uint64_t read_delay = zns->timing.pg_rd_lat[nand_type];
    uint64_t write_delay = zns->timing.pg_wr_lat[nand_type];
    uint64_t erase_delay = zns->timing.blk_er_lat[nand_type];

    switch (c) {
    case NAND_READ:
        nand_stime = (pl->next_plane_avail_time < req_stime) ? req_stime : \
                     pl->next_plane_avail_time;
        pl->next_plane_avail_time = nand_stime + read_delay;
        lat = pl->next_plane_avail_time - req_stime;
	    break;

    case NAND_WRITE:
	    nand_stime = (pl->next_plane_avail_time < req_stime) ? req_stime : \
		            pl->next_plane_avail_time;
	    pl->next_plane_avail_time = nand_stime + write_delay;
	    lat = pl->next_plane_avail_time - req_stime;
	    break;

    case NAND_ERASE:
        nand_stime = (pl->next_plane_avail_time < req_stime) ? req_stime : \
                        pl->next_plane_avail_time;
        pl->next_plane_avail_time = nand_stime + erase_delay;
        lat = pl->next_plane_avail_time - req_stime;
        break;

    default:
        /* To silent warnings */
        ;
    }

    return lat;
}

/*
 * zns_advance_status
 * 基于命令类型和目标 plane 的可用时间计算并更新延迟（纳秒）。
 * 返回子操作引入的延迟（sublat）。该函数模拟了 NAND 的并行性：
 * 同一 plane 上的命令必须串行化，下一可用时间基于 plane 级别维护。
 */

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
static struct ppa get_new_page(struct zns_ssd *zns)
{
    struct write_pointer *wpp = &zns->wp;
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.fc = wpp->lun;
    ppa.g.blk = zns->active_zone;
    ppa.g.V = 1; //not padding page
    if(!valid_ppa(zns,&ppa))
    {
        ftl_err("[Misao] invalid ppa: ch %u lun %u pl %u blk %u pg %u subpg  %u \n",ppa.g.ch,ppa.g.fc,ppa.g.pl,ppa.g.blk,ppa.g.pg,ppa.g.spg);
        ppa.ppa = UNMAPPED_PPA;
    }
    return ppa;
}
*/

/*
 * get_new_page
 * 从写指针位置构造一个新的 ppa（尚未填充 pg/pl 等字段），并标记为有效。
 * 若生成的 ppa 超出范围，会返回 UNMAPPED_PPA 以便上层检测异常。
 */

 /*
 * 修改：get_new_page
 * 逻辑与 zns_advance_write_pointer 类似，写入操作的目标物理块号(blk)必须是当前逻辑Zone映射到的物理Zone的索引。
 * Modified: get_new_page
 * Similar logic to zns_advance_write_pointer. The target physical block number (blk) for the write must be the index of the physical zone mapped by the current logical zone.
*/
static struct ppa get_new_page(struct zns_ssd *zns, FemuCtrl *n)
{
    uint32_t logical_zone_idx = zns->active_zone;
    // 健壮性检查: 确保逻辑索引有效
    if (logical_zone_idx >= n->num_zones) {
        ftl_err("Invalid active_zone index %u in get_new_page\n", logical_zone_idx);
        struct ppa invalid_ppa;
        invalid_ppa.ppa = UNMAPPED_PPA;
        return invalid_ppa; // 返回无效PPA
    }
    uint32_t physical_zone_idx = zns->logical_to_physical_zone_map[logical_zone_idx];
    int sd_idx = physical_zone_idx % zns->num_sd;

    struct write_pointer *wpp = &zns->wp[sd_idx];
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.fc = wpp->lun;
    // 关键：blk应该基于物理Zone的索引
    ppa.g.blk = physical_zone_idx;
    ppa.g.V = 1;
    if(!valid_ppa(zns, &ppa)) {
        ftl_err("[Misao] invalid ppa generated: ch %u lun %u pl %u blk %u pg %u spg %u\n", ppa.g.ch, ppa.g.fc, ppa.g.pl, ppa.g.blk, ppa.g.pg, ppa.g.spg);
        printf("[Misao] invalid ppa generated: ch %u lun %u pl %u blk %u pg %u spg %u\n", ppa.g.ch, ppa.g.fc, ppa.g.pl, ppa.g.blk, ppa.g.pg, ppa.g.spg);
        ppa.ppa = UNMAPPED_PPA;
    }
    return ppa;
}

// static int zns_get_wcidx(struct zns_ssd* zns)
// {
//     int i;
//     for(i = 0;i < zns->cache.num_wc;i++)
//     {
//         if(zns->cache.write_cache[i].sblk==zns->active_zone)
//         {
//             return i;
//         }
//     }
//     return -1;
// }

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

/*
 * zns_read
 * 读取路径的简化实现：遍历涉及的 LPN，查询映射表找出 PPA，
 * 并通过 zns_advance_status 模拟每个页的读延迟，返回最大延迟值。
 * 非映射的 LPN 会被跳过（例如读取空洞时）。
 */

/*
static uint64_t zns_wc_flush(struct zns_ssd* zns, int wcidx, int type,uint64_t stime)
{
    int i,j,p,subpage;
    struct ppa ppa;
    struct ppa oldppa;
    uint64_t lpn;
    int flash_type = zns->flash_type;
    uint64_t sublat = 0, maxlat = 0;

    i = 0;
    while(i < zns->cache.write_cache[wcidx].used)
    {
        for(p = 0;p<zns->num_plane;p++){
            // new write 
            ppa = get_new_page(zns);
            ppa.g.pl = p;
            for(j = 0; j < flash_type ;j++)
            {
                ppa.g.pg = get_blk(zns,&ppa)->page_wp;
                get_blk(zns,&ppa)->page_wp++;
                for(subpage = 0;subpage < ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE;subpage++)
                {
                    if(i+subpage >= zns->cache.write_cache[wcidx].used)
                    {
                        //No need to write an invalid page
                        break;
                    }
                    lpn = zns->cache.write_cache[wcidx].lpns[i+subpage];
                    oldppa = get_maptbl_ent(zns, lpn);
                    if (mapped_ppa(&oldppa)) {
                        // FIXME: Misao: update old page information
                    }
                    ppa.g.spg = subpage;
                    // update maptbl 
                    set_maptbl_ent(zns, lpn, &ppa);
                    //femu_log("[F] lpn:\t%lu\t-->ch:\t%u\tlun:\t%u\tpl:\t%u\tblk:\t%u\tpg:\t%u\tsubpg:\t%u\tlat\t%lu\n",lpn,ppa.g.ch,ppa.g.fc,ppa.g.pl,ppa.g.blk,ppa.g.pg,ppa.g.spg,sublat);
                }
                i+=ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE;
            }
            //FIXME Misao: identify padding page
            if(ppa.g.V)
            {
                struct nand_cmd swr;
                swr.type = type;
                swr.cmd = NAND_WRITE;
                swr.stime = stime;
                // get latency statistics
                sublat = zns_advance_status(zns, &ppa, &swr);
                maxlat = (sublat > maxlat) ? sublat : maxlat;
            }
        }
        // need to advance the write pointer here 
        zns_advance_write_pointer(zns);
    }
    zns->cache.write_cache[wcidx].used = 0;
    return maxlat;
}
*/


/*
 * zns_wc_flush
 * 将写缓存中的条目打包写入闪存：为每个待写 LPN 分配物理页（按 plane 和 flash_type
 * 处理多页写入），更新映射表，并调用 zns_advance_status 获取写延迟。
 * 函数返回处理过程中的最大延迟，用于上层统计/延时累积。
 */
/*
static uint64_t zns_write(struct zns_ssd *zns, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    uint32_t nlb = req->nlb;
    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE/zns->lbasz;
    uint64_t start_lpn = lba / secs_per_pg;
    uint64_t end_lpn = (lba + nlb - 1) / secs_per_pg;
    uint64_t lpn;
    uint64_t sublat = 0, maxlat = 0;
    int i;
    int wcidx = zns_get_wcidx(zns);

    if(wcidx==-1)
    {
        //need flush
        wcidx = 0;
        uint64_t t_used = zns->cache.write_cache[wcidx].used;
        for(i = 1;i < zns->cache.num_wc;i++)
        {
            if(zns->cache.write_cache[i].used==0)
            {
                t_used = 0;
                wcidx = i; //free wc！
                break;
            }
            if(zns->cache.write_cache[i].used > t_used)
            {
                t_used = zns->cache.write_cache[i].used;
                wcidx = i;
            }
        }
        if(t_used) maxlat = zns_wc_flush(zns,wcidx,USER_IO,req->stime);
        zns->cache.write_cache[wcidx].sblk = zns->active_zone;
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        if(zns->cache.write_cache[wcidx].used==zns->cache.write_cache[wcidx].cap)
        {
            femu_log("[W] flush wc %d (%u/%u)\n",wcidx,(int)zns->cache.write_cache[wcidx].used,(int)zns->cache.write_cache[wcidx].cap);
            sublat = zns_wc_flush(zns,wcidx,USER_IO,req->stime);
            femu_log("[W] flush lat: %u\n", (int)sublat);
            maxlat = (sublat > maxlat) ? sublat : maxlat;
            sublat = 0;
        }
        zns->cache.write_cache[wcidx].lpns[zns->cache.write_cache[wcidx].used++]=lpn;
        sublat += SRAM_WRITE_LATENCY_NS; //Simplified timing emulation
        maxlat = (sublat > maxlat) ? sublat : maxlat;
        femu_log("[W] lpn:\t%lu\t-->wc cache:%u, used:%u\n",lpn,(int)wcidx,(int)zns->cache.write_cache[wcidx].used);
    }
    return maxlat;
}
*/

/*
 * zns_write
 * 写路径：先尝试找到与当前 active_zone 匹配的写缓存；若没有可用缓存，
 * 选择一个需要 flush 的缓存并执行 zns_wc_flush。写入操作先写入缓存（模拟 SRAM 延迟），
 * 当缓存满或需要回收时触发实际闪存写入。
 */

/* zns_write 已在之前版本中重写以移除写缓存，此处保持该逻辑，只需确保 get_new_page 和 advance_write_pointer 的调用正确 */
static uint64_t zns_write(struct zns_ssd *zns, NvmeRequest *req)
{
    // 需要获取 FemuCtrl* n 指针
    FemuCtrl *n = req->ns->ctrl;
    uint64_t lba = req->slba;
    uint32_t nlb = req->nlb;
    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE/zns->lbasz;
    uint64_t start_lpn = lba / secs_per_pg;
    uint64_t end_lpn = (lba + nlb - 1) / secs_per_pg;
    uint64_t lpn;
    uint64_t sublat = 0, maxlat = 0;

    // 获取当前写入的逻辑Zone索引
    uint32_t logical_zone_idx = zns_zone_idx(req->ns, lba);
    // 设置当前FTL正在操作的逻辑Zone
    zns->active_zone = logical_zone_idx;

    lpn = start_lpn;
    while(lpn <= end_lpn)
    {
        for(int p = 0; p < zns->num_plane; p++)
        {
            struct ppa ppa = get_new_page(zns, n);
            // 检查 get_new_page 返回的 ppa 是否有效
            if (ppa.ppa == UNMAPPED_PPA) {
                ftl_err("Failed to get a valid new page for LPN %lu\n", lpn);
                // 可能需要返回错误或采取其他恢复措施
                return -1; // 表示错误
            }
            ppa.g.pl = p;

            for(int j = 0; j < zns->flash_type ;j++)
            {
                // 检查将要写入的物理块是否有效
                if (ppa.g.blk >= n->num_zones) {
                     ftl_err("Invalid physical block index %u for LPN %lu\n", ppa.g.blk, lpn);
                     return -1; // 表示错误
                }
                ppa.g.pg = get_blk(zns,&ppa)->page_wp;
                for(int subpage = 0; subpage < ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE; subpage++)
                {
                    if(lpn > end_lpn) break;
                    ppa.g.spg = subpage;
                    set_maptbl_ent(zns, lpn, &ppa);
                    lpn++;
                }
                // 检查将要写入的物理块是否有效
                if (ppa.g.blk >= n->num_zones) {
                    ftl_err("Invalid physical block index %u after page increment for LPN %lu\n", ppa.g.blk, lpn);
                     return -1; // 表示错误
                }
                get_blk(zns,&ppa)->page_wp++;
                if(lpn > end_lpn) break;
            }

            struct nand_cmd swr = { .cmd = NAND_WRITE, .type = USER_IO, .stime = req->stime };
            sublat = zns_advance_status(zns, &ppa, &swr);
            maxlat = (sublat > maxlat) ? sublat : maxlat;

            if(lpn > end_lpn) break;
        }
        zns_advance_write_pointer(zns, n);
    }
    return maxlat;
}


// 202501-10 修改到这里




/*
 * 新增：流水线模式的数据迁移函数。
 * 逐个逻辑页进行读后写操作，延迟逐步累加。
 * Added: Pipelined data migration function.
 * Performs read-then-write operations logical page by logical page, accumulating latency step-by-step.
*/
/*
static uint64_t zns_move_zone_data_pipelined(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx)
{
    struct zns_ssd *zns = n->zns;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    // 检查目标物理Zone是否为空
    if (zns_get_zone_state(physical_dst_zone) != NVME_ZONE_STATE_EMPTY) {
        ftl_err("Pipelined Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0;
    }

    // 计算LPN范围和数量
    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE / zns->lbasz;
    uint64_t start_lpn = (uint64_t)logical_src_idx * n->zone_size / secs_per_pg;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_lpns = valid_lba_count / secs_per_pg;
    if (valid_lba_count % secs_per_pg != 0) num_lpns++;

    uint64_t accumulated_latency = 0; // 累积延迟
    uint64_t current_op_start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME); // 当前操作的开始时间

    ftl_log("Start pipelined moving data for logical zone %u (phy %u -> %u), LPNs: %lu\n",
            logical_src_idx, physical_src_idx, physical_dst_idx, num_lpns);

    // 确定目标物理Zone所属的超级设备
    int dst_sd_idx = physical_dst_idx % zns->num_sd;
    // FTL写操作的目标是逻辑Zone
    zns->active_zone = logical_src_idx;
    // 临时更新映射，让get_new_page指向目标物理Zone
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;

    // 逐个LPN处理
    for (uint64_t i = 0; i < num_lpns; i++) {
        uint64_t lpn = start_lpn + i;
        struct ppa read_ppa = get_maptbl_ent(zns, lpn); // 注意：这里获取的是旧映射
        uint64_t read_lat = 0;
        uint64_t write_lat = 0;

        // 1. 读取当前LPN
        if (mapped_ppa(&read_ppa) && read_ppa.g.blk == physical_src_idx) {
            struct nand_cmd srd = { .cmd = NAND_READ, .type = GC_IO, .stime = current_op_start_time };
            read_lat = zns_advance_status(zns, &read_ppa, &srd);
        }

        // 2. 获取写入位置 (这里简化处理，假设每个LPN写入都需要获取新页并推进指针，实际可能更复杂)
        // 注意：get_new_page现在会返回目标物理Zone的PPA
        struct ppa write_ppa = get_new_page(zns, n);
        if (write_ppa.ppa == UNMAPPED_PPA) {
            ftl_err("Pipelined Move: Failed to get new page for LPN %lu\n", lpn);
            zns->logical_to_physical_zone_map[logical_src_idx] = physical_src_idx; // 恢复映射
            return 0; // 失败
        }
        // 需要手动设置 subpage 和 plane (简化：假设每次都写到第0个subpage和第0个plane，这不完全准确)
        write_ppa.g.spg = 0; // 简化假设
        write_ppa.g.pl = 0;  // 简化假设
        write_ppa.g.pg = get_blk(zns,&write_ppa)->page_wp; // 获取页号
        get_blk(zns,&write_ppa)->page_wp++; // 更新页写入指针 (简化假设)

        // 更新映射表指向新位置
        set_maptbl_ent(zns, lpn, &write_ppa);

        // 3. 写入当前LPN (写入开始时间是读取完成后)
        uint64_t write_start_time_for_lpn = current_op_start_time + read_lat;
        struct nand_cmd swr = { .cmd = NAND_WRITE, .type = GC_IO, .stime = write_start_time_for_lpn };
        write_lat = zns_advance_status(zns, &write_ppa, &swr);

        // 4. 更新下一个操作的开始时间
        current_op_start_time += (read_lat + write_lat);
        accumulated_latency += (read_lat + write_lat);

        // 5. 推进写指针 (简化：每个LPN都推进一次，实际应按物理页)
        zns_advance_write_pointer(zns, n);
    }

    // 恢复active_zone或设置为一个安全值 (可选)
    // zns->active_zone = -1; // Or some default

    // 更新目标物理Zone的元数据
    physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (num_lpns * secs_per_pg); // 使用实际写入的lpn数量
    physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
    physical_dst_zone->reset_count = physical_src_zone->reset_count; // 继承reset_count
    if (physical_dst_zone->w_ptr >= zns_zone_wr_boundary(physical_dst_zone)) {
        zns_assign_zone_state(n->namespaces, physical_dst_zone, NVME_ZONE_STATE_FULL);
    } else {
        zns_assign_zone_state(n->namespaces, physical_dst_zone, NVME_ZONE_STATE_CLOSED);
    }

    // 重置源物理Zone
    NvmeRequest fake_req = { .ns = n->namespaces };
    zns_aio_zone_reset_cb(&fake_req, physical_src_zone);

    ftl_log("Finished pipelined moving data. Total accumulated latency: %lu ns\n", accumulated_latency);
    return accumulated_latency;
}
*/

/*
 * 新增：辅助函数，用于获取NvmeZoneState的字符串表示 (用于日志)
 */
static const char* nvme_zone_state_str(NvmeZoneState state) {
    switch (state) {
        case NVME_ZONE_STATE_EMPTY: return "EMPTY";
        case NVME_ZONE_STATE_IMPLICITLY_OPEN: return "IOPEN";
        case NVME_ZONE_STATE_EXPLICITLY_OPEN: return "EOPEN";
        case NVME_ZONE_STATE_CLOSED: return "CLOSED";
        case NVME_ZONE_STATE_FULL: return "FULL";
        case NVME_ZONE_STATE_READ_ONLY: return "RO";
        case NVME_ZONE_STATE_OFFLINE: return "OFFLINE";
        default: return "UNKNOWN";
    }
}



/* --- 精确流水线模式实现 --- */
/* --- Precise Pipelined Mode Implementation --- */

/*
 * 最终修正版 v3：流水线模式的数据迁移函数。
 * 读取每个LPN并记录其完成时间戳。写入按物理页（跨plane，考虑flash type）组织。
 * 每个物理页写入操作的开始时间，取决于该物理页包含的LPN中，最晚完成读取的时间。
 * 使用静态数组存储读取时间。
 */
static uint64_t zns_move_zone_data_pipelined(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx, uint64_t requested_start_time)
{
    struct zns_ssd *zns = n->zns;
    NvmeNamespace *ns = n->namespaces;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    NvmeZoneState original_source_state = zns_get_zone_state(physical_src_zone);
    NvmeZoneState original_target_state = zns_get_zone_state(physical_dst_zone);
    uint32_t original_source_reset_count = physical_src_zone->reset_count;
    uint32_t original_target_reset_count = physical_dst_zone->reset_count; // Should be low/0 as target is EMPTY

    // 新增：查找当前映射到 目标物理Zone 的 逻辑Zone ("受害者")
    // Added: Find the logical zone ("victim") currently mapped to the target physical zone
    uint32_t logical_dst_idx = -1;
    for (uint32_t i = 0; i < n->num_zones; i++) {
        if (zns->logical_to_physical_zone_map[i] == physical_dst_idx) {
            logical_dst_idx = i;
            break;
        }
    }

    // 健壮性检查
    if (logical_dst_idx == -1) {
        ftl_err("Move Error: No logical zone maps to target physical zone %u!\n", physical_dst_idx);
        printf("Move Error: No logical zone maps to target physical zone %u!\n", physical_dst_idx);
        return 0; // 失败
    }

    // 检查是否在尝试将一个Zone移动到自己（逻辑上）
    if (logical_dst_idx == logical_src_idx) {
         ftl_err("Move Error: Source and victim logical zones are the same (%u)!\n", logical_src_idx);
         printf("Move Error: Source and victim logical zones are the same (%u)!\n", logical_src_idx);
        return 0; // 失败
    }

    if (original_target_state != NVME_ZONE_STATE_EMPTY) {
        ftl_err("Pipelined Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        printf("Pipelined Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0; // 失败
    }

    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE / zns->lbasz;
    uint64_t start_lpn = (uint64_t)logical_src_idx * n->zone_size / secs_per_pg;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_lpns_in_zone = n->zone_size / secs_per_pg;
    uint64_t num_valid_lpns = valid_lba_count / secs_per_pg;
    if (valid_lba_count > 0 && valid_lba_count % secs_per_pg != 0) num_valid_lpns++;

    if (num_valid_lpns == 0) {
        ftl_log("Pipelined Move: No valid data to move for logical zone %u.\n", logical_src_idx);
        zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
        physical_dst_zone->w_ptr = physical_dst_zone->d.zslba;
        physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
        // 目标继承源 reset_count
        physical_dst_zone->reset_count = original_source_reset_count;
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
            zns_aor_inc_active(ns);
            // Since target was EMPTY, if source was OPEN, target becomes OPEN
            if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
                 zns_aor_inc_open(ns);
            }
        }

        zns_assign_zone_state(ns, physical_dst_zone, original_source_state); // Target inherits source state
        
        
        // Source state was already handled by assign_zone_state implicitly if it was in a list
        // 源继承目标 reset_count
        physical_src_zone->reset_count = original_target_reset_count;
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
             zns_aor_dec_open(ns); // Decrement because source is now EMPTY
        }
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
             zns_aor_dec_active(ns); // Decrement because source is now EMPTY
        }
         physical_src_zone->w_ptr = physical_src_zone->d.zslba; // Reset WP for source
         physical_src_zone->d.wp = physical_src_zone->w_ptr;
        zns_assign_zone_state(ns, physical_src_zone, original_target_state); // Source becomes EMPTY
        return 0; // 没有延迟
    }

    // 使用 MAX_LPNS_PER_ZONE 保证数组足够大
    if (num_lpns_in_zone > MAX_LPNS_PER_ZONE) {
        ftl_err("Pipelined Move: Zone LPN count (%lu) exceeds static limit (%d)!\n", num_lpns_in_zone, MAX_LPNS_PER_ZONE);
        printf("Pipelined Move: Zone LPN count (%lu) exceeds static limit (%d)!\n", num_lpns_in_zone, MAX_LPNS_PER_ZONE);
        return 0; // 失败
    }
    static uint64_t lpn_read_finish_times[MAX_LPNS_PER_ZONE]; // 静态数组

    /*
     * 新增：根据 requested_start_time 确定实际的 start_time
     * Added: Determine the actual start_time based on requested_start_time
    */
    uint64_t current_sim_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t start_time;
    if (requested_start_time == 0 || current_sim_time > requested_start_time) {
        start_time = current_sim_time;
    } else {
        start_time = requested_start_time;
    }
    // uint64_t start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t max_overall_finish_time = start_time;

    ftl_log("Start pipelined v3 moving data for logical zone %u (phy %u [%s] -> %u [%s]), Check LPNs: %lu, Valid LPNs: %lu\n",
            logical_src_idx, physical_src_idx, nvme_zone_state_str(original_source_state),
            physical_dst_idx, nvme_zone_state_str(original_target_state), num_lpns_in_zone, num_valid_lpns);

    // 1. 读取阶段
    for (uint64_t i = 0; i < num_lpns_in_zone; i++) {
        uint64_t lpn = start_lpn + i;
        if (lpn >= zns->l2p_sz) {
             lpn_read_finish_times[i] = start_time; // Out of bounds LPN
             continue;
        }
        struct ppa read_ppa = get_maptbl_ent(zns, lpn);
        if (mapped_ppa(&read_ppa) && read_ppa.g.blk == physical_src_idx) {
            struct nand_cmd srd = { .cmd = NAND_READ, .type = GC_IO, .stime = start_time };
            uint64_t read_lat = zns_advance_status(zns, &read_ppa, &srd);
            lpn_read_finish_times[i] = start_time + read_lat;
        } else {
            lpn_read_finish_times[i] = start_time; // 无效或不在源块
        }
    }

    // 临时更新映射
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
    zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;

    uint32_t source_active_zone = zns->active_zone; // 保存当前活动逻辑Zone索引
    zns->active_zone = logical_src_idx; // 我仅仅是更新映射，感觉不需要更新active_zone

    // 2. 写入阶段
    uint64_t processed_valid_lpns = 0; // 已处理的有效LPN计数
    uint64_t current_lpn_idx_offset = 0; // 当前检查的LPN索引偏移
    uint64_t lpns_per_phy_page = ZNS_PAGE_SIZE / LOGICAL_PAGE_SIZE;

    while (processed_valid_lpns < num_valid_lpns) {
        // 跨plane写入
        for (int p = 0; p < zns->num_plane; p++) {
            struct ppa ppa = get_new_page(zns, n);
            if (ppa.ppa == UNMAPPED_PPA) {
                 ftl_err("Pipelined Move: Failed to get new page in write phase.\n");
                 zns->logical_to_physical_zone_map[logical_src_idx] = physical_src_idx; // 恢复映射
                 zns->logical_to_physical_zone_map[logical_dst_idx] = physical_dst_idx; // 恢复映射
                 zns->active_zone = source_active_zone; // 恢复活动逻辑Zone索引
                 return 0; // 失败
            }
            ppa.g.pl = p;
            ppa.g.blk = physical_dst_idx;

            // bool page_written_in_plane = false;
            uint64_t latest_read_finish_for_this_page = start_time;
            // 栈上分配是安全的，因为大小固定且不大
            // uint64_t lpns_in_this_page[zns->flash_type * lpns_per_phy_page];
            uint64_t lpns_in_this_page[128]; // 假设最大128个LPN足够 本实验也只有16个
            uint64_t lpn_count_this_page = 0;

            // 确定本物理页包含的有效LPN及最晚读取时间
            uint64_t potential_lpns_in_page = zns->flash_type * lpns_per_phy_page;
            uint64_t start_check_offset = current_lpn_idx_offset + p * potential_lpns_in_page;

             for(uint64_t k=0; k < potential_lpns_in_page; ++k) {
                 uint64_t current_check_idx = start_check_offset + k;
                if (current_check_idx >= num_lpns_in_zone) break;

                if (lpn_read_finish_times[current_check_idx] > start_time) {
                     latest_read_finish_for_this_page = MAX(latest_read_finish_for_this_page, lpn_read_finish_times[current_check_idx]);
                     if(lpn_count_this_page < sizeof(lpns_in_this_page)/sizeof(lpns_in_this_page[0])) {
                         lpns_in_this_page[lpn_count_this_page++] = start_lpn + current_check_idx;
                     } else {
                          ftl_err("Pipelined Move: Exceeded temp LPN array size for page write!\n");
                          printf("Pipelined Move: Exceeded temp LPN array size for page write!\n");
                          // 可以在这里添加更详细的错误处理
                     }
                }
            }


            // 如果此物理页需要写入数据
            if (lpn_count_this_page > 0) {
                uint64_t write_start_time_for_page = latest_read_finish_for_this_page;
                 ppa.g.pg = get_blk(zns, &ppa)->page_wp;

                // 重新循环设置映射
                for(uint64_t page_lpn_idx = 0; page_lpn_idx < lpn_count_this_page; ++page_lpn_idx) {
                     uint64_t lpn = lpns_in_this_page[page_lpn_idx];
                     ppa.g.spg = page_lpn_idx % lpns_per_phy_page;
                     set_maptbl_ent(zns, lpn, &ppa);
                }

                 get_blk(zns, &ppa)->page_wp++;
                //  page_written_in_plane = true;

                // 计算写入延迟
                struct nand_cmd swr = { .cmd = NAND_WRITE, .type = GC_IO, .stime = write_start_time_for_page };
                uint64_t write_lat = zns_advance_status(zns, &ppa, &swr);
                uint64_t write_finish = write_start_time_for_page + write_lat;
                max_overall_finish_time = MAX(max_overall_finish_time, write_finish);

                processed_valid_lpns += lpn_count_this_page;
            }

            if (processed_valid_lpns >= num_valid_lpns) break;

        } // end plane loop

        // 更新下一次迭代开始检查的 LPN 索引偏移
         current_lpn_idx_offset += zns->num_plane * zns->flash_type * lpns_per_phy_page;

        // 完成跨plane写入后推进写指针
        zns_advance_write_pointer(zns, n);

        if (processed_valid_lpns >= num_valid_lpns) break;
        if (current_lpn_idx_offset >= num_lpns_in_zone) break;


    } // end while loop

    /* --- 更新目标和源物理Zone的元数据 --- */
    physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (num_valid_lpns * secs_per_pg); // 使用 num_valid_lpns
    physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
    // 目标继承源的 reset_count
    physical_dst_zone->reset_count = original_source_reset_count;
    // AOR Inc
    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
        zns_aor_inc_active(ns);
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
            zns_aor_inc_open(ns);
        }
    }

    /*
     * 修改：直接将目标 Zone 状态设置为源 Zone 的原始状态。
     * Modified: Directly set the target Zone state to the original source state.
     */
    zns_assign_zone_state(ns, physical_dst_zone, original_source_state);
    // AOR Dec
    if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
        zns_aor_dec_open(ns);
    }
    if (original_source_state != NVME_ZONE_STATE_EMPTY) {
         zns_aor_dec_active(ns);
    }
    // 源继承目标的 reset_count
    physical_src_zone->reset_count = original_target_reset_count;
    physical_src_zone->w_ptr = physical_src_zone->d.zslba;
    physical_src_zone->d.wp = physical_src_zone->d.zslba;
    zns_assign_zone_state(ns, physical_src_zone, original_target_state); // Assign EMPTY state
    /* --- 元数据更新结束 --- */
    zns->active_zone = source_active_zone; // 恢复活动逻辑Zone索引

    /*
     * 修改：返回相对于 requested_start_time 的总延迟
     * (max_overall_finish_time 是绝对完成时间)
     * Modified: Return total latency relative to requested_start_time
     * (max_overall_finish_time is the absolute finish time)
    */
    uint64_t return_latency = (max_overall_finish_time > requested_start_time) ? (max_overall_finish_time - requested_start_time) : 0;

    ftl_log("Finished pipelined v3 moving data. (Req: %lu, Start: %lu, Finish: %lu) Total relative latency: %lu ns\n",
            requested_start_time, start_time, max_overall_finish_time, return_latency);
    
    return return_latency;
}


/* --- 批处理模式实现 --- */
/* --- Batched Mode Implementation --- */

/*
 * 最终版：批处理模式的数据迁移函数。
 * 先完成所有读操作，记录最大读延迟；然后基于此开始所有写操作，记录最大写延迟。
 * 状态处理同上。
 */
static uint64_t zns_move_zone_data_batched(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx, uint64_t requested_start_time)
{
    struct zns_ssd *zns = n->zns;
    NvmeNamespace *ns = n->namespaces;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    NvmeZoneState original_source_state = zns_get_zone_state(physical_src_zone);
    NvmeZoneState original_target_state = zns_get_zone_state(physical_dst_zone);
    // 保存双方原始的 reset_count
    uint32_t original_source_reset_count = physical_src_zone->reset_count;
    uint32_t original_target_reset_count = physical_dst_zone->reset_count;

    // 新增：查找当前映射到 目标物理Zone 的 逻辑Zone ("受害者")
    // Added: Find the logical zone ("victim") currently mapped to the target physical zone
    uint32_t logical_dst_idx = -1;
    for (uint32_t i = 0; i < n->num_zones; i++) {
        if (zns->logical_to_physical_zone_map[i] == physical_dst_idx) {
            logical_dst_idx = i;
            break;
        }
    }

    // 健壮性检查
    if (logical_dst_idx == -1) {
        ftl_err("Move Error: No logical zone maps to target physical zone %u!\n", physical_dst_idx);
        printf("Move Error: No logical zone maps to target physical zone %u!\n", physical_dst_idx);
        return 0; // 失败
    }

    // 检查是否在尝试将一个Zone移动到自己（逻辑上）
    if (logical_dst_idx == logical_src_idx) {
         ftl_err("Move Error: Source and victim logical zones are the same (%u)!\n", logical_src_idx);
         printf("Move Error: Source and victim logical zones are the same (%u)!\n", logical_src_idx);
        return 0; // 失败
    }

    if (original_target_state != NVME_ZONE_STATE_EMPTY) {
        printf("Batched Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0; // 失败
    }

    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE / zns->lbasz;
    uint64_t start_lpn = (uint64_t)logical_src_idx * n->zone_size / secs_per_pg;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_valid_lpns = valid_lba_count / secs_per_pg;
    if (valid_lba_count > 0 && valid_lba_count % secs_per_pg != 0) num_valid_lpns++;

    if (num_valid_lpns == 0) {
        ftl_log("Batched Move: No valid data to move for logical zone %u.\n", logical_src_idx);
        zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
        zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;
        physical_dst_zone->w_ptr = physical_dst_zone->d.zslba;
        physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
        // 目标继承源 reset_count
        physical_dst_zone->reset_count = original_source_reset_count;
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
            zns_aor_inc_active(ns);
            // Since target was EMPTY, if source was OPEN, target becomes OPEN
            if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
                 zns_aor_inc_open(ns);
            }
        }
        zns_assign_zone_state(ns, physical_dst_zone, original_source_state); // Target inherits source state
        // Source state was already handled by assign_zone_state implicitly if it was in a list
        // 源继承目标 reset_count
        physical_src_zone->reset_count = original_target_reset_count;
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) {
             zns_aor_dec_open(ns); // Decrement because source is now EMPTY
        }
        if (original_source_state != NVME_ZONE_STATE_EMPTY) {
             zns_aor_dec_active(ns); // Decrement because source is now EMPTY
        }
         physical_src_zone->w_ptr = physical_src_zone->d.zslba; // Reset WP for source
         physical_src_zone->d.wp = physical_src_zone->w_ptr;
        zns_assign_zone_state(ns, physical_src_zone, original_target_state); // Source becomes EMPTY
        return 0; // 没有延迟
    }

    uint64_t max_read_lat = 0, max_write_lat = 0;
    /*
     * 新增：根据 requested_start_time 确定实际的 start_time
     * Added: Determine the actual start_time based on requested_start_time
    */
    uint64_t current_sim_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t start_time; // 替换原来的 'current_time'
    if (requested_start_time == 0 || current_sim_time > requested_start_time) {
        start_time = current_sim_time;
    } else {
        start_time = requested_start_time;
    }

    ftl_log("Start batched moving data for logical zone %u (phy %u -> %u) at %lu ns\n",
            logical_src_idx, physical_src_idx, physical_dst_idx, start_time);


    // 1. 读取阶段
    for (uint64_t i = 0; i < num_valid_lpns; i++) { // 只迭代有效 LPN 数量
        uint64_t lpn = start_lpn + i;
        if (lpn >= zns->l2p_sz) continue;
        struct ppa ppa = get_maptbl_ent(zns, lpn);
        if (mapped_ppa(&ppa) && ppa.g.blk == physical_src_idx) {
            /* 修改：使用新的 'start_time' */
            struct nand_cmd srd = { .cmd = NAND_READ, .type = GC_IO, .stime = start_time };
            uint64_t sublat = zns_advance_status(zns, &ppa, &srd);
            max_read_lat = (sublat > max_read_lat) ? sublat : max_read_lat;
        }
    }

    // 2. 写入阶段
    uint64_t write_start_time = start_time + max_read_lat;
    uint32_t source_active_zone = zns->active_zone; // 保存当前活动逻辑Zone索引
    zns->active_zone = logical_src_idx;
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;
    zns->logical_to_physical_zone_map[logical_dst_idx] = physical_src_idx;

    uint64_t lpn_offset = 0; // 跟踪实际写入的有效 LPN 数量
    while (lpn_offset < num_valid_lpns) {
        for(int p = 0; p < zns->num_plane; p++) {
            struct ppa ppa = get_new_page(zns, n);
            if (ppa.ppa == UNMAPPED_PPA) {
                 ftl_err("Batched Move: Failed to get new page in write phase.\n");
                 zns->logical_to_physical_zone_map[logical_src_idx] = physical_src_idx; // 恢复映射
                 zns->logical_to_physical_zone_map[logical_dst_idx] = physical_dst_idx; // 恢复映射
                 zns->active_zone = source_active_zone; // 恢复活动逻辑Zone索引
                 return 0; // 失败
            }
            ppa.g.pl = p;
            ppa.g.blk = physical_dst_idx;

            bool page_written = false;
            for(int j = 0; j < zns->flash_type ;j++) {
                ppa.g.pg = get_blk(zns,&ppa)->page_wp;
                for(int subpage = 0; subpage < ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE; subpage++) {
                    if (lpn_offset >= num_valid_lpns) break;
                    uint64_t lpn = start_lpn + lpn_offset; // 获取下一个有效 LPN
                     // 可以在此添加检查，确保只迁移有效的LPN
                     // Optional check: ensure only valid LPNs are migrated

                    ppa.g.spg = subpage;
                    set_maptbl_ent(zns, lpn, &ppa);
                    page_written = true;
                    lpn_offset++;
                }
                if (page_written) get_blk(zns,&ppa)->page_wp++;
                if (lpn_offset >= num_valid_lpns) break;
            }

            if (page_written) {
                struct nand_cmd swr = { .cmd = NAND_WRITE, .type = GC_IO, .stime = write_start_time };
                uint64_t sublat = zns_advance_status(zns, &ppa, &swr);
                max_write_lat = (sublat > max_write_lat) ? sublat : max_write_lat;
            }
            if (lpn_offset >= num_valid_lpns) break;
        }
        zns_advance_write_pointer(zns, n);
    }

    /* --- 更新目标和源物理Zone的元数据 (代码不变) --- */
     physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (num_valid_lpns * secs_per_pg); // 使用 num_valid_lpns
     physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
     physical_dst_zone->reset_count = original_source_reset_count;
     // AOR Inc
     if (original_source_state != NVME_ZONE_STATE_EMPTY) { 
        zns_aor_inc_active(ns);
        if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN) 
        zns_aor_inc_open(ns);
     }
        /*
        * 修改：直接将目标 Zone 状态设置为源 Zone 的原始状态。
        * Modified: Directly set the target Zone state to the original source state.
        */
        zns_assign_zone_state(ns, physical_dst_zone, original_source_state);

     // AOR Dec
     if (original_source_state == NVME_ZONE_STATE_IMPLICITLY_OPEN || original_source_state == NVME_ZONE_STATE_EXPLICITLY_OPEN)
      zns_aor_dec_open(ns);

     if (original_source_state != NVME_ZONE_STATE_EMPTY) 
     zns_aor_dec_active(ns);
     // 源继承目标的 reset_count
     physical_src_zone->reset_count = original_target_reset_count;
     physical_src_zone->w_ptr = physical_src_zone->d.zslba;
     physical_src_zone->d.wp = physical_src_zone->d.zslba;
     zns_assign_zone_state(ns, physical_src_zone, original_target_state);
    /* --- 元数据更新结束 --- */
    zns->active_zone = source_active_zone; // 恢复活动逻辑Zone索引

    /*
     * 修改：返回相对于 requested_start_time 的总延迟
     * (绝对完成时间 = start_time + max_read_lat + max_write_lat)
     * Modified: Return total latency relative to requested_start_time
     * (Absolute finish time = start_time + max_read_lat + max_write_lat)
     */
    uint64_t op_duration = max_read_lat + max_write_lat;
    uint64_t absolute_finish_time = start_time + op_duration;

    uint64_t return_latency = (absolute_finish_time > requested_start_time) ? (absolute_finish_time - requested_start_time) : 0;

    ftl_log("Finished batched moving data. (Req: %lu, Start: %lu, Finish: %lu) Total relative latency: %lu ns (R: %lu + W: %lu)\n",
            requested_start_time, start_time, absolute_finish_time, return_latency, max_read_lat, max_write_lat);
            
    return return_latency;
}


/* --- 均衡检查函数 --- */

/*
 * 最终版：zns_check_and_balance_super_devices 函数。
 * 查找超载SD上的逻辑冷Zone和轻载SD上的物理空Zone，调用批处理模式进行迁移。
 */
/*
static void zns_check_and_balance_super_devices(FemuCtrl *n)
{
    struct zns_ssd *zns = n->zns;
    bool use_batch = true; // 设置为使用批处理模式
    // 健壮性检查
    if (!zns || n->num_zones == 0 || zns->num_sd == 0) return;
    uint32_t zones_per_sd = n->num_zones / zns->num_sd;
    
    // 如果每个SD的Zone数少于1，则无法均衡
    if (zones_per_sd == 0) return;

    // uint32_t cold_zone_counts[zns->num_sd];
    uint32_t cold_zone_counts[2];//zns的超级设备数目固定为2
    int sd_above_thresh = -1, sd_below_thresh = -1;

    memset(cold_zone_counts, 0, sizeof(cold_zone_counts));

    // 统计每个物理SD上的冷Zone数量
    for (uint32_t i = 0; i < n->num_zones; i++) {
        NvmeZone *p_zone = &n->zone_array[i];
        int sd_idx = i % zns->num_sd;
        if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
            cold_zone_counts[sd_idx]++;
        }
    }

    // 寻找超载和轻载的SD
    for (int i = 0; i < zns->num_sd; i++) {
        uint32_t threshold = (zones_per_sd * CRITICAL_THRESHOLD_PERCENT) / 100;
        // 阈值至少为1，避免0阈值
        threshold = MAX(threshold, 1);
        if (cold_zone_counts[i] > threshold) sd_above_thresh = i;
        else if (sd_below_thresh == -1 || cold_zone_counts[i] < cold_zone_counts[sd_below_thresh]) sd_below_thresh = i;
    }

    if (sd_above_thresh != -1 && sd_below_thresh != -1 && sd_above_thresh != sd_below_thresh) {
        uint32_t logical_src_idx = -1;
        uint32_t physical_dst_idx = -1;

        // 寻找源逻辑冷Zone
        for (uint32_t i = 0; i < n->num_zones; i++) {
            uint32_t p_idx = zns->logical_to_physical_zone_map[i];
            if ((p_idx % zns->num_sd) == sd_above_thresh) {
                 NvmeZone *p_zone = &n->zone_array[p_idx];
                if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
                    logical_src_idx = i;
                    break;
                }
            }
        }

        // 寻找目标物理空Zone
        for (uint32_t i = 0; i < n->num_zones; i++) {
            if ((i % zns->num_sd) == sd_below_thresh) {
                 if (zns_get_zone_state(&n->zone_array[i]) == NVME_ZONE_STATE_EMPTY) {
                    physical_dst_idx = i;
                    break;
                }
            }
        }

        if (logical_src_idx != -1 && physical_dst_idx != -1) {
             ftl_log("Balancing triggered: Moving logical zone %u (from physical SD %d, phy_zone %u) to physical zone %u (on SD %d).\n",
                    logical_src_idx, sd_above_thresh, zns->logical_to_physical_zone_map[logical_src_idx],
                    physical_dst_idx, sd_below_thresh);
            // 调用批处理模式进行迁移 
            
            uint64_t latency; 
            if(use_batch){
            latency = zns_move_zone_data_batched(n, logical_src_idx, physical_dst_idx);
            }else{
            // 调用流水线模式进行迁移 
            zns_move_zone_data_pipelined(n, logical_src_idx, physical_dst_idx);
            }
            printf("Balancing completed with estimated latency: %lu ns\n", latency);
        } else {
             ftl_log("Balancing check: Could not find suitable source logical zone (%d) or destination physical zone (%d).\n",
                     logical_src_idx, physical_dst_idx);
        }
    }
}
*/

/*
 * 最终版 (修改后)：zns_check_and_balance_super_devices 函数。
 * 查找超载SD上的 *所有* 逻辑冷Zone和轻载SD上的 *所有* 物理空Zone，
 * 然后批量调用迁移函数，最多迁移 MAX_ZONES_TO_BALANCE_PER_CYCLE 个 Zone。
 */
static void zns_check_and_balance_super_devices(FemuCtrl *n)
{
    struct zns_ssd *zns = n->zns;
    bool use_batch = true; // 您仍然可以在这里切换批处理/流水线模式
    
    // 健壮性检查 (同前)
    if (!zns || n->num_zones == 0 || zns->num_sd == 0) return;
    uint32_t zones_per_sd = n->num_zones / zns->num_sd;
    if (zones_per_sd == 0) return;

    // zns->num_sd 固定为 2
    uint32_t cold_zone_counts[2];
    int sd_above_thresh = -1, sd_below_thresh = -1;

    memset(cold_zone_counts, 0, sizeof(cold_zone_counts));

    // 1. 统计每个物理SD上的冷Zone数量 (同前)
    for (uint32_t i = 0; i < n->num_zones; i++) {
        NvmeZone *p_zone = &n->zone_array[i];
        int sd_idx = i % zns->num_sd;
        // 判定为冷Zone：reset_count 低于阈值且 Zone 非空
        if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
            cold_zone_counts[sd_idx]++;
        }
    }

    // 2. 寻找超载和轻载的SD (同前)
    for (int i = 0; i < zns->num_sd; i++) {
        uint32_t threshold = (zones_per_sd * CRITICAL_THRESHOLD_PERCENT) / 100;
        threshold = MAX(threshold, 1);
        if (cold_zone_counts[i] > threshold) sd_above_thresh = i;
        else if (sd_below_thresh == -1 || cold_zone_counts[i] < cold_zone_counts[sd_below_thresh]) sd_below_thresh = i;
    }

    // 3. (***修改***) 寻找 *所有* 可迁移的Zone并批量执行
    if (sd_above_thresh != -1 && sd_below_thresh != -1 && sd_above_thresh != sd_below_thresh) {
        
        // 使用静态数组存储待迁移的Zone列表
        uint32_t source_zones[MAX_ZONES_TO_BALANCE_PER_CYCLE];
        uint32_t target_zones[MAX_ZONES_TO_BALANCE_PER_CYCLE];
        uint32_t source_count = 0;
        uint32_t target_count = 0;

        // 3a. 寻找 *所有* 源 (逻辑冷Zone)
        for (uint32_t i = 0; i < n->num_zones; i++) {
            if (source_count >= MAX_ZONES_TO_BALANCE_PER_CYCLE) break; // 防止数组溢出

            uint32_t p_idx = zns->logical_to_physical_zone_map[i];
            if ((p_idx % zns->num_sd) == sd_above_thresh) {
                 NvmeZone *p_zone = &n->zone_array[p_idx];
                 // 必须是冷Zone且非空
                if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
                    source_zones[source_count++] = i;
                }
            }
        }

        // 3b. 寻找 *所有* 目标 (物理空Zone)
        for (uint32_t i = 0; i < n->num_zones; i++) {
            if (target_count >= MAX_ZONES_TO_BALANCE_PER_CYCLE) break; // 防止数组溢出

            if ((i % zns->num_sd) == sd_below_thresh) {
                 if (zns_get_zone_state(&n->zone_array[i]) == NVME_ZONE_STATE_EMPTY) {
                    target_zones[target_count++] = i;
                }
            }
        }

        // 3c. 确定迁移数量并循环执行
        uint32_t num_to_migrate = MIN(source_count, target_count);
        // uint64_t total_latency = 0; // 累积总延迟
        if (num_to_migrate > 0) {
            
            uint64_t current_sim_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            uint64_t start_time_for_next_op; // 用于批处理模式
            uint64_t start_time_for_all_ops; // 用于流水线模式
            uint64_t total_accumulated_latency = 0; // 批处理模式：延迟总和
            uint64_t max_parallel_latency = 0;      // 流水线模式：最大延迟
            
            if (use_batch) {
                // 批处理模式：串行执行。下一个操作的开始时间是当前时间。
                start_time_for_next_op = current_sim_time;
                ftl_log("Balancing triggered (Batch Mode): Moving %u zones serially from SD %d to SD %d. Start time: %lu ns\n",
                        num_to_migrate, sd_above_thresh, sd_below_thresh, start_time_for_next_op);
            } else {
                // 流水线模式：并行执行。所有操作都在同一时间开始。
                start_time_for_all_ops = current_sim_time;
                ftl_log("Balancing triggered (Pipeline Mode): Moving %u zones in parallel from SD %d to SD %d. Start time: %lu ns\n",
                        num_to_migrate, sd_above_thresh, sd_below_thresh, start_time_for_all_ops);
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
                    duration = zns_move_zone_data_pipelined(n, logical_src_idx, physical_dst_idx, start_time_for_all_ops);
                    // 2. 记录最大延迟 (并行)
                    max_parallel_latency = MAX(max_parallel_latency, duration);
                }
            }

            // 循环外，根据模式打印总报告
            if (use_batch) {
                 printf("Balancing completed %u migrations (Batch Mode). Total accumulated (serial) latency: %lu ns\n",
                       num_to_migrate, total_accumulated_latency);
            } else {
                 // 流水线模式下，总延迟等于所有并行操作中，耗时最长的那个操作的延迟
                 printf("Balancing completed %u migrations (Pipeline Mode). Total operation (parallel) latency: %lu ns\n",
                       num_to_migrate, max_parallel_latency);
            }

        } else {
             ftl_log("Balancing check: Found %u sources and %u targets. No migration possible.\n",
                     source_count, target_count);
        }

    /*         if (num_to_migrate > 0) {
            ftl_log("Balancing triggered: Moving %u zones from SD %d to SD %d.\n",
                    num_to_migrate, sd_above_thresh, sd_below_thresh);
            
            for (uint32_t i = 0; i < num_to_migrate; i++) {
                uint32_t logical_src_idx = source_zones[i];
                uint32_t physical_dst_idx = target_zones[i];

                ftl_log("  -> Migrating logical %u (phy %u) to phy %u\n",
                        logical_src_idx, zns->logical_to_physical_zone_map[logical_src_idx], physical_dst_idx);

                uint64_t latency; 
                if(use_batch){
                    // 调用批处理模式
                    latency = zns_move_zone_data_batched(n, logical_src_idx, physical_dst_idx);
                }else{
                    // 调用流水线模式
                    latency = zns_move_zone_data_pipelined(n, logical_src_idx, physical_dst_idx);
                }
                // 假设迁移是串行执行的，累加总延迟
                total_latency += latency;
            }

            // 在FEMU控制台打印总报告
            printf("Balancing completed %u migrations. Total accumulated latency: %lu ns\n",
                   num_to_migrate, total_latency);
        } else {
             ftl_log("Balancing check: Found %u sources and %u targets. No migration possible.\n",
                     source_count, target_count);
        } */

    }
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
    bool use_batch = true; // 仍然可以在这里切换批处理/流水线模式
    
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
                uint64_t start_time_for_all_ops; // 用于流水线模式
                uint64_t total_accumulated_latency = 0; // 批处理模式：延迟总和
                uint64_t max_parallel_latency = 0;      // 流水线模式：最大延迟
                
                if (use_batch) {
                    start_time_for_next_op = current_sim_time;
                    ftl_log("Balancing triggered (Batch Mode, Threshold+Avg): Need %u, Migrating %u zones serially from SD %d (has %u) to SD %d (has %u). Start: %lu ns\n",
                            num_to_migrate_needed, num_to_migrate, sd_source, cold_zone_counts[sd_source], sd_target, cold_zone_counts[sd_target], start_time_for_next_op);
                } else {
                    start_time_for_all_ops = current_sim_time;
                    ftl_log("Balancing triggered (Pipeline Mode, Threshold+Avg): Need %u, Migrating %u zones in parallel from SD %d (has %u) to SD %d (has %u). Start: %lu ns\n",
                            num_to_migrate_needed, num_to_migrate, sd_source, cold_zone_counts[sd_source], sd_target, cold_zone_counts[sd_target], start_time_for_all_ops);
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
                        duration = zns_move_zone_data_pipelined(n, logical_src_idx, physical_dst_idx, start_time_for_all_ops);
                        // 2. 记录最大延迟 (并行)
                        max_parallel_latency = MAX(max_parallel_latency, duration);
                    }
                }

                // 循环外，根据模式打印总报告
                if (use_batch) {
                     printf("Balancing completed %u migrations (Batch Mode). Total accumulated (serial) latency: %lu ns\n",
                           num_to_migrate, total_accumulated_latency);
                } else {
                     // 流水线模式下，总延迟等于所有并行操作中，耗时最长的那个操作的延迟
                     printf("Balancing completed %u migrations (Pipeline Mode). Total operation (parallel) latency: %lu ns\n",
                           num_to_migrate, max_parallel_latency);
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
