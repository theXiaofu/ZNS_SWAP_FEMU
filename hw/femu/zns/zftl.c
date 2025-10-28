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
 * valid_ppa / mapped_ppa
 * 校验 ppa 是否落在设备几何范围内，以及是否已被映射（判定为 UNMAPPED_PPA）。
 */

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
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
        ppa.ppa = UNMAPPED_PPA;
    }
    return ppa;
}

static int zns_get_wcidx(struct zns_ssd* zns)
{
    int i;
    for(i = 0;i < zns->cache.num_wc;i++)
    {
        if(zns->cache.write_cache[i].sblk==zns->active_zone)
        {
            return i;
        }
    }
    return -1;
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


/*
 * 新增：批处理模式的数据迁移函数。
 * 先完成所有读操作，记录最大读延迟；然后基于此开始所有写操作，记录最大写延迟。
 * Added: Batched (non-pipelined) data migration function.
 * Completes all reads first, records max read latency; then starts all writes based on that, records max write latency.
*/
static uint64_t zns_move_zone_data_batched(FemuCtrl *n, uint32_t logical_src_idx, uint32_t physical_dst_idx)
{
    struct zns_ssd *zns = n->zns;
    uint32_t physical_src_idx = zns->logical_to_physical_zone_map[logical_src_idx];
    NvmeZone *physical_src_zone = &n->zone_array[physical_src_idx];
    NvmeZone *physical_dst_zone = &n->zone_array[physical_dst_idx];

    // 检查目标物理Zone是否为空
    if (zns_get_zone_state(physical_dst_zone) != NVME_ZONE_STATE_EMPTY) {
        ftl_err("Batched Move: Target physical zone %u is not empty!\n", physical_dst_idx);
        return 0;
    }

    // 计算LPN范围和数量
    uint64_t secs_per_pg = LOGICAL_PAGE_SIZE / zns->lbasz;
    uint64_t start_lpn = (uint64_t)logical_src_idx * n->zone_size / secs_per_pg;
    uint64_t valid_lba_count = physical_src_zone->d.wp - physical_src_zone->d.zslba;
    uint64_t num_lpns = valid_lba_count / secs_per_pg;
    if (valid_lba_count % secs_per_pg != 0) num_lpns++;

    uint64_t max_read_lat = 0, max_write_lat = 0;
    uint64_t current_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);

    ftl_log("Start batched moving data for logical zone %u (phy %u -> %u), LPNs: %lu\n",
            logical_src_idx, physical_src_idx, physical_dst_idx, num_lpns);

    // 1. 读取阶段：模拟读取所有LPN，找出最大延迟
    // 1. Read Phase: Simulate reading all LPNs, find the maximum latency
    for (uint64_t i = 0; i < num_lpns; i++) {
        struct ppa ppa = get_maptbl_ent(zns, start_lpn + i);
        // 读取旧映射中的有效数据
        if (mapped_ppa(&ppa) && ppa.g.blk == physical_src_idx) {
            // 所有读取操作假设同时开始
            struct nand_cmd srd = { .cmd = NAND_READ, .type = GC_IO, .stime = current_time };
            uint64_t sublat = zns_advance_status(zns, &ppa, &srd);
            max_read_lat = (sublat > max_read_lat) ? sublat : max_read_lat;
        }
    }

    // 2. 写入阶段：使用类似zns_write的批处理逻辑写入所有LPN
    // 2. Write Phase: Use batch processing logic similar to zns_write to write all LPNs
    uint64_t write_start_time = current_time + max_read_lat;
    // 确定目标物理Zone所属的超级设备
    int dst_sd_idx = physical_dst_idx % zns->num_sd;
    // FTL写操作的目标是逻辑Zone
    zns->active_zone = logical_src_idx;
    // 临时更新映射，让get_new_page指向目标物理Zone
    zns->logical_to_physical_zone_map[logical_src_idx] = physical_dst_idx;

    uint64_t lpn_offset = 0;
    while (lpn_offset < num_lpns)
    {
        // 模拟跨所有plane的并行写入
        for(int p = 0; p < zns->num_plane; p++)
        {
            // 获取一个PPA基地址，注意blk号应是目标物理Zone
            struct ppa ppa = get_new_page(zns, n);
            if (ppa.ppa == UNMAPPED_PPA) {
                ftl_err("Batched Move: Failed to get new page for LPN offset %lu\n", lpn_offset);
                 zns->logical_to_physical_zone_map[logical_src_idx] = physical_src_idx; // 恢复映射
                 return 0; // 失败
            }
            ppa.g.pl = p; // 设置plane号
            ppa.g.blk = physical_dst_idx; // 确保blk是目标物理Zone

            // 模拟多level cell技术和填充物理页
            for(int j = 0; j < zns->flash_type ;j++)
            {
                ppa.g.pg = get_blk(zns,&ppa)->page_wp; // 获取当前页号
                for(int subpage = 0; subpage < ZNS_PAGE_SIZE/LOGICAL_PAGE_SIZE; subpage++)
                {
                    if (lpn_offset >= num_lpns) break; // 所有LPN处理完毕
                    uint64_t lpn = start_lpn + lpn_offset;
                    ppa.g.spg = subpage;
                    // 更新映射表，将LPN指向新的PPA（位于目标物理Zone）
                    set_maptbl_ent(zns, lpn, &ppa);
                    lpn_offset++;
                }
                // 更新目标物理Zone的页写入指针
                get_blk(zns,&ppa)->page_wp++;
                if (lpn_offset >= num_lpns) break;
            }

            // 如果这个物理页确实写入了数据 (lpn_offset 在循环中增加了)
            if (ppa.g.V) // 或者检查 lpn_offset 是否真的移动了
            {
                // 发出一个NAND写命令并计算延迟，所有写入操作假设同时开始
                struct nand_cmd swr = { .cmd = NAND_WRITE, .type = GC_IO, .stime = write_start_time };
                uint64_t sublat = zns_advance_status(zns, &ppa, &swr);
                max_write_lat = (sublat > max_write_lat) ? sublat : max_write_lat;
            }

            if (lpn_offset >= num_lpns) break; // 如果所有LPN已处理，跳出plane循环
        }
        // 在完成一次跨所有plane的写入后，推进目标物理Zone所在SD的写指针
        zns_advance_write_pointer(zns, n);
    }

    // 恢复active_zone或设置为一个安全值 (可选)
    // zns->active_zone = -1; // Or some default

    // 3. 更新目标物理Zone的元数据
    physical_dst_zone->w_ptr = physical_dst_zone->d.zslba + (lpn_offset * secs_per_pg); // 使用实际写入的lpn数量
    physical_dst_zone->d.wp = physical_dst_zone->w_ptr;
    physical_dst_zone->reset_count = physical_src_zone->reset_count; // 继承reset_count
    if (physical_dst_zone->w_ptr >= zns_zone_wr_boundary(physical_dst_zone)) {
        zns_assign_zone_state(n->namespaces, physical_dst_zone, NVME_ZONE_STATE_FULL);
    } else {
        zns_assign_zone_state(n->namespaces, physical_dst_zone, NVME_ZONE_STATE_CLOSED);
    }

    // 4. 重置源物理Zone
    NvmeRequest fake_req = { .ns = n->namespaces };
    zns_aio_zone_reset_cb(&fake_req, physical_src_zone);

    ftl_log("Finished batched moving data. Total latency estimate: %lu ns (max_read %lu + max_write %lu)\n",
            max_read_lat + max_write_lat, max_read_lat, max_write_lat);
    return max_read_lat + max_write_lat;
}





/*
 * 最终合并版：zns_check_and_balance_super_devices 函数。
 * 此函数查找一个位于超载SD的逻辑冷Zone，和一个位于轻载SD的物理空Zone，然后调用数据迁移函数。
 * Final Merged Version: zns_check_and_balance_super_devices function.
 * This function finds a logical cold zone whose physical zone is on an overloaded SD, and a physical empty zone on an underloaded SD, then calls the data migration function.
*/
void zns_check_and_balance_super_devices(FemuCtrl *n)
{
    struct zns_ssd *zns = n->zns;
    uint32_t zones_per_sd = n->num_zones / zns->num_sd;
    uint32_t cold_zone_counts[zns->num_sd];
    int sd_above_thresh = -1, sd_below_thresh = -1;

    // 健壮性检查：确保num_sd有效
    if (zns->num_sd == 0) return; // 避免除零

    memset(cold_zone_counts, 0, sizeof(cold_zone_counts));

    // 统计每个物理SD上的冷Zone数量
    // Count cold zones on each physical SD
    for (uint32_t i = 0; i < n->num_zones; i++) {
        NvmeZone *p_zone = &n->zone_array[i];
        int sd_idx = i % zns->num_sd; // 物理Zone的索引决定了其所属SD
        if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
            cold_zone_counts[sd_idx]++;
        }
    }

    // 寻找超载和轻载的SD
    // Find overloaded and underloaded SDs
    for (int i = 0; i < zns->num_sd; i++) {
        uint32_t threshold = (zones_per_sd * CRITICAL_THRESHOLD_PERCENT) / 100;
        if (cold_zone_counts[i] > threshold) sd_above_thresh = i;
        else if (sd_below_thresh == -1 || cold_zone_counts[i] < cold_zone_counts[sd_below_thresh]) sd_below_thresh = i;
    }

    // 如果找到需要均衡的源SD和目标SD
    // If an overloaded source SD and an underloaded destination SD are found
    if (sd_above_thresh != -1 && sd_below_thresh != -1 && sd_above_thresh != sd_below_thresh) {
        uint32_t logical_src_idx = -1; // 要迁移的逻辑Zone索引
        uint32_t physical_dst_idx = -1; // 目标物理空Zone索引

        // 1. 寻找一个逻辑冷Zone，其当前映射的物理Zone位于超载SD上
        // 1. Find a logical cold zone whose currently mapped physical zone is on the overloaded SD
        for (uint32_t i = 0; i < n->num_zones; i++) {
            uint32_t p_idx = zns->logical_to_physical_zone_map[i];
            if ((p_idx % zns->num_sd) == sd_above_thresh) {
                NvmeZone *p_zone = &n->zone_array[p_idx];
                if (p_zone->reset_count < ZONE_RESET_THRESHOLD && zns_get_zone_state(p_zone) != NVME_ZONE_STATE_EMPTY) {
                    logical_src_idx = i;
                    break; // 找到第一个就够了 Find the first one
                }
            }
        }

        // 2. 寻找一个物理空Zone，其位于轻载SD上
        // 2. Find a physical empty zone located on the underloaded SD
        for (uint32_t i = 0; i < n->num_zones; i++) {
            if ((i % zns->num_sd) == sd_below_thresh) {
                if (zns_get_zone_state(&n->zone_array[i]) == NVME_ZONE_STATE_EMPTY) {
                    physical_dst_idx = i;
                    break; // 找到第一个就够了 Find the first one
                }
            }
        }

        // 如果成功找到源和目标
        // If both source and destination are successfully found
        if (logical_src_idx != -1 && physical_dst_idx != -1) {
            ftl_log("Balancing triggered: Moving logical zone %u (from physical SD %d) to physical zone %u (on SD %d).\n",
                    logical_src_idx, sd_above_thresh, physical_dst_idx, sd_below_thresh);
            // 执行数据迁移和映射更新
            zns_move_zone_data(n, logical_src_idx, physical_dst_idx);
        } else {
             ftl_log("Balancing check: Could not find suitable source logical zone or destination physical zone.\n");
        }
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
