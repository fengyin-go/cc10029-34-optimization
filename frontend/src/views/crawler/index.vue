<template>
  <section class="page" data-module="crawler">
    <header class="page-head">
      <div><h2>爬虫引擎</h2><p class="page-desc">管理爬虫任务的调度配置、并发度和批次参数，驱动采集引擎执行。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记爬虫任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <!-- 爬虫任务存量数据集：迁移、断点续跑、预检发布与版本取舍都在这条调用链上 -->
    <section class="stock-panel" data-testid="stock-panel">
      <header class="page-head">
        <div>
          <h3>爬虫任务存量数据集</h3>
          <p class="page-desc">把进程内存中的爬虫任务迁移为持久化存量数据集，重启后仍可连续追踪；任务版本、归属、已下发批次三者齐全才进入迁移完成范围。</p>
        </div>
        <div class="page-actions">
          <button class="btn primary" type="button" data-testid="stock-migrate" :disabled="stockBusy" @click="runMigration">迁移/续跑内存存量</button>
          <button class="btn" type="button" data-testid="stock-refresh" @click="reloadStock">刷新</button>
        </div>
      </header>

      <div class="stat-row" v-if="stockOverview.指针">
        <article class="stat-card" data-testid="pointer-card">
          <span class="stat-label">下游当前指针</span>
          <strong class="stat-value">{{ stockOverview.指针.数据集批次 }}</strong>
          <span class="stat-label">保留来源：{{ stockOverview.指针.保留来源 }}</span>
        </article>
      </div>
      <p v-if="stockMessage" class="error-text" data-testid="stock-message">{{ stockMessage }}</p>

      <h4>迁移批次与进度（中断后只续跑未完成记录）</h4>
      <table class="data-table" data-testid="migration-table">
        <thead>
          <tr><th>数据集批次</th><th>状态</th><th>已完成/总数</th><th>校验未通过</th><th>当前指针记录</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in stockOverview.迁移批次" :key="batch.数据集批次" :data-testid="`migration-row-${batch.数据集批次}`">
            <td>{{ batch.数据集批次 }}</td>
            <td>{{ batch.状态 }}</td>
            <td>{{ batch.已完成数 }}/{{ batch.存量总数 }}</td>
            <td>{{ batch.校验未通过数 }}</td>
            <td>{{ batch.当前指针 ?? '—' }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`precheck-${batch.数据集批次}`" @click="runPrecheck(batch.数据集批次)">依赖核对</button>
              <button class="link" type="button" :data-testid="`release-${batch.数据集批次}`" @click="runRelease(batch.数据集批次)">流水线发布</button>
              <button class="link" type="button" :data-testid="`detail-${batch.数据集批次}`" @click="openDetail(batch.数据集批次)">明细</button>
            </td>
          </tr>
          <tr v-if="!stockOverview.迁移批次?.length"><td colspan="6" class="empty-state">尚未生成存量数据集，点击「迁移/续跑内存存量」开始</td></tr>
        </tbody>
      </table>

      <h4>存量记录（任务编号、归属、下发批次迁移前后对照）</h4>
      <table class="data-table" data-testid="stock-table">
        <thead>
          <tr>
            <th v-for="column in stockColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in stockOverview.存量记录" :key="row.id" :data-testid="`stock-row-${row.id}`" :data-state="row.迁移状态">
            <td v-for="column in stockColumns" :key="column">{{ row[column] ?? '—' }}</td>
          </tr>
          <tr v-if="!stockOverview.存量记录?.length"><td :colspan="stockColumns.length" class="empty-state">暂无存量记录</td></tr>
        </tbody>
      </table>

      <h4>上线流水线检查（先核对依赖、构建环境与数据库，再发布）</h4>
      <table class="data-table" data-testid="pipeline-table">
        <thead><tr><th>数据集批次</th><th>阶段</th><th>结果</th><th>说明</th><th>检查时间</th></tr></thead>
        <tbody>
          <tr v-for="stage in stockOverview.流水线" :key="stage.id" :data-testid="`pipeline-row-${stage.id}`">
            <td>{{ stage.数据集批次 }}</td>
            <td>{{ stage.阶段 }}</td>
            <td>{{ stage.通过 ? '通过' : '未通过' }}</td>
            <td>{{ stage.说明 }}</td>
            <td>{{ stage.检查时间 }}</td>
          </tr>
          <tr v-if="!stockOverview.流水线?.length"><td colspan="5" class="empty-state">暂无流水线记录</td></tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记爬虫任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <div v-if="showDetail" class="modal-backdrop" data-testid="detail-modal">
      <div class="modal">
        <h3>数据集明细：{{ detailBatch }}</h3>
        <p v-if="detailConflict" class="error-text" data-testid="detail-conflict">
          检测到 {{ detailConflict.length }} 处版本冲突，发布时必须指定保留来源，下游只采用该来源的完整存量数据集。
        </p>
        <table class="data-table">
          <thead><tr><th>编号</th><th>任务编号</th><th>任务版本</th><th>归属</th><th>下发批次</th><th>迁移状态</th></tr></thead>
          <tbody>
            <tr v-for="row in detailRecords" :key="row.id" :data-testid="`detail-row-${row.id}`">
              <td>{{ row.id }}</td><td>{{ row.任务编号 }}</td><td>{{ row.任务版本 ?? '—' }}</td>
              <td>{{ row.归属 ?? '—' }}</td><td>{{ row.下发批次 ?? '—' }}</td><td>{{ row.迁移状态 }}</td>
            </tr>
          </tbody>
        </table>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showDetail=false">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/crawler'
const STOCK_ENDPOINT = '/api/crawler/stock-datasets'
const columns = ["任务编号", "任务名称", "关联站点", "关联规则", "并发线程", "调度周期", "超时秒数", "下发批次"]
const stockColumns = ["id", ...columns.slice(0, 1), "任务版本", "归属", ...columns.slice(-1), "来源", "保留来源", "数据集批次", "迁移状态"]
const actions = ["提交校验", "启用配置", "停用配置"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const stockOverview = ref<Record<string, any>>({ 迁移批次: [], 存量记录: [], 流水线: [] })
const stockBusy = ref(false)
const stockMessage = ref('')
const showDetail = ref(false)
const detailBatch = ref('')
const detailRecords = ref<any[]>([])
const detailConflict = ref<any[]>([])
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

async function reloadStock() {
  try {
    const res = await request(STOCK_ENDPOINT)
    if (!res.ok) throw new Error('存量数据集读取失败')
    stockOverview.value = await res.json()
  } catch (e: any) { stockMessage.value = e.message }
}

async function callAction(url: string, options?: RequestInit) {
  stockBusy.value = true
  stockMessage.value = ''
  try {
    const res = await request(url, options)
    const payload = await res.json()
    stockMessage.value = payload.message || (payload.ok ? '操作成功' : '操作失败')
    await reloadStock()
  } catch (e: any) {
    stockMessage.value = e.message
  } finally {
    stockBusy.value = false
  }
}

function runMigration() {
  return callAction(`${STOCK_ENDPOINT}/migration`, { method: 'POST' })
}

function runPrecheck(batch: string) {
  return callAction(`${STOCK_ENDPOINT}/${encodeURIComponent(batch)}/precheck`, { method: 'POST' })
}

async function runRelease(batch: string) {
  // 存在版本冲突时业务侧必须显式选择保留来源。
  let retainedSource: string | null = null
  try {
    const conflictRes = await request(`${STOCK_ENDPOINT}/${encodeURIComponent(batch)}/conflicts`)
    const conflictPayload = await conflictRes.json()
    const conflicts = conflictPayload.版本冲突 || []
    if (conflicts.length) {
      const sources = Array.from(new Set(conflicts.flatMap((c: any) => c.候选.map((x: any) => x.来源)))) as string[]
      const picked = window.prompt(`批次 ${batch} 存在版本冲突，请输入保留来源（${sources.join(' / ')}），下游只采用该来源：`, sources[0] || '')
      if (!picked) { stockMessage.value = '已取消发布：版本冲突必须指定保留来源'; return }
      retainedSource = picked
    }
  } catch {
    // 冲突查询失败不阻断，交给后端发布校验兜底
  }
  await callAction(`${STOCK_ENDPOINT}/${encodeURIComponent(batch)}/release`, {
    method: 'POST',
    body: JSON.stringify(retainedSource ? { retained_source: retainedSource } : {}),
  })
}

async function openDetail(batch: string) {
  detailBatch.value = batch
  try {
    const res = await request(`${STOCK_ENDPOINT}/${encodeURIComponent(batch)}`)
    const payload = await res.json()
    detailRecords.value = payload.存量记录 || []
    detailConflict.value = payload.版本冲突 || []
    showDetail.value = true
  } catch (e: any) { stockMessage.value = e.message }
}

onMounted(() => { void reload(); void reloadStock() })
</script>
