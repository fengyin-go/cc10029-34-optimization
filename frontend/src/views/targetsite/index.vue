<template>
  <section class="page" data-module="targetsite">
    <header class="page-head">
      <div>
        <h2>采集目标管理</h2>
        <p class="page-desc">维护采集目标站点的入口、协议约束和字符编码配置。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记目标站点</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出采集目标清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无采集目标数据，可先登记目标站点</td>
        </tr>
      </tbody>
    </table>

    <section class="snapshot-panel" data-testid="snapshot-panel">
      <header class="page-head">
        <div>
          <h3>目标站点执行快照清单</h3>
          <p class="page-desc">读取爬虫任务存量数据集生成执行快照，快照引用迁移后的任务归属、任务版本与已下发批次；重复生成只保留一份。</p>
        </div>
        <div class="page-actions">
          <button class="btn primary" data-testid="snapshot-generate" type="button" :disabled="snapshotBusy" @click="generateSnapshots">生成执行快照</button>
          <button class="btn" data-testid="snapshot-refresh" type="button" @click="reloadSnapshots">刷新</button>
        </div>
      </header>
      <p v-if="snapshotMessage" class="error-text" data-testid="snapshot-message">{{ snapshotMessage }}</p>
      <table class="data-table" data-testid="snapshot-table">
        <thead>
          <tr>
            <th v-for="column in snapshotColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="snap in snapshots" :key="String(snap.id)" :data-testid="`snapshot-row-${snap.id}`">
            <td v-for="column in snapshotColumns" :key="column">{{ snap[column] ?? '—' }}</td>
          </tr>
          <tr v-if="!snapshots.length">
            <td :colspan="snapshotColumns.length" class="empty-state">暂无执行快照，请先在爬虫引擎发布存量数据集后生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记目标站点</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集目标记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/targetsite'
const columns = ["站点编号", "站点名称", "入口URL", "请求方法", "请求头模板", "字符编码", "重试上限", "抓取间隔"]
const actions = ["提交校验", "启用配置", "停用配置"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const metricNames = ["目标站点总数", "待校验目标站点", "已启用目标站点"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'targetsite.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集目标动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集目标操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('目标站点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集目标列表读取失败'
  }
}

const SNAPSHOT_ENDPOINT = '/api/targetsite/snapshots'
const snapshotColumns = ['快照编号', '站点编号', '站点名称', '任务编号', '任务版本', '归属', '下发批次', '数据集批次', '状态', '生成时间']
const snapshots = ref<Row[]>([])
const snapshotBusy = ref(false)
const snapshotMessage = ref('')

async function reloadSnapshots() {
  snapshotMessage.value = ''
  try {
    const response = await request(SNAPSHOT_ENDPOINT)
    if (!response.ok) throw new Error('执行快照清单读取失败')
    const payload = await response.json()
    snapshots.value = payload.items ?? []
  } catch (error) {
    snapshotMessage.value = error instanceof Error ? error.message : '执行快照清单读取失败'
  }
}

async function generateSnapshots() {
  snapshotBusy.value = true
  snapshotMessage.value = ''
  try {
    const response = await request(`${SNAPSHOT_ENDPOINT}/generate`, { method: 'POST' })
    const payload = await response.json()
    snapshotMessage.value = payload.message || (payload.ok ? '快照已生成' : '快照生成失败')
    await reloadSnapshots()
  } catch (error) {
    snapshotMessage.value = error instanceof Error ? error.message : '快照生成失败'
  } finally {
    snapshotBusy.value = false
  }
}

onMounted(() => {
  void reload()
  void reloadSnapshots()
})
</script>
