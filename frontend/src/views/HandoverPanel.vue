<template>
  <section class="handover-panel" data-testid="handover-panel">
    <header class="handover-head">
      <div>
        <h3>交接流水线核对</h3>
        <p class="page-desc">
          数据流 {{ dataflow }}：规则版本发布、事件接纳、转换输出与关联/去重消费均按源日志位点排列。
        </p>
      </div>
      <div class="handover-stages">
        <span>已接纳 {{ stage.accepted }}</span>
        <span>挂起 {{ stage.hung }}</span>
        <span>已发布输出 {{ stage.published_outputs.length }}</span>
        <span>已消费 {{ stage.consumed_outputs.length }}</span>
      </div>
    </header>

    <p v-if="snapshot.pending_reason" class="handover-alert" data-testid="handover-pending-reason">
      待核原因：{{ snapshot.pending_reason }}
    </p>

    <dl class="handover-meta">
      <div><dt>交接点</dt><dd>{{ snapshot.handover?.交接编号 ?? '—' }} / {{ snapshot.handover?.status ?? '—' }}</dd></div>
      <div><dt>位点范围</dt><dd>{{ snapshot.handover?.位点范围 ?? '—' }}</dd></div>
      <div><dt>续做位点</dt><dd>{{ snapshot.handover?.续做位点 ?? '—' }}</dd></div>
      <div><dt>交接说明</dt><dd>{{ snapshot.handover?.冲突说明 ?? '—' }}</dd></div>
    </dl>

    <table class="data-table">
      <thead>
        <tr>
          <th>源端位点</th><th>事件类型</th><th>规则版本</th><th>列版本</th>
          <th>接入状态</th><th>输出状态</th><th>关联/去重</th><th>原日志位置</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in timeline" :key="row.offset" :data-testid="`handover-${row.offset}`">
          <td>{{ row.location }}</td>
          <td>{{ row.eventType }}</td>
          <td>{{ row.ruleVersion }}</td>
          <td>{{ row.columnVersion }}</td>
          <td>{{ row.ingestStatus }}</td>
          <td>{{ row.outputStatus }}<span v-if="row.blocked" class="blocked">（{{ row.blocked }}）</span></td>
          <td>{{ row.consumedStages.length ? row.consumedStages.join('、') : '未消费' }}</td>
          <td>{{ row.rawLocation }}</td>
        </tr>
        <tr v-if="!timeline.length">
          <td colspan="8" class="empty-state">暂无接入事件</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import { request } from '@/api/client'

type Snapshot = {
  dataflow: string
  events: Record<string, unknown>[]
  outputs: Record<string, unknown>[]
  consumptions: Record<string, unknown>[]
  pending_reason: string
  stage: {
    accepted: number
    hung: number
    published_outputs: string[]
    consumed_outputs: string[]
  }
  handover: Record<string, unknown> | null
}

const props = defineProps<{ dataflow: string }>()
const snapshot = ref<Snapshot>(blank())

function blank(): Snapshot {
  return {
    dataflow: '',
    events: [],
    outputs: [],
    consumptions: [],
    pending_reason: '',
    stage: { accepted: 0, hung: 0, published_outputs: [], consumed_outputs: [] },
    handover: null,
  }
}

const stage = computed(() => snapshot.value.stage)

const timeline = computed(() => {
  const outputs = new Map(snapshot.value.outputs.map((row) => [String(row.源端位点), row]))
  const consumed = new Map<string, string[]>()
  for (const row of snapshot.value.consumptions) {
    const key = String(row.源端位点)
    consumed.set(key, [...(consumed.get(key) ?? []), String(row.消费阶段)])
  }
  return snapshot.value.events.map((row) => {
    const location = String(row.源端位点)
    const output = outputs.get(location)
    return {
      offset: location,
      location,
      eventType: String(row.事件类型 ?? '—'),
      ruleVersion: String(row.规则版本 ?? '—'),
      columnVersion: String(row.列版本 ?? output?.列版本 ?? '—'),
      ingestStatus: String(row.status ?? '—'),
      outputStatus: String(output?.status ?? '未生成'),
      blocked: String(output?.消费阻塞原因 || ''),
      consumedStages: consumed.get(location) ?? [],
      rawLocation: String(output?.原日志位置 ?? row.原日志位置 ?? '—'),
    }
  })
})

async function load() {
  try {
    const response = await request(`/api/stream-handover/${props.dataflow}`)
    if (response.ok) snapshot.value = await response.json()
  } catch {
    snapshot.value = blank()
  }
}

watch(() => props.dataflow, load)
onMounted(load)
</script>

<style scoped>
.handover-panel { margin-top: 20px; }
.handover-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.handover-stages { display: flex; gap: 10px; font-size: 12px; color: var(--muted); flex-wrap: wrap; }
.handover-stages span { background: #fff; border: 1px solid var(--border); border-radius: 999px; padding: 4px 10px; }
.handover-alert { background: #fff7ed; border: 1px solid #fdba74; color: #9a3412; border-radius: 8px; padding: 8px 12px; font-size: 13px; }
.handover-meta { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.handover-meta dt { font-size: 12px; color: var(--muted); }
.handover-meta dd { margin: 2px 0 0; font-size: 13px; }
.blocked { color: #b45309; }
</style>
