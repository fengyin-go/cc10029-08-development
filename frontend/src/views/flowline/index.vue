<template>
  <section class="page" data-module="flowline">
    <header class="page-head">
      <div><h2>采集流程</h2><p class="page-desc">编排采集流程的多步骤序列，组合站点、提取和存储。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流程编排</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
        <button class="btn primary" type="button" :data-testid="`run-${selectedNode.id}`" @click="startRun(selectedNode.id)">执行流程</button>
      </div>
    </div>

    <section class="run-panel">
      <header class="run-panel-head">
        <h3>流程运行</h3>
        <button class="btn ghost" type="button" data-testid="reload-runs" @click="reloadRuns">刷新运行记录</button>
      </header>
      <div class="run-list" data-testid="run-list">
        <button
          v-for="run in runs"
          :key="run.执行编号"
          type="button"
          :class="['run-chip', { active: selectedRun?.执行编号 === run.执行编号 }]"
          :data-testid="`run-chip-${run.执行编号}`"
          @click="selectRun(run)"
        >
          {{ run.执行编号 }} · {{ run.执行状态 }} · {{ run.步骤回执数 }}/{{ run.步骤总数 }}
        </button>
        <span v-if="!runs.length" class="empty-state">暂无运行记录，选中流程节点后执行流程</span>
      </div>

      <div v-if="selectedRun" class="run-detail" data-testid="run-detail">
        <dl><dt>运行编号</dt><dd>{{ selectedRun.执行编号 }}</dd></dl>
        <dl><dt>执行状态</dt><dd>{{ selectedRun.执行状态 }}</dd></dl>
        <dl><dt>流程版本</dt><dd>{{ selectedRun.流程版本 }}</dd></dl>
        <dl><dt>完成步骤</dt><dd>{{ selectedRun.完成步骤数 }} / {{ selectedRun.步骤总数 }}</dd></dl>
        <dl><dt>失败步骤</dt><dd>{{ selectedRun.失败步骤 ?? '—' }}</dd></dl>
        <dl><dt>失败处理</dt><dd>{{ selectedRun.失败处理 ?? '—' }}</dd></dl>
        <dl v-if="selectedRun.失败原因" style="grid-column:1/-1"><dt>失败原因</dt><dd class="error-text">{{ selectedRun.失败原因 }}</dd></dl>
        <div style="grid-column:1/-1; display:flex; gap:6px; flex-wrap:wrap; margin-top:4px;">
          <button class="btn primary" type="button" data-testid="run-flow" @click="startRun(selectedRun.流程ID)">执行流程</button>
          <button class="btn" type="button" data-testid="resume-run" :disabled="selectedRun.执行状态 === '已完成'" @click="resumeRun(selectedRun)">
            从未完成步骤继续
          </button>
          <button class="btn" type="button" data-testid="land-batch" :disabled="selectedRun.执行状态 !== '已完成'" @click="landRun(selectedRun)">
            生成落地批次回执
          </button>
        </div>

        <h4 style="grid-column:1/-1; margin:8px 0 0;">步骤回执</h4>
        <table class="data-table" data-testid="receipt-table" style="grid-column:1/-1">
          <thead>
            <tr><th>步骤序号</th><th>步骤名称</th><th>环节类型</th><th>完成状态</th><th>凭据编号</th><th>输出目标</th><th>完成时间</th></tr>
          </thead>
          <tbody>
            <tr v-for="receipt in receipts" :key="receipt.凭据编号" :data-testid="`receipt-${receipt.步骤序号}`">
              <td>{{ receipt.步骤序号 }}</td>
              <td>{{ receipt.步骤名称 }}</td>
              <td>{{ receipt.环节类型 }}</td>
              <td>{{ receipt.完成状态 }}</td>
              <td>{{ receipt.凭据编号 }}</td>
              <td>{{ receipt.输出目标 }}</td>
              <td>{{ receipt.完成时间 }}</td>
            </tr>
            <tr v-if="!receipts.length"><td colspan="7" class="empty-state">暂无步骤回执</td></tr>
          </tbody>
        </table>
        <footer class="run-foot" data-testid="receipt-count" style="grid-column:1/-1">
          已落地 {{ receipts.length }} / {{ selectedRun.步骤总数 }} 份步骤回执
          <span v-if="validation">· 校验结论：{{ validation.校验结论 }}（{{ validation.步骤回执数 }} 份）</span>
        </footer>

        <div v-if="landing" class="landing-receipt" data-testid="landing-receipt" style="grid-column:1/-1">
          <dl><dt>落地批次号</dt><dd>{{ landing.落地批次号 }}</dd></dl>
          <dl><dt>流程版本</dt><dd>{{ landing.流程版本 }}</dd></dl>
          <dl><dt>输出凭据</dt><dd>{{ landing.输出凭据 }}</dd></dl>
          <dl><dt>校验结论</dt><dd>{{ landing.校验结论 }}</dd></dl>
          <div style="grid-column:1/-1">
            <span class="stat-label">步骤引用</span>
            <span v-for="ref in landing.步骤引用" :key="ref.凭据编号" class="step-ref">{{ ref.步骤序号 }}·{{ ref.凭据编号 }} → {{ ref.输出目标 }}</span>
          </div>
        </div>
      </div>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记流程编排</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span>{{ graph.nodes.length }} 个节点</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/flowline'
const columns = ["流程编号", "流程名称", "步骤序列", "输入站点", "输出目标", "失败处理", "触发条件", "版本号"]
const actions = ["提交校验", "启用配置", "停用配置"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const runs = ref<any[]>([])
const selectedRun = ref<any>(null)
const receipts = ref<any[]>([])
const validation = ref<any>(null)
const landing = ref<any>(null)
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function reloadRuns() {
  try {
    const res = await request(`${ENDPOINT}/runs`)
    const data = await res.json()
    runs.value = data.items || []
    if (selectedRun.value) {
      const fresh = runs.value.find((run: any) => run.执行编号 === selectedRun.value.执行编号)
      if (fresh) await selectRun(fresh)
    }
  } catch(e) { errorMessage.value = '运行记录读取失败' }
}
async function selectRun(run: any) {
  selectedRun.value = run
  landing.value = null
  validation.value = null
  try {
    const res = await request(`${ENDPOINT}/snapshots/${encodeURIComponent(run.执行编号)}/receipts`)
    if (res.ok) {
      const data = await res.json()
      receipts.value = data.items || []
    } else {
      receipts.value = []
    }
  } catch(e) { receipts.value = []; errorMessage.value = '步骤回执读取失败' }
  if (run.执行状态 === '已完成') {
    try {
      const [vRes, lRes] = await Promise.all([
        request(`${ENDPOINT}/snapshots/${encodeURIComponent(run.执行编号)}/validation`),
        request(`${ENDPOINT}/snapshots/${encodeURIComponent(run.执行编号)}/landing`),
      ])
      if (vRes.ok) validation.value = await vRes.json()
      if (lRes.ok) landing.value = await lRes.json()
    } catch(e) { /* 校验结论与落地回执按需刷新 */ }
  }
}
async function startRun(flowId: number) {
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${flowId}/run`, { method: 'POST', body: JSON.stringify({}) })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message || '流程执行失败')
    await reload()
    await reloadRuns()
    const executed = runs.value.find((run: any) => run.执行编号 === data.entry?.执行编号)
    if (executed) await selectRun(executed)
  } catch(e: any) { errorMessage.value = e.message; await reloadRuns() }
}
async function resumeRun(run: any) {
  errorMessage.value = ''
  try {
    const resumeFrom = Number(run.失败步骤 || run.完成步骤数 + 1)
    const res = await request(`${ENDPOINT}/${run.流程ID}/run`, {
      method: 'POST', body: JSON.stringify({ resume_from: resumeFrom }),
    })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message || '续跑失败')
    await reload()
    await reloadRuns()
    const executed = runs.value.find((item: any) => item.执行编号 === run.执行编号)
    if (executed) await selectRun(executed)
  } catch(e: any) { errorMessage.value = e.message; await reloadRuns() }
}
async function landRun(run: any) {
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/snapshots/${encodeURIComponent(run.执行编号)}/land`, { method: 'POST', body: '{}' })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message || '落地失败')
    landing.value = data.entry
    await selectRun(run)
  } catch(e: any) { errorMessage.value = e.message }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(async () => { await reload(); await reloadRuns() })
</script>
