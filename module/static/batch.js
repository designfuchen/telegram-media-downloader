(function () {
'use strict';

const statusText = {
  pending: '待手动加入', queued: '等待验证', resolving: '正在验证', imported: '已添加',
  not_joined: '尚未加入', transient_error: '临时连接失败', invalid: '链接失效', skipped: '已跳过'
};
let currentBatch = null;
let manifestSignature = null;
const panel = document.getElementById('panel-imports');
const $ = id => panel.querySelector('#' + id);
let refreshInFlight = false;
function escapeHtml(value) { return String(value || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
async function requestJson(url, options) {
  const response = await fetch(url, options);
  let data = {};
  try { data = await response.json(); } catch (_) { data = {}; }
  if (!response.ok) throw new Error(data.error || ('请求失败：' + response.status));
  return data;
}
function setMessage(text, kind) { const el = $('message'); el.textContent = text || ''; el.className = 'message ' + (kind || ''); }
function renderSummary(s) {
  $('summary').innerHTML =
    '<div class="stat"><span>已添加</span><b>' + Number(s.imported || 0) + '</b></div>' +
    '<div class="stat"><span>待处理</span><b>' + Number(s.pending || 0) + '</b></div>' +
    '<div class="stat"><span>验证中</span><b>' + Number(s.queued || 0) + '</b></div>' +
    '<div class="stat"><span>需处理</span><b>' + Number(s.failed || 0) + '</b></div>' +
    '<div class="stat"><span>总计</span><b>' + Number(s.total || 0) + '</b></div>';
}
function telegramLink(value) {
  try {
    const url = new URL(String(value || ''));
    return url.protocol === 'https:' && ['t.me', 'telegram.me'].includes(url.hostname) ? url.href : '';
  } catch (_) { return ''; }
}
function actionButtons(row) {
  const link = telegramLink(row.invite_link);
  const open = link ? '<a class="link-btn" href="' + escapeHtml(link) + '" target="_blank" rel="noopener noreferrer">打开 Telegram</a>' : '';
  if (row.status === 'imported' || row.status === 'skipped') return '';
  if (row.status === 'queued' || row.status === 'resolving') return '<button class="btn outline small" disabled>验证中</button>';
  const retry = '<button class="btn small retry-one" data-id="' + Number(row.order_no) + '">重新验证</button>';
  const skip = '<button class="btn outline small skip-one" data-id="' + Number(row.order_no) + '">跳过</button>';
  return open + retry + skip;
}
function renderBatch(batch, records) {
  currentBatch = batch;
  $('manual_invite_sheet').hidden = !Number(batch.total || 0);
  $('manual_invite_help').hidden = !Number(batch.total || 0);
  const done = Number(batch.imported || 0) + Number(batch.skipped || 0);
  const total = Number(batch.total || 0);
  $('batchTitle').textContent = total ? ('第 ' + batch.number + ' 批 · 固定 ' + total + ' 个频道') : '还没有邀请链接清单';
  $('batchScore').textContent = done + '/' + total;
  $('progress').style.width = (total ? Math.round(done / total * 100) : 0) + '%';
  $('verifyBtn').disabled = !batch.can_verify;
  $('retryAllBtn').hidden = !batch.can_retry;
  $('retryAllBtn').textContent = '重试临时失败的 ' + Number(batch.transient || 0) + ' 个';
  $('advanceBtn').disabled = !batch.can_advance;
  if (batch.complete && !batch.has_more) { $('advanceBtn').textContent = '全部批次已处理'; }
  else { $('advanceBtn').textContent = '我已确认下载完成，下一批'; }
  const signature = JSON.stringify(records);
  if (signature === manifestSignature) return;
  manifestSignature = signature;
  const box = $('manifest');
  if (!records.length) { box.innerHTML = '<div class="empty">队列中还没有邀请链接清单。</div>'; return; }
  box.innerHTML = records.map(function(row, index) {
    const error = row.error ? '<div class="error-note">' + escapeHtml(row.error) + (row.attempts ? ' · 已尝试 ' + Number(row.attempts) + ' 次' : '') + '</div>' : '';
    return '<article class="row" style="animation-delay:' + Math.min(index * 18, 220) + 'ms">' +
      '<div class="row-index">' + String(index + 1).padStart(2, '0') + '</div>' +
      '<div><div class="channel-title">' + escapeHtml(row.title || ('频道 #' + row.order_no)) + '</div><span class="channel-link">' + escapeHtml(row.invite_link) + '</span></div>' +
      '<div class="row-status"><span class="status ' + escapeHtml(row.status) + '">' + escapeHtml(statusText[row.status] || row.status) + '</span></div>' +
      '<div class="row-actions">' + actionButtons(row) + '</div>' + error + '</article>';
  }).join('');
}
async function refresh(silent) {
  if (refreshInFlight || panel.hidden) return;
  refreshInFlight = true;
  try {
    const results = await Promise.all([requestJson('/api/batch/summary'), requestJson('/api/batch/current')]);
    renderSummary(results[0]); renderBatch(results[1].batch, results[1].records || []);
  } catch (error) { setMessage(error.message, 'err'); } finally { refreshInFlight = false; }
}
async function runAction(button, url, payload, successText) {
  const old = button.textContent; button.disabled = true; button.textContent = '处理中…'; setMessage('');
  try {
    const data = await requestJson(url, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload || {})});
    setMessage(typeof successText === 'function' ? successText(data) : successText, 'ok');
  } catch (error) { setMessage(error.message, 'err'); }
  finally { button.textContent = old; button.disabled = false; await refresh(true); }
}
$('verifyBtn').addEventListener('click', function(){ runAction(this, '/api/batch/next', {}, d => '已提交 ' + d.dispatched + ' 个频道，后台正在验证并导入。'); });
$('retryAllBtn').addEventListener('click', function(){ runAction(this, '/api/batch/retry', {}, d => '已重新提交 ' + d.retried + ' 个临时失败频道。'); });
$('advanceBtn').addEventListener('click', function(){
  if (!confirm('请先在频道队列确认本批下载已经完成。现在进入下一批？')) return;
  runAction(this, '/api/batch/advance', {}, '已锁定下一批频道，请逐个手动加入 Telegram。');
});
$('manifest').addEventListener('click', function(event){
  const retry = event.target.closest('.retry-one'); const skip = event.target.closest('.skip-one');
  if (retry) runAction(retry, '/api/batch/retry', {order_no:Number(retry.dataset.id)}, '该频道已重新提交验证。');
  if (skip && confirm('确定跳过这个频道？跳过后会计入本批已处理。')) runAction(skip, '/api/batch/skip', {order_no:Number(skip.dataset.id)}, '已跳过该频道。');
});
window.addEventListener('tmd:viewchange', function(event) { if (event.detail === 'imports') refresh(false); });
setInterval(function(){ refresh(true); }, 8000);

})();
