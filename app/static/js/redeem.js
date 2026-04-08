// 用户兑换页面JavaScript

// HTML转义函数 - 防止XSS攻击
function escapeHtml(unsafe) {
    if (unsafe === null || unsafe === undefined) {
        return '';
    }
    return String(unsafe)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// 全局变量
let currentEmail = '';
let currentCode = '';
let availableTeams = [];
let selectedTeamId = null;
let currentTopTab = 'redeem';

// Toast提示函数
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    if (!toast) return;

    let icon = 'info';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'alert-circle';

    toast.innerHTML = `<i data-lucide="${icon}"></i><span>${escapeHtml(message)}</span>`;
    toast.className = `toast ${type} show`;

    if (window.lucide) {
        lucide.createIcons();
    }

    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}


function escapeForHtml(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function renderMarkdownSafe(markdownText) {
    const escaped = escapeForHtml(markdownText || '');
    const lines = escaped.split(/\r?\n/);
    let html = '';
    let inList = false;

    const applyInline = (line) => line
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        .replace(/`([^`]+)`/g, '<code>$1</code>')
        .replace(/\[(.+?)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');

    for (const rawLine of lines) {
        const line = rawLine.trim();

        if (!line) {
            if (inList) {
                html += '</ul>';
                inList = false;
            }
            continue;
        }

        const heading = line.match(/^(#{1,3})\s+(.*)$/);
        if (heading) {
            if (inList) {
                html += '</ul>';
                inList = false;
            }
            const level = heading[1].length;
            html += `<h${level}>${applyInline(heading[2])}</h${level}>`;
            continue;
        }

        const bullet = line.match(/^[-*]\s+(.*)$/);
        if (bullet) {
            if (!inList) {
                html += '<ul>';
                inList = true;
            }
            html += `<li>${applyInline(bullet[1])}</li>`;
            continue;
        }

        if (inList) {
            html += '</ul>';
            inList = false;
        }
        html += `<p>${applyInline(line)}</p>`;
    }

    if (inList) html += '</ul>';
    return html || '<p>暂无公告内容</p>';
}

function extractErrorText(payload) {
    if (payload === null || payload === undefined) return '';
    if (typeof payload === 'string') return payload;

    if (Array.isArray(payload)) {
        return payload
            .map(item => {
                if (!item) return '';
                if (typeof item === 'string') return item;
                if (item.msg) return String(item.msg);
                if (item.detail !== undefined) return extractErrorText(item.detail);
                try {
                    return JSON.stringify(item);
                } catch (_) {
                    return String(item);
                }
            })
            .filter(Boolean)
            .join('；');
    }

    if (typeof payload === 'object') {
        if (payload.detail !== undefined) return extractErrorText(payload.detail);
        if (payload.error !== undefined) return extractErrorText(payload.error);
        if (payload.message !== undefined) return extractErrorText(payload.message);
        if (payload.msg !== undefined) return extractErrorText(payload.msg);
        if (payload.reason !== undefined) return extractErrorText(payload.reason);
        try {
            return JSON.stringify(payload);
        } catch (_) {
            return String(payload);
        }
    }

    return String(payload);
}

function normalizeRawErrorMessage(rawMessage) {
    let message = extractErrorText(rawMessage).trim();
    if (!message) return '';

    for (let i = 0; i < 2; i++) {
        const trimmed = message.trim();
        if (!trimmed || (trimmed[0] !== '{' && trimmed[0] !== '[')) break;

        try {
            const parsed = JSON.parse(trimmed);
            const extracted = extractErrorText(parsed).trim();
            if (!extracted || extracted === trimmed) break;
            message = extracted;
        } catch (_) {
            break;
        }
    }

    return message.replace(/\s+/g, ' ').trim();
}

function isTechnicalLogMessage(message) {
    const normalized = String(message || '').trim();
    if (!normalized) return false;

    const lower = normalized.toLowerCase();
    if (normalized.length > 220) return true;

    const technicalKeywords = [
        'traceback',
        'exception',
        'stack',
        'sqlalchemy',
        'asyncsession',
        'httpx',
        'aiohttp',
        'error_code',
        'status_code',
        'file "',
        'line ',
        'detail:',
        'db_session',
        'invite_res',
        'redeem_flow'
    ];

    return technicalKeywords.some(keyword => lower.includes(keyword));
}

function getFriendlyRedeemErrorMessage(rawMessage, statusCode = 0) {
    const message = normalizeRawErrorMessage(rawMessage);
    const lower = message.toLowerCase();
    const includesAny = (...keywords) => keywords.some(keyword => lower.includes(String(keyword).toLowerCase()));

    if (includesAny('兑换失败次数过多')) {
        const marker = '最后报错:';
        const index = message.lastIndexOf(marker);
        if (index !== -1) {
            const lastError = message.slice(index + marker.length).trim();
            if (lastError && lastError !== message) {
                return getFriendlyRedeemErrorMessage(lastError, statusCode);
            }
        }
        return '请求重试次数过多，请稍后再试';
    }

    if (
        includesAny('value is not a valid email', 'invalid email', '邮箱格式', 'email address is not valid') ||
        (includesAny('field required', 'missing') && includesAny('email'))
    ) {
        return '邮箱格式不正确，请检查后重试';
    }

    if (includesAny('field required', 'missing') && includesAny('code', '兑换码')) {
        return '兑换码不能为空，请重新输入';
    }

    if (includesAny('兑换码不存在', 'invalid code', 'code not found', 'invalid redemption code')) {
        return '兑换码不存在或输入有误，请检查后重试';
    }

    if (includesAny('兑换码已被使用', '兑换码已使用', 'already used')) {
        return '该兑换码已使用，不能重复兑换';
    }

    if (includesAny('质保已过期')) {
        return '该兑换码质保已过期，无法再次兑换';
    }
    if (includesAny('质保次数已用完')) {
        return '该兑换码的质保次数已用完，无法再次兑换';
    }

    if (includesAny('兑换码已过期', '超过首次兑换截止时间', 'expired')) {
        return '兑换码已过期，请联系管理员更换新码';
    }

    if (includesAny('次数已用完', 'limit reached', 'no remaining')) {
        return '该兑换码可用次数已用完，请联系管理员';
    }

    if (includesAny('兑换码已失效', '最新福利通用兑换码')) {
        return '该兑换码已失效，请使用最新兑换码';
    }

    if (includesAny('已在 team', 'already in workspace', 'already in team', 'already a member')) {
        return '该邮箱已在所选 Team 中，当前兑换码不会被消耗，请改选其他 Team';
    }

    if (includesAny('已加入所有可用 team', '没有新的可用 team')) {
        return '您已加入当前所有可用 Team，当前兑换码不会被消耗';
    }

    if (includesAny('没有可用的 team')) {
        return '当前没有可用 Team 席位，请稍后重试';
    }

    if (includesAny('席位已满', 'maximum number of seats', 'no seats', 'team 已满', 'team已满', '请选择其他 team', ' full')) {
        return '所选 Team 席位已满，请选择其他 Team 重试';
    }

    if (includesAny('账号受限', '风控', '账单', 'billing', 'restricted', 'blocked')) {
        return '目标 Team 当前状态异常（可能账单或风控限制），请联系管理员处理';
    }

    if (
        includesAny('token', 'access token', 'session token') &&
        includesAny('过期', '失效', 'invalid', 'expired', 'invalidated')
    ) {
        return 'Team 登录状态已失效，请联系管理员刷新 Token 后重试';
    }

    if (includesAny('获取 team 访问权限失败')) {
        return '无法获取 Team 访问权限，请稍后重试或联系管理员';
    }

    if (includesAny('服务器响应格式错误', 'cannot parse', 'json')) {
        return '服务器返回异常，请稍后重试';
    }

    if (includesAny('proxy', 'connection', 'timeout', 'timed out', 'network', '连接', 'dns', 'ssl', 'socket')) {
        return '网络连接异常，请稍后重试';
    }

    if (statusCode === 409) {
        return 'Team 状态发生变化（如席位已满），请重试或选择其他 Team';
    }

    if (statusCode >= 500) {
        return '服务器繁忙，请稍后重试';
    }

    if (!message) {
        return statusCode >= 500 ? '服务器繁忙，请稍后重试' : '兑换失败，请稍后重试';
    }

    if (isTechnicalLogMessage(message)) {
        return statusCode >= 500 ? '服务器繁忙，请稍后重试' : '请求处理失败，请稍后重试或联系管理员';
    }

    return message;
}

function getFriendlyWarrantyErrorMessage(rawMessage, statusCode = 0) {
    const message = normalizeRawErrorMessage(rawMessage);
    const lower = message.toLowerCase();
    const includesAny = (...keywords) => keywords.some(keyword => lower.includes(String(keyword).toLowerCase()));

    if (includesAny('必须提供邮箱或兑换码')) {
        return '请输入兑换码或邮箱后再查询';
    }

    if (includesAny('查询太频繁')) {
        const waitMatch = message.match(/(\d+)\s*秒/);
        if (waitMatch) {
            return `查询过于频繁，请 ${waitMatch[1]} 秒后再试`;
        }
        return '查询过于频繁，请稍后再试';
    }

    if (includesAny('未登录', 'api key 无效', 'unauthorized', 'forbidden')) {
        return '登录状态已失效，请刷新页面后重试';
    }

    if (includesAny('兑换码不存在')) {
        return '未找到该兑换码，请检查输入是否正确';
    }

    if (includesAny('未找到兑换记录', '未找到相关记录')) {
        return '未找到相关记录，请确认邮箱或兑换码是否正确';
    }

    if (includesAny('质保已过期')) {
        return '该兑换码质保已过期';
    }
    if (includesAny('质保次数已用完')) {
        return '该兑换码质保次数已用完';
    }

    if (includesAny('服务器响应格式错误', 'cannot parse', 'json')) {
        return '服务器返回异常，请稍后重试';
    }

    if (includesAny('proxy', 'connection', 'timeout', 'timed out', 'network', '连接', 'dns', 'ssl', 'socket')) {
        return '网络连接异常，请稍后重试';
    }

    if (statusCode === 429) {
        return '查询过于频繁，请稍后再试';
    }

    if (statusCode === 401 || statusCode === 403) {
        return '登录状态已失效，请刷新页面后重试';
    }

    if (statusCode >= 500) {
        return '系统繁忙，请稍后重试';
    }

    if (!message) {
        return '查询失败，请稍后重试';
    }

    if (isTechnicalLogMessage(message)) {
        return statusCode >= 500 ? '系统繁忙，请稍后重试' : '查询失败，请稍后重试';
    }

    return message;
}

function getFriendlyDeviceAuthErrorMessage(rawMessage, statusCode = 0) {
    const message = normalizeRawErrorMessage(rawMessage);
    const lower = message.toLowerCase();
    const includesAny = (...keywords) => keywords.some(keyword => lower.includes(String(keyword).toLowerCase()));

    if (includesAny('开启设备身份验证失败:')) {
        const marker = '开启设备身份验证失败:';
        const index = message.lastIndexOf(marker);
        if (index !== -1) {
            const innerMessage = message.slice(index + marker.length).trim();
            if (innerMessage && innerMessage !== message) {
                return getFriendlyDeviceAuthErrorMessage(innerMessage, statusCode);
            }
        }
    }

    if (includesAny('未登录', 'api key 无效', 'unauthorized', 'forbidden')) {
        return '登录状态已失效，请先登录管理员后重试';
    }

    if (includesAny('team id', 'team 不存在', '目标 team', 'not found')) {
        return '目标 Team 不存在或已失效，请刷新后重试';
    }

    if (
        includesAny('token', 'access token', 'session token') &&
        includesAny('过期', '失效', 'invalid', 'expired', 'invalidated')
    ) {
        return 'Team 登录状态已失效，请先在后台刷新 Token';
    }

    if (includesAny('服务器响应格式错误', 'cannot parse', 'json')) {
        return '服务器返回异常，请稍后重试';
    }

    if (includesAny('proxy', 'connection', 'timeout', 'timed out', 'network', '连接', 'dns', 'ssl', 'socket')) {
        return '网络连接异常，请稍后重试';
    }

    if (statusCode === 401 || statusCode === 403) {
        return '登录状态已失效，请先登录管理员后重试';
    }

    if (statusCode >= 500) {
        return '开启失败，请稍后重试';
    }

    if (!message) {
        return statusCode >= 500 ? '开启失败，请稍后重试' : '开启失败，请重试';
    }

    if (isTechnicalLogMessage(message)) {
        return statusCode >= 500 ? '开启失败，请稍后重试' : '开启失败，请重试';
    }

    return message;
}

function initAnnouncementModal() {
    const announcement = window.REDEEM_ANNOUNCEMENT || {};
    if (!announcement.enabled || !announcement.markdown || !String(announcement.markdown).trim()) {
        return;
    }

    const modal = document.getElementById('announcementModal');
    const content = document.getElementById('announcementContent');
    const closeBtn = document.getElementById('announcementCloseBtn');
    const confirmBtn = document.getElementById('announcementConfirmBtn');
    const backdrop = document.getElementById('announcementBackdrop');

    if (!modal || !content) return;

    content.innerHTML = renderMarkdownSafe(String(announcement.markdown));
    modal.classList.add('show');
    modal.setAttribute('aria-hidden', 'false');

    const closeModal = () => {
        modal.classList.remove('show');
        modal.setAttribute('aria-hidden', 'true');
    };

    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (confirmBtn) confirmBtn.addEventListener('click', closeModal);
    if (backdrop) backdrop.addEventListener('click', closeModal);
}

// 切换步骤
function showStep(stepNumber) {
    document.querySelectorAll('.step').forEach(step => {
        step.classList.remove('active');
        step.style.display = ''; // 清除内联样式，交由CSS类控制显隐
    });
    const targetStep = document.getElementById(`step${stepNumber}`);
    if (targetStep) {
        targetStep.classList.add('active');
    }
}

function updateTabIndicator(activeTab) {
    const indicator = document.getElementById('tabIndicator');
    if (!indicator || !activeTab) return;

    indicator.style.left = `${activeTab.offsetLeft}px`;
    indicator.style.width = `${activeTab.offsetWidth}px`;
}

function switchTopTab(tabName) {
    currentTopTab = tabName;

    const redeemPanel = document.getElementById('redeemPanel');
    const warrantyPanel = document.getElementById('warrantyPanel');
    const tabRedeem = document.getElementById('tabRedeem');
    const tabWarranty = document.getElementById('tabWarranty');

    if (redeemPanel) redeemPanel.classList.toggle('active', tabName === 'redeem');
    if (warrantyPanel) warrantyPanel.classList.toggle('active', tabName === 'warranty');
    if (tabRedeem) tabRedeem.classList.toggle('active', tabName === 'redeem');
    if (tabWarranty) tabWarranty.classList.toggle('active', tabName === 'warranty');

    updateTabIndicator(tabName === 'redeem' ? tabRedeem : tabWarranty);
}

// 返回步骤1
function backToStep1() {
    showStep(1);
    switchTopTab('redeem');
    selectedTeamId = null;
}

document.addEventListener('DOMContentLoaded', () => {
    const tabRedeem = document.getElementById('tabRedeem');
    const tabWarranty = document.getElementById('tabWarranty');

    if (tabRedeem) tabRedeem.addEventListener('click', () => switchTopTab('redeem'));
    if (tabWarranty) tabWarranty.addEventListener('click', () => switchTopTab('warranty'));

    switchTopTab('redeem');
    initAnnouncementModal();
    window.addEventListener('resize', () => {
        const activeTab = document.querySelector('.top-tab.active');
        updateTabIndicator(activeTab);
    });
});

// 步骤1: 验证兑换码并直接兑换
document.getElementById('verifyForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const email = document.getElementById('email').value.trim();
    const code = document.getElementById('code').value.trim();
    const verifyBtn = document.getElementById('verifyBtn');

    // 验证
    if (!email || !code) {
        showToast('请填写完整信息', 'error');
        return;
    }

    // 保存到全局变量
    currentEmail = email;
    currentCode = code;

    // 禁用按钮
    verifyBtn.disabled = true;
    verifyBtn.innerHTML = '<i data-lucide="loader-circle" class="spinning"></i> 正在兑换...';
    if (window.lucide) lucide.createIcons();

    // 直接调用兑换接口 (team_id = null 表示自动选择)
    await confirmRedeem(null);

    // 恢复按钮状态 (如果 confirmRedeem 失败并显示了错误也没关系，因为用户可以点返回重试)
    verifyBtn.disabled = false;
    verifyBtn.innerHTML = '<i data-lucide="shield-check"></i> 验证并激活兑换码';
    if (window.lucide) lucide.createIcons();
});

// 渲染Team列表
function renderTeamsList() {
    const teamsList = document.getElementById('teamsList');
    teamsList.innerHTML = '';

    availableTeams.forEach(team => {
        const teamCard = document.createElement('div');
        teamCard.className = 'team-card';
        teamCard.onclick = () => selectTeam(team.id, teamCard);

        const planBadge = team.subscription_plan === 'Plus' ? 'badge-plus' : 'badge-pro';

        teamCard.innerHTML = `
            <div class="team-name">${escapeHtml(team.team_name) || 'Team ' + team.id}</div>
            <div class="team-info">
                <div class="team-info-item">
                    <i data-lucide="users" style="width: 14px; height: 14px;"></i>
                    <span>${team.current_members}/${team.max_members} 成员</span>
                </div>
                <div class="team-info-item">
                    <span class="team-badge ${planBadge}">${escapeHtml(team.subscription_plan) || 'Plus'}</span>
                </div>
                ${team.expires_at ? `
                <div class="team-info-item">
                    <i data-lucide="calendar" style="width: 14px; height: 14px;"></i>
                    <span>到期: ${formatDate(team.expires_at)}</span>
                </div>
                ` : ''}
            </div>
        `;

        teamsList.appendChild(teamCard);
        if (window.lucide) lucide.createIcons();
    });
}

// 选择Team
function selectTeam(teamId, teamCard) {
    selectedTeamId = teamId;

    // 更新UI
    document.querySelectorAll('.team-card').forEach(card => {
        card.classList.remove('selected');
    });
    if (teamCard) {
        teamCard.classList.add('selected');
    }

    // 立即确认兑换
    confirmRedeem(teamId);
}

// 自动选择Team
function autoSelectTeam() {
    if (availableTeams.length === 0) {
        showToast('没有可用的 Team', 'error');
        return;
    }

    // 自动选择第一个Team(后端会按过期时间排序)
    confirmRedeem(null);
}

// 确认兑换
async function confirmRedeem(teamId) {
    console.log('Starting redemption process, teamId:', teamId);

    // Safety check: Ensure confirmRedeem doesn't run if already running? 
    // The button disable logic handles that.

    try {
        const response = await fetch('/redeem/confirm', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                email: currentEmail,
                code: currentCode,
                team_id: teamId
            })
        });

        console.log('Response status:', response.status);

        let data;
        const text = await response.text();
        try {
            data = JSON.parse(text);
        } catch (e) {
            console.error('Failed to parse response JSON:', text);
            throw new Error('服务器响应格式错误');
        }

        if (response.ok && data.success) {
            // 兑换成功
            console.log('Redemption success');
            showSuccessResult(data);
        } else {
            // 兑换失败
            console.warn('Redemption failed:', data);

            const rawError = (data && (data.detail ?? data.error ?? data.message ?? data.reason)) || text;
            const errorMessage = getFriendlyRedeemErrorMessage(rawError, response.status);
            showErrorResult(errorMessage);
        }
    } catch (error) {
        console.error('Network or logic error:', error);
        const errorMessage = getFriendlyRedeemErrorMessage(error?.message || '');
        showErrorResult(errorMessage || '网络错误，请稍后重试');
    }
}

// 显示成功结果
function showSuccessResult(data) {
    const resultContent = document.getElementById('resultContent');
    const teamInfo = data.team_info || {};

    resultContent.innerHTML = `
        <div class="result-success">
            <div class="result-icon"><i data-lucide="check-circle" style="width: 64px; height: 64px; color: var(--success);"></i></div>
            <div class="result-title">兑换成功!</div>
            <div class="result-message">${escapeHtml(data.message) || '您已成功加入 Team'}</div>

            <div class="result-details">
                <div class="result-detail-item">
                    <span class="result-detail-label">Team 名称</span>
                    <span class="result-detail-value">${escapeHtml(teamInfo.team_name) || '-'}</span>
                </div>
                <div class="result-detail-item">
                    <span class="result-detail-label">邮箱地址</span>
                    <span class="result-detail-value">${escapeHtml(currentEmail)}</span>
                </div>
                ${teamInfo.expires_at ? `
                <div class="result-detail-item">
                    <span class="result-detail-label">到期时间</span>
                    <span class="result-detail-value">${formatDate(teamInfo.expires_at)}</span>
                </div>
                ` : ''}
            </div>

            <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 2rem; background: rgba(255,255,255,0.05); padding: 1rem; border-radius: 8px; text-align: left;">
                <i data-lucide="mail" style="width: 16px; height: 16px; vertical-align: middle; margin-right: 5px;"></i>
                邀请邮件已发送到您的邮箱，请查收并按照邮件指引接受邀请。
            </p>

            <div style="margin-bottom: 2rem; border-top: 1px solid var(--border-base); padding-top: 1.5rem;">
                <p style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 1rem;">
                    <strong>没收到邀请邮件？</strong><br>
                    如果您在 1-5 分钟后仍未收到邮件（或被拦截），请前往“质保查询”进行自助修复。
                </p>
                <button onclick="goToWarrantyFromSuccess()" class="btn btn-secondary" style="width: 100%; border-style: dashed;">
                    <i data-lucide="shield"></i> 前往质保查询 / 自助修复
                </button>
            </div>

            <button onclick="location.reload()" class="btn btn-primary" style="width: 100%;">
                <i data-lucide="refresh-cw"></i> 再次兑换
            </button>
        </div>
    `;
    if (window.lucide) lucide.createIcons();

    showStep(3);
}

// 显示错误结果
function showErrorResult(errorMessage) {
    const resultContent = document.getElementById('resultContent');

    resultContent.innerHTML = `
        <div class="result-error">
            <div class="result-icon"><i data-lucide="x-circle" style="width: 64px; height: 64px; color: var(--danger);"></i></div>
            <div class="result-title">兑换失败</div>
            <div class="result-message">${escapeHtml(errorMessage)}</div>

            <div style="display: flex; gap: 1rem; justify-content: center; margin-top: 2rem;">
                <button onclick="backToStep1()" class="btn btn-secondary">
                    <i data-lucide="arrow-left"></i> 返回重试
                </button>
                <button onclick="location.reload()" class="btn btn-primary">
                    <i data-lucide="rotate-ccw"></i> 重新开始
                </button>
            </div>
        </div>
    `;
    if (window.lucide) lucide.createIcons();

    showStep(3);
}

// 格式化日期
function formatDate(dateString) {
    if (!dateString) return '-';

    try {
        const date = new Date(dateString);
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    } catch (e) {
        return dateString;
    }
}

// ========== 质保查询功能 ==========

// 查询质保状态
async function checkWarranty() {
    const input = document.getElementById('warrantyInput').value.trim();

    // 验证输入
    if (!input) {
        showToast('请输入原兑换码或邮箱进行查询', 'error');
        return;
    }

    let email = null;
    let code = null;

    // 简单判断是邮箱还是兑换码
    if (input.includes('@')) {
        email = input;
    } else {
        code = input;
    }

    const checkBtn = document.getElementById('checkWarrantyBtn');
    const warrantyResultContainer = document.getElementById('warrantyResultContainer');
    if (warrantyResultContainer) {
        warrantyResultContainer.style.display = 'none';
    }

    checkBtn.disabled = true;
    checkBtn.innerHTML = '<i data-lucide="loader" class="spinning"></i> 查询中...';
    if (window.lucide) lucide.createIcons();

    try {
        const response = await fetch('/warranty/check', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                email: email || null,
                code: code || null
            })
        });

        const data = await response.json();

        if (response.ok && data.success) {
            showWarrantyResult(data);
        } else {
            const rawError = data?.error ?? data?.detail ?? data?.message ?? data?.reason;
            const errorMessage = getFriendlyWarrantyErrorMessage(rawError, response.status);
            showToast(errorMessage, 'error');
        }
    } catch (error) {
        const errorMessage = getFriendlyWarrantyErrorMessage(error?.message || '');
        showToast(errorMessage || '网络错误，请稍后重试', 'error');
    } finally {
        checkBtn.disabled = false;
        checkBtn.innerHTML = '<i data-lucide="search"></i> 查询质保状态';
        if (window.lucide) lucide.createIcons();
    }
}

// 显示质保查询结果
function showWarrantyResult(data) {
    const warrantyContent = document.getElementById('warrantyContent');

    // 处理“虚假成功自愈”后的特殊提示
    if ((!data.records || data.records.length === 0) && data.can_reuse) {
        warrantyContent.innerHTML = `
            <div class="result-info" style="text-align: center; padding: 2rem;">
                <div class="result-icon"><i data-lucide="check-circle" style="width: 56px; height: 56px; color: var(--success);"></i></div>
                <div class="result-title" style="font-size: 1.25rem; margin: 1.2rem 0; color: var(--success);">修复成功！</div>
                <div class="result-message" style="color: var(--text-primary); background: rgba(34, 197, 94, 0.05); padding: 1.2rem; border-radius: 12px; border: 1px solid rgba(34, 197, 94, 0.2); line-height: 1.6;">
                    ${escapeHtml(data.message || '系统检测到异常并已自动修复')}
                </div>
                
                <div style="margin-top: 2rem; text-align: left; background: rgba(255,255,255,0.03); padding: 1.2rem; border-radius: 12px; border: 1px dashed var(--border-base);">
                    <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.8rem;">请复制您的兑换码返回主页重试：</div>
                    <div style="display: flex; gap: 0.5rem; align-items: center;">
                        <input type="text" value="${escapeHtml(data.original_code)}" readonly 
                            style="flex: 1; padding: 0.75rem; background: rgba(0,0,0,0.2); border: 1px solid var(--border-base); border-radius: 8px; color: var(--text-primary); font-family: monospace; font-size: 1.1rem;">
                        <button onclick='copyWarrantyCode(${JSON.stringify(String(data.original_code || ''))})' class="btn btn-secondary" style="white-space: nowrap;">
                            <i data-lucide="copy"></i> 复制
                        </button>
                    </div>
                </div>

                <div style="margin-top: 2rem;">
                    <button onclick="backToStep1()" class="btn btn-primary" style="width: 100%;">
                        <i data-lucide="arrow-left"></i> 立即返回重兑
                    </button>
                </div>
            </div>
        `;
        if (window.lucide) lucide.createIcons();
        return;
    }

    if (!data.records || data.records.length === 0) {
        warrantyContent.innerHTML = `
            <div class="result-info" style="text-align: center; padding: 2rem;">
                <div class="result-icon"><i data-lucide="info" style="width: 48px; height: 48px; color: var(--text-muted);"></i></div>
                <div class="result-title" style="font-size: 1.2rem; margin: 1rem 0;">未找到兑换记录</div>
                <div class="result-message" style="color: var(--text-muted);">${escapeHtml(data.message || '未找到相关记录')}</div>
            </div>
        `;
    } else {
        // 1. 顶部状态概览 (如果有质保码)
        let summaryHtml = '';
        if (data.has_warranty) {
            const warrantyType = data.warranty_type === 'uses' ? 'uses' : 'days';
            const warrantyRuleText = warrantyType === 'uses'
                ? `剩余 ${Number(data.warranty_uses_remaining || 0)} / ${Number(data.warranty_uses || 0)} 次`
                : `${Number(data.warranty_days || 0)} 天`;
            const warrantyStatus = data.warranty_valid
                ? '<span class="badge badge-success">✓ 质保有效</span>'
                : (warrantyType === 'uses'
                    ? '<span class="badge badge-error">✗ 质保次数已用完</span>'
                    : '<span class="badge badge-error">✗ 质保已过期</span>');

            summaryHtml = `
                <div class="warranty-summary" style="margin-bottom: 2rem; padding: 1.2rem; background: rgba(255,255,255,0.03); border-radius: 12px; border: 1px solid var(--border-base);">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.4rem;">当前质保状态</div>
                            <div style="font-size: 1.1rem; font-weight: 600;">${warrantyStatus}</div>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.4rem;">质保规则</div>
                            <div style="font-size: 1rem;">${warrantyType === 'uses' ? '按次数质保' : '按天数质保'} / ${warrantyRuleText}</div>
                        </div>
                        ${warrantyType === 'days' && data.warranty_expires_at ? `
                        <div style="text-align: right;">
                            <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.4rem;">质保到期时间</div>
                            <div style="font-size: 1rem;">${formatDate(data.warranty_expires_at)}</div>
                        </div>
                        ` : ''}
                    </div>
                </div>
            `;
        }

        // 2. 兑换记录列表
        const recordsHtml = `
            <div class="records-section">
                <h4 style="margin: 0 0 1rem 0; font-size: 1rem; color: var(--text-primary);">我的兑换记录</h4>
                <div style="display: flex; flex-direction: column; gap: 1rem;">
                    ${data.records.map(record => {
            const warrantyType = record.warranty_type === 'uses' ? 'uses' : 'days';
            const typeMarker = record.has_warranty ?
                '<span class="badge badge-warranty" style="background: var(--primary); color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;">质保码</span>' :
                '<span class="badge badge-normal" style="background: rgba(255,255,255,0.1); color: var(--text-muted); padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;">常规码</span>';

            let teamStatusBadge = '';
            if (record.team_status === 'active') teamStatusBadge = '<span style="color: var(--success); font-size: 0.8rem;">● 正常</span>';
            else if (record.team_status === 'full') teamStatusBadge = '<span style="color: var(--success); font-size: 0.8rem;">● 已满</span>';
            else if (record.team_status === 'banned') teamStatusBadge = '<span style="color: var(--danger); font-size: 0.8rem;">● 封号</span>';
            else if (record.team_status === 'error') teamStatusBadge = '<span style="color: var(--warning); font-size: 0.8rem;">● 异常</span>';
            else if (record.team_status === 'expired') teamStatusBadge = '<span style="color: var(--text-muted); font-size: 0.8rem;">● 过期</span>';
            else teamStatusBadge = `<span style="color: var(--text-muted); font-size: 0.8rem;">● ${record.team_status || '未知'}</span>`;

            return `
                            <div class="record-card" style="padding: 1rem; background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.05); border-radius: 10px;">
                                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.8rem;">
                                    <div style="font-family: monospace; font-size: 1.1rem; color: var(--text-primary);">${record.code}</div>
                                    <div>${typeMarker}</div>
                                </div>
                                <div style="display: grid; grid-template-columns: 1fr 1.2fr; gap: 1rem; font-size: 0.9rem;">
                                    <div>
                                        <div style="color: var(--text-muted); margin-bottom: 0.2rem;">加入 Team</div>
                                         <div style="font-weight: 500; display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap;">
                                             <span>${escapeHtml(record.team_name || '未知 Team')}</span>
                                             <span>${teamStatusBadge}</span>
                                             ${(record.has_warranty && data.can_reuse && record.code === data.original_code && record.team_status === 'banned') ? `
                                             <button onclick='oneClickReplace(${JSON.stringify(String(record.code || ''))}, ${JSON.stringify(String(record.email || currentEmail || ''))}, this)' class="btn btn-xs btn-primary" style="padding: 2px 8px; font-size: 0.75rem; height: auto; min-height: 0;">
                                                 一键换车
                                             </button>
                                             ` : ''}
                                         </div>
                                     </div>
                                     <div>
                                         <div style="color: var(--text-muted); margin-bottom: 0.2rem;">兑换时间</div>
                                         <div>${formatDate(record.used_at)}</div>
                                     </div>
                                     <div style="grid-column: span 2;">
                                         <div style="color: var(--text-muted); margin-bottom: 0.2rem;">Team 到期</div>
                                         <div style="font-weight: 500;">${formatDate(record.team_expires_at)}</div>
                                     </div>
                                    ${record.has_warranty ? `
                                    <div style="grid-column: span 2;">
                                        <div style="color: var(--text-muted); margin-bottom: 0.2rem;">质保规则</div>
                                        <div style="${record.warranty_valid ? 'color: var(--success);' : 'color: var(--danger);'}">
                                            ${warrantyType === 'uses'
                                                ? `按次数质保，总计 ${Number(record.warranty_uses || 0)} 次，剩余 ${Number(record.warranty_uses_remaining || 0)} 次`
                                                : (record.warranty_expires_at ? `${formatDate(record.warranty_expires_at)} ${record.warranty_valid ? '(有效)' : '(已过期)'}` : '尚未开始计算 (首次使用后开启)')}
                                        </div>
                                    </div>
                                    ` : ''}
                                     <div style="grid-column: span 2; display: flex; align-items: center; justify-content: space-between; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 0.8rem; margin-top: 0.2rem;">
                                         <div>
                                             <div style="color: var(--text-muted); margin-bottom: 0.2rem;">设备身份验证 (Codex)</div>
                                             <div style="font-weight: 500;">
                                                 ${record.device_code_auth_enabled ? '<span style="color: var(--success);">已开启</span>' : '<span style="color: var(--warning);">未开启</span>'}
                                             </div>
                                         </div>
                                         ${(!record.device_code_auth_enabled && record.team_status !== 'banned' && record.team_status !== 'expired') ? `
                                         <button onclick='enableUserDeviceAuth(${Number(record.team_id)}, ${JSON.stringify(String(record.code || ''))}, ${JSON.stringify(String(record.email || ''))}, this)' class="btn btn-xs btn-primary" style="padding: 4px 10px; font-size: 0.75rem; height: auto;">
                                             一键开启
                                         </button>
                                         ` : ''}
                                     </div>
                                 </div>
                             </div>
                         `;
        }).join('')}
                </div>
            </div>
        `;

        // 3. 可重兑区域
        const canReuseHtml = data.can_reuse ? `
            <div style="margin-top: 2rem; padding: 1.5rem; background: rgba(34, 197, 94, 0.1); border-radius: 12px; border: 1px solid rgba(34, 197, 94, 0.3);">
                <div style="display: flex; align-items: center; gap: 0.5rem; color: var(--success); margin-bottom: 0.8rem;">
                    <i data-lucide="check-circle" style="width: 20px; height: 20px;"></i> 
                    <span style="font-weight: 600;">发现失效 Team，质保可触发</span>
                </div>
                <p style="margin: 0 0 1.2rem 0; color: var(--text-secondary); font-size: 0.95rem;">
                    监测到您所在的 Team 已失效。由于您的质保码仍在有效期内，您可以立即复制兑换码进行重兑。
                    ${data.warranty_type === 'uses' ? ' 当前将按剩余质保次数判断是否还能重兑。' : ''}
                </p>
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <input type="text" value="${escapeHtml(data.original_code)}" readonly 
                        style="flex: 1; padding: 0.75rem; background: rgba(0,0,0,0.2); border: 1px solid var(--border-base); border-radius: 8px; color: var(--text-primary); font-family: monospace; font-size: 1.1rem;">
                    <button onclick='copyWarrantyCode(${JSON.stringify(String(data.original_code || ''))})' class="btn btn-secondary" style="white-space: nowrap;">
                        <i data-lucide="copy"></i> 复制
                    </button>
                </div>
            </div>
        ` : '';

        warrantyContent.innerHTML = `
            <div class="warranty-view">
                ${summaryHtml}
                ${recordsHtml}
                ${canReuseHtml}
                <div style="margin-top: 2rem; text-align: center;">
                    <button onclick="backToStep1()" class="btn btn-secondary" style="width: 100%;">
                        <i data-lucide="arrow-left"></i> 返回兑换
                    </button>
                </div>
            </div>
        `;
    }

    if (window.lucide) lucide.createIcons();

    // 在顶部导航下展示质保结果
    showStep(1);
    switchTopTab('warranty');
    const warrantyResultContainer = document.getElementById('warrantyResultContainer');
    if (warrantyResultContainer) {
        warrantyResultContainer.style.display = 'block';
    }
}

// 复制质保兑换码
async function copyWarrantyCode(code) {
    try {
        if (navigator.clipboard && window.isSecureContext) {
            await navigator.clipboard.writeText(code);
        } else {
            const textArea = document.createElement('textarea');
            textArea.value = code;
            textArea.style.position = 'fixed';
            textArea.style.opacity = '0';
            document.body.appendChild(textArea);
            textArea.focus();
            textArea.select();
            const copied = document.execCommand('copy');
            document.body.removeChild(textArea);
            if (!copied) throw new Error('copy failed');
        }
        showToast('兑换码已复制到剪贴板', 'success');
    } catch (_) {
        showToast('复制失败，请手动复制', 'error');
    }
}

// 一键换车
async function oneClickReplace(code, email, btn) {
    if (!code || !email) {
        showToast('无法获取完整信息，请手动重试', 'error');
        return;
    }

    // 更新全局变量
    currentEmail = email;
    currentCode = code;

    // 填充Step1表单 (以便如果失败返回可以看到)
    const emailInput = document.getElementById('email');
    const codeInput = document.getElementById('code');
    if (emailInput) emailInput.value = email;
    if (codeInput) codeInput.value = code;

    const originalContent = btn ? btn.innerHTML : "";

    // 禁用所有按钮防止重复提交
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i data-lucide="loader" class="spinning"></i> 处理中...';
    }
    if (window.lucide) lucide.createIcons();

    showToast('正在为您尝试自动兑换...', 'info');

    try {
        // 直接调用confirmRedeem，传入null表示自动选择Team
        await confirmRedeem(null);
    } catch (e) {
        console.error(e);
        showToast('一键换车请求失败', 'error');
    } finally {
        // 如果页面未跳转（失败情况），恢复按钮
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = originalContent;
            if (window.lucide) lucide.createIcons();
        }
    }
}

// 用户一键开启设备身份验证
async function enableUserDeviceAuth(teamId, code, email, btn) {
    if (!confirm('确定要在该 Team 中开启设备代码身份验证吗？')) {
        return;
    }

    const originalContent = btn ? btn.innerHTML : "";
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i data-lucide="loader" class="spinning"></i> 开启中...';
    }
    if (window.lucide) lucide.createIcons();

    try {
        const response = await fetch('/warranty/enable-device-auth', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                team_id: teamId,
                code: code,
                email: email
            })
        });

        const data = await response.json();
        if (response.ok && data.success) {
            showToast(data.message || '开启成功', 'success');
            // 刷新当前状态
            checkWarranty();
        } else {
            const rawError = data?.error ?? data?.detail ?? data?.message ?? data?.reason;
            const errorMessage = getFriendlyDeviceAuthErrorMessage(rawError, response.status);
            showToast(errorMessage, 'error');
            btn.disabled = false;
            btn.innerHTML = originalContent;
            if (window.lucide) lucide.createIcons();
        }
    } catch (error) {
        const errorMessage = getFriendlyDeviceAuthErrorMessage(error?.message || '');
        showToast(errorMessage || '网络错误，请稍后重试', 'error');
        btn.disabled = false;
        btn.innerHTML = originalContent;
        if (window.lucide) lucide.createIcons();
    }
}

// 从成功页面跳转到质保查询
function goToWarrantyFromSuccess() {
    const warrantyInput = document.getElementById('warrantyInput');
    if (warrantyInput) {
        // 优先填入邮箱，因为邮箱查询更全面
        warrantyInput.value = currentEmail || currentCode || '';
    }

    showStep(1);
    switchTopTab('warranty');

    // 自动触发查询
    checkWarranty();
}
