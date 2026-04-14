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
const REDEEM_DEBUG = window.REDEEM_DEBUG === true;

function debugConsole(method, ...args) {
    if (!REDEEM_DEBUG || typeof console === 'undefined') {
        return;
    }

    const logger = console[method];
    if (typeof logger === 'function') {
        logger.apply(console, args);
    }
}

// Toast提示函数
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    if (!toast) return;

    let icon = 'info';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'alert-circle';

    toast.innerHTML = `<i data-lucide="${icon}"></i><span>${message}</span>`;
    toast.className = `toast ${type} show`;

    if (window.lucide) {
        lucide.createIcons();
    }

    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
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

// 返回步骤1
function backToStep1() {
    showStep(1);
}

// 步骤1: 提交兑换信息并直接兑换
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
    verifyBtn.textContent = '正在兑换...';

    // 直接调用兑换接口 (team_id = null 表示自动选择)
    await confirmRedeem(null);

    // 恢复按钮状态 (如果 confirmRedeem 失败并显示了错误也没关系，因为用户可以点返回重试)
    verifyBtn.disabled = false;
    verifyBtn.textContent = '立即兑换';
});

// 确认兑换
async function confirmRedeem(teamId) {
    debugConsole('log', 'Starting redemption process, teamId:', teamId);

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

        debugConsole('log', 'Response status:', response.status);

        let data;
        const text = await response.text();
        try {
            data = JSON.parse(text);
        } catch (e) {
            debugConsole('error', 'Failed to parse response JSON:', text);
            throw new Error('服务器响应格式错误');
        }

        if (response.ok && data.success) {
            debugConsole('log', 'Redemption accepted');
            if (data.pending) {
                showPendingResult(data);
            } else {
                showSuccessResult(data);
            }
        } else {
            // 兑换失败
            debugConsole('warn', 'Redemption failed:', data);

            // Extract error message safely
            let errorMessage = '兑换失败';

            if (data.detail) {
                if (typeof data.detail === 'string') {
                    errorMessage = data.detail;
                } else if (Array.isArray(data.detail)) {
                    // Handle FastAPI validation errors (array of objects)
                    errorMessage = data.detail.map(err => err.msg || JSON.stringify(err)).join('; ');
                } else {
                    errorMessage = JSON.stringify(data.detail);
                }
            } else if (data.error) {
                errorMessage = data.error;
            }

            showErrorResult(errorMessage);
        }
    } catch (error) {
        debugConsole('error', 'Network or logic error:', error);
        showErrorResult(error.message || '网络错误,请稍后重试');
    }
}

// 显示待确认结果
function showPendingResult(data) {
    const resultContent = document.getElementById('resultContent');
    const teamInfo = data.team_info || {};

    resultContent.innerHTML = `
        <div class="result-info">
            <div class="result-icon"><i data-lucide="clock-3" style="width: 64px; height: 64px; color: var(--warning);"></i></div>
            <div class="result-title">邀请提交中</div>
            <div class="result-message">${escapeHtml(data.message) || '邀请已提交，等待系统同步确认'}</div>

            <div class="result-details">
                <div class="result-detail-item">
                    <span class="result-detail-label">Team 名称</span>
                    <span class="result-detail-value">${escapeHtml(teamInfo.team_name) || '-'}</span>
                </div>
                <div class="result-detail-item">
                    <span class="result-detail-label">邮箱地址</span>
                    <span class="result-detail-value">${escapeHtml(currentEmail)}</span>
                </div>
            </div>

            <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 2rem; background: rgba(255,255,255,0.05); padding: 1rem; border-radius: 8px; text-align: left;">
                <i data-lucide="mail" style="width: 16px; height: 16px; vertical-align: middle; margin-right: 5px;"></i>
                如果上游列表还没来得及回写，系统会保留当前兑换。请稍后查收邮件，或再次使用相同邮箱和兑换码继续确认。
            </p>

            <div style="display: flex; gap: 1rem; justify-content: center; margin-top: 2rem;">
                <button onclick="backToStep1()" class="btn btn-secondary">
                    <i data-lucide="arrow-left"></i> 返回
                </button>
                <button onclick="confirmRedeem(null)" class="btn btn-primary">
                    <i data-lucide="refresh-cw"></i> 再次确认
                </button>
            </div>
        </div>
    `;
    if (window.lucide) lucide.createIcons();

    showStep(3);
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
    const email = document.getElementById('warrantyEmail').value.trim();
    const code = document.getElementById('warrantyCode').value.trim();

    if (!email || !code) {
        showToast('请同时输入邮箱和兑换码', 'error');
        return;
    }

    currentEmail = email;
    currentCode = code;

    const checkBtn = document.getElementById('checkWarrantyBtn');

    try {
        if (checkBtn) {
            checkBtn.disabled = true;
            checkBtn.innerHTML = '<i data-lucide="loader" class="spinning"></i> Processing...';
            if (window.lucide) lucide.createIcons();
        }

        const response = await fetch('/warranty/check', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                email: email,
                code: code
            })
        });

        const data = await response.json();

        if (response.ok && data.success) {
            showWarrantyResult(data);
        } else {
            showToast(data.error || data.detail || '查询失败', 'error');
        }
    } catch (error) {
        showToast('网络错误，请稍后重试', 'error');
    } finally {
        if (checkBtn) {
            checkBtn.disabled = false;
            checkBtn.innerHTML = '<i data-lucide="search"></i> 查询质保状态';
            if (window.lucide) lucide.createIcons();
        }
    }
}

// 显示质保查询结果
function showWarrantyResult(data) {
    const warrantyContent = document.getElementById('warrantyContent');

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
            const warrantyStatus = data.warranty_valid ?
                '<span class="badge badge-success">✓ 质保有效</span>' :
                '<span class="badge badge-error">✗ 质保已过期</span>';

            summaryHtml = `
                <div class="warranty-summary" style="margin-bottom: 2rem; padding: 1.2rem; background: rgba(255,255,255,0.03); border-radius: 12px; border: 1px solid var(--border-base);">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.4rem;">当前质保状态</div>
                            <div style="font-size: 1.1rem; font-weight: 600;">${warrantyStatus}</div>
                        </div>
                        ${data.warranty_expires_at ? `
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
                                             ${(data.can_reuse && data.original_code === record.code && record.has_warranty && record.warranty_valid && record.team_status === 'banned') ? `
                                             <button onclick="oneClickReplace('${escapeHtml(record.code)}', this)" class="btn btn-xs btn-primary" style="padding: 2px 8px; font-size: 0.75rem; height: auto; min-height: 0;">
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
                                        <div style="color: var(--text-muted); margin-bottom: 0.2rem;">质保到期</div>
                                        <div style="${record.warranty_valid ? 'color: var(--success);' : 'color: var(--danger);'}">
                                            ${record.warranty_expires_at ? `${formatDate(record.warranty_expires_at)} ${record.warranty_valid ? '(有效)' : '(已过期)'}` : '尚未开始计算 (首次使用后开启)'}
                                        </div>
                                    </div>
                                    ` : ''}
                                 </div>
                             </div>
                         `;
        }).join('')}
                </div>
            </div>
        `;

        // 3. 可重兑区域
        const canReuseHtml = data.can_reuse && data.original_code ? `
            <div style="margin-top: 2rem; padding: 1.5rem; background: rgba(34, 197, 94, 0.1); border-radius: 12px; border: 1px solid rgba(34, 197, 94, 0.3);">
                <div style="display: flex; align-items: center; gap: 0.5rem; color: var(--success); margin-bottom: 0.8rem;">
                    <i data-lucide="check-circle" style="width: 20px; height: 20px;"></i> 
                    <span style="font-weight: 600;">发现失效 Team，质保可触发</span>
                </div>
                <p style="margin: 0 0 1.2rem 0; color: var(--text-secondary); font-size: 0.95rem;">
                    监测到您所在的 Team 已失效。由于您的质保码仍在有效期内，您可以立即复制兑换码进行重兑。
                </p>
                <div style="display: flex; gap: 0.5rem; align-items: center;">
                    <input type="text" value="${escapeHtml(data.original_code)}" readonly 
                        style="flex: 1; padding: 0.75rem; background: rgba(0,0,0,0.2); border: 1px solid var(--border-base); border-radius: 8px; color: var(--text-primary); font-family: monospace; font-size: 1.1rem;">
                    <button onclick="copyWarrantyCode('${escapeHtml(data.original_code)}')" class="btn btn-secondary" style="white-space: nowrap;">
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

    // 显示质保结果区域
    document.querySelectorAll('.step').forEach(step => step.style.display = 'none');
    document.getElementById('warrantyResult').style.display = 'block';
}

// 复制质保兑换码
function copyWarrantyCode(code) {
    navigator.clipboard.writeText(code).then(() => {
        showToast('兑换码已复制到剪贴板', 'success');
    }).catch(() => {
        showToast('复制失败，请手动复制', 'error');
    });
}

// 一键换车
async function oneClickReplace(code, button) {
    const email = currentEmail;
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

    const btn = button;
    const originalContent = btn ? btn.innerHTML : '';
    const restoreButton = () => {
        if (!btn) {
            return;
        }

        btn.disabled = false;
        btn.innerHTML = originalContent;
        if (window.lucide) lucide.createIcons();
    };
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i data-lucide="loader" class="spinning"></i> Processing...';
        if (window.lucide) lucide.createIcons();
    }

    showToast('Trying automatic redemption...','info');

    try {
        // 直接调用confirmRedeem，传入null表示自动选择Team
        await confirmRedeem(null);
    } catch (e) {
        debugConsole('error', e);
        showToast('One-click replacement failed', 'error');
    } finally {
        // 如果页面未跳转（失败情况），恢复按钮
        restoreButton();
    }
}

// 从成功页面跳转到质保查询
function goToWarrantyFromSuccess() {
    const warrantyEmail = document.getElementById('warrantyEmail');
    const warrantyCode = document.getElementById('warrantyCode');
    if (warrantyEmail) warrantyEmail.value = currentEmail || '';
    if (warrantyCode) warrantyCode.value = currentCode || '';

    // 切换视图
    document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
    document.getElementById('step1').classList.add('active');
    document.getElementById('step3').style.display = 'none';

    // 滚动到质保区域
    const warrantySection = document.querySelector('.warranty-section');
    if (warrantySection) {
        warrantySection.scrollIntoView({ behavior: 'smooth' });
    }

    // 自动触发查询
    checkWarranty();
}
