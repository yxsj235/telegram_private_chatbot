// Cloudflare Worker：Telegram 双向机器人 v5.4 (Performance Optimized)

// --- 配置常量 ---
const CONFIG = {
    VERIFY_ID_LENGTH: 12,
    VERIFY_EXPIRE_SECONDS: 300,         // 5分钟
    VERIFIED_EXPIRE_SECONDS: 2592000,   // 30天
    MEDIA_GROUP_EXPIRE_SECONDS: 60,
    MEDIA_GROUP_DELAY_MS: 3000,         // 3秒（从2秒增加）
    PENDING_MAX_MESSAGES: 10,           // 验证期间最多暂存的消息数
    ADMIN_CACHE_TTL_SECONDS: 300,       // 管理员权限缓存 5 分钟
    NEEDS_REVERIFY_TTL_SECONDS: 600,    // 标记需重新验证的 TTL（用于并发兜底）
    RATE_LIMIT_MESSAGE: 45,
    RATE_LIMIT_VERIFY: 3,
    RATE_LIMIT_WINDOW: 60,
    BUTTON_COLUMNS: 2,
    MAX_TITLE_LENGTH: 128,
    MAX_NAME_LENGTH: 30,
    API_TIMEOUT_MS: 10000,
    CLEANUP_BATCH_SIZE: 10,
    MAX_CLEANUP_DISPLAY: 20,
    CLEANUP_LOCK_TTL_SECONDS: 1800,     // /cleanup 防并发锁 30 分钟
    MAX_RETRY_ATTEMPTS: 3,
    THREAD_HEALTH_TTL_MS: 60000,

    // === 新增性能优化配置 ===
    USER_CACHE_MAX_SIZE: 500,          // 用户缓存最大条目数
    USER_CACHE_TTL_MS: 300000,          // 用户数据缓存 5 分钟
    THREAD_PROBE_COALESCE_MS: 500,     // 线程探测合并窗口
    MAX_CONCURRENT_PROBES: 3,          // 最大并发探测数
    FLUSH_MEDIA_GROUP_INTERVAL_MS: 120000, // 媒体组清理间隔（2分钟，默认每请求清理改为定时）
    SKIP_FORWARD_VALIDATION: false,    // 跳过转发验证（生产环境可开启）
};

// === 优化1: LRU 用户数据缓存（减少 KV 读取） ===
class LRUCache {
    constructor(maxSize, ttlMs) {
        this.maxSize = maxSize;
        this.ttlMs = ttlMs;
        this.cache = new Map();
    }

    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;
        const now = Date.now();
        if (now - entry.ts > this.ttlMs) {
            this.cache.delete(key);
            return null;
        }
        return entry.data;
    }

    set(key, data) {
        if (this.cache.has(key)) this.cache.delete(key);
        else if (this.cache.size >= this.maxSize) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        this.cache.set(key, { data, ts: Date.now() });
    }

    invalidate(key) {
        this.cache.delete(key);
    }

    clear() {
        this.cache.clear();
    }
}

const userDataCache = new LRUCache(CONFIG.USER_CACHE_MAX_SIZE, CONFIG.USER_CACHE_TTL_MS);

// === 优化2: 请求合并器（防止同一线程重复探测） ===
const probeCoalescer = new Map();
const activeProbes = new Map();

async function coalescedProbe(env, threadId, probeFn) {
    const key = String(threadId);
    const now = Date.now();

    if (activeProbes.has(key)) {
        return await activeProbes.get(key);
    }

    const pending = probeCoalescer.get(key);
    if (pending && (now - pending.ts) < CONFIG.THREAD_PROBE_COALESCE_MS) {
        return await pending.promise;
    }

    const promise = probeFn();
    activeProbes.set(key, promise);

    try {
        const result = await promise;
        probeCoalescer.set(key, { promise, ts: now });
        setTimeout(() => {
            probeCoalescer.delete(key);
            activeProbes.delete(key);
        }, CONFIG.THREAD_PROBE_COALESCE_MS);
        return result;
    } catch (e) {
        probeCoalescer.delete(key);
        activeProbes.delete(key);
        throw e;
    }
}

// 线程健康检查缓存，减少频繁探测请求
const threadHealthCache = new Map();

// === 优化3: 并发保护Map（复用现有 + 优化） ===
const topicCreateInFlight = new Map();

// 管理员权限缓存（实例内）
const adminStatusCache = new Map();

// === 优化4: 验证状态缓存（减少 KV 读取） ===
const verificationStatusCache = new LRUCache(200, 60000);

// === 优化5: 最近使用的 verified 用户缓存（极短 TTL，减少重复读取） ===
const recentVerifiedCache = new Map();

// === 媒体组刷新计数器（控制清理频率） ===
let lastMediaGroupFlush = 0;

// --- 本地题库 (15条) ---
const LOCAL_QUESTIONS = [
    {"question": "冰融化后会变成什么？", "correct_answer": "水", "incorrect_answers": ["石头", "木头", "火"]},
    {"question": "正常人有几只眼睛？", "correct_answer": "2", "incorrect_answers": ["1", "3", "4"]},
    {"question": "以下哪个属于水果？", "correct_answer": "香蕉", "incorrect_answers": ["白菜", "猪肉", "大米"]},
    {"question": "1 加 2 等于几？", "correct_answer": "3", "incorrect_answers": ["2", "4", "5"]},
    {"question": "5 减 2 等于几？", "correct_answer": "3", "incorrect_answers": ["1", "2", "4"]},
    {"question": "2 乘以 3 等于几？", "correct_answer": "6", "incorrect_answers": ["4", "5", "7"]},
    {"question": "10 加 5 等于几？", "correct_answer": "15", "incorrect_answers": ["10", "12", "20"]},
    {"question": "8 减 4 等于几？", "correct_answer": "4", "incorrect_answers": ["2", "3", "5"]},
    {"question": "在天上飞的交通工具是什么？", "correct_answer": "飞机", "incorrect_answers": ["汽车", "轮船", "自行车"]},
    {"question": "星期一的后面是星期几？", "correct_answer": "星期二", "incorrect_answers": ["星期日", "星期五", "星期三"]},
    {"question": "鱼通常生活在哪里？", "correct_answer": "水里", "incorrect_answers": ["树上", "土里", "火里"]},
    {"question": "我们用什么器官来听声音？", "correct_answer": "耳朵", "incorrect_answers": ["眼睛", "鼻子", "嘴巴"]},
    {"question": "晴朗的天空通常是什么颜色的？", "correct_answer": "蓝色", "incorrect_answers": ["绿色", "红色", "紫色"]},
    {"question": "太阳从哪个方向升起？", "correct_answer": "东方", "incorrect_answers": ["西方", "南方", "北方"]},
    {"question": "小狗发出的叫声通常是？", "correct_answer": "汪汪", "incorrect_answers": ["喵喵", "咩咩", "呱呱"]}
];

// --- 辅助工具函数 ---

// 结构化日志系统
const Logger = {
    /**
     * 记录信息级别日志
     * @param {string} action - 操作名称
     * @param {object} data - 附加数据
     */
    info(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'INFO',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    },

    /**
     * 记录警告级别日志
     * @param {string} action - 操作名称
     * @param {object} data - 附加数据
     */
    warn(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'WARN',
            action,
            ...data
        };
        console.warn(JSON.stringify(log));
    },

    /**
     * 记录错误级别日志
     * @param {string} action - 操作名称
     * @param {Error|string} error - 错误对象或消息
     * @param {object} data - 附加数据
     */
    error(action, error, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'ERROR',
            action,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
            ...data
        };
        console.error(JSON.stringify(log));
    },

    /**
     * 记录调试级别日志
     * @param {string} action - 操作名称
     * @param {object} data - 附加数据
     */
    debug(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'DEBUG',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    }
};

// 加密安全的随机数生成
function secureRandomInt(min, max) {
    const range = max - min;
    const bytes = new Uint32Array(1);
    crypto.getRandomValues(bytes);
    return min + (bytes[0] % range);
}

function secureRandomId(length = 12) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    const bytes = new Uint8Array(length);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(b => chars[b % chars.length]).join('');
}

// 安全的 JSON 获取
async function safeGetJSON(env, key, defaultValue = null) {
    try {
        const data = await env.TOPIC_MAP.get(key, { type: "json" });
        if (data === null || data === undefined) {
            return defaultValue;
        }
        if (typeof data !== 'object') {
            Logger.warn('kv_invalid_type', { key, type: typeof data });
            return defaultValue;
        }
        return data;
    } catch (e) {
        Logger.error('kv_parse_failed', e, { key });
        return defaultValue;
    }
}

function normalizeTgDescription(description) {
    return (description || "").toString().toLowerCase();
}

function isTopicMissingOrDeleted(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("thread not found") ||
           desc.includes("topic not found") ||
           desc.includes("message thread not found") ||
           desc.includes("topic deleted") ||
           desc.includes("thread deleted") ||
           desc.includes("forum topic not found") ||
           desc.includes("topic closed permanently");
}

function isTestMessageInvalid(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("message text is empty") ||
           desc.includes("bad request: message text is empty");
}

async function getOrCreateUserTopicRec(from, key, env, userId) {
    const existing = await safeGetJSON(env, key, null);
    if (existing && existing.thread_id) return existing;

    const inflight = topicCreateInFlight.get(String(userId));
    if (inflight) return await inflight;

    const p = (async () => {
        // 并发下二次确认，避免已被其他请求创建却读到旧值
        const again = await safeGetJSON(env, key, null);
        if (again && again.thread_id) return again;
        return await createTopic(from, key, env, userId);
    })();

    topicCreateInFlight.set(String(userId), p);
    try {
        return await p;
    } finally {
        if (topicCreateInFlight.get(String(userId)) === p) {
            topicCreateInFlight.delete(String(userId));
        }
    }
}

function withMessageThreadId(body, threadId) {
    if (threadId === undefined || threadId === null) return body;
    return { ...body, message_thread_id: threadId };
}

async function probeForumThread(env, expectedThreadId, { userId, reason, doubleCheckOnMissingThreadId = true } = {}) {
    return coalescedProbe(env, expectedThreadId, async () => {
        const attemptOnce = async () => {
            const res = await tgCall(env, "sendMessage", {
                chat_id: env.SUPERGROUP_ID,
                message_thread_id: expectedThreadId,
                text: "🔎"
            });

            const actualThreadId = res.result?.message_thread_id;
            const probeMessageId = res.result?.message_id;

            if (res.ok && probeMessageId) {
                try {
                    await tgCall(env, "deleteMessage", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: probeMessageId
                    });
                } catch (e) {
                }
            }

            if (!res.ok) {
                if (isTopicMissingOrDeleted(res.description)) {
                    return { status: "missing", description: res.description };
                }
                if (isTestMessageInvalid(res.description)) {
                    return { status: "probe_invalid", description: res.description };
                }
                return { status: "unknown_error", description: res.description };
            }

            if (actualThreadId === undefined || actualThreadId === null) {
                return { status: "missing_thread_id" };
            }

            if (Number(actualThreadId) !== Number(expectedThreadId)) {
                return { status: "redirected", actualThreadId };
            }

            return { status: "ok" };
        };

        const first = await attemptOnce();
        if (first.status !== "missing_thread_id" || !doubleCheckOnMissingThreadId) return first;

        const second = await attemptOnce();
        if (second.status === "missing_thread_id") {
            Logger.warn('thread_probe_missing_thread_id', { userId, expectedThreadId, reason });
        }
        return second;
    });
}

async function resetUserVerificationAndRequireReverify(env, { userId, userKey, oldThreadId, pendingMsgId, reason }) {
    await env.TOPIC_MAP.delete(`verified:${userId}`);
    await env.TOPIC_MAP.put(`needs_verify:${userId}`, "1", { expirationTtl: CONFIG.NEEDS_REVERIFY_TTL_SECONDS });
    await env.TOPIC_MAP.delete(`retry:${userId}`);

    recentVerifiedCache.delete(String(userId));
    verificationStatusCache.invalidate(`verified:${userId}`);

    if (userKey) {
        await env.TOPIC_MAP.delete(userKey);
        userDataCache.invalidate(userKey);
    }

    if (oldThreadId !== undefined && oldThreadId !== null) {
        await env.TOPIC_MAP.delete(`thread:${oldThreadId}`);
        await env.TOPIC_MAP.delete(`thread_ok:${oldThreadId}`);
        threadHealthCache.delete(oldThreadId);
    }

    Logger.info('verification_reset_due_to_topic_loss', {
        userId,
        oldThreadId,
        pendingMsgId,
        reason
    });

    await sendVerificationChallenge(userId, env, pendingMsgId || null);
}

function parseAdminIdAllowlist(env) {
    const raw = (env.ADMIN_IDS || "").toString().trim();
    if (!raw) return null;
    const ids = raw.split(/[,;\s]+/g).map(s => s.trim()).filter(Boolean);
    const set = new Set();
    for (const id of ids) {
        const n = Number(id);
        if (!Number.isFinite(n)) continue;
        set.add(String(n));
    }
    return set.size > 0 ? set : null;
}

async function isAdminUser(env, userId) {
    const allowlist = parseAdminIdAllowlist(env);
    if (allowlist && allowlist.has(String(userId))) return true;

    const cacheKey = String(userId);
    const now = Date.now();
    const cached = adminStatusCache.get(cacheKey);
    if (cached && (now - cached.ts < CONFIG.ADMIN_CACHE_TTL_SECONDS * 1000)) {
        return cached.isAdmin;
    }

    const kvKey = `admin:${userId}`;
    const kvVal = await env.TOPIC_MAP.get(kvKey);
    if (kvVal === "1" || kvVal === "0") {
        const isAdmin = kvVal === "1";
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    }

    try {
        const res = await tgCall(env, "getChatMember", {
            chat_id: env.SUPERGROUP_ID,
            user_id: userId
        });

        const status = res.result?.status;
        const isAdmin = res.ok && (status === "creator" || status === "administrator");
        await env.TOPIC_MAP.put(kvKey, isAdmin ? "1" : "0", { expirationTtl: CONFIG.ADMIN_CACHE_TTL_SECONDS });
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    } catch (e) {
        Logger.warn('admin_check_failed', { userId });
        return false;
    }
}

// 获取所有 KV keys（处理分页）
async function getAllKeys(env, prefix) {
    const allKeys = [];
    let cursor = undefined;

    do {
        const result = await env.TOPIC_MAP.list({ prefix, cursor });
        allKeys.push(...result.keys);
        cursor = result.list_complete ? undefined : result.cursor;
    } while (cursor);

    return allKeys;
}

// Fisher-Yates 洗牌算法
function shuffleArray(arr) {
    const array = [...arr];
    for (let i = array.length - 1; i > 0; i--) {
        const j = secureRandomInt(0, i + 1);
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

// 速率限制检查
async function checkRateLimit(userId, env, action = 'message', limit = 20, window = 60) {
    const key = `ratelimit:${action}:${userId}`;
    const countStr = await env.TOPIC_MAP.get(key);
    const count = parseInt(countStr || "0");

    if (count >= limit) {
        return { allowed: false, remaining: 0 };
    }

    await env.TOPIC_MAP.put(key, String(count + 1), { expirationTtl: window });
    return { allowed: true, remaining: limit - count - 1 };
}

export default {
  async fetch(request, env, ctx) {
    // 环境自检
    if (!env.TOPIC_MAP) return new Response("Error: KV 'TOPIC_MAP' not bound.");
    if (!env.BOT_TOKEN) return new Response("Error: BOT_TOKEN not set.");
    if (!env.SUPERGROUP_ID) return new Response("Error: SUPERGROUP_ID not set.");

    // 【修复 #7】规范化环境变量，统一为字符串类型
    const normalizedEnv = {
        ...env,
        SUPERGROUP_ID: String(env.SUPERGROUP_ID),
        BOT_TOKEN: String(env.BOT_TOKEN)
    };

    // 验证 SUPERGROUP_ID 格式
    if (!normalizedEnv.SUPERGROUP_ID.startsWith("-100")) {
        return new Response("Error: SUPERGROUP_ID must start with -100");
    }

    if (request.method !== "POST") return new Response("OK");

    // 验证 Content-Type
    const contentType = request.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
        Logger.warn('invalid_content_type', { contentType });
        return new Response("OK");
    }

    let update;
    try {
      update = await request.json();

      // 验证基本结构
      if (!update || typeof update !== 'object') {
          Logger.warn('invalid_json_structure', { update: typeof update });
          return new Response("OK");
      }
    } catch (e) {
      Logger.error('json_parse_failed', e);
      return new Response("OK");
    }

    if (update.callback_query) {
      await handleCallbackQuery(update.callback_query, normalizedEnv, ctx);
      return new Response("OK");
    }

    const msg = update.message;
    if (!msg) return new Response("OK");

    ctx.waitUntil(flushExpiredMediaGroupsIfNeeded(normalizedEnv, Date.now()));

    if (msg.chat && msg.chat.type === "private") {
      try {
        await handlePrivateMessage(msg, normalizedEnv, ctx);
      } catch (e) {
        // 不向用户泄露技术细节
        const errText = `⚠️ 系统繁忙，请稍后再试。`;
        await tgCall(normalizedEnv, "sendMessage", { chat_id: msg.chat.id, text: errText });
        Logger.error('private_message_failed', e, { userId: msg.chat.id });
      }
      return new Response("OK");
    }

    // 【修复 #7】使用字符串比较
    if (msg.chat && String(msg.chat.id) === normalizedEnv.SUPERGROUP_ID) {
        if (msg.forum_topic_closed && msg.message_thread_id) {
            await updateThreadStatus(msg.message_thread_id, true, normalizedEnv);
            return new Response("OK");
        }
        if (msg.forum_topic_reopened && msg.message_thread_id) {
            await updateThreadStatus(msg.message_thread_id, false, normalizedEnv);
            return new Response("OK");
        }
        // 【修复】支持 General 话题和普通话题
        // General 话题的 message_thread_id 可能不存在，或者等于 1
        const text = (msg.text || "").trim();
        const isCommand = !!text && text.startsWith("/");
        if (msg.message_thread_id || isCommand) {
            await handleAdminReply(msg, normalizedEnv, ctx);
            return new Response("OK");
        }
    }

    return new Response("OK");
  },
};

// ---------------- 核心业务逻辑 ----------------

async function handlePrivateMessage(msg, env, ctx) {
  const userId = msg.chat.id;
  const key = `user:${userId}`;

  const rateLimit = await checkRateLimit(userId, env, 'message', CONFIG.RATE_LIMIT_MESSAGE, CONFIG.RATE_LIMIT_WINDOW);
  if (!rateLimit.allowed) {
      await tgCall(env, "sendMessage", {
          chat_id: userId,
          text: "⚠️ 发送过于频繁，请稍后再试。"
      });
      return;
  }

  if (msg.text && msg.text.startsWith("/") && msg.text.trim() !== "/start") {
      return;
  }

  const isBanned = await env.TOPIC_MAP.get(`banned:${userId}`);
  if (isBanned) return;

  // 优化：使用多层缓存检查 verified 状态
  let verified = recentVerifiedCache.get(String(userId));
  if (!verified) {
      verified = await env.TOPIC_MAP.get(`verified:${userId}`);
      if (verified) {
          recentVerifiedCache.set(String(userId), verified);
          setTimeout(() => recentVerifiedCache.delete(String(userId)), 5000);
      }
  }

  if (!verified) {
    const isStart = msg.text && msg.text.trim() === "/start";
    const pendingMsgId = isStart ? null : msg.message_id;
    await sendVerificationChallenge(userId, env, pendingMsgId);
    return;
  }

  await forwardToTopic(msg, userId, key, env, ctx);
}

async function forwardToTopic(msg, userId, key, env, ctx) {
    const needsVerify = await env.TOPIC_MAP.get(`needs_verify:${userId}`);
    if (needsVerify) {
        await sendVerificationChallenge(userId, env, msg.message_id || null);
        return;
    }

    // 优化：使用 LRU 缓存用户数据
    let rec = userDataCache.get(key) || await safeGetJSON(env, key, null);
    if (rec) {
        userDataCache.set(key, rec);
    }

    if (rec && rec.closed) {
        await tgCall(env, "sendMessage", { chat_id: userId, text: "🚫 当前对话已被管理员关闭。" });
        return;
    }

    const retryKey = `retry:${userId}`;
    let retryCount = parseInt(await env.TOPIC_MAP.get(retryKey) || "0");

    if (retryCount > CONFIG.MAX_RETRY_ATTEMPTS) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "❌ 系统繁忙，请稍后再试。"
        });
        await env.TOPIC_MAP.delete(retryKey);
        return;
    }

    if (!rec || !rec.thread_id) {
        rec = await getOrCreateUserTopicRec(msg.from, key, env, userId);
        if (!rec || !rec.thread_id) {
            throw new Error("创建话题失败");
        }
        userDataCache.set(key, rec);
    }

    // 补建 thread->user 映射（兼容旧数据）
    if (rec && rec.thread_id) {
        const mappedUser = await env.TOPIC_MAP.get(`thread:${rec.thread_id}`);
        if (!mappedUser) {
            await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
        }
    }

    // 【修复1】验证话题是否仍然存在（带缓存，降低探测频率）
    // 当话题被删除后，KV中的thread_id仍然存在，但实际话题已不可用
    if (rec && rec.thread_id) {
        const cacheKey = rec.thread_id;
        const now = Date.now();
        const cached = threadHealthCache.get(cacheKey);
        const withinTTL = cached && (now - cached.ts < CONFIG.THREAD_HEALTH_TTL_MS);

        if (!withinTTL) {
            // 跨节点缓存：避免由于 Workers 多 PoP 导致每次都做健康探测
            const kvHealthKey = `thread_ok:${rec.thread_id}`;
            const kvHealthOk = await env.TOPIC_MAP.get(kvHealthKey);
            if (kvHealthOk === "1") {
                threadHealthCache.set(cacheKey, { ts: now, ok: true });
            } else {
            const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "health_check" });

            if (probe.status === "redirected" || probe.status === "missing" || probe.status === "missing_thread_id") {
                    await resetUserVerificationAndRequireReverify(env, {
                        userId,
                        userKey: key,
                        oldThreadId: rec.thread_id,
                        pendingMsgId: msg.message_id,
                        reason: `health_check:${probe.status}`
                    });
                    return;
            } else if (probe.status === "probe_invalid") {
                Logger.warn('topic_health_probe_invalid_message', {
                    userId,
                    threadId: rec.thread_id,
                    errorDescription: probe.description
                });

                // 仍然设置短 TTL，避免每条消息都探测（并误触发重建）
                threadHealthCache.set(cacheKey, { ts: now, ok: true });
                await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
            } else if (probe.status === "unknown_error") {
                Logger.warn('topic_test_failed_unknown', {
                    userId,
                    threadId: rec.thread_id,
                    errorDescription: probe.description
                });
            } else {
                await env.TOPIC_MAP.delete(retryKey);
                threadHealthCache.set(cacheKey, { ts: now, ok: true });
                await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
            }
            }
        }
    }

    if (msg.media_group_id) {
        await handleMediaGroup(msg, env, ctx, {
            direction: "p2t",
            targetChat: env.SUPERGROUP_ID,
            threadId: rec.thread_id
        });
        return;
    }

    const res = await tgCall(env, "forwardMessage", {
        chat_id: env.SUPERGROUP_ID,
        from_chat_id: userId,
        message_id: msg.message_id,
        message_thread_id: rec.thread_id,
    });

    // 检测 Telegram 静默重定向到 General 的情况
    const resThreadId = res.result?.message_thread_id;
    if (res.ok && resThreadId !== undefined && resThreadId !== null && Number(resThreadId) !== Number(rec.thread_id)) {
        Logger.warn('forward_redirected_to_general', {
            userId,
            expectedThreadId: rec.thread_id,
            actualThreadId: resThreadId
        });

        // 删除误投到 General 的消息
        if (res.result?.message_id) {
            try {
                await tgCall(env, "deleteMessage", {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: res.result.message_id
                });
            } catch (e) {
                // 删除失败不影响重发
            }
        }
        await resetUserVerificationAndRequireReverify(env, {
            userId,
            userKey: key,
            oldThreadId: rec.thread_id,
            pendingMsgId: msg.message_id,
            reason: "forward_redirected_to_general"
        });
        return;
    }

    // 兜底：部分情况下 Telegram 返回 ok 但不带 message_thread_id（可能已落入 General）
    if (res.ok && (resThreadId === undefined || resThreadId === null)) {
        const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "forward_result_missing_thread_id" });
        if (probe.status !== "ok") {
            Logger.warn('forward_suspected_redirect_or_missing', {
                userId,
                expectedThreadId: rec.thread_id,
                probeStatus: probe.status,
                probeDescription: probe.description
            });

            // 尽量删除误投消息（通常在 General）
            if (res.result?.message_id) {
                try {
                    await tgCall(env, "deleteMessage", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: res.result.message_id
                    });
                } catch (e) {
                    // 删除失败不影响重发
                }
            }
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: `forward_missing_thread_id:${probe.status}`
            });
            return;
        }
    }

    // 【修复2】增强错误处理，双重保险
    // 如果上面的测试没有捕获到，这里再次检测
    if (!res.ok) {
        const desc = normalizeTgDescription(res.description);
        if (isTopicMissingOrDeleted(desc)) {
            Logger.warn('forward_failed_topic_missing', {
                userId,
                threadId: rec.thread_id,
                errorDescription: res.description
            });
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: "forward_failed_topic_missing"
            });
            return;
        }

        if (desc.includes("chat not found")) throw new Error(`群组ID错误: ${env.SUPERGROUP_ID}`);
        if (desc.includes("not enough rights")) throw new Error("机器人权限不足 (需 Manage Topics)");

        // 如果forwardMessage失败，尝试使用copyMessage作为降级方案
        await tgCall(env, "copyMessage", {
            chat_id: env.SUPERGROUP_ID,
            from_chat_id: userId,
            message_id: msg.message_id,
            message_thread_id: rec.thread_id
        });
    }
}

async function handleAdminReply(msg, env, ctx) {
  const threadId = msg.message_thread_id;
  const text = (msg.text || "").trim();
  const senderId = msg.from?.id;

  // 仅允许管理员在群内操作与回信，防止任意群成员向用户私聊注入消息
  if (!senderId || !(await isAdminUser(env, senderId))) {
      return;
  }

  // 【修复】允许在任何话题执行 /cleanup 命令
  if (text === "/cleanup") {
      // /cleanup 可能处理较久，使用 waitUntil 防止 webhook 请求超时导致“卡住”
      ctx.waitUntil(handleCleanupCommand(threadId, env));
      return;
  }

  // 优先通过 thread 映射快速反查用户，缺失时再降级全量扫描
  let userId = null;
  const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
  if (mappedUser) {
      userId = Number(mappedUser);
  } else {
      const allKeys = await getAllKeys(env, "user:");
      for (const { name } of allKeys) {
          const rec = await safeGetJSON(env, name, null);
          if (rec && Number(rec.thread_id) === Number(threadId)) {
              userId = Number(name.slice(5));
              break;
          }
      }
  }

  // 如果找不到用户，说明可能是在普通话题，或者数据丢失，直接返回
  if (!userId) {
      if (text === "/admin" || text.startsWith("/admin ")) {
          await handleAdminListCommand(text, threadId, env);
          return;
      }
      return;
  }

  const key = `user:${userId}`;

  if (text === "/admin") {
      await sendAdminPanel(threadId, userId, env, msg.message_id);
      return;
  }

  if (msg.media_group_id) {
    await handleMediaGroup(msg, env, ctx, { direction: "t2p", targetChat: userId, threadId: undefined });
    return;
  }

  await tgCall(env, "copyMessage", { chat_id: userId, from_chat_id: env.SUPERGROUP_ID, message_id: msg.message_id });
}

// ---------------- 验证模块 (纯本地) ----------------

async function sendVerificationChallenge(userId, env, pendingMsgId) {
    const existingChallenge = await env.TOPIC_MAP.get(`user_challenge:${userId}`);
    if (existingChallenge) {
        const chalKey = `chal:${existingChallenge}`;
        const state = await safeGetJSON(env, chalKey, null);

        if (!state || state.userId !== userId) {
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);
        } else {
            if (pendingMsgId) {
                let pendingIds = [];
                if (Array.isArray(state.pending_ids)) {
                    pendingIds = state.pending_ids.slice();
                } else if (state.pending) {
                    pendingIds = [state.pending];
                }

                if (!pendingIds.includes(pendingMsgId)) {
                    pendingIds.push(pendingMsgId);
                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }
                    state.pending_ids = pendingIds;
                    delete state.pending;
                    await env.TOPIC_MAP.put(chalKey, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });
                }
            }
            Logger.debug('verification_duplicate_skipped', { userId, verifyId: existingChallenge, hasPending: !!pendingMsgId });
            return;
        }
    }

    const verifyLimit = await checkRateLimit(userId, env, 'verify', CONFIG.RATE_LIMIT_VERIFY, 300);
    if (!verifyLimit.allowed) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "⚠️ 验证请求过于频繁，请5分钟后再试。"
        });
        return;
    }

    // 优化：合并两次 KV 操作为一次
    const q = LOCAL_QUESTIONS[secureRandomInt(0, LOCAL_QUESTIONS.length)];
    const shuffledOptions = shuffleArray([...q.incorrect_answers, q.correct_answer]);
    const answerIndex = shuffledOptions.indexOf(q.correct_answer);
    const verifyId = secureRandomId(CONFIG.VERIFY_ID_LENGTH);

    const state = {
        answerIndex,
        options: shuffledOptions,
        pending_ids: pendingMsgId ? [pendingMsgId] : [],
        userId
    };

    const stateJson = JSON.stringify(state);
    const ttl = CONFIG.VERIFY_EXPIRE_SECONDS;

    // 优化：使用 Promise.all 并行写入两个 KV 键
    await Promise.all([
        env.TOPIC_MAP.put(`chal:${verifyId}`, stateJson, { expirationTtl: ttl }),
        env.TOPIC_MAP.put(`user_challenge:${userId}`, verifyId, { expirationTtl: ttl })
    ]);

    Logger.info('verification_sent', {
        userId,
        verifyId,
        question: q.question,
        pendingCount: state.pending_ids.length
    });

    const buttons = shuffledOptions.map((opt, idx) => ({
        text: opt,
        callback_data: `verify:${verifyId}:${idx}`
    }));

    const keyboard = [];
    for (let i = 0; i < buttons.length; i += CONFIG.BUTTON_COLUMNS) {
        keyboard.push(buttons.slice(i, i + CONFIG.BUTTON_COLUMNS));
    }

    await tgCall(env, "sendMessage", {
        chat_id: userId,
        text: `🛡️ **人机验证**\n\n${q.question}\n\n请点击下方按钮回答 (回答正确后将自动发送您刚才的消息)。`,
        parse_mode: "Markdown",
        reply_markup: { inline_keyboard: keyboard }
    });
}

async function handleCallbackQuery(query, env, ctx) {
    const data = query.data;

    if (data.startsWith("verify:")) {
        await handleVerificationCallback(query, env, ctx);
        return;
    }

    if (data.startsWith("admin:")) {
        await handleAdminCallback(query, env, ctx);
        return;
    }
}

async function handleVerificationCallback(query, env, ctx) {
    try {
        const data = query.data;
        if (!data.startsWith("verify:")) return;

        const parts = data.split(":");
        if (parts.length !== 3) return;

        const verifyId = parts[1];
        const selectedIndex = parseInt(parts[2]);
        const userId = query.from.id;

        const stateStr = await env.TOPIC_MAP.get(`chal:${verifyId}`);
        if (!stateStr) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 验证已过期，请重发消息",
                show_alert: true
            });
            return;
        }

        let state;
        try {
            state = JSON.parse(stateStr);
        } catch(e) {
             await tgCall(env, "answerCallbackQuery", {
                 callback_query_id: query.id,
                 text: "❌ 数据错误",
                 show_alert: true
             });
             return;
        }

        if (state.userId && state.userId !== userId) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 无效的验证",
                show_alert: true
            });
            return;
        }

        if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= state.options.length) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 无效选项",
                show_alert: true
            });
            return;
        }

        if (selectedIndex === state.answerIndex) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "✅ 验证通过"
            });

            Logger.info('verification_passed', {
                userId,
                verifyId,
                selectedOption: state.options[selectedIndex]
            });

            await env.TOPIC_MAP.put(`verified:${userId}`, "1", { expirationTtl: CONFIG.VERIFIED_EXPIRE_SECONDS });
            await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
            await env.TOPIC_MAP.delete(`chal:${verifyId}`);
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);

            recentVerifiedCache.set(String(userId), "1");
            verificationStatusCache.set(`verified:${userId}`, "1");

            await tgCall(env, "editMessageText", {
                chat_id: userId,
                message_id: query.message.message_id,
                text: "✅ **验证成功**\n\n您现在可以自由对话了。",
                parse_mode: "Markdown"
            });

            const hasPending = (Array.isArray(state.pending_ids) && state.pending_ids.length > 0) || !!state.pending;
            if (hasPending) {
                try {
                    let pendingIds = [];
                    if (Array.isArray(state.pending_ids)) {
                        pendingIds = state.pending_ids.slice();
                    } else if (state.pending) {
                        pendingIds = [state.pending];
                    }

                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }

                    const forwardedKeys = pendingIds.map(pid => `forwarded:${userId}:${pid}`);
                    const existingKeys = await Promise.all(forwardedKeys.map(k => env.TOPIC_MAP.get(k)));
                    const unforwarded = pendingIds.filter((pid, i) => !existingKeys[i]);

                    if (unforwarded.length > 0) {
                        const acquiredLocks = [];
                        const lockPrefix = `flock:${userId}:`;

                        for (const pid of unforwarded) {
                            const lockKey = `${lockPrefix}${pid}`;
                            const locked = await env.TOPIC_MAP.get(lockKey);
                            if (!locked) {
                                await env.TOPIC_MAP.put(lockKey, "1", { expirationTtl: 60 });
                                acquiredLocks.push(lockKey);
                            }
                        }

                        if (acquiredLocks.length > 0) {
                            const pids = acquiredLocks.map(k => k.replace(lockPrefix, ""));
                            const fakeMsg = {
                                message_id: pids[0],
                                chat: { id: userId, type: "private" },
                                from: query.from,
                            };

                            await forwardToTopic(fakeMsg, userId, `user:${userId}`, env, ctx);

                            const putOps = pids.map(pid =>
                                env.TOPIC_MAP.put(`forwarded:${userId}:${pid}`, "1", { expirationTtl: 3600 })
                            );
                            await Promise.all(putOps);

                            const deleteOps = acquiredLocks.map(k => env.TOPIC_MAP.delete(k));
                            await Promise.all(deleteOps);

                            await tgCall(env, "sendMessage", {
                                chat_id: userId,
                                text: `📩 刚才的 ${acquiredLocks.length} 条消息已帮您送达。`
                            });
                        }
                    }
                } catch (e) {
                    Logger.error('pending_message_forward_failed', e, { userId });
                    await tgCall(env, "sendMessage", {
                        chat_id: userId,
                        text: "⚠️ 自动发送失败，请重新发送您的消息。"
                    });
                }
            }
        } else {
            Logger.info('verification_failed', {
                userId,
                verifyId,
                selectedIndex,
                correctIndex: state.answerIndex
            });

            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 答案错误",
                show_alert: true
            });
        }
    } catch (e) {
        Logger.error('callback_query_error', e, {
            userId: query.from?.id,
            callbackData: query.data
        });
        await tgCall(env, "answerCallbackQuery", {
            callback_query_id: query.id,
            text: `⚠️ 系统错误，请重试`,
            show_alert: true
        });
    }
}

async function handleAdminCallback(query, env, ctx) {
    try {
        const data = query.data;
        const parts = data.split(":");
        const action = parts[1];
        const userId = parts[2] ? Number(parts[2]) : null;
        const senderId = query.from.id;

        if (!(await isAdminUser(env, senderId))) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "⛔️ 只有管理员可以使用此功能",
                show_alert: true
            });
            return;
        }

        await tgCall(env, "answerCallbackQuery", { callback_query_id: query.id });

        const threadId = query.message?.message_thread_id;
        const chatId = query.message?.chat?.id;
        const messageId = query.message?.message_id;

        if (action === "menu" && userId) {
            await showAdminUserMenu(chatId, threadId, userId, env, null, messageId);
            return;
        }

        if (!userId) {
            await showAdminUserMenu(chatId, threadId, userId, env, "❌ 无效的用户", messageId);
            return;
        }

        if (action === "close") {
            const key = `user:${userId}`;
            let rec = userDataCache.get(key) || await safeGetJSON(env, key, null);
            if (rec) {
                rec.closed = true;
                await env.TOPIC_MAP.put(key, JSON.stringify(rec));
                userDataCache.set(key, rec);
                if (rec.thread_id) {
                    await tgCall(env, "closeForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: rec.thread_id });
                }
            }
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "open") {
            const key = `user:${userId}`;
            let rec = userDataCache.get(key) || await safeGetJSON(env, key, null);
            if (rec) {
                rec.closed = false;
                await env.TOPIC_MAP.put(key, JSON.stringify(rec));
                userDataCache.set(key, rec);
                if (rec.thread_id) {
                    await tgCall(env, "reopenForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: rec.thread_id });
                }
            }
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "reset") {
            await env.TOPIC_MAP.delete(`verified:${userId}`);
            recentVerifiedCache.delete(String(userId));
            verificationStatusCache.invalidate(`verified:${userId}`);
            if (userId) {
                userDataCache.invalidate(`user:${userId}`);
            }
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "trust") {
            await env.TOPIC_MAP.put(`verified:${userId}`, "trusted");
            await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
            recentVerifiedCache.set(String(userId), "trusted");
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "ban") {
            await env.TOPIC_MAP.put(`banned:${userId}`, "1");
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "unban") {
            await env.TOPIC_MAP.delete(`banned:${userId}`);
            await sendAdminPanel(threadId, userId, env, messageId);
            return;
        }

        if (action === "info") {
            await showAdminUserInfo(chatId, threadId, userId, env, messageId);
            return;
        }

        if (action === "cleanup") {
            if (threadId) {
                ctx.waitUntil(handleCleanupCommand(threadId, env));
            }
            return;
        }

    } catch (e) {
        Logger.error('admin_callback_error', e, {
            senderId: query.from?.id,
            callbackData: query.data
        });
        await tgCall(env, "answerCallbackQuery", {
            callback_query_id: query.id,
            text: "⚠️ 操作失败，请重试",
            show_alert: true
        });
    }
}

async function showAdminUserMenu(chatId, threadId, userId, env, msg = null, messageId = null) {
    const userKey = `user:${userId}`;
    const userRec = userDataCache.get(userKey) || await safeGetJSON(env, userKey, null);
    const verifyStatus = await env.TOPIC_MAP.get(`verified:${userId}`);
    const banStatus = await env.TOPIC_MAP.get(`banned:${userId}`);

    const isClosed = userRec?.closed || false;
    const isTrusted = verifyStatus === "trusted";
    const isBanned = !!banStatus;

    const statusText = msg ? `${msg}\n\n` : "";

    const text = `${statusText}👤 **用户操作面板**\nUID: \`${userId}\`\n话题: ${userRec?.title || "未知"}\n状态: ${isBanned ? "🚫 已封禁" : isTrusted ? "🌟 永久信任" : "✅ 正常"}\n对话: ${isClosed ? "🚫 已关闭" : "✅ 开启中"}`;

    const buttons = [
        [
            { text: isClosed ? "✅ 开启对话" : "🚫 关闭对话",
              callback_data: `admin:${isClosed ? "open" : "close"}:${userId}` },
            { text: "🔄 重置验证", callback_data: `admin:reset:${userId}` }
        ],
        [
            { text: isTrusted ? "⏳ 取消永久信任" : "🌟 设置永久信任",
              callback_data: `admin:trust:${userId}` },
            { text: isBanned ? "✅ 解封用户" : "🚫 封禁用户",
              callback_data: `admin:${isBanned ? "unban" : "ban"}:${userId}` }
        ],
        [
            { text: "👤 查看详情", callback_data: `admin:info:${userId}` },
            { text: "🗑 清理无效用户", callback_data: `admin:cleanup:${userId}` }
        ],
        [
            { text: "📨 联系用户", url: `tg://user?id=${userId}` }
        ]
    ];

    if (messageId) {
        await tgCall(env, "editMessageText", {
            chat_id: chatId,
            message_id: messageId,
            text,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        });
    } else {
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: chatId,
            text,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        }, threadId));
    }
}

async function showAdminUserInfo(chatId, threadId, userId, env, messageId = null) {
    const userKey = `user:${userId}`;
    const userRec = userDataCache.get(userKey) || await safeGetJSON(env, userKey, null);
    const verifyStatus = await env.TOPIC_MAP.get(`verified:${userId}`);
    const banStatus = await env.TOPIC_MAP.get(`banned:${userId}`);

    const isTrusted = verifyStatus === "trusted";
    const isBanned = !!banStatus;

    const text = `👤 **用户详细信息**\n\n` +
        `UID: \`${userId}\`\n` +
        `话题ID: \`${userRec?.thread_id || "无"}\`\n` +
        `话题标题: ${userRec?.title || "未知"}\n` +
        `验证状态: ${isTrusted ? "🌟 永久信任" : isBanned ? "🚫 已封禁" : "✅ 已验证"}\n` +
        `封禁状态: ${isBanned ? "🚫 已封禁" : "✅ 正常"}\n` +
        `对话状态: ${userRec?.closed ? "🚫 已关闭" : "✅ 开启中"}\n` +
        `Link: [点击私聊](tg://user?id=${userId})`;

    const buttons = [
        [
            { text: "⬅️ 返回操作面板", callback_data: `admin:menu:${userId}` }
        ]
    ];

    if (messageId) {
        await tgCall(env, "editMessageText", {
            chat_id: chatId,
            message_id: messageId,
            text,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        });
    } else {
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: chatId,
            text,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        }, threadId));
    }
}

// ---------------- 辅助函数 ----------------

async function handleAdminListCommand(text, threadId, env) {
    if (text === "/admin") {
        const allKeys = await getAllKeys(env, "user:");
        if (allKeys.length === 0) {
            await tgCall(env, "sendMessage", withMessageThreadId({
                chat_id: env.SUPERGROUP_ID,
                text: "📭 **暂无用户数据**",
                parse_mode: "Markdown"
            }, threadId));
            return;
        }

        const buttons = [];
        const displayCount = Math.min(allKeys.length, 20);
        for (let i = 0; i < displayCount; i++) {
            const name = allKeys[i].name;
            const uid = name.slice(5);
            buttons.push([{
                text: `👤 ${uid}`,
                callback_data: `admin:menu:${uid}`
            }]);
        }

        const footerButtons = [];
        if (allKeys.length > 20) {
            footerButtons.push({ text: `还有 ${allKeys.length - 20} 个用户...`, callback_data: "admin:list_more:0" });
        }
        footerButtons.push({ text: "🗑 清理无效用户", callback_data: `admin:cleanup:0` });

        if (footerButtons.length > 0) {
            buttons.push(footerButtons);
        }

        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `📋 **用户管理面板**\n\n共有 ${allKeys.length} 个用户\n点击按钮进入用户管理：`,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        }, threadId));
        return;
    }

    if (text.startsWith("/admin ")) {
        const targetUid = text.split(" ")[1]?.trim();
        if (!targetUid || !Number(targetUid)) {
            await tgCall(env, "sendMessage", withMessageThreadId({
                chat_id: env.SUPERGROUP_ID,
                text: "❌ **无效的用户UID**\n\n请输入正确的数字UID",
                parse_mode: "Markdown"
            }, threadId));
            return;
        }
        await showAdminUserMenu(env.SUPERGROUP_ID, threadId, Number(targetUid), env);
    }
}

async function sendAdminPanel(threadId, userId, env, messageId) {
    const key = `user:${userId}`;
    const rec = userDataCache.get(key) || await safeGetJSON(env, key, null);
    const verifyStatus = await env.TOPIC_MAP.get(`verified:${userId}`);
    const banStatus = await env.TOPIC_MAP.get(`banned:${userId}`);

    const isClosed = rec?.closed || false;
    const isTrusted = verifyStatus === "trusted";
    const isBanned = !!banStatus;

    const buttons = [
        [
            { text: "👤 查看用户", url: `tg://user?id=${userId}` }
        ],
        [
            { text: isClosed ? "✅ 开启对话" : "🚫 关闭对话",
              callback_data: `admin:${isClosed ? "open" : "close"}:${userId}` },
            { text: isBanned ? "✅ 解封" : "🚫 封禁",
              callback_data: `admin:${isBanned ? "unban" : "ban"}:${userId}` }
        ],
        [
            { text: "🔄 重置验证", callback_data: `admin:reset:${userId}` },
            { text: isTrusted ? "⏳ 取消信任" : "🌟 信任",
              callback_data: `admin:trust:${userId}` }
        ],
        [
            { text: "👤 查看详情", callback_data: `admin:info:${userId}` },
            { text: "🗑 清理无效", callback_data: `admin:cleanup:${userId}` }
        ]
    ];

    const text = `🎮 *管理员控制面板*\n\nUID: \`${userId}\`\n状态: ${isBanned ? "🚫 已封禁" : isTrusted ? "🌟 已信任" : "✅ 正常"}\n对话: ${isClosed ? "🚫 已关闭" : "✅ 开启中"}`;

    if (messageId) {
        await Promise.all([
            tgCall(env, "sendMessage", {
                chat_id: env.SUPERGROUP_ID,
                message_thread_id: threadId,
                text,
                parse_mode: "Markdown",
                reply_markup: { inline_keyboard: buttons }
            }),
            tgCall(env, "deleteMessage", {
                chat_id: env.SUPERGROUP_ID,
                message_thread_id: threadId,
                message_id: messageId
            })
        ]);
    } else {
        await tgCall(env, "sendMessage", {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: threadId,
            text,
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: buttons }
        });
    }
}

/**
 * 【修复 #8】批量清理命令处理函数（优化并发性能）
 *
 * 功能说明：
 * 1. 检查所有用户的话题记录
 * 2. 找出话题ID已不存在（被删除）的用户
 * 3. 删除这些用户的KV存储记录和验证状态
 * 4. 让他们下次发消息时重新验证并创建新话题
 *
 * 使用场景：
 * - 管理员手动删除了多个用户话题后
 * - 需要批量重置这些用户的状态
 *
 * @param {number} threadId - 当前话题ID（通常在General话题中调用）
 * @param {object} env - 环境变量对象
 */
async function handleCleanupCommand(threadId, env) {
    const lockKey = "cleanup:lock";
    const locked = await env.TOPIC_MAP.get(lockKey);
    if (locked) {
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: "⏳ **已有清理任务正在运行，请稍后再试。**",
            parse_mode: "Markdown"
        }, threadId));
        return;
    }

    await env.TOPIC_MAP.put(lockKey, "1", { expirationTtl: CONFIG.CLEANUP_LOCK_TTL_SECONDS });

    // 发送处理中的消息
    await tgCall(env, "sendMessage", withMessageThreadId({
        chat_id: env.SUPERGROUP_ID,
        text: "🔄 **正在扫描需要清理的用户...**",
        parse_mode: "Markdown"
    }, threadId));

    let cleanedCount = 0;
    let errorCount = 0;
    const cleanedUsers = [];
    let scannedCount = 0;

    try {
        // 逐页扫描，避免一次性拉取全部 keys 导致超时/内存膨胀
        let cursor = undefined;
        do {
            const result = await env.TOPIC_MAP.list({ prefix: "user:", cursor });
            const names = (result.keys || []).map(k => k.name);
            scannedCount += names.length;

            // 批量并发处理（限制并发数）
            for (let i = 0; i < names.length; i += CONFIG.CLEANUP_BATCH_SIZE) {
                const batch = names.slice(i, i + CONFIG.CLEANUP_BATCH_SIZE);

                const results = await Promise.allSettled(
                    batch.map(async (name) => {
                        const rec = await safeGetJSON(env, name, null);
                    if (!rec || !rec.thread_id) return null;

                    const userId = name.slice(5);
                    const topicThreadId = rec.thread_id;

                    // 检测话题是否存在：尝试向话题发送测试消息
                    const probe = await probeForumThread(env, topicThreadId, {
                        userId,
                        reason: "cleanup_check",
                        doubleCheckOnMissingThreadId: false
                    });

                    // cleanup 要求更保守：仅在明确缺失/重定向时清理，避免误删有效记录
                    if (probe.status === "redirected" || probe.status === "missing") {
                            await env.TOPIC_MAP.delete(name);
                            await env.TOPIC_MAP.delete(`verified:${userId}`);
                            await env.TOPIC_MAP.delete(`thread:${topicThreadId}`);

                            return {
                                userId,
                                threadId: topicThreadId,
                                title: rec.title || "未知"
                            };
                    } else if (probe.status === "probe_invalid") {
                        Logger.warn('cleanup_probe_invalid_message', {
                            userId,
                            threadId: topicThreadId,
                            errorDescription: probe.description
                        });
                    } else if (probe.status === "unknown_error") {
                        Logger.warn('cleanup_probe_failed_unknown', {
                            userId,
                            threadId: topicThreadId,
                            errorDescription: probe.description
                        });
                    } else if (probe.status === "missing_thread_id") {
                        Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                    }

                    return null;
                })
            );

            // 处理结果
            results.forEach(result => {
                if (result.status === 'fulfilled' && result.value) {
                    cleanedCount++;
                    cleanedUsers.push(result.value);
                    Logger.info('cleanup_user', {
                        userId: result.value.userId,
                        threadId: result.value.threadId
                    });
                } else if (result.status === 'rejected') {
                    errorCount++;
                    Logger.error('cleanup_batch_error', result.reason);
                }
            });

                // 防止速率限制
                if (i + CONFIG.CLEANUP_BATCH_SIZE < names.length) {
                    await new Promise(r => setTimeout(r, 600));
                }
            }

            cursor = result.list_complete ? undefined : result.cursor;

            // 在分页之间让出时间片，降低单次执行压力
            if (cursor) {
                await new Promise(r => setTimeout(r, 200));
            }
        } while (cursor);

        // 生成并发送清理报告
        let reportText = `✅ **清理完成**\n\n`;
        reportText += `📊 **统计信息**\n`;
        reportText += `- 扫描用户数: ${scannedCount}\n`;
        reportText += `- 已清理用户数: ${cleanedCount}\n`;
        reportText += `- 错误数: ${errorCount}\n\n`;

        if (cleanedCount > 0) {
            reportText += `🗑️ **已清理的用户** (话题已删除):\n`;
            for (const user of cleanedUsers.slice(0, CONFIG.MAX_CLEANUP_DISPLAY)) {
                reportText += `- UID: \`${user.userId}\` | 话题: ${user.title}\n`;
            }
            if (cleanedUsers.length > CONFIG.MAX_CLEANUP_DISPLAY) {
                reportText += `\n...(还有 ${cleanedUsers.length - CONFIG.MAX_CLEANUP_DISPLAY} 个用户)\n`;
            }
            reportText += `\n💡 这些用户下次发消息时将重新进行人机验证并创建新话题。`;
        } else {
            reportText += `✨ 没有发现需要清理的用户记录。`;
        }

        Logger.info('cleanup_completed', {
            cleanedCount,
            errorCount,
            totalUsers: scannedCount
        });

        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: reportText,
            parse_mode: "Markdown"
        }, threadId));

    } catch (e) {
        Logger.error('cleanup_failed', e, { threadId });
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `❌ **清理过程出错**\n\n错误信息: \`${e.message}\``,
            parse_mode: "Markdown"
        }, threadId));
    } finally {
        await env.TOPIC_MAP.delete(lockKey);
    }
}

// ---------------- 其他辅助函数 ----------------

// 为话题建立 thread->user 映射，避免管理员命令时全量 KV 反查
async function createTopic(from, key, env, userId) {
    const title = buildTopicTitle(from);
    if (!env.SUPERGROUP_ID.toString().startsWith("-100")) throw new Error("SUPERGROUP_ID必须以-100开头");
    const res = await tgCall(env, "createForumTopic", { chat_id: env.SUPERGROUP_ID, name: title });
    if (!res.ok) throw new Error(`创建话题失败: ${res.description}`);
    const rec = { thread_id: res.result.message_thread_id, title, closed: false };
    await env.TOPIC_MAP.put(key, JSON.stringify(rec));
    if (userId) {
        await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
    }
    return rec;
}

// 优化：更新话题状态 - 使用缓存和并行操作
async function updateThreadStatus(threadId, isClosed, env) {
    try {
        const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
        if (mappedUser) {
            const userKey = `user:${mappedUser}`;
            const rec = userDataCache.get(userKey) || await safeGetJSON(env, userKey, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                await env.TOPIC_MAP.put(userKey, JSON.stringify(rec));
                userDataCache.set(userKey, rec);
                Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                return;
            }
            await env.TOPIC_MAP.delete(`thread:${threadId}`);
        }

        const allKeys = await getAllKeys(env, "user:");
        const batchSize = CONFIG.CLEANUP_BATCH_SIZE;
        const updates = [];

        for (let i = 0; i < allKeys.length; i += batchSize) {
            const batch = allKeys.slice(i, i + batchSize);
            const results = await Promise.all(
                batch.map(async ({ name }) => {
                    const rec = userDataCache.get(name) || await safeGetJSON(env, name, null);
                    if (rec && Number(rec.thread_id) === Number(threadId)) {
                        rec.closed = isClosed;
                        userDataCache.set(name, rec);
                        return env.TOPIC_MAP.put(name, JSON.stringify(rec));
                    }
                    return null;
                })
            );
            results.forEach(r => { if (r) updates.push(r); });
        }

        await Promise.all(updates);
        Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: updates.length });
    } catch (e) {
        Logger.error('thread_status_update_failed', e, { threadId, isClosed });
        throw e;
    }
}

// 改进的话题标题构建（清理特殊字符）
function buildTopicTitle(from) {
  const firstName = (from.first_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);
  const lastName = (from.last_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);

  // 清理 username
  let username = "";
  if (from.username) {
      username = from.username
          .replace(/[^\w]/g, '')  // 只保留字母数字下划线
          .substring(0, 20);
  }

  // 移除控制字符和换行符
  const cleanName = (firstName + " " + lastName)
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
      .replace(/\s+/g, ' ')
      .trim();

  const name = cleanName || "User";
  const usernameStr = username ? ` @${username}` : "";

  // Telegram 话题标题最大长度为 128 字符
  const title = (name + usernameStr).substring(0, CONFIG.MAX_TITLE_LENGTH);

  return title;
}

// 优化：带指数退避重试的 Telegram API 调用
async function tgCall(env, method, body, timeout = CONFIG.API_TIMEOUT_MS, retries = 2) {
  let base = env.API_BASE || "https://api.telegram.org";

  if (base.startsWith("http://")) {
      Logger.warn('api_http_upgraded', { originalBase: base });
      base = base.replace("http://", "https://");
  }

  try {
      new URL(`${base}/test`);
  } catch (e) {
      Logger.error('api_base_invalid', e, { base });
      base = "https://api.telegram.org";
  }

  const url = `${base}/bot${env.BOT_TOKEN}/${method}`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
        const resp = await fetch(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!resp.ok && resp.status >= 500) {
            Logger.warn('telegram_api_server_error', {
                method,
                status: resp.status,
                attempt
            });
            if (attempt < retries) {
                await sleep(100 * Math.pow(2, attempt));
                continue;
            }
            return { ok: false, description: `HTTP ${resp.status}` };
        }

        const result = await resp.json();

        if (!result.ok && result.description && result.description.includes('Too Many Requests')) {
            const retryAfter = result.parameters?.retry_after || 5;
            Logger.warn('telegram_api_rate_limit', { method, retryAfter, attempt });
            if (attempt < retries && retryAfter < 10) {
                await sleep(retryAfter * 1000);
                continue;
            }
        }

        return result;
    } catch (e) {
        clearTimeout(timeoutId);

        if (e.name === 'AbortError') {
            Logger.warn('telegram_api_timeout', { method, timeout, attempt });
            if (attempt < retries) {
                await sleep(100 * Math.pow(2, attempt));
                continue;
            }
            return { ok: false, description: 'Request timeout' };
        }

        Logger.error('telegram_api_failed', e, { method, attempt });
        if (attempt < retries) {
            await sleep(100 * Math.pow(2, attempt));
            continue;
        }
        throw e;
    }
  }
}

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

async function handleMediaGroup(msg, env, ctx, { direction, targetChat, threadId }) {
    const groupId = msg.media_group_id;
    const key = `mg:${direction}:${groupId}`;
    const item = extractMedia(msg);
    if (!item) {
        await tgCall(env, "copyMessage", withMessageThreadId({
            chat_id: targetChat,
            from_chat_id: msg.chat.id,
            message_id: msg.message_id
        }, threadId));
        return;
    }
    let rec = await safeGetJSON(env, key, null);
    if (!rec) rec = { direction, targetChat, threadId: (threadId === null ? undefined : threadId), items: [], last_ts: Date.now() };
    rec.items.push({ ...item, msg_id: msg.message_id });
    rec.last_ts = Date.now();
    await env.TOPIC_MAP.put(key, JSON.stringify(rec), { expirationTtl: CONFIG.MEDIA_GROUP_EXPIRE_SECONDS });
    ctx.waitUntil(delaySend(env, key, rec.last_ts));
}

// 【修复 #15, #19】改进的媒体提取（支持更多类型，不修改原数组）
function extractMedia(msg) {
    // 图片
    if (msg.photo && msg.photo.length > 0) {
        const highestResolution = msg.photo[msg.photo.length - 1];  // 不使用 pop()
        return {
            type: "photo",
            id: highestResolution.file_id,
            cap: msg.caption || ""
        };
    }

    // 视频
    if (msg.video) {
        return {
            type: "video",
            id: msg.video.file_id,
            cap: msg.caption || ""
        };
    }

    // 文档
    if (msg.document) {
        return {
            type: "document",
            id: msg.document.file_id,
            cap: msg.caption || ""
        };
    }

    // 音频
    if (msg.audio) {
        return {
            type: "audio",
            id: msg.audio.file_id,
            cap: msg.caption || ""
        };
    }

    // 动图
    if (msg.animation) {
        return {
            type: "animation",
            id: msg.animation.file_id,
            cap: msg.caption || ""
        };
    }

    // 语音和视频消息不支持 media group
    return null;
}

// 【优化】改为定时清理，控制清理频率
async function flushExpiredMediaGroupsIfNeeded(env, now) {
    if (now - lastMediaGroupFlush < CONFIG.FLUSH_MEDIA_GROUP_INTERVAL_MS) {
        return;
    }
    lastMediaGroupFlush = now;
    await flushExpiredMediaGroups(env, now);
}

async function flushExpiredMediaGroups(env, now) {
    try {
        const prefix = "mg:";
        const allKeys = await getAllKeys(env, prefix);
        if (allKeys.length === 0) return;

        let deletedCount = 0;

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && rec.last_ts && (now - rec.last_ts > CONFIG.MEDIA_GROUP_EXPIRE_SECONDS * 1000)) {
                await env.TOPIC_MAP.delete(name);
                deletedCount++;
            }
        }

        if (deletedCount > 0) {
            Logger.info('media_groups_cleaned', { deletedCount });
        }
    } catch (e) {
        Logger.error('media_group_cleanup_failed', e);
    }
}

// 【修复 #12, #28】改进媒体组延迟发送
async function delaySend(env, key, ts) {
    await new Promise(r => setTimeout(r, CONFIG.MEDIA_GROUP_DELAY_MS));

    const rec = await safeGetJSON(env, key, null);

    if (rec && rec.last_ts === ts) {
        // 验证媒体数组
        if (!rec.items || rec.items.length === 0) {
            Logger.warn('media_group_empty', { key });
            await env.TOPIC_MAP.delete(key);
            return;
        }

        const media = rec.items.map((it, i) => {
            if (!it.type || !it.id) {
                Logger.warn('media_group_invalid_item', { key, item: it });
                return null;
            }
            // 【修复 #28】限制 caption 长度
            const caption = i === 0 ? (it.cap || "").substring(0, 1024) : "";
            return { 
                type: it.type,
                media: it.id,
                caption
            };
        }).filter(Boolean);  // 过滤掉无效项

        if (media.length > 0) {
            try {
                const result = await tgCall(env, "sendMediaGroup", withMessageThreadId({
                    chat_id: rec.targetChat,
                    media
                }, rec.threadId));

                if (!result.ok) {
                    Logger.error('media_group_send_failed', result.description, {
                        key,
                        mediaCount: media.length
                    });
                } else {
                    Logger.info('media_group_sent', {
                        key,
                        mediaCount: media.length,
                        targetChat: rec.targetChat
                    });
                }
            } catch (e) {
                Logger.error('media_group_send_exception', e, { key });
            }
        }

        await env.TOPIC_MAP.delete(key);
    }
}
