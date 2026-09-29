# -*- coding: utf-8 -*-
"""
河海大学教务抢课器  ——  xk_runner.py

登录(Playwright) -> 导出会话到 requests -> 拉课程 -> 本地 Web UI 勾选入队 -> 排队抢课

用法:
    python xk_runner.py --acc <学号>
    python xk_runner.py --acc <学号> --pwd <密码> --port 8899 --interval 0.5
    python xk_runner.py --no-browser --round-every 1
    环境变量: HHU_ACC / HHU_PWD
"""
import os
import sys
import json
import re
import time
import random
import threading
import queue
import argparse
import http.cookiejar as cookiejar
import http.server
from urllib.parse import urlparse, parse_qs
from datetime import datetime

import requests
from requests.packages import urllib3

urllib3.disable_warnings()

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

BASE = 'https://jwxt.hhu.edu.cn'
CAS_URL = 'https://authserver.hhu.edu.cn/authserver/login'
SERVICE = BASE + '/jsxsd/sso.jsp'
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, 'session_state.json')
UI_FILE = os.path.join(HERE, 'webui.html')
QUEUE_FILE = os.path.join(HERE, 'queue.json')

TASK_KEYS = ('jx0404id', 'jx02id', 'cfbs', 'kcmc', 'kch', 'skls', 'sksj',
             'xkzy', 'trjf', 'kind', 'oper', 'xf')


def save_queue():
    try:
        with S.lock:
            data = []
            for t in S.queue_tasks.values():
                d = {k: t.get(k) for k in TASK_KEYS}
                d['status'] = t.get('status')
                d['tries'] = t.get('tries', 0)
                data.append(d)
        with open(QUEUE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    except Exception as e:
        S.log(f'保存队列失败: {str(e)[:80]}', 'warn')


def load_queue():
    if not os.path.exists(QUEUE_FILE):
        return []
    try:
        with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


# ----------------------------------------------------------------------------
# 全局状态
# ----------------------------------------------------------------------------
class State:
    def __init__(self):
        self.lock = threading.RLock()
        self.courses = []          # 全部可选课程
        self.queue_tasks = {}      # jx0404id -> task dict
        self.logs = []             # 最近日志
        self.running = False
        self.acc = ''
        self.login_ok = False
        self.term = ''
        self.xklc_id = ''
        self.window_start = None   # datetime
        self.window_end = None
        self.preview_start = None
        self.preview_end = None
        self.interval = 0.5
        self.subs = []             # SSE 订阅者队列

    def log(self, msg, level='info'):
        ts = time.strftime('%H:%M:%S')
        line = {'t': ts, 'msg': msg, 'level': level}
        with self.lock:
            self.logs.append(line)
            if len(self.logs) > 500:
                del self.logs[:200]
            for q in list(self.subs):
                try:
                    q.put_nowait(('log', line))
                except Exception:
                    pass
        print(f'[{ts}] {msg}', flush=True)

    def broadcast(self, kind, payload):
        with self.lock:
            for q in list(self.subs):
                try:
                    q.put_nowait((kind, payload))
                except Exception:
                    pass

    def snapshot(self):
        with self.lock:
            tasks = []
            for t in self.queue_tasks.values():
                tasks.append({
                    'jx0404id': t['jx0404id'],
                    'kcmc': t['kcmc'],
                    'kch': t.get('kch', ''),
                    'xf': t.get('xf', ''),
                    'skls': t.get('skls', ''),
                    'sksj': t.get('sksj', ''),
                    'status': t['status'],
                    'tries': t['tries'],
                    'last': t['last'],
                    'kind': t.get('kind', ''),
                })
            return {
                'login_ok': self.login_ok,
                'acc': self.acc,
                'running': self.running,
                'term': self.term,
                'xklc_id': self.xklc_id,
                'window_start': self.window_start.strftime('%Y-%m-%d %H:%M:%S') if self.window_start else None,
                'window_end': self.window_end.strftime('%Y-%m-%d %H:%M:%S') if self.window_end else None,
                'preview_start': self.preview_start.strftime('%Y-%m-%d %H:%M:%S') if self.preview_start else None,
                'preview_end': self.preview_end.strftime('%Y-%m-%d %H:%M:%S') if self.preview_end else None,
                'server_now': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'courses': len(self.courses),
                'kinds': [{'key': k, 'name': v.get('name', k)} for k, v in KINDS.items()],
                'queue': tasks,
                'interval': self.interval,
            }


S = State()


# ----------------------------------------------------------------------------
# 登录
# ----------------------------------------------------------------------------
def login_with_playwright(acc, pwd, headless=True):
    """用真实浏览器过 CAS（缺浏览器指纹 cookie 会被服务端 500）。"""
    from playwright.sync_api import sync_playwright

    state = {'cookies': []}
    with sync_playwright() as p:
        launch_kw = dict(headless=headless, args=['--ignore-certificate-errors'])
        browser = None
        for extra in ({'channel': 'msedge'}, {'channel': 'chrome'}, {}):
            try:
                browser = p.chromium.launch(**launch_kw, **extra)
                S.log(f'浏览器已启动 {extra.get("channel", "chromium")}')
                break
            except Exception as e:
                S.log(f'启动失败 {extra}: {str(e)[:80]}', 'warn')
        if browser is None:
            raise RuntimeError('无法启动浏览器，请安装 Edge/Chrome 或执行 playwright install chromium')

        ctx = browser.new_context(ignore_https_errors=True, locale='zh-CN',
                                  user_agent=UA, viewport={'width': 1440, 'height': 900})
        pg = ctx.new_page()
        url = f'{CAS_URL}?service={requests.utils.quote(SERVICE, safe="")}'
        pg.goto(url, wait_until='domcontentloaded', timeout=60000)
        pg.evaluate(
            """([a,p])=>{
                document.querySelector('#username').value=a;
                var el=document.querySelector('#password');
                el.removeAttribute('readonly');
                el.value=p;
            }""", [acc, pwd])
        pg.click('#login_submit', timeout=20000)
        pg.wait_for_timeout(6000)

        cur = pg.url
        S.log(f'登录后落地: {cur[:90]}')
        if 'authserver' in cur:
            body = pg.content()
            m = re.search(r'id="showErrorTip"[^>]*>(.{0,200}?)</', body, re.S)
            tip = re.sub(r'<[^>]+>', '', m.group(1)).strip() if m else '未知错误'
            browser.close()
            raise RuntimeError(f'登录失败: {tip}')

        # 抖一下，确保服务端把会话写全
        try:
            pg.goto(BASE + '/jsxsd/framework/xsrkxz.htmlx', wait_until='domcontentloaded', timeout=30000)
            pg.wait_for_timeout(1200)
        except Exception:
            pass

        state = ctx.storage_state()
        browser.close()

    names = [c['name'] for c in state.get('cookies', []) if 'jwxt.hhu.edu.cn' in c['domain']]
    S.log(f'会话 cookie: {names}')
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False)
    return state


def state_to_session(state):
    jar = cookiejar.CookieJar()
    for c in state.get('cookies', []):
        dom = c.get('domain', '')
        if 'hhu.edu.cn' not in dom:
            continue
        exp = c.get('expires')
        if exp is not None and exp < 0:
            exp = None
        jar.set_cookie(cookiejar.Cookie(
            version=0, name=c['name'], value=c['value'], port=None, port_specified=False,
            domain=dom, domain_specified=bool(dom), domain_initial_dot=dom.startswith('.'),
            path=c.get('path', '/'), path_specified=True, secure=bool(c.get('secure')),
            expires=exp, discard=False, comment=None, comment_url=None,
            rest={'HttpOnly': ''}, rfc2109=False))
    sess = requests.Session()
    sess.cookies = jar
    sess.verify = False
    sess.headers.update({
        'User-Agent': UA,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': BASE + '/jsxsd/xsxk/xklc_list',
    })
    return sess


HTTP = {'sess': None}


# 教务的失效形态不止一种：页面跳 CAS 是一种，JSON 里塞
# {"flag1":2,"msgContent":"请先登录系统"} 是另一种，后者不带 authserver 字样。
RELOGIN_MARKS = ('authserver', '统一身份认证', '请先登录', '请重新登录',
                 '登录超时', '登录已失效', '会话已失效', '未登录系统')


def need_relogin(resp_text):
    head = resp_text[:3000]
    return any(m in head for m in RELOGIN_MARKS)


# ----------------------------------------------------------------------------
# 教务数据
# ----------------------------------------------------------------------------
def fetch_round(sess):
    """轮次列表 -> 取当天可用的轮次 ID"""
    r = sess.get(BASE + '/jsxsd/xsxk/xklc_list?Ves632DSdyV=NEW_XSD_PYGL', timeout=25)
    html = r.text
    if need_relogin(html):
        raise RuntimeError('会话已失效，请重新登录')
    # 各校/各版本的入口函数名不一致，行里到底是 toxk / jrxk / comeInXkIndx
    # 事先看不出来（没轮次时表格是空的），所以全都捞一遍再保序去重。
    ids = []
    for pat in (r"""toxk\(['"]([0-9A-Za-z_-]{6,64})['"]\)""",
                r"""jrxk\(['"]([0-9A-Za-z_-]{6,64})['"]\)""",
                r"""comeInXkIndx\(['"]([0-9A-Za-z_-]{6,64})['"]\)""",
                r"""jrxk_zgny\(['"]([0-9A-Za-z_-]{6,64})['"]\)""",
                r'xklc_view\?jx0502zbid=([0-9A-Za-z_-]{6,64})',
                r'xsxk_index\?jx0502zbid=([0-9A-Za-z_-]{6,64})'):
        ids.extend(re.findall(pat, html))
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]
    name = ''
    m = re.search(r'(\d{4}-\d{4}-\d\s*学期[^<\s]{2,40})', re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html)))
    if m:
        name = m.group(1)
    return ids, html, name


def fetch_round_detail(sess, xklc_id):
    r = sess.get(BASE + f'/jsxsd/xsxk/xklc_view?jx0502zbid={xklc_id}', timeout=25)
    h = r.text
    txt = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', h))

    def grab(label):
        # 加负向后顾：'预览选课时间' 里也含 '选课时间'，不排掉会抓错时间段
        m = re.search(r'(?<!预览)' + label +
                      r'[：:]\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})(?::\d{2})?\s*[~～-]\s*'
                      r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})(?::\d{2})?', txt)
        if not m:
            return None, None
        return (datetime.strptime(m.group(1), '%Y-%m-%d %H:%M'),
                datetime.strptime(m.group(2), '%Y-%m-%d %H:%M'))

    ws, we = grab('选课时间')
    ps, pe = grab('预览选课时间')
    name = ''
    m = re.search(r'选课名称\s*[:：]?\s*([^\s]+)', txt)
    if m:
        name = m.group(1)
    return ws, we, ps, pe, name, txt


def wait_for_round(sess, every=1.0):
    """守着选课轮次。轮次只在自己那段时间才出现在列表里，早启动必然拉空。
    出现即返回 (xklc_id, window_start, window_end, name)。

    every 默认 1 秒——轮次一放出来就得立刻抓住，晚十几秒热门课就没了。
    查询连续失败时逐步拉长间隔，服务端抖动期间别跟着添乱。"""
    n = 0
    err = 0
    while True:
        try:
            ids, _, rname = fetch_round(sess)
            err = 0
        except Exception as e:
            err += 1
            ids, rname = [], ''
            if err <= 3 or err % 20 == 0:
                S.log(f'轮次查询异常(第{err}次): {str(e)[:70]}', 'warn')
        if ids:
            xid = ids[0]
            ws = we = None
            name = rname
            try:
                ws, we, _, _, rn, _ = fetch_round_detail(sess, xid)
                name = rn or rname
            except Exception:
                pass
            S.log(f'轮次已放出，用时 {n + 1} 次查询', 'ok')
            return xid, ws, we, name
        n += 1
        if n % 20 == 1:
            S.log(f'教务还没放出选课轮次，已查 {n} 次…')
        time.sleep(every if err < 5 else min(every * err, 30.0))


# ---------------------------------------------------------------------------
# 选课类别
# 强智按轮次开放不同类别（公选课 / 体育课 / 本专业选课 / 跨专业选课 ...），
# 每个类别的列表接口、查询串、列定义、提交接口都不一样。下面先从选课入口页
# 动态解析本轮实际开放的类别；解析不到才退回静态注册表，而表里只放了实测过
# 配置的那两类，其余类别一律按页面解析，所以表短不代表本轮类别少。
# ---------------------------------------------------------------------------
FALLBACK_KINDS = {
    'Ggxxkxk': {
        'name': '公选课',
        'come_in': '/jsxsd/xsxkkc/comeInGgxxkxk',
        'list_path': '/jsxsd/xsxkkc/xsxkGgxxkxk',
        'query': '?kcxx=&skls=&skxq=&skjc=&endJc=&sfym=false&sfct=false&szjylb=&sfxx=false',
        'cols': ['jx0404id', 'kch', 'kcmc', 'xf', 'skls', 'sksj', 'skdd', 'xqmc',
                 'xkrs', 'syrs', 'ctsm', 'szkcflmc'],
        'oper': '/jsxsd/xsxkkc/ggxxkxkOper',
    },
    'Tyxk': {
        'name': '体育课',
        'come_in': '/jsxsd/xsxkkc/comeInTyxk',
        'list_path': '/jsxsd/xsxkkc/xsxkTyxk',
        'query': '?1=1&kcxx=&skls=&skfs=',
        'cols': ['kch', 'kcmc', 'fzmc', 'ktmc', 'xf', 'skls', 'sksj', 'skdd', 'xqmc',
                 'syrs', 'ctsm'],
        'oper': '/jsxsd/xsxkkc/bxxkOper',
    },
}

DEFAULT_OPER = '/jsxsd/xsxkkc/bxxkOper'
KINDS = dict(FALLBACK_KINDS)


def _js_param(text, stop):
    """还原选课页的查询串：取 sAjaxSource 之前最后一个 "?..." 赋值，再接上其 += 续写。"""
    head = text[:stop]
    base, tail_at = '', 0
    for m in re.finditer(r'param\s*=\s*([^;]{1,600});', head):
        s = ''.join(re.findall(r'"([^"]*)"', m.group(1)))
        if s.startswith('?'):
            base, tail_at = s, m.end()
    if not base:
        return ''
    for m in re.finditer(r'param\s*\+=\s*([^;]{1,600});', head[tail_at:]):
        base += ''.join(re.findall(r'"([^"]*)"', m.group(1)))
    return base


def _js_columns(text):
    """取出选课页 aoColumns 里的 mDataProp 列表。"""
    i = text.find('"aoColumns"')
    if i < 0:
        return []
    j = text.find('[', i)
    if j < 0:
        return []
    depth = 0
    for k in range(j, len(text)):
        if text[k] == '[':
            depth += 1
        elif text[k] == ']':
            depth -= 1
            if depth == 0:
                return re.findall(r'"mDataProp"\s*:\s*"([^"]+)"', text[j:k])
    return []


def parse_kind_page(sess, come_in, key, label):
    """拉类别页，解析出列表接口 / 查询串 / 列定义 / 提交接口。"""
    try:
        r = sess.get(BASE + come_in, timeout=25,
                     headers={'Referer': BASE + '/jsxsd/xsxk/xklc_list'})
    except Exception as e:
        S.log(f'读取类别页 {label} 失败: {str(e)[:60]}', 'warn')
        return None
    h = r.text
    if need_relogin(h) or len(h) < 500:
        return None
    m = re.search(r'sAjaxSource"\s*:\s*"([^"]+)"', h)
    if not m:
        return None
    cols = _js_columns(h)
    if not cols:
        return None
    # 已知类别的查询串沿用实测过的写法，只有新类别才用页面解析出来的
    query = _js_param(h, m.start())
    op = re.search(r'url\s*:\s*"(/jsxsd/xsxkkc/[A-Za-z0-9_]+Oper)"', h)
    # 已知类别一律沿用实测过的配置，只补充本轮暴露的入口；新类别完全按页面解析
    fb = FALLBACK_KINDS.get(key, {})
    return {
        'name': label or fb.get('name', key),
        'come_in': fb.get('come_in') or come_in,
        'list_path': fb.get('list_path') or m.group(1).split('?')[0],
        'query': fb.get('query') or query,
        'cols': fb.get('cols') or cols,
        'oper': fb.get('oper') or (op.group(1) if op else DEFAULT_OPER),
    }


def set_kinds(found):
    global KINDS
    if found:
        KINDS = found


def kind_names():
    return '、'.join(f"{c['name']}({k})" for k, c in KINDS.items())


def discover_kinds(sess, xklc_id):
    """解析本轮开放的选课类别。正选入口优先，预览入口兜底。"""
    if not xklc_id:
        return {}
    found = {}
    for idx in ('/jsxsd/xsxk/xsxk_index?jx0502zbid=%s',
                '/jsxsd/xsxk/yxxsxk_index?jx0502zbid=%s'):
        try:
            h = sess.get(BASE + idx % xklc_id, timeout=25).text
        except Exception as e:
            S.log(f'读取选课入口失败: {str(e)[:60]}', 'warn')
            continue
        if need_relogin(h):
            continue
        for path, label in re.findall(
                r'href="(/jsxsd/xsxkkc/[A-Za-z0-9_]*comeIn[A-Za-z0-9_]+)"[^>]*>\s*([^<]{1,24}?)\s*</a>',
                h):
            key = re.sub(r'^.*?comeIn', '', path)
            if not key or key in found:
                continue
            cfg = parse_kind_page(sess, path, key, label)
            if cfg:
                found[key] = cfg
    return found



def dt_form(cols, start=0, length=3000):
    b = {'sEcho': 1, 'iColumns': len(cols), 'sColumns': '',
         'iDisplayStart': start, 'iDisplayLength': length}
    for i, c in enumerate(cols):
        b[f'mDataProp_{i}'] = c
    return b


def bind_round(sess, xklc_id):
    """把选课轮次绑进服务端 session —— 缺这步，所有选课子页面都会返「未开放」。"""
    for u in (f'/jsxsd/xsxk/xklc_view?jx0502zbid={xklc_id}',
              f'/jsxsd/xsxk/xsxk_index?jx0502zbid={xklc_id}',
              f'/jsxsd/xsxk/yxxsxk_index?jx0502zbid={xklc_id}'):
        try:
            sess.get(BASE + u, timeout=25)
        except Exception as e:
            S.log(f'绑定轮次 {u} 失败: {str(e)[:80]}', 'warn')


def _clean(v):
    """清洗教务返回的富文本：<br> 归一到换行，去掉 &nbsp; 和多余空白。"""
    s = str(v if v is not None else '')
    if not s:
        return ''
    s = re.sub(r'(?i)<br\s*/?>', '\n', s)
    s = re.sub(r'(?i)</?(p|div|span|font)[^>]*>', '', s)
    s = s.replace('&nbsp;', ' ').replace('\u00a0', ' ')
    s = re.sub(r'[ \t]+', ' ', s)
    return '\n'.join(ln.strip() for ln in s.split('\n') if ln.strip()).strip()


def fetch_courses(sess):
    out = []
    for kind, cfg in KINDS.items():
        cols, path, qs = cfg['cols'], cfg['list_path'], cfg['query']
        label = cfg.get('name', kind)
        try:
            # 预热：先访问选课页面，服务端要靠它建立选课 session 状态
            page = BASE + cfg['come_in']
            try:
                r0 = sess.get(page, timeout=25, headers={'Referer': BASE + '/jsxsd/xsxk/xklc_list'})
                if len(r0.text) < 3000:
                    S.log(f'{label} 预热页异常({len(r0.text)}字节)，轮次可能未绑定', 'warn')
            except Exception:
                pass

            r = sess.post(BASE + path + qs, data=dt_form(cols), timeout=30,
                          headers={'Referer': page,
                                   'Origin': BASE,
                                   'X-Requested-With': 'XMLHttpRequest'})
            if need_relogin(r.text):
                raise RuntimeError('会话已失效')
            try:
                data = r.json()
            except Exception:
                S.log(f'{label} 返回非 JSON (status={r.status_code}, len={len(r.text)}): '
                      f'{r.text[:160]!r}', 'err')
                continue
            rows = data.get('aaData') or []
            for x in rows:
                out.append({
                    'kind': kind,
                    'oper': cfg.get('oper', DEFAULT_OPER),
                    'jx0404id': x.get('jx0404id') or '',
                    'jx02id': x.get('jx02id') or '',
                    'kch': x.get('kch') or '',
                    'kcmc': _clean(x.get('kcmc')),
                    'fzmc': _clean(x.get('fzmc')),
                    'ktmc': _clean(x.get('ktmc')),
                    'xf': x.get('xf') or '',
                    'skls': _clean(x.get('skls')),
                    'sksj': _clean(x.get('sksj')),
                    'skdd': _clean(x.get('skdd')),
                    'xqmc': _clean(x.get('xqmc')),
                    'xkrs': x.get('xkrs') or '',
                    'syrs': x.get('syrs') or '',
                    'ctsm': _clean(x.get('ctsm')),
                    'cat': _clean(x.get('szkcflmc') or x.get('tsTskflMc') or ''),
                    'cfbs': x.get('cfbs') if x.get('cfbs') is not None else 'null',
                    'kcsx': _clean(x.get('kcsxmc')),
                    'zx': x.get('zxs') or '',
                })
            S.log(f'{label} 课程 {len(rows)} 条')
        except RuntimeError as e:
            if '会话' in str(e):
                raise
            S.log(f'拉取 {label} 失败: {str(e)[:120]}', 'err')
        except Exception as e:
            S.log(f'拉取 {label} 失败: {str(e)[:120]}', 'err')
    return out


# ----------------------------------------------------------------------------
# 选课提交
# ----------------------------------------------------------------------------
def submit(sess, task):
    """返回 (success, message)"""
    kcid = task.get('jx02id') or ''
    cfbs = task.get('cfbs') or 'null'
    oper = task.get('oper') or KINDS.get(task.get('kind'), {}).get('oper') or DEFAULT_OPER
    url = f'{BASE}{oper}?kcid={kcid}&cfbs={cfbs}'
    data = {
        'jx0404id': task['jx0404id'],
        'xkzy': task.get('xkzy', '') or '',
        'trjf': task.get('trjf', '') or '',
    }
    try:
        r = sess.post(url, data=data, timeout=20)
    except Exception as e:
        return False, f'网络异常 {str(e)[:60]}', None
    body = r.text.strip()
    if not body:
        return False, '空响应', None
    if body.startswith('{'):
        try:
            j = json.loads(body)
            ok = bool(j.get('success'))
            # 强智的提示字段不统一，message / msgContent 都出现过
            msg = j.get('message') or j.get('msgContent') or j.get('msg')
            if not msg:
                msg = '未到选课时间' if not ok else '选课成功'
            msg = str(msg)
            if not ok and need_relogin(msg):
                return False, '会话失效，请重新登录', j
            return ok, msg, j
        except Exception:
            return False, body[:120], None
    if need_relogin(body):
        return False, '会话失效，请重新登录', None
    return False, body[:120], None


# 判硬拒绝的词必须够窄。'不是' / '不允许' 这种泛词会把
# 「当前不是选课时间，不允许选课」也判成放弃——那是要等，不是要死。
FAIL_STOP = ('冲突', '不符合', '学分', '超出', '超限', '上限', '限选', '已修',
             '无权限', '年级', '专业', '禁选')
FAIL_DONE = ('已选', '重复', '已经选')


def classify(msg):
    # 先判硬拒绝：「与已选课程冲突」这类消息同时含「已选」，
    # 若先查 FAIL_DONE 会把冲突误报成"已选上"。
    if any(k in msg for k in FAIL_STOP):
        return 'stop'
    if any(k in msg for k in FAIL_DONE):
        return 'done'
    return 'retry'


# 同一会话的请求节奏。教务对密集提交有拦截，压着这条线跑。
SESSION_QPS = 2

# 多门课排队时，一门课占住队头多久就让位给后面的课。
TURN_SECONDS = 15


class Throttle:
    """全局最小请求间隔，避免请求扎堆冲出去触发风控。"""

    def __init__(self, gap):
        self.gap = gap
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self, gap=None):
        """距上一次请求不满 gap 秒就等着。gap 只放宽不收紧，
        比基准值松才生效，紧的那一头由基准值兜底。"""
        g = self.gap if gap is None else max(gap, self.gap)
        with self.lock:
            now = time.time()
            d = self.last + g - now
            if d > 0:
                time.sleep(d)
            self.last = time.time()


class Engine:
    """任务进队列，后台一个工作线程依次取出来抢。

    同一时刻只有一门课在提交，教务那边看到的就是一条稳定的请求流。多门课
    同时打过去对拿课名次没有帮助——先到的那个请求抢到了，后面的全是白打，
    还容易让服务端提前把会话盯上。
    """

    def __init__(self, sess, interval):
        self.sess = sess
        self.interval = interval
        self.stop_flag = threading.Event()
        self.todo = queue.Queue()
        self.lock = threading.Lock()
        self.active = None      # 正在抢的任务
        self.waiting = set()    # 已排队、还没轮到的 jx0404id
        self.throttle = Throttle(1.0 / SESSION_QPS)
        threading.Thread(target=self._loop, daemon=True, name='xk-engine').start()

    def _pace(self):
        """选课未开始时放慢节奏；到点后按设定间隔提速。
        轮次都还没解析出来（window_start 为 None）说明选课没开，绝不能提速。"""
        ws = S.window_start
        try:
            if not ws or datetime.now() < ws:
                return max(self.interval, 3.0)
        except Exception:
            pass
        return self.interval

    def _rotate_due(self, t0):
        """该不该把这门课让回队尾。

        后面还有课在等、当前这门又已经占够队头，就轮转，否则排在后面的课
        一个请求都发不出去。只剩一门的时候不轮转，让它一路抢到底。
        选课时间没解析出来就不拦——那种情况更该让每门课都轮着摸一遍，
        而不是让队头那门一直占着。
        """
        if self.todo.empty():
            return False
        ws = S.window_start
        try:
            if ws and datetime.now() < ws:
                return False
        except Exception:
            pass
        return time.time() - t0 >= TURN_SECONDS

    def add(self, task):
        """把任务排进队列，同一门课不会重复排队。"""
        jid = task['jx0404id']
        with self.lock:
            if jid in self.waiting:
                return False
            if self.active is not None and self.active['jx0404id'] == jid:
                return False
            self.waiting.add(jid)
        self.todo.put(task)
        return True

    def stop_all(self, wait=3.0):
        """停掉所有任务。还没轮到的直接退回队列外，停完还能再入队。"""
        self.stop_flag.set()
        with self.lock:
            pending = []
            while True:
                try:
                    pending.append(self.todo.get_nowait())
                except queue.Empty:
                    break
            for t in pending:
                t['status'] = 'stopped'
                self.waiting.discard(t['jx0404id'])
            n = len(pending) + (1 if self.active else 0)
        end = time.time() + wait
        settled = False
        while time.time() < end:
            with self.lock:
                if self.active is None:
                    settled = True
                    break
            time.sleep(0.05)
        # 停稳了才复位标志。还在飞的那一发要等它自己回来——submit 最长阻塞
        # 20 秒，提前复位等于让它接着抢，「停止」就成了一句空话。
        if settled:
            self.stop_flag.clear()
        save_queue()
        return n

    def resume(self):
        """把停下来的任务重新丢回引擎。"""
        self.stop_flag.clear()
        with S.lock:
            pend = [t for t in S.queue_tasks.values()
                    if t['status'] in ('stopped', 'pending')]
        back = []
        for t in pend:
            t['status'] = 'pending'
            t['last'] = '已恢复，重新排队'
            if self.add(t):
                back.append(t['jx0404id'])
        save_queue()
        return back

    def _loop(self):
        """工作线程主体：取一个、抢一个。"""
        while True:
            task = self.todo.get()
            with self.lock:
                self.waiting.discard(task['jx0404id'])
                self.active = task
            try:
                if not self.stop_flag.is_set():
                    self._body(task)
            except Exception as e:
                # 工作线程只有一个，被一门课的意外带走，后面排队的就全卡住了。
                task['status'] = 'failed'
                task['last'] = f'任务异常: {str(e)[:80]}'
                S.log(f"❌ 任务异常: {str(e)[:120]}", 'err')
            finally:
                with self.lock:
                    self.active = None
                save_queue()

    def _body(self, task):
        jid = task['jx0404id']
        if task.get('status') == 'stopped':
            return      # 排队期间被移除，别再开抢
        task['status'] = 'running'
        task['last'] = '已入队，等待开抢'
        S.broadcast('queue', None)
        S.log(f"开始抢 [{task['kcmc']}] {task.get('skls','')} | kcid={task.get('jx02id','')[:12]}... | cfbs={task.get('cfbs')}")

        turn_start = time.time()
        while not self.stop_flag.is_set():
            self.throttle.wait(self._pace())
            if task['status'] != 'running':
                return      # 等这一轮节流的时候被移除，收手
            ok, msg, raw = submit(self.sess, task)
            task['tries'] += 1
            task['last'] = (msg or '')[:160]

            if ok:
                task['status'] = 'success'
                S.log(f"✅ 抢到 [{task['kcmc']}] {msg}", 'ok')
                S.broadcast('queue', None)
                return

            if need_relogin(msg or ''):
                task['status'] = 'error'
                task['last'] = '会话失效，请重新登录'
                S.log('会话失效，抢课暂停', 'err')
                S.broadcast('queue', None)
                return

            verdict = classify(msg)
            if verdict == 'done':
                task['status'] = 'done'
                S.log(f"ℹ️ [{task['kcmc']}] {msg}", 'warn')
                S.broadcast('queue', None)
                return
            if verdict == 'stop':
                task['status'] = 'failed'
                S.log(f"❌ [{task['kcmc']}] 放弃: {msg}", 'err')
                S.broadcast('queue', None)
                return

            if task['tries'] % 20 == 1:
                S.log(f"… [{task['kcmc']}] 第{task['tries']}次: {(msg or '')[:80]}")
            S.broadcast('queue', None)
            if self._rotate_due(turn_start):
                task['status'] = 'pending'
                task['last'] = '这一轮没抢到，让给后面的课，稍后再来'
                S.broadcast('queue', None)
                self.todo.put(task)
                return
            time.sleep(random.uniform(0, self.interval * 0.4))

        task['status'] = 'stopped'
        S.broadcast('queue', None)


# ----------------------------------------------------------------------------
# Web UI
# ----------------------------------------------------------------------------
def make_handler():

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype='application/json; charset=utf-8'):
            if isinstance(body, str):
                body = body.encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj, code=200):
            self._send(code, json.dumps(obj, ensure_ascii=False))

        def do_GET(self):
            path = self.path.split('?')[0]
            if path == '/favicon.ico':
                self._send(204, b'', 'image/x-icon')
                return
            if path in ('/', '/index.html'):
                try:
                    with open(UI_FILE, 'r', encoding='utf-8') as f:
                        self._send(200, f.read(), 'text/html; charset=utf-8')
                except Exception as e:
                    self._send(500, f'缺少 webui.html: {e}', 'text/plain; charset=utf-8')
                return

            if path == '/api/state':
                with S.lock:
                    snap = S.snapshot()
                    snap['logs'] = S.logs[-120:]
                self._json(snap)
                return

            if path == '/api/courses':
                q = parse_qs(urlparse(self.path).query)
                kw = ((q.get('kw') or [''])[0] or '').strip()
                kind = ((q.get('kind') or [''])[0] or '').strip()
                rows = S.courses
                if kind:
                    rows = [c for c in rows if c['kind'] == kind]
                if kw:
                    k = kw.lower()
                    rows = [c for c in rows
                            if k in c['kcmc'].lower() or k in c['kch'].lower()
                            or k in c['skls'].lower() or k in c['cat'].lower()]
                self._json({'total': len(rows), 'rows': rows[:600]})
                return

            if path == '/api/events':
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
                self.send_header('Cache-Control', 'no-cache')
                self.send_header('Connection', 'keep-alive')
                self.end_headers()
                q = queue.Queue()
                with S.lock:
                    S.subs.append(q)
                try:
                    while True:
                        try:
                            kind, payload = q.get(timeout=15)
                            if kind == 'log':
                                data = json.dumps(payload, ensure_ascii=False)
                                self.wfile.write(f'event: log\ndata: {data}\n\n'.encode('utf-8'))
                            else:
                                with S.lock:
                                    snap = S.snapshot()
                                self.wfile.write(
                                    f"event: state\ndata: {json.dumps(snap, ensure_ascii=False)}\n\n".encode('utf-8'))
                        except queue.Empty:
                            self.wfile.write(b': ping\n\n')
                        self.wfile.flush()
                except Exception:
                    pass
                finally:
                    with S.lock:
                        if q in S.subs:
                            S.subs.remove(q)
                return

            self._send(404, '{"error":"not found"}')

        def do_POST(self):
            path = self.path.split('?')[0]
            n = int(self.headers.get('Content-Length') or 0)
            raw = self.rfile.read(n) if n else b'{}'
            try:
                payload = json.loads(raw.decode('utf-8') or '{}')
            except Exception:
                payload = {}

            if path == '/api/enqueue':
                items = payload.get('items') or []
                added = 0
                for it in items:
                    jid = it.get('jx0404id')
                    if not jid:
                        continue
                    with S.lock:
                        if jid in S.queue_tasks:
                            continue
                        task = {
                            'jx0404id': jid,
                            'jx02id': it.get('jx02id') or '',
                            'cfbs': it.get('cfbs') if it.get('cfbs') is not None else 'null',
                            'kcmc': it.get('kcmc') or '',
                            'kch': it.get('kch') or '',
                            'xf': it.get('xf') or '',
                            'skls': it.get('skls') or '',
                            'sksj': it.get('sksj') or '',
                            'xkzy': it.get('xkzy') or '',
                            'trjf': it.get('trjf') or '',
                            'oper': it.get('oper') or DEFAULT_OPER,
                            'status': 'pending',
                            'tries': 0,
                            'last': '',
                            'kind': it.get('kind') or '',
                        }
                        S.queue_tasks[jid] = task
                    added += 1
                    ENGINE.add(task)
                S.log(f'入队 {added} 门课')
                save_queue()
                self._json({'ok': True, 'added': added})
                return

            if path == '/api/stop':
                n = ENGINE.stop_all()
                S.running = False
                S.log(f'已停 {n} 个任务（点「恢复」可继续）', 'warn')
                self._json({'ok': True, 'stopped': n})
                return

            if path == '/api/resume':
                back = ENGINE.resume()
                S.running = bool(back)
                S.log(f'已恢复 {len(back)} 个任务' + ('' if back else '（没有可恢复的任务）'),
                      'ok' if back else 'warn')
                self._json({'ok': True, 'resumed': back})
                return

            if path == '/api/remove':
                jid = payload.get('jx0404id')
                with S.lock:
                    t = S.queue_tasks.pop(jid, None)
                if t:
                    t['status'] = 'stopped'
                save_queue()
                self._json({'ok': True})
                return

            if path == '/api/clear':
                with S.lock:
                    done = [k for k, v in S.queue_tasks.items()
                            if v['status'] in ('success', 'failed', 'done', 'stopped', 'error')]
                    for k in done:
                        S.queue_tasks.pop(k, None)
                save_queue()
                self._json({'ok': True})
                return

            if path == '/api/refresh':
                try:
                    S.courses = fetch_courses(HTTP['sess'])
                    S.log(f'课程已刷新，共 {len(S.courses)} 条', 'ok')
                    self._json({'ok': True, 'total': len(S.courses)})
                except Exception as e:
                    S.log(f'刷新失败: {str(e)[:120]}', 'err')
                    self._json({'ok': False, 'error': str(e)[:200]}, 500)
                return

            self._send(404, '{"error":"not found"}')

    return H


class _Server(http.server.ThreadingHTTPServer):
    # 不让 Windows 的 SO_REUSEADDR 抢端口：重复启动时宁可报错，也不要两个进程
    # 抢同一个端口、界面串到旧代码上去
    allow_reuse_address = False
    daemon_threads = True


def serve(port):
    try:
        srv = _Server(('127.0.0.1', port), make_handler())
    except OSError as e:
        raise SystemExit(f'端口 {port} 起不来：可能已经有一个抢课器在跑。'
                         f'先关掉那个窗口，或用 --port 换一个。({e})')
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
ENGINE = None


def main():
    global ENGINE, KINDS
    ap = argparse.ArgumentParser()
    ap.add_argument('--acc', default=os.environ.get('HHU_ACC', ''))
    ap.add_argument('--pwd', default=os.environ.get('HHU_PWD', ''))
    ap.add_argument('--port', type=int, default=8899)
    ap.add_argument('--interval', type=float, default=0.5, help='抢课轮询间隔(秒)')
    ap.add_argument('--round-every', type=float, default=1.0,
                    help='等选课轮次放出来的查询间隔(秒)，默认 1')
    ap.add_argument('--headless', action='store_true', help='登录时不显示浏览器窗口')
    ap.add_argument('--no-browser', action='store_true', help='不自动打开 UI 页面')
    args = ap.parse_args()

    acc = (args.acc or os.environ.get('HHU_ACC', '')).strip()
    pwd = (args.pwd or os.environ.get('HHU_PWD', '')).strip()
    S.acc = acc
    S.interval = args.interval

    # 1. 登录
    state = None
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                cand = json.load(f)
            session = state_to_session(cand)
            test = session.get(BASE + '/jsxsd/xsxk/xklc_list?Ves632DSdyV=NEW_XSD_PYGL', timeout=20)
            if not need_relogin(test.text) and 'toxk' in test.text:
                state = cand
                S.log('复用已保存的登录会话', 'ok')
        except Exception:
            state = None

    if state is None:
        # 只有真要登录时才问账号密码，会话还有效就直接进
        if not acc:
            acc = input('学号: ').strip()
        if not pwd:
            pwd = input('密码: ').strip()
        S.acc = acc
        state = login_with_playwright(acc, pwd, headless=args.headless)

    session = state_to_session(state)
    HTTP['sess'] = session
    S.login_ok = True

    # 2. 轮次
    try:
        ids, _, round_name = fetch_round(session)
        if not ids:
            raise RuntimeError('没找到可用选课轮次')
        S.xklc_id = ids[0]
        ws, we, ps, pe, name, _ = fetch_round_detail(session, S.xklc_id)
        S.window_start, S.window_end = ws, we
        S.preview_start, S.preview_end = ps, pe
        S.term = name or round_name
        S.log(f'轮次 {S.term or S.xklc_id}')
        S.log(f'选课时间 {ws} ~ {we}')
        if ps:
            S.log(f'预览时间 {ps} ~ {pe}')
    except Exception as e:
        S.log(f'轮次解析失败: {str(e)[:150]}', 'err')

    # 3. 绑定轮次 -> 解析本轮开放的选课类别 -> 拉课程
    if S.xklc_id:
        bind_round(session, S.xklc_id)
        try:
            found = discover_kinds(session, S.xklc_id)
        except Exception as e:
            found = {}
            S.log(f'解析选课类别失败: {str(e)[:120]}', 'warn')
        if found:
            set_kinds(found)
            S.log('本轮选课类别: ' + kind_names(), 'ok')
        else:
            S.log('未解析到选课类别，用内置清单: '
                  + '、'.join(c['name'] for c in KINDS.values()), 'warn')
    try:
        S.courses = fetch_courses(session)
    except Exception as e:
        S.log(f'拉课程失败: {str(e)[:150]}', 'err')
        if '会话' in str(e):
            S.login_ok = False
            S.log('会话已失效，这次启动一门课都没拉到。关掉重开，重新登录。', 'err')

    # 3.2 启动时没有轮次（选课还没开）就守着，轮次一出现立刻绑定并拉课
    if not S.xklc_id:
        def _wait_open():
            xid, ws, we, name = wait_for_round(session, every=args.round_every)
            S.xklc_id = xid
            S.window_start, S.window_end = ws, we
            S.term = name or xid
            S.log(f'轮次已出现: {S.term} | 选课时间 {ws} ~ {we}', 'ok')
            bind_round(session, xid)
            try:
                found = discover_kinds(session, xid)
                if found:
                    set_kinds(found)
            except Exception as e:
                S.log(f'解析选课类别失败: {str(e)[:100]}', 'warn')
            S.log('本轮选课类别: ' + kind_names(), 'ok')
            try:
                S.courses = fetch_courses(session)
                S.log(f'课程已拉取，共 {len(S.courses)} 条', 'ok')
            except Exception as e:
                S.log(f'拉课程失败: {str(e)[:120]}', 'err')

        threading.Thread(target=_wait_open, daemon=True).start()
        S.log(f'当前没有开放中的选课轮次。已按 {args.round_every} 秒的节奏守候，'
              f'轮次一出现立刻拉课。', 'warn')

    ENGINE = Engine(session, args.interval)

    # 3.5 到开抢时刻重解析类别：预览期只暴露预览入口，正选入口到点才开
    def _refresh_on_open():
        ws = S.window_start
        if not ws:
            return
        while True:
            d = (ws - datetime.now()).total_seconds()
            if d <= 0:
                break
            time.sleep(min(max(d, 1), 30))
        try:
            found = discover_kinds(session, S.xklc_id)
            if found:
                set_kinds(found)
            S.courses = fetch_courses(session)
            S.log('已到开抢时刻，课程已重拉: ' + kind_names(), 'ok')
        except Exception as e:
            S.log(f'开抢时刻刷新失败: {str(e)[:100]}', 'warn')

    if S.window_start:
        threading.Thread(target=_refresh_on_open, daemon=True).start()

    # 4. 恢复上次未完成的队列
    saved = load_queue()
    restored = []
    for t in saved:
        if t.get('status') in ('success', 'done'):
            continue
        jid = t.get('jx0404id')
        if not jid or jid in S.queue_tasks:
            continue
        task = {k: t.get(k) for k in TASK_KEYS}
        task['status'] = 'pending'
        # 旧队列里的 oper 可能是错的：早期 Web UI 入队不带 oper，
        # 服务端一律填了默认口子。以本轮解析出的类别配置为准。
        kc = KINDS.get(task.get('kind') or '', {})
        if kc.get('oper'):
            task['oper'] = kc['oper']
        task['tries'] = int(t.get('tries') or 0)
        task['last'] = '已从上次会话恢复'
        with S.lock:
            S.queue_tasks[jid] = task
        restored.append(task)
    if restored:
        S.log(f'恢复上次队列 {len(restored)} 门课', 'ok')
        for t in restored:
            ENGINE.add(t)
    else:
        save_queue()

    # 4. UI
    serve(args.port)
    url = f'http://127.0.0.1:{args.port}/'
    S.log(f'控制台已就绪: {url}', 'ok')
    if not args.no_browser:
        try:
            import webbrowser
            webbrowser.open(url)
        except Exception:
            pass

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        S.log('退出', 'warn')
        ENGINE.stop_all()


if __name__ == '__main__':
    main()
