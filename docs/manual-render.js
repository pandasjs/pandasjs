// shared rendering functions — returns HTML strings
// used by both browser (manual.js) and SSR (manual-ssr.js)

function escapeHtml(text) {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

// syntax highlight rules — monaco dark+ theme colors
const jsRules = [
    [/(\/\/.*)/g, '<span class="cm">$1</span>'],
    [/\b(const|let|var|return|function|if|else|for|of|in|new|await|async|import|from|export|default|true|false|null|undefined)\b/g, '<span class="kw">$1</span>'],
    [/('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|`(?:[^`\\]|\\.)*`)/g, '<span class="str">$1</span>'],
    [/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>'],
    [/\.(toString|col|DataFrame|Series|groupby|rolling|expanding|ewm|mean|sum|std|max|min|count|median|agg|merge|concat|readCsv|toCsv|readJson|toJson|toDatetime|dateRange|toNumeric|getDummies|read_csv|to_csv|read_json|to_json|to_datetime|date_range|to_numeric|get_dummies|iloc|loc|iat|at|head|tail|describe|shape|columns|dtypes|apply|filter|sort|upper|lower|contains|split|fillna|dropna|str|dt|run)\b/g, '.<span class="fn">$1</span>'],
    [/\b(console)\b/g, '<span class="fn">$1</span>'],
    [/\b(pd)\b/g, '<span class="fn">$1</span>'],
]

const pyRules = [
    [/(#.*)/g, '<span class="cm">$1</span>'],
    [/\b(import|from|as|def|class|return|if|else|elif|for|in|not|and|or|True|False|None|print|lambda|with)\b/g, '<span class="kw">$1</span>'],
    [/('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")/g, '<span class="str">$1</span>'],
    [/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>'],
    [/\.(DataFrame|Series|groupby|rolling|expanding|ewm|mean|sum|std|max|min|count|median|agg|merge|concat|read_csv|to_csv|to_datetime|date_range|iloc|loc|iat|at|head|tail|describe|shape|columns|dtypes|apply|str|dt|tolist|upper|lower|contains|split|fillna|dropna|StringIO)\b/g, '.<span class="fn">$1</span>'],
    [/\b(pd|io)\b/g, '<span class="fn">$1</span>'],
]

const htmlCommentRule = [/(&lt;!--.*?--&gt;)/g, '<span class="cm">$1</span>']

function highlight(code, lang) {
    const escaped = escapeHtml(code)
    const rules = lang === 'python' ? pyRules : jsRules
    const matches = []
    for (const [pattern, replacement] of rules) {
        const re = new RegExp(pattern.source, pattern.flags)
        let m
        while ((m = re.exec(escaped)) !== null) {
            const replaced = m[0].replace(new RegExp(pattern.source), replacement)
            matches.push({start: m.index, end: m.index + m[0].length, replaced})
        }
    }
    matches.sort((a, b) => a.start - b.start)
    const kept = []
    let lastEnd = 0
    for (const m of matches) {
        if (m.start >= lastEnd) {
            kept.push(m)
            lastEnd = m.end
        }
    }
    let result = ''
    let pos = 0
    for (const m of kept) {
        result += escaped.slice(pos, m.start)
        result += m.replaced
        pos = m.end
    }
    result += escaped.slice(pos)
    return result
}

function highlightCodeBlock(code) {
    const escaped = escapeHtml(code)
    const matches = []
    // html comments
    const commentRe = new RegExp(htmlCommentRule[0].source, htmlCommentRule[0].flags)
    let m
    while ((m = commentRe.exec(escaped)) !== null) {
        matches.push({start: m.index, end: m.index + m[0].length, replaced: m[0].replace(new RegExp(htmlCommentRule[0].source), htmlCommentRule[1])})
    }
    // js rules
    for (const [pattern, replacement] of jsRules) {
        const re = new RegExp(pattern.source, pattern.flags)
        while ((m = re.exec(escaped)) !== null) {
            const replaced = m[0].replace(new RegExp(pattern.source), replacement)
            matches.push({start: m.index, end: m.index + m[0].length, replaced})
        }
    }
    matches.sort((a, b) => a.start - b.start)
    const kept = []
    let lastEnd = 0
    for (const mt of matches) {
        if (mt.start >= lastEnd) {
            kept.push(mt)
            lastEnd = mt.end
        }
    }
    let result = ''
    let pos = 0
    for (const mt of kept) {
        result += escaped.slice(pos, mt.start)
        result += mt.replaced
        pos = mt.end
    }
    result += escaped.slice(pos)
    return result
}

function renderSidebar(sections) {
    return sections.map(s => {
        return `<a href="#${s.id}">${s.sidebar}</a>`
    }).join('\n')
}

function renderCodePanel(lang, code) {
    const label = lang === 'js' ? 'JavaScript' : 'Python'
    return `<div class="code-panel" data-lang="${lang === 'js' ? 'js' : 'python'}">
    <div class="label"><span class="lang">${label}</span><button class="run-btn"><span class="play-icon">&#9654;</span> Run</button></div>
    <pre>${highlight(code.trimEnd(), lang === 'js' ? 'js' : 'python')}</pre>
    <div class="output-block empty">click run</div>
</div>`
}

function renderCodeBlock(code) {
    return `<div class="code-block"><pre>${highlightCodeBlock(code.trimEnd())}</pre></div>`
}

function renderSection(section) {
    let html = `<h2 id="${section.id}">${escapeHtml(section.title)}</h2>\n`
    html += `<p>${escapeHtml(section.desc)}</p>\n`

    if (section.type === 'code-compare') {
        const examples = section.examples || [{js: section.js, python: section.python}]
        for (const ex of examples) {
            html += `<div class="code-compare">\n`
            html += renderCodePanel('js', ex.js)
            html += '\n'
            html += renderCodePanel('python', ex.python)
            html += `\n</div>\n`
        }
    }

    if (section.type === 'code-blocks' || section.type === 'mixed') {
        for (const block of section.blocks) {
            if (block.code) {
                html += renderCodeBlock(block.code) + '\n'
            }
            if (block.text) {
                html += `<p>${block.text}</p>\n`
            }
            if (block.list) {
                html += `<ul style="color:var(--text-muted);margin-left:20px;margin-bottom:16px">\n`
                for (const item of block.list) {
                    html += `    <li>${item}</li>\n`
                }
                html += `</ul>\n`
            }
        }
    }

    return html
}

function renderContent(sections) {
    return sections.map(renderSection).join('\n')
}

export { renderSidebar, renderContent, renderSection }
