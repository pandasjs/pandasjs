// Python pandas → pandasjs transpiler
// transpiles Python code string to JS, then evaluates it

function transpile(pyCode) {
    const lines = pyCode.split('\n')
    const jsLines = []
    for (let line of lines) {
        const trimmed = line.trim()
        // skip empty lines and import pandas
        if (trimmed === '' || /^import\s+pandas/.test(trimmed) || /^from\s+pandas/.test(trimmed)) {
            jsLines.push('')
            continue
        }
        // skip comments, convert # to //
        if (trimmed.startsWith('#')) {
            jsLines.push(trimmed.replace(/^#/, '//'))
            continue
        }
        let out = line
        // True/False/None → true/false/null
        out = out.replace(/\bTrue\b/g, 'true')
        out = out.replace(/\bFalse\b/g, 'false')
        out = out.replace(/\bNone\b/g, 'null')
        // print(...) → console.log(...)
        out = out.replace(/\bprint\s*\(/g, 'console.log(')
        // dict keys: {'key': val} → {key: val} (simple string keys)
        out = out.replace(/\{(\s*)'(\w+)'\s*:/g, '{$1$2:')
        out = out.replace(/,(\s*)'(\w+)'\s*:/g, ',$1$2:')
        // assignment: x = expr → let x = expr (only simple names, not obj.attr or indexed)
        const assignMatch = out.match(/^(\s*)([a-zA-Z_]\w*)\s*=\s*(.+)$/)
        if (assignMatch) {
            const [, indent, varName, expr] = assignMatch
            // skip if it looks like comparison (==) or augmented assignment
            if (!/[=!<>]=/.test(expr.charAt(0))) {
                out = `${indent}let ${varName} = ${expr}`
            }
        }
        // .iloc[x] → .iloc(x), .loc[x] → .loc(x)
        out = out.replace(/\.iloc\[([^\]]+)\]/g, '.iloc($1)')
        out = out.replace(/\.loc\[([^\]]+)\]/g, '.loc($1)')
        // df['col'] → df.col('col') only for simple string access on variables
        out = out.replace(/(\w)\[['"](\w+)['"]\]/g, "$1.col('$2')")
        // len(x) → x.length
        out = out.replace(/\blen\((\w+)\)/g, '$1.length')
        // Python slice [a:b] → .slice(a, b) — basic case
        out = out.replace(/\.iloc\((\d+):(\d+)\)/g, '.iloc($1, $2)')
        jsLines.push(out)
    }
    // make last non-empty expression a return
    let lastIdx = -1
    for (let i = jsLines.length - 1; i >= 0; i--) {
        if (jsLines[i].trim() !== '') { lastIdx = i; break }
    }
    if (lastIdx >= 0) {
        const last = jsLines[lastIdx].trim()
        // add return if it's not already an assignment or console.log
        if (!last.startsWith('let ') && !last.startsWith('const ') && !last.startsWith('console.')) {
            jsLines[lastIdx] = jsLines[lastIdx].replace(last, `return ${last}`)
        }
    }
    return jsLines.join('\n')
}

function run(pyCode, pd) {
    const jsCode = transpile(pyCode)
    // execute with pd in scope
    const fn = new Function('pd', jsCode)
    return fn(pd)
}

export { run, transpile }
