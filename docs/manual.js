// pandasjs manual - client-side YAML loading + interactive code runner
import yaml from 'https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/+esm'
import { renderSidebar, renderContent } from './manual-render.js'

const SECTIONS = [
    '01-install.yaml',
    '02-quickstart.yaml',
    '03-creation.yaml',
    '04-selection.yaml',
    '05-computation.yaml',
    '06-reshape.yaml',
    '07-groupby.yaml',
    '08-window.yaml',
    '09-io.yaml',
    '10-datetime.yaml',
    '11-string.yaml',
    '12-bridge.yaml',
]

let pyodidePromise = null

// lazy load Pyodide on first Python run
function ensurePyodide() {
    if (pyodidePromise) return pyodidePromise
    pyodidePromise = (async () => {
        const mod = await import('https://cdn.jsdelivr.net/pyodide/v0.27.0/full/pyodide.mjs')
        const pyodide = await mod.loadPyodide()
        await pyodide.loadPackage('pandas')
        await pyodide.runPythonAsync('import pandas as pd')
        return pyodide
    })()
    return pyodidePromise
}

// run Python code and capture output
async function runPython(code, $output) {
    $output.textContent = 'Loading Pyodide + pandas...'
    $output.className = 'output-block loading'
    try {
        const pyodide = await ensurePyodide()
        pyodide.runPython(`
import io, sys
_capture = io.StringIO()
sys.stdout = _capture
`)
        await pyodide.runPythonAsync(code)
        const printed = pyodide.runPython('_capture.getvalue()')
        pyodide.runPython('sys.stdout = sys.__stdout__')
        let result = ''
        const lines = code.trim().split('\n')
        const lastLine = lines[lines.length - 1].trim()
        if (lastLine && !lastLine.startsWith('import ') && !lastLine.includes('=') && !lastLine.startsWith('#')) {
            try {
                const val = await pyodide.runPythonAsync(`str(${lastLine})`)
                result = val
            } catch (e) {
                // not an expression
            }
        }
        const output = (printed + result).trim() || '(no output)'
        $output.textContent = output
        $output.className = 'output-block'
    } catch (err) {
        $output.textContent = err.message
        $output.className = 'output-block error'
    }
}

// run JS code with pd in scope
function runJs(code, $output) {
    $output.className = 'output-block'
    code = code.split('\n').filter(l => !l.trim().startsWith('import ')).join('\n')
    try {
        const pd = window.pd
        let output = []
        const log = console.log
        console.log = function() {
            const args = Array.from(arguments)
            output.push(args.map(a => {
                if (a && (a._isPandasDataFrame || a._isPandasSeries)) {
                    return a.toString()
                }
                if (typeof a === 'object') return JSON.stringify(a)
                return String(a)
            }).join(' '))
        }
        const fn = new Function('pd', code)
        const result = fn(pd)
        console.log = log
        if (result !== undefined) {
            if (result && (result._isPandasDataFrame || result._isPandasSeries)) {
                output.push(result.toString())
            } else if (typeof result === 'object') {
                output.push(JSON.stringify(result))
            } else {
                output.push(String(result))
            }
        }
        $output.textContent = output.join('\n') || '(no output)'
    } catch (err) {
        $output.textContent = err.message
        $output.className = 'output-block error'
    }
}

function initRunButtons() {
    document.querySelectorAll('.run-btn').forEach($btn => {
        $btn.addEventListener('click', async function() {
            if (this.disabled) return
            const $panel = this.closest('.code-panel')
            const lang = $panel.dataset.lang
            const code = $panel.querySelector('pre').textContent
            const $output = $panel.querySelector('.output-block')
            this.disabled = true
            this.textContent = '...'
            if (lang === 'python') {
                await runPython(code, $output)
            } else {
                runJs(code, $output)
            }
            this.disabled = false
            this.innerHTML = '<span class="play-icon">&#9654;</span> Run'
        })
    })
}

function initEditable() {
    document.querySelectorAll('.code-panel pre').forEach($pre => {
        $pre.contentEditable = 'true'
        $pre.spellcheck = false
    })
}

function initSidebar() {
    const $links = document.querySelectorAll('.sidebar a')
    const sections = []
    $links.forEach($link => {
        const id = $link.getAttribute('href').slice(1)
        const $section = document.getElementById(id)
        if ($section) sections.push({id, $link, $section})
    })

    function updateActive() {
        const scrollY = window.scrollY + 100
        let current = sections[0]
        for (const s of sections) {
            if (s.$section.offsetTop <= scrollY) current = s
        }
        $links.forEach($l => $l.classList.remove('active'))
        if (current) current.$link.classList.add('active')
    }

    window.addEventListener('scroll', updateActive)
    updateActive()
}

// load YAML sections and render
async function loadSections() {
    const sections = await Promise.all(
        SECTIONS.map(f =>
            fetch(`sections/${f}`).then(r => r.text()).then(t => yaml.load(t))
        )
    )
    document.querySelector('.sidebar').innerHTML = renderSidebar(sections)
    document.querySelector('.manual-content').innerHTML = renderContent(sections)
    initEditable()
    initRunButtons()
    initSidebar()
}

loadSections()
