// String accessor - extended str operations
import { Series } from './series.js'

function StrAccessor(values, options) {
    const {name, index} = options

    function mapStr(fn) {
        return Series(values.map(v => v === null || v === undefined ? null : fn(v)), {name, index: [...index]})
    }

    function mapBool(fn) {
        return values.map(v => v === null || v === undefined ? false : fn(v))
    }

    return {
        upper() { return mapStr(v => v.toUpperCase()) },
        lower() { return mapStr(v => v.toLowerCase()) },
        len() { return mapStr(v => v.length) },
        strip() { return mapStr(v => v.trim()) },
        lstrip() { return mapStr(v => v.trimStart()) },
        rstrip() { return mapStr(v => v.trimEnd()) },
        contains(pat) { return mapBool(v => v.includes(pat)) },
        startswith(pat) { return mapBool(v => v.startsWith(pat)) },
        endswith(pat) { return mapBool(v => v.endsWith(pat)) },
        replace(pat, repl) { return mapStr(v => v.split(pat).join(repl)) },
        slice(start, stop) { return mapStr(v => v.slice(start, stop)) },
        title() { return mapStr(v => v.replace(/\w\S*/g, t => t.charAt(0).toUpperCase() + t.substr(1).toLowerCase())) },
        capitalize() { return mapStr(v => v.charAt(0).toUpperCase() + v.slice(1).toLowerCase()) },
        swapcase() { return mapStr(v => v.split('').map(c => c === c.toUpperCase() ? c.toLowerCase() : c.toUpperCase()).join('')) },
        split(sep) { return Series(values.map(v => v === null || v === undefined ? null : v.split(sep)), {name, index: [...index]}) },
        get(i) {
            return Series(values.map(v => {
                if (v === null || v === undefined) return null
                if (Array.isArray(v)) return v[i] !== undefined ? v[i] : null
                return v[i] !== undefined ? v[i] : null
            }), {name, index: [...index]})
        },
        find(sub) { return mapStr(v => v.indexOf(sub)) },
        count(pat) {
            return mapStr(v => {
                let count = 0, pos = 0
                while ((pos = v.indexOf(pat, pos)) !== -1) { count++; pos += pat.length }
                return count
            })
        },
        pad(width, options) {
            const opts = options || {}
            const side = opts.side || 'left'
            const fillchar = opts.fillchar || ' '
            return mapStr(v => {
                if (side === 'left') return v.padStart(width, fillchar)
                if (side === 'right') return v.padEnd(width, fillchar)
                // both
                const total = width - v.length
                if (total <= 0) return v
                const left = Math.floor(total / 2)
                const right = total - left
                return fillchar.repeat(left) + v + fillchar.repeat(right)
            })
        },
        zfill(width) { return mapStr(v => v.padStart(width, '0')) },
        isalpha() { return mapBool(v => v.length > 0 && /^[a-zA-Z]+$/.test(v)) },
        isdigit() { return mapBool(v => v.length > 0 && /^\d+$/.test(v)) },
        isnumeric() { return mapBool(v => v.length > 0 && /^\d+$/.test(v)) },
        cat(options) {
            const opts = options || {}
            const sep = opts.sep || ''
            return values.filter(v => v !== null && v !== undefined).join(sep)
        },
        extract(pattern) {
            const re = new RegExp(pattern)
            return mapStr(v => {
                const m = v.match(re)
                if (m && m[1]) return m[1]
                return null
            })
        },
        center(width, fillchar) {
            const fc = fillchar || ' '
            return mapStr(v => {
                const total = width - v.length
                if (total <= 0) return v
                const left = Math.floor(total / 2)
                const right = total - left
                return fc.repeat(left) + v + fc.repeat(right)
            })
        },
        ljust(width, fillchar) {
            const fc = fillchar || ' '
            return mapStr(v => v.padEnd(width, fc))
        },
        rjust(width, fillchar) {
            const fc = fillchar || ' '
            return mapStr(v => v.padStart(width, fc))
        },
        rfind(sub) { return mapStr(v => v.lastIndexOf(sub)) },
        match(pattern) {
            const re = new RegExp('^' + pattern)
            return mapBool(v => re.test(v))
        },
        fullmatch(pattern) {
            const re = new RegExp('^' + pattern + '$')
            return mapBool(v => re.test(v))
        },
        isalnum() { return mapBool(v => v.length > 0 && /^[a-zA-Z0-9]+$/.test(v)) },
        isspace() { return mapBool(v => v.length > 0 && /^\s+$/.test(v)) },
        islower() { return mapBool(v => v.length > 0 && v === v.toLowerCase() && v !== v.toUpperCase()) },
        isupper() { return mapBool(v => v.length > 0 && v === v.toUpperCase() && v !== v.toLowerCase()) },
        istitle() { return mapBool(v => v.length > 0 && v === v.replace(/\w\S*/g, t => t.charAt(0).toUpperCase() + t.substr(1).toLowerCase())) },
        removeprefix(prefix) { return mapStr(v => v.startsWith(prefix) ? v.slice(prefix.length) : v) },
        removesuffix(suffix) { return mapStr(v => v.endsWith(suffix) ? v.slice(0, -suffix.length) : v) },
        rsplit(sep) { return Series(values.map(v => v === null || v === undefined ? null : v.split(sep)), {name, index: [...index]}) },
        join(sep) { return mapStr(v => Array.isArray(v) ? v.join(sep) : v) },
        repeat(n) { return mapStr(v => v.repeat(n)) }
    }
}

export { StrAccessor }
