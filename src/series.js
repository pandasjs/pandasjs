// Series - pandas-compatible Series factory function
import { Rolling, Expanding, Ewm } from './window.js'
import { DtAccessor } from './datetime.js'
import { StrAccessor } from './str.js'

let _DataFrame = null
function setDataFrame(DF) { _DataFrame = DF }

function inferDtype(values) {
    if (values.length === 0) return 'object'
    const first = values.find(v => v !== null && v !== undefined)
    if (first === undefined) return 'object'
    if (typeof first === 'number') {
        if (values.every(v => v === null || v === undefined || Number.isInteger(v))) return 'int64'
        return 'float64'
    }
    if (typeof first === 'boolean') return 'bool'
    if (typeof first === 'string') return 'object'
    return 'object'
}

function Series(data, options) {
    if (data instanceof Object && data._isPandasSeries) return data
    const opts = options || {}
    const values = Array.isArray(data) ? [...data] : []
    const name = opts.name || null
    const index = opts.index ? [...opts.index] : values.map((_, i) => i)
    const dtype = opts.dtype || inferDtype(values)

    const series = {
        _isPandasSeries: true,
        values,
        name,
        index,
        dtype,

        get shape() {
            return [values.length]
        },

        get size() {
            return values.length
        },

        head(n) {
            const count = (n === undefined) ? 5 : n
            return Series(values.slice(0, count), {name, index: index.slice(0, count), dtype})
        },

        tail(n) {
            const count = (n === undefined) ? 5 : n
            return Series(values.slice(-count), {name, index: index.slice(-count), dtype})
        },

        tolist() {
            return [...values]
        },

        toString() {
            const lines = []
            const maxIdx = index.reduce((a, b) => String(a).length > String(b).length ? a : b, '')
            const pad = String(maxIdx).length
            for (let i = 0; i < values.length; i++) {
                lines.push(String(index[i]).padStart(pad) + '    ' + String(values[i]))
            }
            if (name) lines.push(`Name: ${name}, `)
            lines.push(`dtype: ${dtype}`)
            return lines.join('\n')
        },

        // integer-location based indexing
        iloc(arg) {
            if (typeof arg === 'number') {
                const idx = arg < 0 ? values.length + arg : arg
                return values[idx]
            }
            // array of ints
            if (Array.isArray(arg)) {
                const sel = arg.map(i => i < 0 ? values.length + i : i)
                return Series(sel.map(i => values[i]), {name, index: sel.map(i => index[i]), dtype})
            }
            // slice object {start, stop, step}
            if (typeof arg === 'object' && arg !== null) {
                const {start = 0, stop = values.length, step = 1} = arg
                const s = start < 0 ? values.length + start : start
                const e = stop < 0 ? values.length + stop : stop
                const vals = []
                const idxs = []
                for (let i = s; (step > 0 ? i < e : i > e); i += step) {
                    vals.push(values[i])
                    idxs.push(index[i])
                }
                return Series(vals, {name, index: idxs, dtype})
            }
            return undefined
        },

        // label-based indexing
        loc(arg) {
            if (!Array.isArray(arg) && typeof arg !== 'object') {
                // single label
                const pos = index.indexOf(arg)
                if (pos === -1) return undefined
                return values[pos]
            }
            // array of labels
            if (Array.isArray(arg)) {
                // boolean mask
                if (arg.length === values.length && typeof arg[0] === 'boolean') {
                    const vals = []
                    const idxs = []
                    for (let i = 0; i < arg.length; i++) {
                        if (arg[i]) {
                            vals.push(values[i])
                            idxs.push(index[i])
                        }
                    }
                    return Series(vals, {name, index: idxs, dtype})
                }
                // label list
                const positions = arg.map(l => index.indexOf(l))
                return Series(positions.map(p => values[p]), {name, index: arg, dtype})
            }
            // slice {start, stop} by label (inclusive both ends)
            if (typeof arg === 'object' && arg !== null) {
                const s = arg.start !== undefined ? index.indexOf(arg.start) : 0
                const e = arg.stop !== undefined ? index.indexOf(arg.stop) : values.length - 1
                return Series(values.slice(s, e + 1), {name, index: index.slice(s, e + 1), dtype})
            }
            return undefined
        },

        // single value by integer position
        iat(i) {
            const idx = i < 0 ? values.length + i : i
            return values[idx]
        },

        // single value by label
        at(label) {
            const pos = index.indexOf(label)
            if (pos === -1) return undefined
            return values[pos]
        },

        // --- Computation & Aggregation ---
        sum() {
            let s = 0
            for (const v of values) if (v !== null && v !== undefined) s += v
            return s
        },

        mean() {
            let s = 0, c = 0
            for (const v of values) if (v !== null && v !== undefined) { s += v; c++ }
            return c > 0 ? s / c : NaN
        },

        median() {
            const nums = values.filter(v => v !== null && v !== undefined).sort((a, b) => a - b)
            if (nums.length === 0) return NaN
            const mid = Math.floor(nums.length / 2)
            if (nums.length % 2 === 0) return (nums[mid - 1] + nums[mid]) / 2
            return nums[mid]
        },

        min() {
            let m = Infinity
            for (const v of values) if (v !== null && v !== undefined && v < m) m = v
            return m === Infinity ? NaN : m
        },

        max() {
            let m = -Infinity
            for (const v of values) if (v !== null && v !== undefined && v > m) m = v
            return m === -Infinity ? NaN : m
        },

        std(ddof) {
            const d = (ddof === undefined) ? 1 : ddof
            let s = 0, s2 = 0, c = 0
            for (const v of values) {
                if (v !== null && v !== undefined) { s += v; s2 += v * v; c++ }
            }
            if (c <= d) return NaN
            const mean = s / c
            return Math.sqrt((s2 - c * mean * mean) / (c - d))
        },

        var(ddof) {
            const d = (ddof === undefined) ? 1 : ddof
            let s = 0, s2 = 0, c = 0
            for (const v of values) {
                if (v !== null && v !== undefined) { s += v; s2 += v * v; c++ }
            }
            if (c <= d) return NaN
            const mean = s / c
            return (s2 - c * mean * mean) / (c - d)
        },

        count() {
            let c = 0
            for (const v of values) if (v !== null && v !== undefined) c++
            return c
        },

        prod() {
            let p = 1
            for (const v of values) if (v !== null && v !== undefined) p *= v
            return p
        },

        quantile(q) {
            const nums = values.filter(v => v !== null && v !== undefined).sort((a, b) => a - b)
            if (nums.length === 0) return NaN
            const pos = (nums.length - 1) * q
            const lo = Math.floor(pos)
            const hi = Math.ceil(pos)
            if (lo === hi) return nums[lo]
            return nums[lo] + (nums[hi] - nums[lo]) * (pos - lo)
        },

        mode() {
            const counts = {}
            for (const v of values) {
                if (v !== null && v !== undefined) counts[v] = (counts[v] || 0) + 1
            }
            let maxCount = 0
            for (const k in counts) if (counts[k] > maxCount) maxCount = counts[k]
            const modes = []
            for (const k in counts) {
                if (counts[k] === maxCount) modes.push(Number(k))
            }
            modes.sort((a, b) => a - b)
            return Series(modes, {name, dtype})
        },

        skew() {
            let s = 0, c = 0
            for (const v of values) if (v !== null && v !== undefined) { s += v; c++ }
            if (c < 3) return NaN
            const m = s / c
            let m2 = 0, m3 = 0
            for (const v of values) {
                if (v !== null && v !== undefined) {
                    const d = v - m
                    m2 += d * d
                    m3 += d * d * d
                }
            }
            const variance = m2 / (c - 1)
            const sd = Math.sqrt(variance)
            return (c / ((c - 1) * (c - 2))) * (m3 / (sd * sd * sd))
        },

        kurt() {
            let s = 0, c = 0
            for (const v of values) if (v !== null && v !== undefined) { s += v; c++ }
            if (c < 4) return NaN
            const m = s / c
            let m2 = 0, m4 = 0
            for (const v of values) {
                if (v !== null && v !== undefined) {
                    const d = v - m
                    m2 += d * d
                    m4 += d * d * d * d
                }
            }
            const variance = m2 / (c - 1)
            const num = (c * (c + 1)) / ((c - 1) * (c - 2) * (c - 3)) * (m4 / (variance * variance))
            const correction = (3 * (c - 1) * (c - 1)) / ((c - 2) * (c - 3))
            return num - correction
        },

        abs() {
            return Series(values.map(v => v !== null && v !== undefined ? Math.abs(v) : v), {name, index: [...index], dtype})
        },

        cumsum() {
            let s = 0
            const result = values.map(v => {
                if (v !== null && v !== undefined) { s += v; return s }
                return NaN
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        cumprod() {
            let p = 1
            const result = values.map(v => {
                if (v !== null && v !== undefined) { p *= v; return p }
                return NaN
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        cummax() {
            let m = -Infinity
            const result = values.map(v => {
                if (v !== null && v !== undefined) { m = Math.max(m, v); return m }
                return NaN
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        cummin() {
            let m = Infinity
            const result = values.map(v => {
                if (v !== null && v !== undefined) { m = Math.min(m, v); return m }
                return NaN
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        diff(periods) {
            const p = (periods === undefined) ? 1 : periods
            const result = values.map((v, i) => {
                if (i < p) return NaN
                const prev = values[i - p]
                if (v === null || v === undefined || prev === null || prev === undefined) return NaN
                return v - prev
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        pctChange(periods) {
            const p = (periods === undefined) ? 1 : periods
            const result = values.map((v, i) => {
                if (i < p) return NaN
                const prev = values[i - p]
                if (v === null || v === undefined || prev === null || prev === undefined || prev === 0) return NaN
                return (v - prev) / prev
            })
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        round(decimals) {
            const d = (decimals === undefined) ? 0 : decimals
            const factor = Math.pow(10, d)
            return Series(values.map(v => {
                if (v === null || v === undefined) return v
                return Math.round(v * factor) / factor
            }), {name, index: [...index], dtype})
        },

        clip(options) {
            const {lower, upper} = options
            return Series(values.map(v => {
                if (v === null || v === undefined) return v
                let r = v
                if (lower !== undefined && r < lower) r = lower
                if (upper !== undefined && r > upper) r = upper
                return r
            }), {name, index: [...index], dtype})
        },

        unique() {
            return [...new Set(values)]
        },

        nunique() {
            return new Set(values).size
        },

        valueCounts() {
            const counts = {}
            const firstSeen = {}
            for (let i = 0; i < values.length; i++) {
                const key = String(values[i])
                counts[key] = (counts[key] || 0) + 1
                if (firstSeen[key] === undefined) firstSeen[key] = i
            }
            // sort by count desc, then by first appearance asc (matches pandas behavior)
            const entries = Object.entries(counts).sort((a, b) => {
                if (b[1] !== a[1]) return b[1] - a[1]
                return firstSeen[a[0]] - firstSeen[b[0]]
            })
            return Series(entries.map(e => e[1]), {name, index: entries.map(e => isNaN(Number(e[0])) ? e[0] : Number(e[0]))})
        },

        idxmin() {
            let minVal = Infinity, minIdx = null
            for (let i = 0; i < values.length; i++) {
                if (values[i] !== null && values[i] !== undefined && values[i] < minVal) {
                    minVal = values[i]
                    minIdx = index[i]
                }
            }
            return minIdx
        },

        idxmax() {
            let maxVal = -Infinity, maxIdx = null
            for (let i = 0; i < values.length; i++) {
                if (values[i] !== null && values[i] !== undefined && values[i] > maxVal) {
                    maxVal = values[i]
                    maxIdx = index[i]
                }
            }
            return maxIdx
        },

        // element-wise arithmetic
        add(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => v + ov[i]), {name, index: [...index]})
        },

        sub(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => v - ov[i]), {name, index: [...index]})
        },

        mul(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => v * ov[i]), {name, index: [...index]})
        },

        div(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => v / ov[i]), {name, index: [...index]})
        },

        // apply function to each element
        apply(fn) {
            return Series(values.map(fn), {name, index: [...index]})
        },

        // boolean mask filter
        filter(mask) {
            const vals = []
            const idxs = []
            for (let i = 0; i < mask.length; i++) {
                if (mask[i]) {
                    vals.push(values[i])
                    idxs.push(index[i])
                }
            }
            return Series(vals, {name, index: idxs, dtype})
        },

        // --- Reshaping ---
        sortValues(options) {
            const ascending = (!options || options.ascending === undefined) ? true : options.ascending
            const paired = values.map((v, i) => ({v, idx: index[i]}))
            paired.sort((a, b) => {
                if (a.v < b.v) return ascending ? -1 : 1
                if (a.v > b.v) return ascending ? 1 : -1
                return 0
            })
            return Series(paired.map(p => p.v), {name, index: paired.map(p => p.idx), dtype})
        },

        sortIndex(options) {
            const ascending = (!options || options.ascending === undefined) ? true : options.ascending
            const paired = index.map((idx, i) => ({idx, v: values[i]}))
            paired.sort((a, b) => {
                if (a.idx < b.idx) return ascending ? -1 : 1
                if (a.idx > b.idx) return ascending ? 1 : -1
                return 0
            })
            return Series(paired.map(p => p.v), {name, index: paired.map(p => p.idx), dtype})
        },

        drop(labels) {
            const dropSet = new Set(Array.isArray(labels) ? labels : [labels])
            const vals = []
            const idxs = []
            for (let i = 0; i < values.length; i++) {
                if (!dropSet.has(index[i])) {
                    vals.push(values[i])
                    idxs.push(index[i])
                }
            }
            return Series(vals, {name, index: idxs, dtype})
        },

        rename(newName) {
            return Series([...values], {name: newName, index: [...index], dtype})
        },

        resetIndex(options) {
            const drop = options && options.drop
            if (drop) {
                return Series([...values], {name, index: values.map((_, i) => i), dtype})
            }
            // return a DataFrame with old index as column
            return _DataFrame({index: [...index], [name || 0]: [...values]}, {columns: ['index', name || 0]})
        },

        dropna() {
            const vals = []
            const idxs = []
            for (let i = 0; i < values.length; i++) {
                if (values[i] !== null && values[i] !== undefined && !Number.isNaN(values[i])) {
                    vals.push(values[i])
                    idxs.push(index[i])
                }
            }
            return Series(vals, {name, index: idxs, dtype})
        },

        fillna(value) {
            return Series(values.map(v => (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) ? value : v), {name, index: [...index]})
        },

        concat(other) {
            const ov = other._isPandasSeries ? other : Series(other)
            return Series([...values, ...ov.values], {name, index: [...index, ...ov.index]})
        },

        // map: apply function to each element
        map(fn) {
            return Series(values.map(fn), {name, index: [...index]})
        },

        // replace values
        replace(toReplace, value) {
            if (typeof toReplace === 'object' && !Array.isArray(toReplace)) {
                // dict replacement
                return Series(values.map(v => toReplace[v] !== undefined ? toReplace[v] : v), {name, index: [...index]})
            }
            return Series(values.map(v => v === toReplace ? value : v), {name, index: [...index]})
        },

        // isin
        isin(list) {
            const set = new Set(list)
            return values.map(v => set.has(v))
        },

        // between (inclusive)
        between(left, right) {
            return values.map(v => v >= left && v <= right)
        },

        // nlargest
        nlargest(n) {
            const paired = values.map((v, i) => ({v, idx: index[i], pos: i}))
            paired.sort((a, b) => b.v - a.v || a.pos - b.pos)
            const top = paired.slice(0, n)
            return Series(top.map(p => p.v), {name, index: top.map(p => p.idx), dtype})
        },

        // nsmallest
        nsmallest(n) {
            const paired = values.map((v, i) => ({v, idx: index[i], pos: i}))
            paired.sort((a, b) => a.v - b.v || a.pos - b.pos)
            const top = paired.slice(0, n)
            return Series(top.map(p => p.v), {name, index: top.map(p => p.idx), dtype})
        },

        // rank (average method)
        rank() {
            const sorted = values.map((v, i) => ({v, i})).sort((a, b) => a.v - b.v)
            const ranks = new Array(values.length)
            let i = 0
            while (i < sorted.length) {
                let j = i
                while (j < sorted.length && sorted[j].v === sorted[i].v) j++
                const avgRank = (i + 1 + j) / 2
                for (let k = i; k < j; k++) ranks[sorted[k].i] = avgRank
                i = j
            }
            return Series(ranks, {name, index: [...index], dtype: 'float64'})
        },

        // shift
        shift(periods) {
            const p = (periods === undefined) ? 1 : periods
            const result = new Array(values.length).fill(NaN)
            for (let i = 0; i < values.length; i++) {
                const src = i - p
                if (src >= 0 && src < values.length) {
                    result[i] = values[src]
                }
            }
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        // where: keep values where cond is true, else NaN
        where(cond) {
            return Series(values.map((v, i) => cond[i] ? v : NaN), {name, index: [...index], dtype: 'float64'})
        },

        // mask: set values to NaN where cond is true
        mask(cond) {
            return Series(values.map((v, i) => cond[i] ? NaN : v), {name, index: [...index], dtype: 'float64'})
        },

        // duplicated
        duplicated(options) {
            const keep = (options && options.keep) || 'first'
            const result = new Array(values.length).fill(false)
            if (keep === 'first') {
                const seen = new Set()
                for (let i = 0; i < values.length; i++) {
                    if (seen.has(values[i])) {
                        result[i] = true
                    } else {
                        seen.add(values[i])
                    }
                }
            } else if (keep === 'last') {
                const seen = new Set()
                for (let i = values.length - 1; i >= 0; i--) {
                    if (seen.has(values[i])) {
                        result[i] = true
                    } else {
                        seen.add(values[i])
                    }
                }
            }
            return result
        },

        // drop_duplicates
        dropDuplicates(options) {
            const keep = (options && options.keep) || 'first'
            const duped = series.duplicated({keep})
            const vals = []
            const idxs = []
            for (let i = 0; i < values.length; i++) {
                if (!duped[i]) {
                    vals.push(values[i])
                    idxs.push(index[i])
                }
            }
            return Series(vals, {name, index: idxs, dtype})
        },

        // str accessor
        get str() {
            return StrAccessor(values, {name, index})
        },

        // dt accessor
        get dt() {
            return DtAccessor(values, {name, index})
        },

        explode() {
            const newVals = []
            const newIdx = []
            for (let i = 0; i < values.length; i++) {
                if (Array.isArray(values[i])) {
                    for (const v of values[i]) {
                        newVals.push(v)
                        newIdx.push(index[i])
                    }
                } else {
                    newVals.push(values[i])
                    newIdx.push(index[i])
                }
            }
            return Series(newVals, {name, index: newIdx})
        },

        // comparison helpers returning boolean arrays
        gt(other) {
            return values.map(v => v > other)
        },

        lt(other) {
            return values.map(v => v < other)
        },

        ge(other) {
            return values.map(v => v >= other)
        },

        le(other) {
            return values.map(v => v <= other)
        },

        eq(other) {
            return values.map(v => v === other)
        },

        ne(other) {
            return values.map(v => v !== other)
        },

        astype(dtype) {
            const castFns = {
                'int64': v => v === null || v === undefined ? v : Math.trunc(Number(v)),
                'float64': v => v === null || v === undefined ? v : Number(v),
                'string': v => v === null || v === undefined ? v : String(v),
                'bool': v => v === null || v === undefined ? v : Boolean(v)
            }
            const fn = castFns[dtype]
            if (fn) return Series(values.map(fn), {name, index: [...index], dtype})
            return Series([...values], {name, index: [...index], dtype})
        },

        ffill() {
            const result = [...values]
            for (let i = 1; i < result.length; i++) {
                if (result[i] === null || result[i] === undefined || (typeof result[i] === 'number' && Number.isNaN(result[i]))) {
                    result[i] = result[i - 1]
                }
            }
            return Series(result, {name, index: [...index], dtype})
        },

        bfill() {
            const result = [...values]
            for (let i = result.length - 2; i >= 0; i--) {
                if (result[i] === null || result[i] === undefined || (typeof result[i] === 'number' && Number.isNaN(result[i]))) {
                    result[i] = result[i + 1]
                }
            }
            return Series(result, {name, index: [...index], dtype})
        },

        interpolate() {
            const result = [...values]
            for (let i = 0; i < result.length; i++) {
                if (result[i] === null || result[i] === undefined || (typeof result[i] === 'number' && Number.isNaN(result[i]))) {
                    // find prev valid
                    let prevIdx = -1
                    for (let j = i - 1; j >= 0; j--) {
                        if (result[j] !== null && result[j] !== undefined && !(typeof result[j] === 'number' && Number.isNaN(result[j]))) {
                            prevIdx = j
                            break
                        }
                    }
                    // find next valid
                    let nextIdx = -1
                    for (let j = i + 1; j < result.length; j++) {
                        if (values[j] !== null && values[j] !== undefined && !(typeof values[j] === 'number' && Number.isNaN(values[j]))) {
                            nextIdx = j
                            break
                        }
                    }
                    if (prevIdx >= 0 && nextIdx >= 0) {
                        const ratio = (i - prevIdx) / (nextIdx - prevIdx)
                        result[i] = result[prevIdx] + ratio * (values[nextIdx] - result[prevIdx])
                    } else if (prevIdx >= 0) {
                        // trailing NaN: forward fill
                        result[i] = result[prevIdx]
                    }
                }
            }
            return Series(result, {name, index: [...index], dtype: 'float64'})
        },

        copy() {
            return Series([...values], {name, index: [...index], dtype})
        },

        isna() {
            return values.map(v => v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)))
        },

        notna() {
            return values.map(v => v !== null && v !== undefined && !(typeof v === 'number' && Number.isNaN(v)))
        },

        pipe(fn) {
            return fn(series)
        },

        sample(options) {
            const opts = options || {}
            const n = opts.n || (opts.frac ? Math.floor(values.length * opts.frac) : 1)
            const seed = opts.randomState
            // simple seeded random
            let rng = seed !== undefined ? (() => {
                let s = seed
                return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff }
            })() : Math.random
            const idxs = Array.from({length: values.length}, (_, i) => i)
            for (let i = idxs.length - 1; i > 0; i--) {
                const j = Math.floor(rng() * (i + 1))
                const tmp = idxs[i]; idxs[i] = idxs[j]; idxs[j] = tmp
            }
            const sel = idxs.slice(0, n)
            return Series(sel.map(i => values[i]), {name, index: sel.map(i => index[i]), dtype})
        },

        rolling(windowSize, options) {
            const opts = options || {}
            return Rolling(values, {window: windowSize, minPeriods: opts.minPeriods, name, index})
        },

        expanding(options) {
            const opts = options || {}
            return Expanding(values, {minPeriods: opts.minPeriods, name, index})
        },

        ewm(options) {
            const {span} = options
            return Ewm(values, {span, name, index})
        },

        // floor division
        floordiv(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => Math.floor(v / ov[i])), {name, index: [...index]})
        },

        // modulo
        mod(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => v % ov[i]), {name, index: [...index]})
        },

        // power
        pow(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => Math.pow(v, ov[i])), {name, index: [...index]})
        },

        // true division (alias for div)
        truediv(other) {
            return series.div(other)
        },

        // reverse arithmetic
        radd(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => ov[i] + v), {name, index: [...index]})
        },

        rsub(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => ov[i] - v), {name, index: [...index]})
        },

        rmul(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => ov[i] * v), {name, index: [...index]})
        },

        rdiv(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => ov[i] / v), {name, index: [...index]})
        },

        rfloordiv(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => Math.floor(ov[i] / v)), {name, index: [...index]})
        },

        rmod(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => ov[i] % v), {name, index: [...index]})
        },

        rpow(other) {
            const ov = other._isPandasSeries ? other.values : (Array.isArray(other) ? other : values.map(() => other))
            return Series(values.map((v, i) => Math.pow(ov[i], v)), {name, index: [...index]})
        },

        // fill nulls from other Series
        combineFirst(other) {
            const ov = other._isPandasSeries ? other : Series(other)
            const result = values.map((v, i) => {
                if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) {
                    return ov.values[i]
                }
                return v
            })
            return Series(result, {name, index: [...index]})
        },

        // show differences between two Series
        compare(other) {
            const ov = other._isPandasSeries ? other.values : other
            const selfVals = [], otherVals = [], idxs = []
            for (let i = 0; i < values.length; i++) {
                if (values[i] !== ov[i]) {
                    selfVals.push(values[i])
                    otherVals.push(ov[i])
                    idxs.push(index[i])
                }
            }
            return _DataFrame({self: selfVals, other: otherVals}, {index: idxs})
        },

        // repeat values n times
        repeat(n) {
            const result = [], newIdx = []
            for (let i = 0; i < values.length; i++) {
                for (let j = 0; j < n; j++) {
                    result.push(values[i])
                    newIdx.push(index[i])
                }
            }
            return Series(result, {name, index: newIdx, dtype})
        },

        // return indices that would sort
        argsort() {
            const paired = values.map((v, i) => ({v, i}))
            paired.sort((a, b) => a.v - b.v)
            return Series(paired.map(p => p.i), {name, index: [...index], dtype: 'int64'})
        },

        // covariance with another Series
        cov(other) {
            const ov = other._isPandasSeries ? other.values : other
            const n = values.length
            if (n < 2) return NaN
            let sx = 0, sy = 0
            for (let i = 0; i < n; i++) { sx += values[i]; sy += ov[i] }
            const mx = sx / n, my = sy / n
            let sxy = 0
            for (let i = 0; i < n; i++) sxy += (values[i] - mx) * (ov[i] - my)
            return sxy / (n - 1)
        },

        // autocorrelation at given lag
        autocorr(lag) {
            const p = (lag === undefined) ? 1 : lag
            const n = values.length - p
            if (n < 2) return NaN
            let sx = 0, sy = 0
            for (let i = 0; i < n; i++) { sx += values[i]; sy += values[i + p] }
            const mx = sx / n, my = sy / n
            let sxy = 0, sx2 = 0, sy2 = 0
            for (let i = 0; i < n; i++) {
                const dx = values[i] - mx, dy = values[i + p] - my
                sxy += dx * dy; sx2 += dx * dx; sy2 += dy * dy
            }
            return sxy / Math.sqrt(sx2 * sy2)
        },

        // standard error of the mean
        sem() {
            const c = series.count()
            if (c < 2) return NaN
            return series.std() / Math.sqrt(c)
        },

        // convert to single-column DataFrame
        toFrame(colName) {
            const n = colName || name || 0
            return _DataFrame({[n]: [...values]}, {index: [...index]})
        },

        // convert to {index: value} dict
        toDict() {
            const obj = {}
            for (let i = 0; i < values.length; i++) {
                obj[String(index[i])] = values[i]
            }
            return obj
        },

        // iterate as [index, value] pairs
        items() {
            return index.map((idx, i) => [idx, values[i]])
        },

        // return index array
        keys() {
            return [...index]
        },

        toJSON() {
            const obj = {}
            for (let i = 0; i < values.length; i++) {
                obj[String(index[i])] = values[i]
            }
            return obj
        }
    }

    // snake_case aliases for pandas compatibility
    series.sort_values = series.sortValues
    series.sort_index = series.sortIndex
    series.value_counts = series.valueCounts
    series.drop_duplicates = series.dropDuplicates
    series.reset_index = series.resetIndex
    series.pct_change = series.pctChange
    series.to_frame = series.toFrame
    series.to_dict = series.toDict
    series.to_json = series.toJSON
    series.to_list = series.tolist
    series.combine_first = series.combineFirst
    series.transform = series.apply
    series.agg = series.apply
    series.aggregate = series.apply

    // auto toString for console.log in Node.js
    series[Symbol.for('nodejs.util.inspect.custom')] = function() {
        return series.toString()
    }

    return series
}

export { Series, inferDtype, setDataFrame }
