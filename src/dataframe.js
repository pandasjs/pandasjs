// DataFrame - pandas-compatible DataFrame factory function
import { Series, inferDtype } from './series.js'
import { windowAgg, expandingAgg, aggSum, aggMean, aggMin, aggMax, aggStd, aggVar, aggCount, aggMedian } from './window.js'
import { GroupBy } from './groupby.js'

function DataFrame(data, options) {
    if (data instanceof Object && data._isPandasDataFrame) return data
    const opts = options || {}
    let columns = []
    let colData = {} // column name -> array of values
    let rowCount = 0

    // dict of arrays: {col1: [1,2], col2: [3,4]}
    if (data && !Array.isArray(data) && typeof data === 'object') {
        columns = opts.columns || Object.keys(data)
        for (const col of columns) {
            colData[col] = Array.isArray(data[col]) ? [...data[col]] : []
        }
        rowCount = columns.length > 0 ? colData[columns[0]].length : 0
    }
    // array of row objects: [{col1: 1, col2: 3}, {col1: 2, col2: 4}]
    else if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
        const allKeys = new Set()
        for (const row of data) {
            for (const k of Object.keys(row)) allKeys.add(k)
        }
        columns = opts.columns || [...allKeys]
        for (const col of columns) {
            colData[col] = data.map(row => row[col] !== undefined ? row[col] : null)
        }
        rowCount = data.length
    }
    // empty
    else if (!data || (Array.isArray(data) && data.length === 0)) {
        columns = opts.columns || []
        for (const col of columns) {
            colData[col] = []
        }
        rowCount = 0
    }

    const index = opts.index ? [...opts.index] : Array.from({length: rowCount}, (_, i) => i)

    const df = {
        _isPandasDataFrame: true,
        columns: [...columns],
        index,

        get shape() {
            return [rowCount, columns.length]
        },

        get dtypes() {
            const result = {}
            for (const col of columns) {
                result[col] = inferDtype(colData[col])
            }
            return result
        },

        get values() {
            const rows = []
            for (let i = 0; i < rowCount; i++) {
                const row = []
                for (const col of columns) {
                    row.push(colData[col][i])
                }
                rows.push(row)
            }
            return rows
        },

        col(name) {
            if (colData[name] === undefined) return null
            return Series(colData[name], {name, index: [...index]})
        },

        head(n) {
            const count = (n === undefined) ? 5 : n
            const sliced = {}
            for (const col of columns) {
                sliced[col] = colData[col].slice(0, count)
            }
            return DataFrame(sliced, {columns: [...columns], index: index.slice(0, count)})
        },

        tail(n) {
            const count = (n === undefined) ? 5 : n
            const sliced = {}
            for (const col of columns) {
                sliced[col] = colData[col].slice(-count)
            }
            return DataFrame(sliced, {columns: [...columns], index: index.slice(-count)})
        },

        toString() {
            const colWidths = {}
            for (const col of columns) {
                colWidths[col] = col.length
                for (let i = 0; i < rowCount; i++) {
                    colWidths[col] = Math.max(colWidths[col], String(colData[col][i]).length)
                }
            }
            const idxWidth = index.reduce((m, v) => Math.max(m, String(v).length), 0)
            const lines = []
            // header
            let header = ''.padStart(idxWidth) + '  '
            header += columns.map(c => c.padStart(colWidths[c])).join('  ')
            lines.push(header)
            // rows
            for (let i = 0; i < rowCount; i++) {
                let line = String(index[i]).padStart(idxWidth) + '  '
                line += columns.map(c => String(colData[c][i]).padStart(colWidths[c])).join('  ')
                lines.push(line)
            }
            return lines.join('\n')
        },

        toJSON() {
            const rows = []
            for (let i = 0; i < rowCount; i++) {
                const row = {}
                for (const col of columns) {
                    row[col] = colData[col][i]
                }
                rows.push(row)
            }
            return rows
        },

        // integer-location based indexing
        iloc(row, colArg) {
            // single row, no col
            if (colArg === undefined) {
                if (typeof row === 'number') {
                    const r = row < 0 ? rowCount + row : row
                    const obj = {}
                    for (const c of columns) obj[c] = colData[c][r]
                    return obj
                }
                // array of row ints
                if (Array.isArray(row)) {
                    const sliced = {}
                    const idxs = row.map(i => i < 0 ? rowCount + i : i)
                    for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
                    return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
                }
                // slice object {start, stop, step}
                if (typeof row === 'object' && row !== null) {
                    const {start = 0, stop = rowCount, step = 1} = row
                    const s = start < 0 ? rowCount + start : start
                    const e = stop < 0 ? rowCount + stop : stop
                    const idxs = []
                    for (let i = s; (step > 0 ? i < e : i > e); i += step) idxs.push(i)
                    const sliced = {}
                    for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
                    return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
                }
            }
            // row + col: scalar
            if (typeof row === 'number' && typeof colArg === 'number') {
                const r = row < 0 ? rowCount + row : row
                const ci = colArg < 0 ? columns.length + colArg : colArg
                return colData[columns[ci]][r]
            }
            return undefined
        },

        // label-based indexing
        loc(row, colArg) {
            if (colArg === undefined) {
                // single label
                if (!Array.isArray(row) && typeof row !== 'object') {
                    const r = index.indexOf(row)
                    if (r === -1) return undefined
                    const obj = {}
                    for (const c of columns) obj[c] = colData[c][r]
                    return obj
                }
                // boolean mask
                if (Array.isArray(row) && row.length === rowCount && typeof row[0] === 'boolean') {
                    const sliced = {}
                    const idxs = []
                    for (let i = 0; i < rowCount; i++) {
                        if (row[i]) idxs.push(i)
                    }
                    for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
                    return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
                }
                // label list
                if (Array.isArray(row)) {
                    const positions = row.map(l => index.indexOf(l))
                    const sliced = {}
                    for (const c of columns) sliced[c] = positions.map(p => colData[c][p])
                    return DataFrame(sliced, {columns: [...columns], index: row})
                }
            }
            // row label + column name: scalar
            if (typeof colArg === 'string') {
                const r = index.indexOf(row)
                if (r === -1) return undefined
                return colData[colArg][r]
            }
            // row label + array of column names
            if (Array.isArray(colArg)) {
                const r = index.indexOf(row)
                if (r === -1) return undefined
                const obj = {}
                for (const c of colArg) obj[c] = colData[c][r]
                return obj
            }
            return undefined
        },

        // single scalar by integer position
        iat(row, col) {
            const r = row < 0 ? rowCount + row : row
            const ci = col < 0 ? columns.length + col : col
            return colData[columns[ci]][r]
        },

        // single scalar by label
        at(row, col) {
            const r = index.indexOf(row)
            if (r === -1) return undefined
            return colData[col][r]
        },

        // select columns by name(s)
        select(cols) {
            if (typeof cols === 'string') {
                return Series(colData[cols], {name: cols, index: [...index]})
            }
            if (Array.isArray(cols)) {
                const sliced = {}
                for (const c of cols) sliced[c] = [...colData[c]]
                return DataFrame(sliced, {columns: [...cols], index: [...index]})
            }
            return undefined
        },

        // --- Computation & Aggregation ---
        sum() {
            const result = {}
            for (const c of columns) {
                let s = 0
                for (const v of colData[c]) if (v !== null && v !== undefined && typeof v === 'number') s += v
                result[c] = s
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        mean() {
            const result = {}
            for (const c of columns) {
                let s = 0, cnt = 0
                for (const v of colData[c]) if (v !== null && v !== undefined && typeof v === 'number') { s += v; cnt++ }
                result[c] = cnt > 0 ? s / cnt : NaN
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        median() {
            const result = {}
            for (const c of columns) {
                const nums = colData[c].filter(v => v !== null && v !== undefined && typeof v === 'number').sort((a, b) => a - b)
                if (nums.length === 0) { result[c] = NaN; continue }
                const mid = Math.floor(nums.length / 2)
                result[c] = nums.length % 2 === 0 ? (nums[mid - 1] + nums[mid]) / 2 : nums[mid]
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        min() {
            const result = {}
            for (const c of columns) {
                let m = Infinity
                for (const v of colData[c]) if (v !== null && v !== undefined && typeof v === 'number' && v < m) m = v
                result[c] = m === Infinity ? NaN : m
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        max() {
            const result = {}
            for (const c of columns) {
                let m = -Infinity
                for (const v of colData[c]) if (v !== null && v !== undefined && typeof v === 'number' && v > m) m = v
                result[c] = m === -Infinity ? NaN : m
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        std(ddof) {
            const d = (ddof === undefined) ? 1 : ddof
            const result = {}
            for (const c of columns) {
                let s = 0, s2 = 0, cnt = 0
                for (const v of colData[c]) {
                    if (v !== null && v !== undefined && typeof v === 'number') { s += v; s2 += v * v; cnt++ }
                }
                if (cnt <= d) { result[c] = NaN; continue }
                const mean = s / cnt
                result[c] = Math.sqrt((s2 - cnt * mean * mean) / (cnt - d))
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        count() {
            const result = {}
            for (const c of columns) {
                let cnt = 0
                for (const v of colData[c]) if (v !== null && v !== undefined) cnt++
                result[c] = cnt
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        prod() {
            const result = {}
            for (const c of columns) {
                let p = 1
                for (const v of colData[c]) if (v !== null && v !== undefined && typeof v === 'number') p *= v
                result[c] = p
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        quantile(q) {
            const result = {}
            for (const c of columns) {
                const nums = colData[c].filter(v => v !== null && v !== undefined && typeof v === 'number').sort((a, b) => a - b)
                if (nums.length === 0) { result[c] = NaN; continue }
                const pos = (nums.length - 1) * q
                const lo = Math.floor(pos)
                const hi = Math.ceil(pos)
                result[c] = lo === hi ? nums[lo] : nums[lo] + (nums[hi] - nums[lo]) * (pos - lo)
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        rolling(windowSize, options) {
            const opts = options || {}
            const mp = opts.minPeriods
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            function makeAgg(fn) {
                const resultData = {}
                for (const c of numCols) {
                    resultData[c] = windowAgg(colData[c], windowSize, mp, fn)
                }
                return DataFrame(resultData, {columns: numCols, index: [...index]})
            }
            return {
                sum() { return makeAgg(aggSum) },
                mean() { return makeAgg(aggMean) },
                min() { return makeAgg(aggMin) },
                max() { return makeAgg(aggMax) },
                std() { return makeAgg(aggStd) },
                median() { return makeAgg(aggMedian) }
            }
        },

        expanding(options) {
            const opts = options || {}
            const mp = opts.minPeriods
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            function makeAgg(fn) {
                const resultData = {}
                for (const c of numCols) {
                    resultData[c] = expandingAgg(colData[c], mp, fn)
                }
                return DataFrame(resultData, {columns: numCols, index: [...index]})
            }
            return {
                sum() { return makeAgg(aggSum) },
                mean() { return makeAgg(aggMean) },
                std() { return makeAgg(aggStd) }
            }
        },

        astype(dtype) {
            const castFns = {
                'int64': v => v === null || v === undefined ? v : Math.trunc(Number(v)),
                'float64': v => v === null || v === undefined ? v : Number(v),
                'string': v => v === null || v === undefined ? v : String(v),
                'bool': v => v === null || v === undefined ? v : Boolean(v)
            }
            if (typeof dtype === 'string') {
                const fn = castFns[dtype]
                const sliced = {}
                for (const c of columns) sliced[c] = colData[c].map(fn)
                return DataFrame(sliced, {columns: [...columns], index: [...index]})
            }
            // dict: {col: dtype}
            const sliced = {}
            for (const c of columns) {
                if (dtype[c]) {
                    const fn = castFns[dtype[c]]
                    sliced[c] = colData[c].map(fn)
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        ffill() {
            const sliced = {}
            for (const c of columns) {
                const arr = [...colData[c]]
                for (let i = 1; i < arr.length; i++) {
                    if (arr[i] === null || arr[i] === undefined || (typeof arr[i] === 'number' && Number.isNaN(arr[i]))) {
                        arr[i] = arr[i - 1]
                    }
                }
                sliced[c] = arr
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        bfill() {
            const sliced = {}
            for (const c of columns) {
                const arr = [...colData[c]]
                for (let i = arr.length - 2; i >= 0; i--) {
                    if (arr[i] === null || arr[i] === undefined || (typeof arr[i] === 'number' && Number.isNaN(arr[i]))) {
                        arr[i] = arr[i + 1]
                    }
                }
                sliced[c] = arr
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        copy() {
            const sliced = {}
            for (const c of columns) sliced[c] = [...colData[c]]
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        var(ddof) {
            const d = (ddof === undefined) ? 1 : ddof
            const result = {}
            for (const c of columns) {
                let s = 0, s2 = 0, cnt = 0
                for (const v of colData[c]) {
                    if (v !== null && v !== undefined && typeof v === 'number') { s += v; s2 += v * v; cnt++ }
                }
                if (cnt <= d) { result[c] = NaN; continue }
                const mean = s / cnt
                result[c] = (s2 - cnt * mean * mean) / (cnt - d)
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        isna() {
            const sliced = {}
            for (const c of columns) {
                sliced[c] = colData[c].map(v => v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)))
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        notna() {
            const sliced = {}
            for (const c of columns) {
                sliced[c] = colData[c].map(v => v !== null && v !== undefined && !(typeof v === 'number' && Number.isNaN(v)))
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        any(options) {
            const axis = (options && options.axis !== undefined) ? options.axis : 0
            if (axis === 0) {
                const result = {}
                for (const c of columns) {
                    result[c] = colData[c].some(v => Boolean(v))
                }
                return Series(columns.map(c => result[c]), {index: [...columns]})
            }
            // axis=1
            const result = []
            for (let i = 0; i < rowCount; i++) {
                result.push(columns.some(c => Boolean(colData[c][i])))
            }
            return Series(result, {index: [...index]})
        },

        all(options) {
            const axis = (options && options.axis !== undefined) ? options.axis : 0
            if (axis === 0) {
                const result = {}
                for (const c of columns) {
                    result[c] = colData[c].every(v => Boolean(v))
                }
                return Series(columns.map(c => result[c]), {index: [...columns]})
            }
            // axis=1
            const result = []
            for (let i = 0; i < rowCount; i++) {
                result.push(columns.every(c => Boolean(colData[c][i])))
            }
            return Series(result, {index: [...index]})
        },

        pipe(fn) {
            return fn(df)
        },

        applymap(fn) {
            const sliced = {}
            for (const c of columns) {
                sliced[c] = colData[c].map(fn)
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        stack() {
            const resultData = {index: [], variable: [], value: []}
            for (let i = 0; i < rowCount; i++) {
                for (const c of columns) {
                    resultData.index.push(index[i])
                    resultData.variable.push(c)
                    resultData.value.push(colData[c][i])
                }
            }
            return Series(resultData.value, {
                index: resultData.index.map((idx, i) => idx + '\x00' + resultData.variable[i]),
                name: null
            })
        },

        sample(options) {
            const opts = options || {}
            const n = opts.n || (opts.frac ? Math.floor(rowCount * opts.frac) : 1)
            const seed = opts.randomState
            let rng = seed !== undefined ? (() => {
                let s = seed
                return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff }
            })() : Math.random
            const idxs = Array.from({length: rowCount}, (_, i) => i)
            for (let i = idxs.length - 1; i > 0; i--) {
                const j = Math.floor(rng() * (i + 1))
                const tmp = idxs[i]; idxs[i] = idxs[j]; idxs[j] = tmp
            }
            const sel = idxs.slice(0, n)
            const sliced = {}
            for (const c of columns) sliced[c] = sel.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: sel.map(i => index[i])})
        },

        describe() {
            const numCols = columns.filter(c => {
                const dt = inferDtype(colData[c])
                return dt === 'int64' || dt === 'float64'
            })
            const stats = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
            const result = {}
            for (const c of numCols) {
                const nums = colData[c].filter(v => v !== null && v !== undefined).sort((a, b) => a - b)
                const n = nums.length
                const s = nums.reduce((a, b) => a + b, 0)
                const mean = s / n
                const s2 = nums.reduce((a, v) => a + v * v, 0)
                const std = n > 1 ? Math.sqrt((s2 - n * mean * mean) / (n - 1)) : NaN
                const percentile = (p) => {
                    const pos = (n - 1) * p
                    const lo = Math.floor(pos)
                    const hi = Math.ceil(pos)
                    if (lo === hi) return nums[lo]
                    return nums[lo] + (nums[hi] - nums[lo]) * (pos - lo)
                }
                result[c] = [n, mean, std, nums[0], percentile(0.25), percentile(0.5), percentile(0.75), nums[n - 1]]
            }
            const descData = {}
            for (const c of numCols) {
                descData[c] = result[c]
            }
            return DataFrame(descData, {columns: numCols, index: stats})
        },

        apply(fn, options) {
            const axis = (options && options.axis !== undefined) ? options.axis : 0
            // axis=0: apply to each column
            if (axis === 0) {
                const result = {}
                for (const c of columns) {
                    result[c] = fn(Series(colData[c], {name: c, index: [...index]}))
                }
                return Series(columns.map(c => result[c]), {index: [...columns]})
            }
            // axis=1: apply to each row
            const result = []
            for (let i = 0; i < rowCount; i++) {
                const row = {}
                for (const c of columns) row[c] = colData[c][i]
                result.push(fn(row))
            }
            return Series(result, {index: [...index]})
        },

        // filter rows by boolean mask
        filter(mask) {
            const sliced = {}
            const idxs = []
            for (let i = 0; i < rowCount; i++) {
                if (mask[i]) idxs.push(i)
            }
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        // --- Reshaping & Combining ---
        sortValues(by, options) {
            const ascending = (!options || options.ascending === undefined) ? true : options.ascending
            const order = Array.from({length: rowCount}, (_, i) => i)
            order.sort((a, b) => {
                const va = colData[by][a]
                const vb = colData[by][b]
                if (va < vb) return ascending ? -1 : 1
                if (va > vb) return ascending ? 1 : -1
                return 0
            })
            const sliced = {}
            for (const c of columns) sliced[c] = order.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: order.map(i => index[i])})
        },

        sortIndex(options) {
            const ascending = (!options || options.ascending === undefined) ? true : options.ascending
            const order = Array.from({length: rowCount}, (_, i) => i)
            order.sort((a, b) => {
                if (index[a] < index[b]) return ascending ? -1 : 1
                if (index[a] > index[b]) return ascending ? 1 : -1
                return 0
            })
            const sliced = {}
            for (const c of columns) sliced[c] = order.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: order.map(i => index[i])})
        },

        drop(labels, options) {
            const axis = (options && options.axis !== undefined) ? options.axis : 0
            const dropList = Array.isArray(labels) ? labels : [labels]
            // axis=1: drop columns
            if (axis === 1) {
                const dropSet = new Set(dropList)
                const newCols = columns.filter(c => !dropSet.has(c))
                const sliced = {}
                for (const c of newCols) sliced[c] = [...colData[c]]
                return DataFrame(sliced, {columns: newCols, index: [...index]})
            }
            // axis=0: drop rows by index label
            const dropSet = new Set(dropList)
            const idxs = []
            for (let i = 0; i < rowCount; i++) {
                if (!dropSet.has(index[i])) idxs.push(i)
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        rename(options) {
            const {cols} = options
            if (cols) {
                const newCols = columns.map(c => cols[c] !== undefined ? cols[c] : c)
                const sliced = {}
                for (let i = 0; i < columns.length; i++) {
                    sliced[newCols[i]] = [...colData[columns[i]]]
                }
                return DataFrame(sliced, {columns: newCols, index: [...index]})
            }
            return DataFrame({...colData}, {columns: [...columns], index: [...index]})
        },

        assign(newCols) {
            const sliced = {}
            for (const c of columns) sliced[c] = [...colData[c]]
            const allCols = [...columns]
            for (const [k, v] of Object.entries(newCols)) {
                if (!allCols.includes(k)) allCols.push(k)
                sliced[k] = typeof v === 'function' ? Array.from({length: rowCount}, (_, i) => {
                    const row = {}
                    for (const c of columns) row[c] = colData[c][i]
                    return v(row)
                }) : [...v]
            }
            return DataFrame(sliced, {columns: allCols, index: [...index]})
        },

        dropna() {
            const idxs = []
            for (let i = 0; i < rowCount; i++) {
                let hasNull = false
                for (const c of columns) {
                    const v = colData[c][i]
                    if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) {
                        hasNull = true
                        break
                    }
                }
                if (!hasNull) idxs.push(i)
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        fillna(value) {
            const sliced = {}
            for (const c of columns) {
                sliced[c] = colData[c].map(v => (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) ? value : v)
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        get T() {
            const newCols = [...index]
            const newIndex = [...columns]
            const sliced = {}
            for (let i = 0; i < newCols.length; i++) {
                const colName = String(newCols[i])
                sliced[colName] = columns.map(c => colData[c][i])
            }
            return DataFrame(sliced, {columns: newCols.map(String), index: newIndex})
        },

        resetIndex(options) {
            const drop = options && options.drop
            if (drop) {
                const sliced = {}
                for (const c of columns) sliced[c] = [...colData[c]]
                return DataFrame(sliced, {columns: [...columns], index: Array.from({length: rowCount}, (_, i) => i)})
            }
            const sliced = {index: [...index]}
            for (const c of columns) sliced[c] = [...colData[c]]
            return DataFrame(sliced, {columns: ['index', ...columns]})
        },

        setIndex(col) {
            const newIndex = [...colData[col]]
            const newCols = columns.filter(c => c !== col)
            const sliced = {}
            for (const c of newCols) sliced[c] = [...colData[c]]
            return DataFrame(sliced, {columns: newCols, index: newIndex})
        },

        groupby(by) {
            return GroupBy(colData, columns, index, rowCount, by, DataFrame)
        },

        merge(other, options) {
            const {on, how = 'inner', suffixes} = options
            const onArr = Array.isArray(on) ? on : [on]
            const sf = suffixes || ['_x', '_y']
            const otherCols = other.columns.filter(c => !onArr.includes(c))
            // handle column name collisions
            const leftOnly = columns.filter(c => !onArr.includes(c))
            const conflicts = new Set(leftOnly.filter(c => otherCols.includes(c)))
            const allCols = []
            const leftMap = {} // output col -> source col
            const rightMap = {} // output col -> source col
            for (const c of columns) {
                const outName = conflicts.has(c) ? c + sf[0] : c
                allCols.push(outName)
                leftMap[outName] = c
            }
            for (const c of otherCols) {
                const outName = conflicts.has(c) ? c + sf[1] : c
                allCols.push(outName)
                rightMap[outName] = c
            }

            const resultData = {}
            for (const c of allCols) resultData[c] = []

            function makeKey(row, cols) {
                return cols.map(c => row[c]).join('\x00')
            }

            // build lookup from other
            const otherMap = {}
            const otherRows = other.toJSON()
            const otherUsed = new Set()
            for (let ri = 0; ri < otherRows.length; ri++) {
                const key = makeKey(otherRows[ri], onArr)
                if (!otherMap[key]) otherMap[key] = []
                otherMap[key].push(ri)
            }

            function pushLeft(i) {
                for (const [outName, srcCol] of Object.entries(leftMap)) {
                    resultData[outName].push(colData[srcCol][i])
                }
            }
            function pushLeftNull() {
                for (const outName of Object.keys(leftMap)) {
                    resultData[outName].push(null)
                }
            }
            function pushRight(row) {
                for (const [outName, srcCol] of Object.entries(rightMap)) {
                    resultData[outName].push(row[srcCol])
                }
            }
            function pushRightNull() {
                for (const outName of Object.keys(rightMap)) {
                    resultData[outName].push(null)
                }
            }

            // inner / left
            if (how === 'inner' || how === 'left') {
                for (let i = 0; i < rowCount; i++) {
                    const key = onArr.map(c => colData[c][i]).join('\x00')
                    const matches = otherMap[key]
                    if (matches) {
                        for (const ri of matches) {
                            pushLeft(i)
                            pushRight(otherRows[ri])
                            otherUsed.add(ri)
                        }
                    } else if (how === 'left') {
                        pushLeft(i)
                        pushRightNull()
                    }
                }
            }

            // right
            if (how === 'right') {
                // build lookup from left
                const leftMap2 = {}
                for (let i = 0; i < rowCount; i++) {
                    const key = onArr.map(c => colData[c][i]).join('\x00')
                    if (!leftMap2[key]) leftMap2[key] = []
                    leftMap2[key].push(i)
                }
                for (let ri = 0; ri < otherRows.length; ri++) {
                    const key = makeKey(otherRows[ri], onArr)
                    const leftMatches = leftMap2[key]
                    if (leftMatches) {
                        for (const li of leftMatches) {
                            pushLeft(li)
                            pushRight(otherRows[ri])
                        }
                    } else {
                        // fill on keys from right
                        for (const [outName, srcCol] of Object.entries(leftMap)) {
                            if (onArr.includes(srcCol)) {
                                resultData[outName].push(otherRows[ri][srcCol])
                            } else {
                                resultData[outName].push(null)
                            }
                        }
                        pushRight(otherRows[ri])
                    }
                }
            }

            // outer
            if (how === 'outer') {
                // left + unmatched right
                for (let i = 0; i < rowCount; i++) {
                    const key = onArr.map(c => colData[c][i]).join('\x00')
                    const matches = otherMap[key]
                    if (matches) {
                        for (const ri of matches) {
                            pushLeft(i)
                            pushRight(otherRows[ri])
                            otherUsed.add(ri)
                        }
                    } else {
                        pushLeft(i)
                        pushRightNull()
                    }
                }
                // unmatched right rows
                for (let ri = 0; ri < otherRows.length; ri++) {
                    if (otherUsed.has(ri)) continue
                    for (const [outName, srcCol] of Object.entries(leftMap)) {
                        if (onArr.includes(srcCol)) {
                            resultData[outName].push(otherRows[ri][srcCol])
                        } else {
                            resultData[outName].push(null)
                        }
                    }
                    pushRight(otherRows[ri])
                }
            }

            return DataFrame(resultData, {columns: allCols})
        },

        melt(options) {
            const {idVars, valueVars} = options
            const vVars = valueVars || columns.filter(c => !idVars.includes(c))
            const resultData = {}
            const meltCols = [...idVars, 'variable', 'value']
            for (const c of meltCols) resultData[c] = []
            for (const v of vVars) {
                for (let i = 0; i < rowCount; i++) {
                    for (const id of idVars) resultData[id].push(colData[id][i])
                    resultData['variable'].push(v)
                    resultData['value'].push(colData[v][i])
                }
            }
            return DataFrame(resultData, {columns: meltCols})
        },

        pivot(options) {
            const {index: pivotIdx, cols: pivotCols, values: pivotVals} = options
            const uniqueIdx = []
            const seenIdx = new Set()
            const uniqueCols = []
            const seenCols = new Set()
            for (let i = 0; i < rowCount; i++) {
                const iv = colData[pivotIdx][i]
                const cv = colData[pivotCols][i]
                if (!seenIdx.has(iv)) { uniqueIdx.push(iv); seenIdx.add(iv) }
                if (!seenCols.has(cv)) { uniqueCols.push(cv); seenCols.add(cv) }
            }
            const resultData = {}
            for (const c of uniqueCols) resultData[String(c)] = new Array(uniqueIdx.length).fill(null)
            for (let i = 0; i < rowCount; i++) {
                const ri = uniqueIdx.indexOf(colData[pivotIdx][i])
                const ci = String(colData[pivotCols][i])
                resultData[ci][ri] = colData[pivotVals][i]
            }
            return DataFrame(resultData, {columns: uniqueCols.map(String), index: uniqueIdx})
        },

        // duplicated
        duplicated(options) {
            const subset = (options && options.subset) || columns
            const result = new Array(rowCount).fill(false)
            const seen = new Set()
            for (let i = 0; i < rowCount; i++) {
                const key = subset.map(c => colData[c][i]).join('\x00')
                if (seen.has(key)) {
                    result[i] = true
                } else {
                    seen.add(key)
                }
            }
            return result
        },

        // drop_duplicates
        dropDuplicates(options) {
            const duped = df.duplicated(options)
            const idxs = []
            for (let i = 0; i < rowCount; i++) {
                if (!duped[i]) idxs.push(i)
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        // nlargest
        nlargest(n, col) {
            const order = Array.from({length: rowCount}, (_, i) => i)
            order.sort((a, b) => colData[col][b] - colData[col][a] || a - b)
            const top = order.slice(0, n)
            const sliced = {}
            for (const c of columns) sliced[c] = top.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: top.map(i => index[i])})
        },

        // nsmallest
        nsmallest(n, col) {
            const order = Array.from({length: rowCount}, (_, i) => i)
            order.sort((a, b) => colData[col][a] - colData[col][b] || a - b)
            const top = order.slice(0, n)
            const sliced = {}
            for (const c of columns) sliced[c] = top.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: top.map(i => index[i])})
        },

        // replace
        replace(toReplace, value) {
            const sliced = {}
            // dict of col -> {old: new}
            if (typeof toReplace === 'object' && !Array.isArray(toReplace) && value === undefined) {
                for (const c of columns) sliced[c] = [...colData[c]]
                for (const [col, mapping] of Object.entries(toReplace)) {
                    if (sliced[col]) {
                        sliced[col] = sliced[col].map(v => mapping[v] !== undefined ? mapping[v] : v)
                    }
                }
            } else {
                // scalar replace
                for (const c of columns) {
                    sliced[c] = colData[c].map(v => v === toReplace ? value : v)
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        // where: keep values where mask is true, else NaN
        where(mask) {
            const sliced = {}
            for (const c of columns) {
                sliced[c] = colData[c].map((v, i) => mask[i][columns.indexOf(c)] ? v : NaN)
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        // gt: element-wise > producing 2D boolean array
        gt(other) {
            return Array.from({length: rowCount}, (_, i) =>
                columns.map(c => colData[c][i] > other)
            )
        },

        // pivotTable
        pivotTable(options) {
            const {index: pivotIdx, cols: pivotCols, values: pivotVals, aggfunc = 'sum'} = options
            const uniqueIdx = []
            const seenIdx = new Set()
            const uniqueCols = []
            const seenCols = new Set()
            for (let i = 0; i < rowCount; i++) {
                const iv = colData[pivotIdx][i]
                const cv = colData[pivotCols][i]
                if (!seenIdx.has(iv)) { uniqueIdx.push(iv); seenIdx.add(iv) }
                if (!seenCols.has(cv)) { uniqueCols.push(cv); seenCols.add(cv) }
            }
            // collect values per cell
            const cells = {}
            for (const ri of uniqueIdx) {
                cells[ri] = {}
                for (const ci of uniqueCols) cells[ri][ci] = []
            }
            for (let i = 0; i < rowCount; i++) {
                cells[colData[pivotIdx][i]][colData[pivotCols][i]].push(colData[pivotVals][i])
            }
            // aggregate
            const aggFn = {
                sum: (arr) => arr.reduce((a, b) => a + b, 0),
                mean: (arr) => arr.reduce((a, b) => a + b, 0) / arr.length,
                count: (arr) => arr.length,
                min: (arr) => Math.min(...arr),
                max: (arr) => Math.max(...arr),
            }[aggfunc]
            const resultData = {}
            for (const ci of uniqueCols) {
                resultData[String(ci)] = uniqueIdx.map(ri => cells[ri][ci].length > 0 ? aggFn(cells[ri][ci]) : null)
            }
            return DataFrame(resultData, {columns: uniqueCols.map(String), index: uniqueIdx})
        },

        // corr
        corr() {
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            const means = {}
            for (const c of numCols) {
                let s = 0
                for (const v of colData[c]) s += v
                means[c] = s / rowCount
            }
            const resultData = {}
            for (const c1 of numCols) {
                resultData[c1] = numCols.map(c2 => {
                    let sumXY = 0, sumX2 = 0, sumY2 = 0
                    for (let i = 0; i < rowCount; i++) {
                        const dx = colData[c1][i] - means[c1]
                        const dy = colData[c2][i] - means[c2]
                        sumXY += dx * dy
                        sumX2 += dx * dx
                        sumY2 += dy * dy
                    }
                    return sumXY / Math.sqrt(sumX2 * sumY2)
                })
            }
            return DataFrame(resultData, {columns: numCols, index: numCols})
        },

        // iterrows
        iterrows() {
            const rows = []
            for (let i = 0; i < rowCount; i++) {
                const vals = columns.map(c => colData[c][i])
                rows.push([index[i], vals])
            }
            return rows
        },

        // query: simple expression filter (supports "col > val", "col == val", etc)
        query(expr) {
            // parse simple expressions like "a > 3", "b == 10"
            const match = expr.match(/^\s*(\w+)\s*(>=|<=|!=|==|>|<)\s*(.+)\s*$/)
            if (!match) return df
            const [, col, op, rawVal] = match
            const val = isNaN(Number(rawVal)) ? rawVal.replace(/['"]/g, '') : Number(rawVal)
            const ops = {
                '>': (a, b) => a > b,
                '<': (a, b) => a < b,
                '>=': (a, b) => a >= b,
                '<=': (a, b) => a <= b,
                '==': (a, b) => a === b,
                '!=': (a, b) => a !== b,
            }
            const fn = ops[op]
            const idxs = []
            for (let i = 0; i < rowCount; i++) {
                if (fn(colData[col][i], val)) idxs.push(i)
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        explode(col) {
            const resultData = {}
            const newIndex = []
            for (const c of columns) resultData[c] = []
            for (let i = 0; i < rowCount; i++) {
                const val = colData[col][i]
                if (Array.isArray(val)) {
                    for (const v of val) {
                        for (const c of columns) {
                            resultData[c].push(c === col ? v : colData[c][i])
                        }
                        newIndex.push(index[i])
                    }
                } else {
                    for (const c of columns) resultData[c].push(colData[c][i])
                    newIndex.push(index[i])
                }
            }
            return DataFrame(resultData, {columns: [...columns], index: newIndex})
        },

        // join on index
        join(other, options) {
            const opts = options || {}
            const how = opts.how || 'left'
            const lsuffix = opts.lsuffix || ''
            const rsuffix = opts.rsuffix || '_r'
            const otherCols = other.columns
            const conflicts = new Set(columns.filter(c => otherCols.includes(c)))
            const allCols = []
            const leftMap = {}
            for (const c of columns) {
                const n = conflicts.has(c) ? c + lsuffix : c
                allCols.push(n); leftMap[n] = c
            }
            const rightMap = {}
            for (const c of otherCols) {
                const n = conflicts.has(c) ? c + rsuffix : c
                allCols.push(n); rightMap[n] = c
            }
            const resultData = {}
            for (const c of allCols) resultData[c] = []
            const otherIdx = {}
            for (let i = 0; i < other.index.length; i++) otherIdx[other.index[i]] = i
            if (how === 'left' || how === 'inner') {
                for (let i = 0; i < rowCount; i++) {
                    const ri = otherIdx[index[i]]
                    if (ri === undefined && how === 'inner') continue
                    for (const [n, c] of Object.entries(leftMap)) resultData[n].push(colData[c][i])
                    for (const [n, c] of Object.entries(rightMap)) resultData[n].push(ri !== undefined ? other._colData(c)[ri] : null)
                }
            }
            if (how === 'right') {
                const selfIdx = {}
                for (let i = 0; i < rowCount; i++) selfIdx[index[i]] = i
                for (let i = 0; i < other.index.length; i++) {
                    const li = selfIdx[other.index[i]]
                    for (const [n, c] of Object.entries(leftMap)) resultData[n].push(li !== undefined ? colData[c][li] : null)
                    for (const [n, c] of Object.entries(rightMap)) resultData[n].push(other._colData(c)[i])
                }
            }
            if (how === 'outer') {
                const seen = new Set()
                for (let i = 0; i < rowCount; i++) {
                    seen.add(index[i])
                    const ri = otherIdx[index[i]]
                    for (const [n, c] of Object.entries(leftMap)) resultData[n].push(colData[c][i])
                    for (const [n, c] of Object.entries(rightMap)) resultData[n].push(ri !== undefined ? other._colData(c)[ri] : null)
                }
                for (let i = 0; i < other.index.length; i++) {
                    if (seen.has(other.index[i])) continue
                    for (const [n] of Object.entries(leftMap)) resultData[n].push(null)
                    for (const [n, c] of Object.entries(rightMap)) resultData[n].push(other._colData(c)[i])
                }
            }
            return DataFrame(resultData, {columns: allCols})
        },

        // insert column at position
        insert(loc, column, value) {
            const newCols = [...columns]
            newCols.splice(loc, 0, column)
            const sliced = {}
            for (const c of columns) sliced[c] = [...colData[c]]
            sliced[column] = Array.isArray(value) ? [...value] : new Array(rowCount).fill(value)
            return DataFrame(sliced, {columns: newCols, index: [...index]})
        },

        // remove and return column as Series
        pop(column) {
            return Series(colData[column], {name: column, index: [...index]})
        },

        // conform to new index, filling gaps with NaN
        reindex(options) {
            const newIdx = options.index || index
            const newCols = options.columns || columns
            const oldIdxMap = {}
            for (let i = 0; i < index.length; i++) oldIdxMap[index[i]] = i
            const sliced = {}
            for (const c of newCols) {
                sliced[c] = newIdx.map(idx => {
                    const ri = oldIdxMap[idx]
                    if (ri === undefined) return NaN
                    if (colData[c] === undefined) return NaN
                    return colData[c][ri]
                })
            }
            return DataFrame(sliced, {columns: newCols, index: newIdx})
        },

        // fill nulls from other DataFrame
        combineFirst(other) {
            const allCols = [...new Set([...columns, ...other.columns])]
            const allIdx = [...new Set([...index, ...other.index])]
            const selfIdxMap = {}
            for (let i = 0; i < index.length; i++) selfIdxMap[index[i]] = i
            const otherIdxMap = {}
            for (let i = 0; i < other.index.length; i++) otherIdxMap[other.index[i]] = i
            const sliced = {}
            for (const c of allCols) {
                sliced[c] = allIdx.map(idx => {
                    const si = selfIdxMap[idx]
                    const oi = otherIdxMap[idx]
                    const sv = (si !== undefined && colData[c]) ? colData[c][si] : null
                    if (sv !== null && sv !== undefined && !(typeof sv === 'number' && Number.isNaN(sv))) return sv
                    if (oi !== undefined && other._colData(c)) return other._colData(c)[oi]
                    return null
                })
            }
            return DataFrame(sliced, {columns: allCols, index: allIdx})
        },

        // update values from other DataFrame
        update(other) {
            const sliced = {}
            for (const c of columns) sliced[c] = [...colData[c]]
            const otherIdxMap = {}
            for (let i = 0; i < other.index.length; i++) otherIdxMap[other.index[i]] = i
            for (const c of columns) {
                if (!other._colData(c)) continue
                for (let i = 0; i < rowCount; i++) {
                    const oi = otherIdxMap[index[i]]
                    if (oi !== undefined) {
                        const ov = other._colData(c)[oi]
                        if (ov !== null && ov !== undefined && !(typeof ov === 'number' && Number.isNaN(ov))) {
                            sliced[c][i] = ov
                        }
                    }
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        // summary info string
        info() {
            const lines = []
            lines.push(`<class 'DataFrame'>`)
            lines.push(`Index: ${rowCount} entries`)
            lines.push(`Data columns (total ${columns.length} columns):`)
            for (const c of columns) {
                let nonNull = 0
                for (const v of colData[c]) {
                    if (v !== null && v !== undefined && !(typeof v === 'number' && Number.isNaN(v))) nonNull++
                }
                lines.push(` ${c}    ${nonNull} non-null    ${inferDtype(colData[c])}`)
            }
            return lines.join('\n')
        },

        // --- Cumulative operations (per column) ---
        cumsum() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let s = 0
                    sliced[c] = colData[c].map(v => { if (v !== null && v !== undefined) { s += v; return s } return NaN })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        cumprod() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let p = 1
                    sliced[c] = colData[c].map(v => { if (v !== null && v !== undefined) { p *= v; return p } return NaN })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        cummin() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let m = Infinity
                    sliced[c] = colData[c].map(v => { if (v !== null && v !== undefined) { m = Math.min(m, v); return m } return NaN })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        cummax() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let m = -Infinity
                    sliced[c] = colData[c].map(v => { if (v !== null && v !== undefined) { m = Math.max(m, v); return m } return NaN })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        diff(periods) {
            const p = (periods === undefined) ? 1 : periods
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    sliced[c] = colData[c].map((v, i) => {
                        if (i < p) return NaN
                        const prev = colData[c][i - p]
                        if (v === null || v === undefined || prev === null || prev === undefined) return NaN
                        return v - prev
                    })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        pctChange(periods) {
            const p = (periods === undefined) ? 1 : periods
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    sliced[c] = colData[c].map((v, i) => {
                        if (i < p) return NaN
                        const prev = colData[c][i - p]
                        if (v === null || v === undefined || prev === null || prev === undefined || prev === 0) return NaN
                        return (v - prev) / prev
                    })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        clip(options) {
            const {lower, upper} = options
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    sliced[c] = colData[c].map(v => {
                        if (v === null || v === undefined) return v
                        let r = v
                        if (lower !== undefined && r < lower) r = lower
                        if (upper !== undefined && r > upper) r = upper
                        return r
                    })
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        round(decimals) {
            const d = (decimals === undefined) ? 0 : decimals
            const factor = Math.pow(10, d)
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    sliced[c] = colData[c].map(v => v === null || v === undefined ? v : Math.round(v * factor) / factor)
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        rank() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const sorted = colData[c].map((v, i) => ({v, i})).sort((a, b) => a.v - b.v)
                    const ranks = new Array(rowCount)
                    let i = 0
                    while (i < sorted.length) {
                        let j = i
                        while (j < sorted.length && sorted[j].v === sorted[i].v) j++
                        const avgRank = (i + 1 + j) / 2
                        for (let k = i; k < j; k++) ranks[sorted[k].i] = avgRank
                        i = j
                    }
                    sliced[c] = ranks
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        idxmin() {
            const result = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let minVal = Infinity, minIdx = null
                    for (let i = 0; i < rowCount; i++) {
                        if (colData[c][i] !== null && colData[c][i] !== undefined && colData[c][i] < minVal) {
                            minVal = colData[c][i]
                            minIdx = index[i]
                        }
                    }
                    result[c] = minIdx
                }
            }
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            return Series(numCols.map(c => result[c]), {index: numCols})
        },

        idxmax() {
            const result = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    let maxVal = -Infinity, maxIdx = null
                    for (let i = 0; i < rowCount; i++) {
                        if (colData[c][i] !== null && colData[c][i] !== undefined && colData[c][i] > maxVal) {
                            maxVal = colData[c][i]
                            maxIdx = index[i]
                        }
                    }
                    result[c] = maxIdx
                }
            }
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            return Series(numCols.map(c => result[c]), {index: numCols})
        },

        skew() {
            const result = {}
            for (const c of columns) {
                const vals = colData[c].filter(v => v !== null && v !== undefined && typeof v === 'number')
                const n = vals.length
                if (n < 3) { result[c] = NaN; continue }
                const m = vals.reduce((a, b) => a + b, 0) / n
                let m2 = 0, m3 = 0
                for (const v of vals) { const d = v - m; m2 += d * d; m3 += d * d * d }
                const variance = m2 / (n - 1)
                const sd = Math.sqrt(variance)
                result[c] = (n / ((n - 1) * (n - 2))) * (m3 / (sd * sd * sd))
            }
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            return Series(numCols.map(c => result[c]), {index: numCols})
        },

        kurt() {
            const result = {}
            for (const c of columns) {
                const vals = colData[c].filter(v => v !== null && v !== undefined && typeof v === 'number')
                const n = vals.length
                if (n < 4) { result[c] = NaN; continue }
                const m = vals.reduce((a, b) => a + b, 0) / n
                let m2 = 0, m4 = 0
                for (const v of vals) { const d = v - m; m2 += d * d; m4 += d * d * d * d }
                const variance = m2 / (n - 1)
                const num = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3)) * (m4 / (variance * variance))
                const correction = (3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3))
                result[c] = num - correction
            }
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            return Series(numCols.map(c => result[c]), {index: numCols})
        },

        sem(ddof) {
            const d = (ddof === undefined) ? 1 : ddof
            const result = {}
            for (const c of columns) {
                let s = 0, s2 = 0, cnt = 0
                for (const v of colData[c]) {
                    if (v !== null && v !== undefined && typeof v === 'number') { s += v; s2 += v * v; cnt++ }
                }
                if (cnt <= d) { result[c] = NaN; continue }
                const mean = s / cnt
                const std = Math.sqrt((s2 - cnt * mean * mean) / (cnt - d))
                result[c] = std / Math.sqrt(cnt)
            }
            return Series(columns.map(c => result[c]), {index: [...columns]})
        },

        cov() {
            const numCols = columns.filter(c => inferDtype(colData[c]) === 'int64' || inferDtype(colData[c]) === 'float64')
            const means = {}
            for (const c of numCols) {
                let s = 0
                for (const v of colData[c]) s += v
                means[c] = s / rowCount
            }
            const resultData = {}
            for (const c1 of numCols) {
                resultData[c1] = numCols.map(c2 => {
                    let sumXY = 0
                    for (let i = 0; i < rowCount; i++) {
                        sumXY += (colData[c1][i] - means[c1]) * (colData[c2][i] - means[c2])
                    }
                    return sumXY / (rowCount - 1)
                })
            }
            return DataFrame(resultData, {columns: numCols, index: numCols})
        },

        map(fn) {
            return df.applymap(fn)
        },

        selectDtypes(options) {
            const {include, exclude} = options
            const selected = columns.filter(c => {
                const dt = inferDtype(colData[c])
                if (include) {
                    const incl = Array.isArray(include) ? include : [include]
                    if (!incl.some(t => dt.includes(t) || (t === 'number' && (dt === 'int64' || dt === 'float64')))) return false
                }
                if (exclude) {
                    const excl = Array.isArray(exclude) ? exclude : [exclude]
                    if (excl.some(t => dt.includes(t) || (t === 'number' && (dt === 'int64' || dt === 'float64')))) return false
                }
                return true
            })
            const sliced = {}
            for (const c of selected) sliced[c] = [...colData[c]]
            return DataFrame(sliced, {columns: selected, index: [...index]})
        },

        // element-wise arithmetic on numeric columns
        add(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => v + (scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        sub(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => v - (scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        mul(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => v * (scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        div(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => v / (scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        floordiv(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => Math.floor(v / (scalar ? other : ov[i])))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        mod(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => v % (scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        pow(other) {
            const scalar = typeof other === 'number'
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    const ov = scalar ? other : (other._isPandasDataFrame ? other._colData(c) : colData[c])
                    sliced[c] = colData[c].map((v, i) => Math.pow(v, scalar ? other : ov[i]))
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        abs() {
            const sliced = {}
            for (const c of columns) {
                const dt = inferDtype(colData[c])
                if (dt === 'int64' || dt === 'float64') {
                    sliced[c] = colData[c].map(v => v !== null && v !== undefined ? Math.abs(v) : v)
                } else {
                    sliced[c] = [...colData[c]]
                }
            }
            return DataFrame(sliced, {columns: [...columns], index: [...index]})
        },

        // iterate [columnName, Series] pairs
        items() {
            return columns.map(c => [c, Series(colData[c], {name: c, index: [...index]})])
        },

        // array of row objects with Index
        itertuples() {
            const rows = []
            for (let i = 0; i < rowCount; i++) {
                const row = {Index: index[i]}
                for (const c of columns) row[c] = colData[c][i]
                rows.push(row)
            }
            return rows
        },

        // convert to dict with orient parameter
        toDict(orient) {
            const o = orient || 'dict'
            if (o === 'list') {
                const result = {}
                for (const c of columns) result[c] = [...colData[c]]
                return result
            }
            if (o === 'records') {
                return df.toJSON()
            }
            if (o === 'index') {
                const result = {}
                for (let i = 0; i < rowCount; i++) {
                    const row = {}
                    for (const c of columns) row[c] = colData[c][i]
                    result[String(index[i])] = row
                }
                return result
            }
            // 'dict' (default): {col: {idx: val}}
            const result = {}
            for (const c of columns) {
                result[c] = {}
                for (let i = 0; i < rowCount; i++) {
                    result[c][String(index[i])] = colData[c][i]
                }
            }
            return result
        },

        // array of {index, ...row}
        toRecords() {
            const rows = []
            for (let i = 0; i < rowCount; i++) {
                const row = {index: index[i]}
                for (const c of columns) row[c] = colData[c][i]
                rows.push(row)
            }
            return rows
        },

        // internal access for column data
        _colData(name) {
            return colData[name]
        }
    }

    // snake_case aliases for pandas compatibility
    df.sort_values = df.sortValues
    df.sort_index = df.sortIndex
    df.drop_duplicates = df.dropDuplicates
    df.reset_index = df.resetIndex
    df.set_index = df.setIndex
    df.pivot_table = df.pivotTable
    df.to_json = df.toJSON
    df.to_dict = df.toDict
    df.to_records = df.toRecords
    df.select_dtypes = df.selectDtypes
    df.pct_change = df.pctChange
    df.combine_first = df.combineFirst

    // auto toString for console.log in Node.js
    df[Symbol.for('nodejs.util.inspect.custom')] = function() {
        return df.toString()
    }

    return df
}

export { DataFrame }
