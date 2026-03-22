// GroupBy - extracted from dataframe.js with multi-column support
import { Series } from './series.js'

function GroupBy(colData, columns, index, rowCount, by, DataFrame) {
    const byArr = Array.isArray(by) ? by : [by]
    const groups = {}
    const groupOrder = []
    for (let i = 0; i < rowCount; i++) {
        const key = byArr.map(b => colData[b][i]).join('\x00')
        if (groups[key] === undefined) {
            groups[key] = []
            groupOrder.push(key)
        }
        groups[key].push(i)
    }
    const aggCols = columns.filter(c => !byArr.includes(c))

    // build index from group keys
    function makeIndex() {
        if (byArr.length === 1) {
            return groupOrder.map(k => colData[byArr[0]][groups[k][0]])
        }
        return groupOrder.map(k => {
            return byArr.map(b => colData[b][groups[k][0]]).join('\x00')
        })
    }

    function aggWith(fn) {
        const numAggCols = aggCols.filter(c => {
            const first = colData[c].find(v => v !== null && v !== undefined)
            return typeof first === 'number'
        })
        const result = {}
        for (const c of numAggCols) {
            result[c] = groupOrder.map(k => fn(groups[k].map(i => colData[c][i])))
        }
        return DataFrame(result, {columns: numAggCols, index: makeIndex()})
    }

    function aggFnByName(name) {
        const fns = {
            sum: arr => arr.filter(v => v !== null && v !== undefined).reduce((a, b) => a + b, 0),
            mean: arr => {
                const valid = arr.filter(v => v !== null && v !== undefined)
                return valid.length > 0 ? valid.reduce((a, b) => a + b, 0) / valid.length : NaN
            },
            count: arr => arr.filter(v => v !== null && v !== undefined).length,
            min: arr => {
                const valid = arr.filter(v => v !== null && v !== undefined)
                return valid.length > 0 ? Math.min(...valid) : NaN
            },
            max: arr => {
                const valid = arr.filter(v => v !== null && v !== undefined)
                return valid.length > 0 ? Math.max(...valid) : NaN
            },
            std: arr => {
                const valid = arr.filter(v => v !== null && v !== undefined)
                if (valid.length < 2) return NaN
                const m = valid.reduce((a, b) => a + b, 0) / valid.length
                const s2 = valid.reduce((a, v) => a + (v - m) * (v - m), 0)
                return Math.sqrt(s2 / (valid.length - 1))
            },
            median: arr => {
                const sorted = arr.filter(v => v !== null && v !== undefined).sort((a, b) => a - b)
                if (sorted.length === 0) return NaN
                const mid = Math.floor(sorted.length / 2)
                if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2
                return sorted[mid]
            },
            first: arr => arr.length > 0 ? arr[0] : null,
            last: arr => arr.length > 0 ? arr[arr.length - 1] : null
        }
        return fns[name]
    }

    const result = {
        sum() { return aggWith(aggFnByName('sum')) },
        mean() { return aggWith(aggFnByName('mean')) },
        count() { return aggWith(aggFnByName('count')) },
        min() { return aggWith(aggFnByName('min')) },
        max() { return aggWith(aggFnByName('max')) },
        std() { return aggWith(aggFnByName('std')) },
        median() { return aggWith(aggFnByName('median')) },
        first() { return aggWith(aggFnByName('first')) },
        last() { return aggWith(aggFnByName('last')) },

        size() {
            const sizes = groupOrder.map(k => groups[k].length)
            return Series(sizes, {index: makeIndex()})
        },

        agg(spec) {
            // string: single agg for all columns
            if (typeof spec === 'string') {
                return aggWith(aggFnByName(spec))
            }
            // dict: {col: 'sum'} or {col: ['sum', 'mean']}
            const resultData = {}
            const resultCols = []
            for (const [col, fns] of Object.entries(spec)) {
                if (typeof fns === 'string') {
                    const fn = aggFnByName(fns)
                    resultData[col] = groupOrder.map(k => fn(groups[k].map(i => colData[col][i])))
                    if (!resultCols.includes(col)) resultCols.push(col)
                } else if (Array.isArray(fns)) {
                    for (const fnName of fns) {
                        const fn = aggFnByName(fnName)
                        const key = col + '_' + fnName
                        resultData[key] = groupOrder.map(k => fn(groups[k].map(i => colData[col][i])))
                        resultCols.push(key)
                    }
                }
            }
            return DataFrame(resultData, {columns: resultCols, index: makeIndex()})
        },

        transform(fn) {
            // apply fn to each group, broadcast result back to original shape
            const resultData = {}
            const numAggCols = aggCols.filter(c => {
                const first = colData[c].find(v => v !== null && v !== undefined)
                return typeof first === 'number'
            })
            for (const c of numAggCols) {
                resultData[c] = new Array(rowCount)
            }
            for (const k of groupOrder) {
                const idxs = groups[k]
                for (const c of numAggCols) {
                    const groupVals = idxs.map(i => colData[c][i])
                    const transformed = fn(groupVals)
                    // if fn returns a scalar, broadcast
                    if (typeof transformed === 'number') {
                        for (const i of idxs) resultData[c][i] = transformed
                    } else {
                        for (let j = 0; j < idxs.length; j++) resultData[c][idxs[j]] = transformed[j]
                    }
                }
            }
            return DataFrame(resultData, {columns: numAggCols, index: [...index]})
        },

        filter(fn) {
            // keep groups where fn(groupDf) is true
            const keepIdxs = []
            for (const k of groupOrder) {
                const idxs = groups[k]
                const groupData = {}
                for (const c of columns) {
                    groupData[c] = idxs.map(i => colData[c][i])
                }
                const groupDf = DataFrame(groupData, {columns: [...columns], index: idxs.map(i => index[i])})
                if (fn(groupDf)) {
                    for (const i of idxs) keepIdxs.push(i)
                }
            }
            const sliced = {}
            for (const c of columns) sliced[c] = keepIdxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: keepIdxs.map(i => index[i])})
        },

        // cumulative operations within groups
        cumsum() {
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                for (const c of aggCols) {
                    let s = 0
                    for (const i of groups[k]) {
                        s += colData[c][i]; resultData[c][i] = s
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        cumprod() {
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                for (const c of aggCols) {
                    let p = 1
                    for (const i of groups[k]) {
                        p *= colData[c][i]; resultData[c][i] = p
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        cummin() {
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                for (const c of aggCols) {
                    let m = Infinity
                    for (const i of groups[k]) {
                        m = Math.min(m, colData[c][i]); resultData[c][i] = m
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        cummax() {
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                for (const c of aggCols) {
                    let m = -Infinity
                    for (const i of groups[k]) {
                        m = Math.max(m, colData[c][i]); resultData[c][i] = m
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        cumcount() {
            const result = new Array(rowCount)
            for (const k of groupOrder) {
                let c = 0
                for (const i of groups[k]) { result[i] = c; c++ }
            }
            return Series(result, {index: [...index]})
        },

        nth(n) {
            const idxs = []
            for (const k of groupOrder) {
                const g = groups[k]
                const pos = n < 0 ? g.length + n : n
                if (pos >= 0 && pos < g.length) idxs.push(g[pos])
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        head(n) {
            const count = (n === undefined) ? 5 : n
            const idxs = []
            for (const k of groupOrder) {
                const g = groups[k]
                for (let i = 0; i < Math.min(count, g.length); i++) idxs.push(g[i])
            }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        tail(n) {
            const count = (n === undefined) ? 5 : n
            const idxSet = new Set()
            for (const k of groupOrder) {
                const g = groups[k]
                const start = Math.max(0, g.length - count)
                for (let i = start; i < g.length; i++) idxSet.add(g[i])
            }
            // maintain original order
            const idxs = []
            for (let i = 0; i < rowCount; i++) { if (idxSet.has(i)) idxs.push(i) }
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        ngroup() {
            const result = new Array(rowCount)
            for (let g = 0; g < groupOrder.length; g++) {
                for (const i of groups[groupOrder[g]]) result[i] = g
            }
            return Series(result, {index: [...index]})
        },

        rank() {
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                const idxs = groups[k]
                const sorted = idxs.map(i => ({v: colData[aggCols[0]][i], i}))
                for (const c of aggCols) {
                    const grpSorted = idxs.map(i => ({v: colData[c][i], i})).sort((a, b) => a.v - b.v)
                    let j = 0
                    while (j < grpSorted.length) {
                        let end = j
                        while (end < grpSorted.length && grpSorted[end].v === grpSorted[j].v) end++
                        const avgRank = (j + 1 + end) / 2
                        for (let t = j; t < end; t++) resultData[c][grpSorted[t].i] = avgRank
                        j = end
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        getGroup(key) {
            const keyStr = Array.isArray(key) ? key.join('\x00') : String(key)
            const idxs = groups[keyStr]
            if (!idxs) return null
            const sliced = {}
            for (const c of columns) sliced[c] = idxs.map(i => colData[c][i])
            return DataFrame(sliced, {columns: [...columns], index: idxs.map(i => index[i])})
        },

        describe() {
            const frames = []
            for (const k of groupOrder) {
                const idxs = groups[k]
                const groupData = {}
                for (const c of columns) groupData[c] = idxs.map(i => colData[c][i])
                const groupDf = DataFrame(groupData, {columns: [...columns], index: idxs.map(i => index[i])})
                frames.push(groupDf.describe())
            }
            return frames
        },

        shift(periods) {
            const p = (periods === undefined) ? 1 : periods
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                const idxs = groups[k]
                for (let j = 0; j < idxs.length; j++) {
                    const srcIdx = j - p
                    resultData[aggCols[0]] // ensure init
                    for (const c of aggCols) {
                        resultData[c][idxs[j]] = (srcIdx >= 0 && srcIdx < idxs.length) ? colData[c][idxs[srcIdx]] : NaN
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        diff(periods) {
            const p = (periods === undefined) ? 1 : periods
            const resultData = {}
            for (const c of aggCols) resultData[c] = new Array(rowCount)
            for (const k of groupOrder) {
                const idxs = groups[k]
                for (let j = 0; j < idxs.length; j++) {
                    for (const c of aggCols) {
                        if (j < p) { resultData[c][idxs[j]] = NaN; continue }
                        resultData[c][idxs[j]] = colData[c][idxs[j]] - colData[c][idxs[j - p]]
                    }
                }
            }
            return DataFrame(resultData, {columns: aggCols, index: [...index]})
        },

        fillna(value) {
            const resultData = {}
            for (const c of columns) resultData[c] = [...colData[c]]
            for (const k of groupOrder) {
                for (const i of groups[k]) {
                    for (const c of aggCols) {
                        const v = resultData[c][i]
                        if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) {
                            resultData[c][i] = value
                        }
                    }
                }
            }
            return DataFrame(resultData, {columns: [...columns], index: [...index]})
        },

        ffill() {
            const resultData = {}
            for (const c of columns) resultData[c] = [...colData[c]]
            for (const k of groupOrder) {
                const idxs = groups[k]
                for (const c of aggCols) {
                    for (let j = 1; j < idxs.length; j++) {
                        const v = resultData[c][idxs[j]]
                        if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) {
                            resultData[c][idxs[j]] = resultData[c][idxs[j - 1]]
                        }
                    }
                }
            }
            return DataFrame(resultData, {columns: [...columns], index: [...index]})
        },

        bfill() {
            const resultData = {}
            for (const c of columns) resultData[c] = [...colData[c]]
            for (const k of groupOrder) {
                const idxs = groups[k]
                for (const c of aggCols) {
                    for (let j = idxs.length - 2; j >= 0; j--) {
                        const v = resultData[c][idxs[j]]
                        if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) {
                            resultData[c][idxs[j]] = resultData[c][idxs[j + 1]]
                        }
                    }
                }
            }
            return DataFrame(resultData, {columns: [...columns], index: [...index]})
        },

        pipe(fn) {
            return fn(result)
        },

        apply(fn) {
            const frames = []
            for (const k of groupOrder) {
                const idxs = groups[k]
                const groupData = {}
                for (const c of columns) groupData[c] = idxs.map(i => colData[c][i])
                const groupDf = DataFrame(groupData, {columns: [...columns], index: idxs.map(i => index[i])})
                frames.push(fn(groupDf))
            }
            // concat results
            if (frames.length === 0) return DataFrame({}, {columns})
            if (frames[0]._isPandasDataFrame) {
                const allCols = frames[0].columns
                const resultData = {}
                for (const c of allCols) resultData[c] = []
                let allIndex = []
                for (const f of frames) {
                    allIndex = allIndex.concat(f.index)
                    for (const c of allCols) {
                        const cd = f._colData(c)
                        for (const v of cd) resultData[c].push(v)
                    }
                }
                return DataFrame(resultData, {columns: allCols, index: allIndex})
            }
            // Series results
            if (frames[0]._isPandasSeries) {
                let vals = [], idxs = []
                for (const f of frames) {
                    vals = vals.concat(f.values)
                    idxs = idxs.concat(f.index)
                }
                return Series(vals, {index: idxs})
            }
            // scalar results
            return Series(frames, {index: makeIndex()})
        }
    }

    result.get_group = result.getGroup
    return result
}

export { GroupBy }
